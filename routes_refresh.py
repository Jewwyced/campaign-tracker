"""
routes_refresh.py — the single endpoint the hourly cron hits.

This is intentionally cross-cutting: it re-ingests fan accounts, campaign
posts, roster accounts, legacy campaign-attached sounds, and every tracked
Song's sounds, in one pass. Kept separate from the other route files since
it genuinely spans every feature rather than belonging to one of them.

Known limitations (acceptable for now):
  - The lock is held for the entire refresh duration. If ingestion grows to
    20-30+ minutes, a proper job queue should replace this global lock.
  - locked_by stores 'refresh' rather than hostname:pid — good enough for
    a single-worker deployment, worth improving if multiple workers are added.
  - If the ingestion_lock row is missing (e.g. fresh DB + failed migration),
    rowcount=0 will be misreported as "already running" rather than "row missing."

Column naming note:
  sounds.sound_id = the TikTok sound ID (string like "7652515059767282462")
  sounds.id       = the database primary key (integer)
  The tuple (sound_db_id, song_id, tiktok_sound_id) maps to (id, song_id, sound_id)
  from the sounds table. This is verified correct against ingestion.ingest_sound().
"""

import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)

LOCK_TIMEOUT_MINUTES = 30


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    # Atomic lock acquisition — single UPDATE prevents race conditions.
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE ingestion_lock
                SET locked = TRUE,
                    locked_at = NOW(),
                    locked_by = 'refresh'
                WHERE id = 1
                  AND (
                      locked = FALSE
                      OR locked_at < NOW() - (%s * INTERVAL '1 minute')
                  )
            """, (LOCK_TIMEOUT_MINUTES,))
            acquired = c.rowcount == 1
        conn.commit()

    if not acquired:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT locked_at FROM ingestion_lock WHERE id=1")
                row = c.fetchone()
        return jsonify({
            "ok": False,
            "reason": "ingestion already running",
            "locked_since": str(row["locked_at"]) if row else None,
        }), 429

    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT username FROM artists")
                artists = [r["username"] for r in c.fetchall()]

                c.execute("SELECT DISTINCT post_id, username FROM campaign_links")
                campaign_posts = [(r["post_id"], r["username"]) for r in c.fetchall()]

                c.execute("SELECT username FROM roster_accounts")
                roster_usernames = [r["username"] for r in c.fetchall()]

                # LEFT JOIN to determine freshness in SQL.
                # COALESCE handles NULL last_ingested_at (never ingested) as not fresh.
                # sound_id in the sounds table is the TikTok sound ID (string).
                c.execute("""
                    SELECT
                        c.id AS campaign_id,
                        c.attached_sound_id,
                        s.id AS sound_db_id,
                        COALESCE(
                            s.last_ingested_at > NOW() - INTERVAL '6 hours',
                            FALSE
                        ) AS sound_is_fresh
                    FROM campaigns c
                    LEFT JOIN sounds s ON s.sound_id = c.attached_sound_id
                    WHERE c.attached_sound_id IS NOT NULL
                """)
                attached_sounds = [dict(r) for r in c.fetchall()]

                # sound_id here is the TikTok sound ID — confirmed correct for
                # ingestion.ingest_sound() which expects tiktok_sound_id.
                c.execute("""
                    SELECT id AS sound_db_id, song_id, sound_id AS tiktok_sound_id
                    FROM sounds
                    WHERE status = 'approved'
                """)
                song_sounds = [dict(r) for r in c.fetchall()]

        for username in artists:
            ingestion.ingest_fan_account(db, username)

        for post_id, username in campaign_posts:
            ingestion.ingest_single_post(db, post_id, username)

        for username in roster_usernames:
            ingestion.ingest_roster_account(db, username)

        for s in attached_sounds:
            # Skip if sound is already tracked and fresh — Songs pipeline owns it.
            # sound_db_id is None when no matching sounds row exists (e.g. Back Home).
            if s["sound_db_id"] is not None and s["sound_is_fresh"]:
                continue
            ingestion.ingest_campaign_attached_sound(
                db, s["campaign_id"], s["attached_sound_id"], max_results=30
            )

        skipped = 0
        ingested = 0
        for s in song_sounds:
            result = ingestion.ingest_sound(
                db, s["song_id"], s["sound_db_id"], s["tiktok_sound_id"], max_results=30
            )
            if result.get("source") == "cache":
                skipped += 1
            else:
                ingested += 1

        logging.info(
            f"[refresh] sounds: {len(song_sounds)} checked, "
            f"{skipped} skipped (fresh), {ingested} processed"
        )

        return jsonify({"ok": True})

    except Exception as e:
        logging.exception("Refresh failed — full traceback:")
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        # Release only if we still own the lock — avoids accidental unlock
        # if a future multi-worker setup introduces concurrent refresh attempts.
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE ingestion_lock
                    SET locked = FALSE, locked_at = NULL, locked_by = NULL
                    WHERE id = 1 AND locked_by = 'refresh'
                """)
            conn.commit()