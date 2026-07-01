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
"""

import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)

LOCK_TIMEOUT_MINUTES = 30


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
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

                c.execute("""
                    SELECT
                        c.id AS campaign_id,
                        c.attached_sound_id,
                        s.id AS sound_db_id,
                        (s.last_ingested_at > NOW() - INTERVAL '6 hours') AS sound_is_fresh
                    FROM campaigns c
                    LEFT JOIN sounds s ON s.sound_id = c.attached_sound_id
                    WHERE c.attached_sound_id IS NOT NULL
                """)
                attached_sounds = [dict(r) for r in c.fetchall()]

                c.execute("SELECT id, song_id, sound_id FROM sounds WHERE status='approved'")
                song_sounds = [(r["id"], r["song_id"], r["sound_id"]) for r in c.fetchall()]

        for username in artists:
            ingestion.ingest_fan_account(db, username)

        for post_id, username in campaign_posts:
            ingestion.ingest_single_post(db, post_id, username)

        for username in roster_usernames:
            ingestion.ingest_roster_account(db, username)

        for s in attached_sounds:
            print(f"  [refresh] campaign row: {s}", flush=True)
            # Skip if this sound is already tracked by the Songs pipeline and fresh.
            # The Songs pipeline owns data fetching; campaigns own relationships.
            if (
                s["sound_db_id"] is not None
                and s["sound_is_fresh"]
            ):
                continue
            ingestion.ingest_campaign_attached_sound(
                db, s["campaign_id"], s["attached_sound_id"], max_results=30
            )

        for sound_db_id, song_id, tiktok_sound_id in song_sounds:
            ingestion.ingest_sound(db, song_id, sound_db_id, tiktok_sound_id, max_results=30)

        return jsonify({"ok": True})

    except Exception as e:
        logging.exception("Refresh failed — full traceback:")
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE ingestion_lock
                    SET locked = FALSE, locked_at = NULL, locked_by = NULL
                    WHERE id = 1
                """)
            conn.commit()