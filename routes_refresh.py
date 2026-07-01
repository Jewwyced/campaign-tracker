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
import time
import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)

# Locks older than this are considered stale and will be overridden.
# Prevents a crash from permanently halting ingestion.
LOCK_TIMEOUT_MINUTES = 30


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    # Atomic lock acquisition — single UPDATE prevents race conditions.
    # Two simultaneous requests can't both see locked=FALSE because only
    # one UPDATE will match the WHERE clause and return rowcount=1.
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

    time.sleep(10)
    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT username FROM artists")
                artists = [r["username"] for r in c.fetchall()]
                c.execute("SELECT DISTINCT post_id, username FROM campaign_links")
                campaign_posts = [(r["post_id"], r["username"]) for r in c.fetchall()]
                c.execute("SELECT username FROM roster_accounts")
                roster_usernames = [r["username"] for r in c.fetchall()]
                c.execute("SELECT id, attached_sound_id FROM campaigns WHERE attached_sound_id IS NOT NULL")
                attached_sounds = [(r["id"], r["attached_sound_id"]) for r in c.fetchall()]
                c.execute("SELECT id, song_id, sound_id FROM sounds WHERE status='approved'")
                song_sounds = [(r["id"], r["song_id"], r["sound_id"]) for r in c.fetchall()]

        for username in artists:
            ingestion.ingest_fan_account(db, username)
        for post_id, username in campaign_posts:
            ingestion.ingest_single_post(db, post_id, username)
        for username in roster_usernames:
            ingestion.ingest_roster_account(db, username)
        for campaign_id, sound_id in attached_sounds:
            ingestion.ingest_campaign_attached_sound(db, campaign_id, sound_id, max_results=30)
        for sound_db_id, song_id, tiktok_sound_id in song_sounds:
            ingestion.ingest_sound(db, song_id, sound_db_id, tiktok_sound_id, max_results=30)

        return jsonify({"ok": True})

    except Exception as e:
        logging.exception("Refresh failed — full traceback:")
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        # Always release the lock — even if something crashes mid-refresh.
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE ingestion_lock
                    SET locked = FALSE, locked_at = NULL, locked_by = NULL
                    WHERE id = 1
                """)
            conn.commit()