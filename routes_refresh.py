"""
routes_refresh.py — the single endpoint the hourly cron hits.

Focused refresh: only ingests sounds for tracked campaigns.
Skips fan accounts and campaign-attached sounds during cron runs
to preserve API quota. Fan accounts can be refreshed manually.
"""

import logging
from flask import Blueprint, jsonify, request
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)

LOCK_TIMEOUT_MINUTES = 30


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    # Atomic lock acquisition
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
        # Only refresh sounds linked to active campaigns via campaign_songs
        # Skip fan accounts and campaign-attached sounds to preserve quota
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT DISTINCT snd.id as sound_db_id, snd.song_id, snd.sound_id as tiktok_sound_id
                    FROM campaign_songs cs
                    JOIN sounds snd ON snd.song_id = cs.song_id
                    JOIN campaigns c ON c.id = cs.campaign_id
                    WHERE snd.status = 'approved'
                    AND c.status = 'In Progress'
                    ORDER BY snd.id
                """)
                song_sounds = [dict(r) for r in c.fetchall()]

        logging.info(f"[refresh] found {len(song_sounds)} sounds to refresh")

        skipped = 0
        ingested = 0
        for s in song_sounds:
            result = ingestion.ingest_song_sound(
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

        return jsonify({
            "ok": True,
            "sounds_checked": len(song_sounds),
            "skipped": skipped,
            "ingested": ingested
        })

    except Exception as e:
        logging.exception("Refresh failed — full traceback:")
        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE ingestion_lock
                    SET locked = FALSE, locked_at = NULL, locked_by = NULL
                    WHERE id = 1 AND locked_by = 'refresh'
                """)
            conn.commit()