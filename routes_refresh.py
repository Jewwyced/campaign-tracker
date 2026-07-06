"""
routes_refresh.py — the single endpoint the cron hits every 4 hours.

Each refresh does two things:
1. Discover new sounds for every active campaign song
2. Refresh posts for all approved sounds (prioritized by activity)
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
                SET locked = TRUE, locked_at = NOW(), locked_by = 'refresh'
                WHERE id = 1
                  AND (locked = FALSE OR locked_at < NOW() - (%s * INTERVAL '1 minute'))
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
        # Step 1: Get active songs from campaigns
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT DISTINCT s.id as song_id, s.name, s.artist
                    FROM campaign_songs cs
                    JOIN campaigns c ON c.id = cs.campaign_id
                    JOIN songs s ON s.id = cs.song_id
                    WHERE c.status = 'In Progress'
                """)
                active_songs = [dict(r) for r in c.fetchall()]

        # Step 2: Discover new sounds for each song
        new_sounds_total = 0
        for song in active_songs:
            try:
                results = ingestion.ingest_song_sounds(
                    db, song["song_id"], song["name"], song["artist"] or ""
                )
                new_sounds_total += len(results) if results else 0
            except Exception as e:
                logging.warning(f"[refresh] sound discovery failed for song {song['song_id']}: {e}")

        logging.info(f"[refresh] discovery: {new_sounds_total} sounds across {len(active_songs)} songs")

        # Step 3: Get all approved sounds, prioritized by recent activity
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT DISTINCT snd.id as sound_db_id, snd.song_id,
                           snd.sound_id as tiktok_sound_id,
                           COALESCE(snd.posts_7d, 0) as posts_7d
                    FROM campaign_songs cs
                    JOIN sounds snd ON snd.song_id = cs.song_id
                    JOIN campaigns c ON c.id = cs.campaign_id
                    WHERE snd.status = 'approved'
                    AND c.status = 'In Progress'
                    ORDER BY snd.posts_7d DESC, snd.id
                """)
                song_sounds = [dict(r) for r in c.fetchall()]

        logging.info(f"[refresh] refreshing {len(song_sounds)} sounds")

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

        return jsonify({
            "ok": True,
            "sounds_checked": len(song_sounds),
            "skipped": skipped,
            "ingested": ingested,
            "new_sounds_discovered": new_sounds_total,
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