"""
routes_refresh.py — two separate refresh endpoints with different schedules.

/api/refresh/discover  — finds new sounds for active songs (every 6h)
/api/refresh/monitor   — refreshes posts for existing sounds (every 1h)
/api/refresh           — legacy endpoint, runs monitor only
"""

import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)

LOCK_TIMEOUT_MINUTES = 30


def _acquire_lock(lock_name='refresh'):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE ingestion_lock
                SET locked = TRUE, locked_at = NOW(), locked_by = %s
                WHERE id = 1
                  AND (locked = FALSE OR locked_at < NOW() - (%s * INTERVAL '1 minute'))
            """, (lock_name, LOCK_TIMEOUT_MINUTES))
            acquired = c.rowcount == 1
        conn.commit()
    return acquired


def _release_lock():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE ingestion_lock
                SET locked = FALSE, locked_at = NULL, locked_by = NULL
                WHERE id = 1
            """)
        conn.commit()


def _get_active_songs():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT DISTINCT s.id as song_id, s.name, s.artist
                FROM campaign_songs cs
                JOIN campaigns c ON c.id = cs.campaign_id
                JOIN songs s ON s.id = cs.song_id
                WHERE c.status = 'In Progress'
            """)
            return [dict(r) for r in c.fetchall()]


def _get_active_sounds():
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
            return [dict(r) for r in c.fetchall()]


@refresh_bp.route("/api/refresh/discover", methods=["POST"])
def refresh_discover():
    """Discovery scan — finds new sounds for active songs.
    Run every 6 hours via cron: 0 */6 * * *"""
    if not _acquire_lock('discover'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        active_songs = _get_active_songs()
        logging.info(f"[discover] starting discovery for {len(active_songs)} songs")

        new_sounds_total = 0
        for song in active_songs:
            try:
                results = ingestion.ingest_song_sounds(
                    db, song["song_id"], song["name"], song["artist"] or ""
                )
                count = len(results) if results else 0
                new_sounds_total += count
                logging.info(f"[discover] song '{song['name']}': {count} sounds found")
            except Exception as e:
                logging.warning(f"[discover] failed for song {song['song_id']}: {e}")

        logging.info(f"[discover] complete: {new_sounds_total} total sounds across {len(active_songs)} songs")
        return jsonify({"ok": True, "songs_scanned": len(active_songs), "sounds_found": new_sounds_total})

    except Exception as e:
        logging.exception("Discovery scan failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh/monitor", methods=["POST"])
def refresh_monitor():
    """Monitoring scan — refreshes posts for existing sounds.
    Run every hour via cron: 0 * * * *"""
    if not _acquire_lock('monitor'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        song_sounds = _get_active_sounds()
        logging.info(f"[monitor] refreshing {len(song_sounds)} sounds")

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

        logging.info(f"[monitor] {len(song_sounds)} sounds: {skipped} fresh, {ingested} refreshed")
        return jsonify({
            "ok": True,
            "sounds_checked": len(song_sounds),
            "skipped": skipped,
            "ingested": ingested,
        })

    except Exception as e:
        logging.exception("Monitor scan failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    """Legacy endpoint — runs monitor scan only."""
    return refresh_monitor()