"""
routes_refresh.py — two separate refresh endpoints with different schedules.

/api/refresh/discover  — finds new sounds for active songs (every 6h)
/api/refresh/monitor   — refreshes posts for existing sounds (every 1h)
/api/refresh           — legacy endpoint, runs monitor only
/api/songs/<id>/quick_refresh — instant discover+qualify+ingest for ONE song,
                                 used right after a song is added so the demo
                                 doesn't have to wait for any cron cycle.
/api/songs/<id>/ingest_only    — fast, targeted post-pull for ONE song's
                                 already-approved sounds. No discovery, no
                                 qualify. Use this when a song's sounds are
                                 already correctly approved and you just
                                 need posts pulled in without risking the
                                 slower/timeout-prone full pipeline.
"""

import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
from ingestion import service as ingestion_service
from ingestion.providers import default_provider as _provider
from ingestion.parsers import parse_sound_info
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
    """Get top 25 approved sounds by priority score (recent activity weighted).
    Only returns stale sounds not refreshed in last 3 hours."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.id as sound_db_id, snd.song_id,
                       snd.sound_id as tiktok_sound_id,
                       COALESCE(snd.posts_7d, 0) as posts_7d,
                       COALESCE(snd.posts_24h, 0) as posts_24h
                FROM sounds snd
                WHERE snd.status = 'approved'
                AND snd.song_id IN (
                    SELECT cs.song_id FROM campaign_songs cs
                    JOIN campaigns c ON c.id = cs.campaign_id
                    WHERE c.status = 'In Progress'
                )
                AND (snd.last_ingested_at IS NULL
                     OR snd.last_ingested_at < NOW() - INTERVAL '3 hours')
                ORDER BY
                    (COALESCE(snd.posts_24h, 0) * 10 +
                     COALESCE(snd.posts_7d, 0) * 3) DESC,
                    snd.last_ingested_at ASC NULLS FIRST
                LIMIT 25
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

        # Auto-qualify after discovery so new sounds become approved immediately.
        # NOTE: this reuses the same shared logic as /api/refresh/qualify and
        # /api/songs/<id>/quick_refresh via ingestion_service.qualify_pending_sounds_for_song,
        # so there is only ONE qualify implementation instead of three.
        auto_approved = 0
        for song in active_songs:
            try:
                result = ingestion_service.qualify_pending_sounds_for_song(db, song["song_id"])
                auto_approved += result.get("approved", 0)
            except Exception as e:
                logging.warning(f"[discover] qualify failed for song {song['song_id']}: {e}")

        logging.info(f"[discover] auto-qualified {auto_approved} sounds")
        return jsonify({"ok": True, "songs_scanned": len(active_songs), "sounds_found": new_sounds_total, "auto_approved": auto_approved})

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


@refresh_bp.route("/api/refresh/qualify", methods=["POST"])
def refresh_qualify():
    """Qualification scan — fetches music-info for pending sounds and promotes based on video_count.
    Run after discovery to decide which sounds deserve monitoring.

    NOTE: shares logic with the auto-qualify step inside /api/refresh/discover and
    with /api/songs/<id>/quick_refresh via ingestion_service.qualify_pending_sounds_for_song.
    """
    if not _acquire_lock('qualify'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT DISTINCT snd.song_id
                    FROM sounds snd
                    WHERE snd.status = 'pending'
                    AND snd.song_id IN (
                        SELECT cs.song_id FROM campaign_songs cs
                        JOIN campaigns c ON c.id = cs.campaign_id
                        WHERE c.status = 'In Progress'
                    )
                """)
                song_ids = [r["song_id"] for r in c.fetchall()]

        logging.info(f"[qualify] checking pending sounds across {len(song_ids)} songs")

        total_checked = total_approved = total_inactive = 0
        for song_id in song_ids:
            result = ingestion_service.qualify_pending_sounds_for_song(db, song_id)
            total_checked += result.get("checked", 0)
            total_approved += result.get("approved", 0)
            total_inactive += result.get("inactive", 0)

        logging.info(f"[qualify] {total_checked} checked: {total_approved} approved, {total_inactive} inactive")
        return jsonify({
            "ok": True,
            "checked": total_checked,
            "approved": total_approved,
            "inactive": total_inactive,
        })

    except Exception as e:
        logging.exception("Qualify scan failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/songs/<int:song_id>/quick_refresh", methods=["POST"])
def quick_refresh_song(song_id):
    """Instant, single-song pipeline: discover -> qualify -> ingest posts.
    Call this right after creating a song so the UI can show a 'loading' state
    and then have everything (sounds AND videos) appear at once — no separate
    manual discover/qualify/monitor steps, no waiting for the next cron cycle.
    """
    if not _acquire_lock(f'quick_refresh_song_{song_id}'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT id, name, artist FROM songs WHERE id=%s", (song_id,))
                song = c.fetchone()

        if not song:
            return jsonify({"ok": False, "error": f"song {song_id} not found"}), 404

        logging.info(f"[quick_refresh] starting full pipeline for song {song_id} ('{song['name']}')")
        result = ingestion_service.initialize_song(
            db, song_id, song["name"], song["artist"] or ""
        )
        logging.info(f"[quick_refresh] song {song_id} complete: {result}")

        return jsonify({"ok": True, "song_id": song_id, **result})

    except Exception as e:
        logging.exception(f"Quick refresh failed for song {song_id}:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/songs/<int:song_id>/ingest_only", methods=["POST"])
def ingest_only_song(song_id):
    """Fast, targeted ingest for ONE song's already-approved sounds — no
    discovery, no qualify. Use this when a song's sounds are already
    correctly approved (e.g. approved manually) and you just need to pull
    posts in without re-running the full (possibly slow/timeout-prone)
    discover -> qualify -> ingest pipeline via quick_refresh."""
    if not _acquire_lock(f'ingest_only_song_{song_id}'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        result = ingestion_service.ingest_approved_sounds_for_song(db, song_id)
        return jsonify({"ok": True, "song_id": song_id, **result})
    except Exception as e:
        logging.exception(f"Ingest-only failed for song {song_id}:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    """Legacy endpoint — runs monitor scan only."""
    return refresh_monitor()