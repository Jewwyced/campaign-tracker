"""
routes_refresh.py — two separate refresh endpoints with different schedules.

/api/refresh/discover  — finds new sounds for active songs (every 6h)
/api/refresh/monitor   — refreshes posts for existing sounds (every 1h)
/api/refresh           — legacy endpoint, runs monitor only
"""

import logging
from flask import Blueprint, jsonify
from ingestion import api as ingestion
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


@refresh_bp.route("/api/refresh/qualify", methods=["POST"])
def refresh_qualify():
    """Qualification scan — fetches music-info for pending sounds and promotes based on video_count.
    Run after discovery to decide which sounds deserve monitoring.
    
    Promotion tiers:
    - video_count > 1000  → approved (hot)
    - video_count > 100   → approved (warm)  
    - video_count > 0     → approved (cold, monitor less frequently)
    - video_count == 0    → inactive (dead)
    """
    if not _acquire_lock('qualify'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        # Get pending sounds that haven't been qualified yet
        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT snd.id, snd.sound_id, snd.song_id
                    FROM sounds snd
                    WHERE snd.status = 'pending'
                    AND snd.song_id IN (
                        SELECT cs.song_id FROM campaign_songs cs
                        JOIN campaigns c ON c.id = cs.campaign_id
                        WHERE c.status = 'In Progress'
                    )
                    ORDER BY snd.id
                    LIMIT 50
                """)
                pending = [dict(r) for r in c.fetchall()]

        logging.info(f"[qualify] checking {len(pending)} pending sounds")

        approved = 0
        inactive = 0
        for s in pending:
            try:
                # Call TikLive directly for flat response with video_count
                raw = _provider.get_sound_info(s["sound_id"])
                print(f"[qualify] sound {s['sound_id']} raw={str(raw)[:80]}", flush=True)
                if not raw:
                    inactive += 1
                    with db() as conn:
                        with conn.cursor() as c:
                            c.execute("UPDATE sounds SET status='inactive' WHERE id=%s", (s["id"],))
                        conn.commit()
                    continue

                # TikLive returns flat format wrapped in TikAPI shape
                video_count = 0
                title = ""
                author = ""

                music_info = raw.get("musicInfo", {})
                if music_info:
                    music = music_info.get("music", {})
                    stats = music_info.get("stats", {})
                    video_count = stats.get("videoCount") or 0
                    title = music.get("title") or ""
                    author = music.get("authorName") or ""
                else:
                    video_count = raw.get("video_count") or 0
                    title = raw.get("title") or ""
                    author = raw.get("author") or ""

                if video_count == 0:
                    new_status = "inactive"
                    inactive += 1
                else:
                    # Relevance check — sound must relate to the song
                    # Get song name and artist for this sound
                    with db() as conn:
                        with conn.cursor() as c:
                            c.execute("SELECT name, artist FROM songs WHERE id=%s", (s["song_id"],))
                            song_row = c.fetchone()

                    song_name = (song_row["name"] if song_row else "").lower()
                    song_artist = (song_row["artist"] if song_row else "").lower()
                    title_lower = title.lower()
                    author_lower = author.lower()

                    # Extract key words from song name (skip short words)
                    song_words = [w for w in song_name.split() if len(w) > 3]

                    is_relevant = (
                        any(w in title_lower for w in song_words) or
                        any(w in author_lower for w in song_words) or
                        (song_artist and song_artist in author_lower) or
                        title_lower == "" or  # no title = original sound, allow
                        "original sound" in title_lower or
                        "original" in title_lower
                    )

                    if is_relevant:
                        new_status = "approved"
                        approved += 1
                    else:
                        new_status = "inactive"
                        inactive += 1
                        print(f"[qualify] rejected '{title}' by '{author}' — not relevant to '{song_name}'", flush=True)

                with db() as conn:
                    with conn.cursor() as c:
                        c.execute("""
                            UPDATE sounds 
                            SET status=%s, current_video_count=%s,
                                title=COALESCE(NULLIF(%s,''), title),
                                author=COALESCE(NULLIF(%s,''), author)
                            WHERE id=%s
                        """, (new_status, video_count, title, author, s["id"]))
                    conn.commit()

            except Exception as e:
                logging.warning(f"[qualify] failed for sound {s['id']}: {e}")

        logging.info(f"[qualify] {len(pending)} checked: {approved} approved, {inactive} inactive")
        return jsonify({
            "ok": True,
            "checked": len(pending),
            "approved": approved,
            "inactive": inactive,
        })

    except Exception as e:
        logging.exception("Qualify scan failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    """Legacy endpoint — runs monitor scan only."""
    return refresh_monitor()