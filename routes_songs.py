"""
routes_songs.py — the Songs dashboard (Chartex-style monitoring).

A Song is independent of any Campaign — it's tracked forever once added,
regardless of whether there's an active marketing push behind it. Each Song
has many Sounds (Original, Sped Up, Remix, etc), each Sound has many Posts.
"""

from flask import Blueprint, jsonify, request, render_template_string
from ingestion import api as ingestion
from db import db

songs_bp = Blueprint("songs", __name__)

# Max number of a song's approved sounds to actually refresh (network calls)
# in one call to /api/songs/<id>/refresh. Standalone Songs (not attached to
# any in-progress Campaign) have NO cron safety net — this manual refresh
# is the ONLY path their sounds ever get updated through, unlike campaign
# sounds which also get picked up by the hourly monitor cron. So instead of
# capping and leaving a remainder to "get picked up automatically" (there's
# nothing to pick it up), this orders by staleness (oldest last_ingested_at
# first) and only processes the top N — repeated manual refreshes then
# naturally rotate through every approved sound over time, each call bounded
# and safe, rather than one call trying to force-refresh everything at once.
SONG_REFRESH_BATCH_SIZE = 15


@songs_bp.route("/api/songs", methods=["GET", "POST"])
def songs_collection():
    if request.method == "POST":
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Request body must be valid JSON"}), 400
        name = str(data.get("name", "")).strip()
        artist = str(data.get("artist", "")).strip()
        if not name:
            return jsonify({"error": "Song name required"}), 400

        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO songs (name, artist) VALUES (%s,%s) RETURNING id
                """, (name, artist))
                song_id = c.fetchone()["id"]
            conn.commit()

        # Discover sounds — store as pending, cron handles qualify + monitor
        print(f"[DEBUG] POST /api/songs discovering song_id={song_id} name={name!r} artist={artist!r}", flush=True)
        results = ingestion.ingest_song_sounds(db, song_id, name, artist)
        sounds_found = len(results) if results else 0

        return jsonify({
            "ok": True,
            "song_id": song_id,
            "sounds_found": sounds_found,
        })

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT
                    s.id, s.name, s.artist, s.created_at,
                    COUNT(DISTINCT snd.id) as sound_count,
                    COUNT(DISTINCT p.post_id) as post_count,
                    COUNT(DISTINCT p.username) as creator_count,
                    COALESCE(SUM(p.views), 0) as total_views,
                    COALESCE(SUM(p.likes), 0) as total_likes
                FROM songs s
                LEFT JOIN sounds snd ON snd.song_id = s.id
                LEFT JOIN posts p ON p.sound_db_id = snd.id
                GROUP BY s.id
                ORDER BY total_views DESC
            """)
            rows = [dict(r) for r in c.fetchall()]
    for r in rows:
        r["created_at"] = str(r["created_at"])
    return jsonify(rows)


@songs_bp.route("/api/songs/<int:song_id>/detail")
def song_detail(song_id):
    window = request.args.get("window", "all")

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()
            if not song_row:
                return jsonify({"error": "Song not found"}), 404
            song = dict(song_row)
            song["created_at"] = str(song["created_at"])

            c.execute("""
                SELECT id, sound_id, title, author, status, current_video_count,
                       posts_24h, posts_7d, velocity
                FROM sounds WHERE song_id=%s AND status='approved'
                ORDER BY velocity DESC NULLS LAST, current_video_count DESC NULLS LAST
            """, (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT
                    COUNT(DISTINCT p.post_id) as post_count,
                    COUNT(DISTINCT p.username) as creator_count,
                    COALESCE(SUM(p.views), 0) as views,
                    COALESCE(SUM(p.likes), 0) as likes,
                    COUNT(DISTINCT s.id) as sound_count
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
            """, (song_id,))
            stats_row = dict(c.fetchone())

            c.execute("""
                (SELECT p.post_id, p.username, p.views, p.likes, p.comments,
                        p.saves, p.shares, p.thumbnail, p.created_at, p.date
                 FROM posts p
                 JOIN sounds s ON s.id = p.sound_db_id
                 WHERE s.song_id = %s AND s.status = 'approved'
                 ORDER BY p.views DESC NULLS LAST
                 LIMIT 20)
                UNION
                (SELECT p.post_id, p.username, p.views, p.likes, p.comments,
                        p.saves, p.shares, p.thumbnail, p.created_at, p.date
                 FROM posts p
                 JOIN sounds s ON s.id = p.sound_db_id
                 WHERE s.song_id = %s AND s.status = 'approved'
                 ORDER BY p.created_at DESC NULLS LAST
                 LIMIT 20)
            """, (song_id, song_id))
            top_posts = [dict(r) for r in c.fetchall()]
            for p in top_posts:
                p["created_at"] = str(p["created_at"]) if p["created_at"] else None
                p["date"] = str(p["date"]) if p["date"] else None

            c.execute("""
                SELECT p.username,
                       COUNT(DISTINCT p.post_id) as post_count,
                       COALESCE(SUM(p.views), 0) as total_views,
                       COALESCE(SUM(p.likes), 0) as total_likes
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
                GROUP BY p.username
                ORDER BY total_views DESC
                LIMIT 10
            """, (song_id,))
            top_creators = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT p.date, COALESCE(SUM(p.views), 0) as views
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
                GROUP BY p.date
                ORDER BY p.date ASC
            """, (song_id,))
            trend = [{"date": str(r["date"]), "views": r["views"]} for r in c.fetchall()]

    return jsonify({
        "song": song,
        "sounds": sounds,
        "header_stats": {
            "post_count": stats_row["post_count"],
            "creator_count": stats_row["creator_count"],
            "views": stats_row["views"],
            "likes": stats_row["likes"],
            "sound_count": stats_row["sound_count"],
        },
        "top_posts": top_posts,
        "top_creators": top_creators,
        "trend": trend,
        "window": window,
    })


@songs_bp.route("/api/songs/<int:song_id>/insight")
def song_insight(song_id):
    from services.ai_service import generate_song_insight
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"insight": None}), 404
            song = dict(row)

            c.execute("""
                SELECT p.username, COUNT(*) as post_count, SUM(p.views) as total_views
                FROM posts p JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s
                GROUP BY p.username ORDER BY total_views DESC LIMIT 5
            """, (song_id,))
            top_creators = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT description FROM posts p JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND description IS NOT NULL AND description != ''
                LIMIT 20
            """, (song_id,))
            descriptions = [r["description"] for r in c.fetchall()]

            c.execute("""
                SELECT COUNT(DISTINCT p.post_id) as post_count,
                       COUNT(DISTINCT p.username) as creator_count,
                       COALESCE(SUM(p.views), 0) as views,
                       COALESCE(SUM(p.likes), 0) as likes,
                       COUNT(DISTINCT s.id) as sound_count
                FROM sounds s LEFT JOIN posts p ON p.sound_db_id = s.id
                WHERE s.song_id = %s
            """, (song_id,))
            stats = dict(c.fetchone())

    insight = generate_song_insight(song["name"], song["artist"], stats, top_creators, descriptions)
    return jsonify({"insight": insight})


@songs_bp.route("/api/songs/<int:song_id>", methods=["DELETE"])
def delete_song(song_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM sounds WHERE song_id=%s", (song_id,))
            c.execute("DELETE FROM songs WHERE id=%s", (song_id,))
        conn.commit()
    return jsonify({"ok": True})


@songs_bp.route("/api/sounds/<int:sound_db_id>", methods=["DELETE"])
def delete_sound(sound_db_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM sounds WHERE id=%s", (sound_db_id,))
        conn.commit()
    return jsonify({"ok": True})


@songs_bp.route("/api/sounds/<int:sound_db_id>/approve", methods=["POST"])
def approve_sound(sound_db_id):
    """Manually approve a pending sound — the human-in-the-loop step for
    candidates find_new_sounds found but deliberately left pending rather
    than auto-approving. Immediately ingests posts for it too, so it
    starts showing real data right away instead of waiting for the next
    refresh cycle."""
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id, song_id, sound_id FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Sound not found"}), 404
            c.execute("UPDATE sounds SET status='approved' WHERE id=%s", (sound_db_id,))
        conn.commit()

    result = ingestion_service.ingest_sound(db, row["song_id"], row["id"], row["sound_id"], max_results=35)
    return jsonify({"ok": True, "sound_id": sound_db_id, "posts_added": result.get("posts_added", 0)})


@songs_bp.route("/api/sounds/<int:sound_db_id>/reject", methods=["POST"])
def reject_sound(sound_db_id):
    """Manually reject a pending sound — explicit human 'no', distinct
    from DELETE (which removes the row entirely). Keeps the sound's
    history/title/author on record as 'inactive' rather than erasing it."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM sounds WHERE id=%s", (sound_db_id,))
            if not c.fetchone():
                return jsonify({"error": "Sound not found"}), 404
            c.execute("UPDATE sounds SET status='inactive' WHERE id=%s", (sound_db_id,))
        conn.commit()
    return jsonify({"ok": True, "sound_id": sound_db_id})


@songs_bp.route("/api/songs/<int:song_id>/pending_review")
def pending_review(song_id):
    """Lists sounds still sitting 'pending' for a song — the review queue
    a human works through after 'Find New Sounds' runs (auto_approve=False
    means genuine matches stay pending instead of auto-becoming canonical).
    Sorted by video_count so the most-likely-real candidates surface first."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id, sound_id, title, author, current_video_count, discovered_via
                FROM sounds
                WHERE song_id = %s AND status = 'pending'
                ORDER BY current_video_count DESC NULLS LAST
            """, (song_id,))
            pending = [dict(r) for r in c.fetchall()]
    return jsonify(pending)


@songs_bp.route("/api/songs/<int:song_id>/posts")
def song_posts(song_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT p.* FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s
                ORDER BY p.views DESC
                LIMIT 100
            """, (song_id,))
            posts = [dict(r) for r in c.fetchall()]
    for p in posts:
        p["date"] = str(p["date"]) if p["date"] else None
        p["created_at"] = str(p["created_at"]) if p["created_at"] else None
    return jsonify(posts)


@songs_bp.route("/api/songs/<int:song_id>/refresh", methods=["POST"])
def refresh_song(song_id):
    """Routine refresh — updates posts/stats for a song's already-approved
    (canonical) sounds ONLY. Never discovers new candidates.

    This used to also re-run discovery on every call. That made sense
    while the pipeline was still being built and discovery/refresh hadn't
    been split into distinct responsibilities yet — now that they have
    (see initialize_song, refresh_approved_sounds_for_song, and
    find_new_sounds_for_song in ingestion/service.py), refresh should only
    ever touch the canonical set, never expand it.
    """
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM songs WHERE id=%s", (song_id,))
            if not c.fetchone():
                return jsonify({"error": "Song not found"}), 404

    result = ingestion_service.refresh_approved_sounds_for_song(db, song_id, batch_size=SONG_REFRESH_BATCH_SIZE)
    return jsonify({"ok": True, **result})


@songs_bp.route("/api/songs/<int:song_id>/find_new_sounds", methods=["POST"])
def find_new_sounds(song_id):
    """Explicit, user-triggered discovery — expands a song's canonical
    sound set beyond what was found initially. This is the ONLY place
    besides initial song creation where discovery should ever run. Capped
    the same way as everywhere else (qualify processes QUALIFY_BATCH_SIZE
    candidates per call) to avoid worker timeouts; a song with a large
    pending backlog may need this called more than once.
    """
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Song not found"}), 404
            name = row["name"]
            artist = row["artist"] or ""

    result = ingestion_service.find_new_sounds_for_song(db, song_id, name, artist)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/api/songs/<int:song_id>/requalify", methods=["POST"])
def requalify_song(song_id):
    """Re-run ONLY the qualify step for a song's pending sounds — no
    discovery, no ingest. Use this after fixing/tuning the matching logic
    to re-judge already-discovered candidates against the corrected rules,
    without paying for full re-discovery.

    Typical flow: reset wrongly-approved sounds back to 'pending' via SQL,
    then call this to re-classify them under the current rules. Capped to
    QUALIFY_BATCH_SIZE candidates per call (same cap as everywhere else),
    so a song with a large pending backlog may need this called more than
    once to fully clear.
    """
    from ingestion import service as ingestion_service
    result = ingestion_service.qualify_pending_sounds_for_song(db, song_id)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/api/songs/<int:song_id>/requalify", methods=["POST"])
def requalify_song(song_id):
    """Re-run ONLY the qualify step for a song's pending sounds — no
    discovery (which hits many expensive search-video/challenge API calls
    and would re-add candidates rather than just re-judging existing ones),
    no ingest. Use this after fixing/tuning the matching logic to re-judge
    already-discovered candidates against the corrected rules, without
    paying for full re-discovery.

    Typical flow: reset wrongly-approved sounds back to 'pending' via SQL,
    then call this to re-classify them under the current rules. Capped to
    QUALIFY_BATCH_SIZE candidates per call (same cap as everywhere else),
    so a song with a large pending backlog may need this called more than
    once to fully clear.
    """
    from ingestion import service as ingestion_service
    result = ingestion_service.qualify_pending_sounds_for_song(db, song_id)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/songs")
def songs_page():
    return render_template_string(open("songs.html").read())


@songs_bp.route("/song/<int:song_id>")
def song_page(song_id):
    return render_template_string(open("song.html").read())