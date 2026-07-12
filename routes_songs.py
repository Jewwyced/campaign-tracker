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
    """Re-discover sounds and refresh posts for a song.
    Safe to call at any time — never deletes existing data.

    IMPORTANT: this used to force-clear last_ingested_at on EVERY approved
    sound before refreshing, which defeated ingest_sound's own freshness
    cache (a 6-hour window meant to skip sounds refreshed recently, cheaply,
    with just a DB read). That guaranteed a full network round-trip for
    every single approved sound on every click, no matter how recently it
    had been refreshed — which is a large part of why this endpoint was
    timing out and crashing gunicorn workers. That forced reset is gone.

    Standalone Songs (not attached to any in-progress Campaign) have no
    cron safety net — this manual refresh is the only path their sounds
    ever get updated through. So rather than capping the batch and letting
    "the rest" get picked up automatically (there's nothing to pick it up
    here), this orders approved sounds by staleness (oldest
    last_ingested_at first, nulls first) and only processes the top
    SONG_REFRESH_BATCH_SIZE per call. Repeated manual refreshes naturally
    rotate through every approved sound over time, each call bounded.
    """
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Song not found"}), 404
            name = row["name"]
            artist = row["artist"] or ""

    # Step 1: Discover new sounds (won't duplicate existing ones)
    new_sounds = ingestion.ingest_song_sounds(db, song_id, name, artist)

    # Step 2: Pick the STALEST approved sounds first, capped to a safe
    # batch size — ingest_sound's own freshness check will still skip any
    # of these that happen to already be fresh (e.g. refreshed by the
    # discover step above), so this is a ceiling on network calls, not a
    # guarantee that all of them hit the provider.
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id, sound_id
                FROM sounds
                WHERE song_id=%s AND status='approved'
                ORDER BY last_ingested_at ASC NULLS FIRST
                LIMIT %s
            """, (song_id, SONG_REFRESH_BATCH_SIZE))
            sounds = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT COUNT(*) as total FROM sounds
                WHERE song_id=%s AND status='approved'
            """, (song_id,))
            total_approved = c.fetchone()["total"]

    posts_added = 0
    ingested = 0
    for s in sounds:
        result = ingestion.ingest_sound(db, song_id, s["id"], s["sound_id"], max_results=35)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1

    remaining = max(total_approved - len(sounds), 0)

    return jsonify({
        "ok": True,
        "new_sounds": len(new_sounds) if new_sounds else 0,
        "sounds_refreshed": len(sounds),
        "sounds_ingested": ingested,
        "posts_added": posts_added,
        "total_approved_sounds": total_approved,
        "remaining_stale_sounds": remaining,
    })


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