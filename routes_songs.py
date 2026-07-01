"""
routes_songs.py — the Songs dashboard (Chartex-style monitoring).

A Song is independent of any Campaign — it's tracked forever once added,
regardless of whether there's an active marketing push behind it. Each Song
has many Sounds (Original, Sped Up, Remix, etc), each Sound has many Posts.
"""

from flask import Blueprint, jsonify, request, render_template_string
from ingestion import api as ingestion
from services import song_catalog 
from db import db

songs_bp = Blueprint("songs", __name__)


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
                campaign_artist_id = None
                if artist:
                    c.execute("""
                        INSERT INTO campaign_artists (name) VALUES (%s)
                        ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
                        RETURNING id
                    """, (artist,))
                    campaign_artist_id = c.fetchone()["id"]
                c.execute("""
                   INSERT INTO songs (name, artist) VALUES (%s,%s) RETURNING id 
                """, (name, artist))
                song_id = c.fetchone()["id"]
            conn.commit()

        # Auto-discover and pull in every distinct sound found for this song —
        # no manual searching, no approval step. User can delete bad matches after the fact.
        search_query = f"{name} {artist}".strip()
        results = ingestion.ingest_song_sounds(db, song_id, name, artist)max_results=30)
        sounds_added = [{"sound_id": r["sound_id"], "title": r["title"], "author": r["author"]} for r in results]

        return jsonify({"ok": True, "song_id": song_id, "sounds_found": len(sounds_added), "sounds": sounds_added})

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs ORDER BY created_at DESC")
            songs = [dict(r) for r in c.fetchall()]
            for song in songs:
                song["created_at"] = str(song["created_at"])
                c.execute("SELECT id FROM sounds WHERE song_id=%s", (song["id"],))
                sound_ids = [r["id"] for r in c.fetchall()]
                song["sound_count"] = len(sound_ids)
                if sound_ids:
                    c.execute("""
                        SELECT COALESCE(SUM(views),0) as views, COUNT(*) as post_count,
                               COUNT(DISTINCT username) as creator_count
                        FROM posts WHERE sound_db_id = ANY(%s)
                    """, (sound_ids,))
                    stats = dict(c.fetchone())
                else:
                    stats = {"views": 0, "post_count": 0, "creator_count": 0}
                song.update(stats)
    return jsonify(songs)


@songs_bp.route("/api/songs/<int:song_id>/detail")
def song_detail(song_id):
    """Bundles everything the Song page needs: header stats + growth, top sounds,
    top creators, and top posts (filterable by timeframe via ?window=24h|7d|all)."""
    window = request.args.get("window", "24h")

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()
            if not song_row:
                return jsonify({"error": f"Song {song_id} doesn't exist"}), 404
            song = dict(song_row)
            song["created_at"] = str(song["created_at"])

            # ── Section 2: Top Sounds, ranked by total creates ──
            c.execute("SELECT * FROM sounds WHERE song_id=%s", (song_id,))
            sounds = [dict(r) for r in c.fetchall()]
            for s in sounds:
                s["discovered_at"] = str(s["discovered_at"])
                # Growth windows from song_stats snapshots
                c.execute("""
                    SELECT date, video_count FROM song_stats
                    WHERE sound_id=%s ORDER BY date DESC LIMIT 8
                """, (s["id"],))
                snapshots = [dict(r) for r in c.fetchall()]
                latest = snapshots[0]["video_count"] if snapshots else s.get("current_video_count")
                day_ago = next((row["video_count"] for row in snapshots[1:2]), None)
                week_ago = snapshots[7]["video_count"] if len(snapshots) > 7 else None

                s["total_creates"] = latest
                s["growth_24h"] = (latest - day_ago) if (latest is not None and day_ago is not None) else None
                s["growth_24h_pct"] = round((s["growth_24h"] / day_ago) * 100, 2) if (s["growth_24h"] is not None and day_ago) else None
                s["growth_7d"] = (latest - week_ago) if (latest is not None and week_ago is not None) else None
                s["growth_7d_pct"] = round((s["growth_7d"] / week_ago) * 100, 2) if (s["growth_7d"] is not None and week_ago) else None

            sounds.sort(key=lambda s: s.get("total_creates") or 0, reverse=True)
            sound_db_ids = [s["id"] for s in sounds]

            # ── Header: overall stats across all sounds for this song ──
            total_creates = sum(s.get("total_creates") or 0 for s in sounds)
            if sound_db_ids:
                c.execute("""
                    SELECT COALESCE(SUM(views),0) as views, COALESCE(SUM(likes),0) as likes,
                           COUNT(*) as post_count, COUNT(DISTINCT username) as creator_count
                    FROM posts WHERE sound_db_id = ANY(%s)
                """, (sound_db_ids,))
                header_stats = dict(c.fetchone())
            else:
                header_stats = {"views": 0, "likes": 0, "post_count": 0, "creator_count": 0}
            header_stats["total_creates"] = total_creates

            # Historical total-creates trend across all sounds combined, for the growth chart
            c.execute("""
                SELECT date, SUM(video_count) as total_video_count
                FROM song_stats WHERE sound_id = ANY(%s)
                GROUP BY date ORDER BY date ASC
            """, (sound_db_ids,)) if sound_db_ids else None
            trend = [{"date": str(r["date"]), "total_creates": r["total_video_count"]} for r in c.fetchall()] if sound_db_ids else []

            # ── Section 4: Top Posts, filtered by timeframe ──
            posts_query = """
                SELECT p.*, s.title as sound_title FROM posts p
                JOIN sounds s ON p.sound_db_id = s.id
                WHERE s.song_id = %s
            """
            params = [song_id]
            if window == "24h":
                posts_query += " AND p.created_at >= EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours')"
            elif window == "7d":
                posts_query += " AND p.created_at >= EXTRACT(EPOCH FROM NOW() - INTERVAL '7 days')"
            posts_query += " ORDER BY p.views DESC LIMIT 100"
            c.execute(posts_query, params)
            top_posts = [dict(r) for r in c.fetchall()]
            for p in top_posts:
                p["date"] = str(p["date"])

            # ── Section 3: Top Creators across all sounds for this song ──
            if sound_db_ids:
                c.execute("""
                    SELECT username, COALESCE(SUM(views),0) as total_views,
                           COALESCE(SUM(likes),0) as total_likes, COUNT(*) as video_count,
                           MAX(followers_at_post) as followers
                    FROM posts WHERE sound_db_id = ANY(%s)
                    GROUP BY username ORDER BY total_views DESC LIMIT 10
                """, (sound_db_ids,))
                top_creators = [dict(r) for r in c.fetchall()]
            else:
                top_creators = []

    return jsonify({
        "song": song,
        "header_stats": header_stats,
        "trend": trend,
        "sounds": sounds,
        "top_creators": top_creators,
        "top_posts": top_posts,
        "window": window,
    })


@songs_bp.route("/api/songs/<int:song_id>", methods=["DELETE"])
def delete_song(song_id):
    with db() as conn:
        with conn.cursor() as c:
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
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()
            if not song_row:
                return jsonify({"error": f"Song {song_id} doesn't exist"}), 404
            song = dict(song_row)
            song["created_at"] = str(song["created_at"])

            c.execute("""
                SELECT p.*, s.title as sound_title, s.sound_id as sound_tiktok_id
                FROM posts p
                JOIN sounds s ON p.sound_db_id = s.id
                WHERE s.song_id = %s
                ORDER BY p.views DESC
            """, (song_id,))
            posts = [dict(r) for r in c.fetchall()]
            for p in posts:
                p["date"] = str(p["date"])
    return jsonify({"song": song, "posts": posts})


@songs_bp.route("/songs")
def songs_page():
    return render_template_string(open("songs.html").read())


@songs_bp.route("/song/<int:song_id>")
def song_page(song_id):
    return render_template_string(open("song.html").read())