"""
routes_artists.py — the Artist Roster feature.

Tracks an artist's connected TikTok accounts (official + fan pages),
combined stats, and follower growth over time. Independent of Songs/Campaigns.
"""

from flask import Blueprint, jsonify, request, render_template_string
from ingestion import api as ingestion
from db import db

artists_bp = Blueprint("artists", __name__)


@artists_bp.route("/api/artists", methods=["GET", "POST"])
def roster_artists():
    if request.method == "POST":
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Request body must be valid JSON"}), 400
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "Artist name required"}), 400
        with db() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO roster_artists (name) VALUES (%s) RETURNING id", (name,))
                new_id = c.fetchone()["id"]
            conn.commit()
        return jsonify({"ok": True, "id": new_id})

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM roster_artists ORDER BY created_at DESC")
            rows = [dict(r) for r in c.fetchall()]
    for r in rows:
        r["created_at"] = str(r["created_at"])
    return jsonify(rows)


@artists_bp.route("/api/artists/<int:artist_id>/accounts", methods=["POST"])
def add_roster_account(artist_id):
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400
    username = str(data.get("username", "")).strip().lstrip("@").lower()
    account_type = str(data.get("account_type", "Fan Account")).strip()
    if not username:
        return jsonify({"error": "Username required"}), 400

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM roster_artists WHERE id=%s", (artist_id,))
            if not c.fetchone():
                return jsonify({"error": f"Artist {artist_id} doesn't exist"}), 404

    ok = ingestion.ingest_roster_account(db, username)
    if not ok:
        return jsonify({"error": f"Could not fetch @{username} — double check the username"}), 400

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO roster_accounts (username, artist_id, account_type)
                VALUES (%s,%s,%s)
                ON CONFLICT (username) DO UPDATE SET artist_id=EXCLUDED.artist_id, account_type=EXCLUDED.account_type
            """, (username, artist_id, account_type))
        conn.commit()
    return jsonify({"ok": True})


@artists_bp.route("/api/artists/<int:artist_id>/accounts/<username>", methods=["DELETE"])
def remove_roster_account(artist_id, username):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM roster_accounts WHERE artist_id=%s AND username=%s", (artist_id, username))
        conn.commit()
    return jsonify({"ok": True})


@artists_bp.route("/api/artists/<int:artist_id>")
def roster_artist_detail(artist_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM roster_artists WHERE id=%s", (artist_id,))
            artist_row = c.fetchone()
            if not artist_row:
                return jsonify({"error": f"Artist {artist_id} doesn't exist"}), 404
            artist = dict(artist_row)

            c.execute("SELECT username, account_type FROM roster_accounts WHERE artist_id=%s", (artist_id,))
            accounts = [dict(r) for r in c.fetchall()]

            account_details = []
            total_followers = total_likes = total_videos = 0
            total_views = total_comments = total_shares = 0
            for acc in accounts:
                username = acc["username"]
                c.execute("""
                    SELECT * FROM roster_stats WHERE username=%s
                    ORDER BY date DESC LIMIT 2
                """, (username,))
                rows = [dict(r) for r in c.fetchall()]
                latest = rows[0] if rows else None
                prev = rows[1] if len(rows) > 1 else None

                followers = latest["followers"] if latest else 0
                likes = latest["total_likes"] if latest else 0
                videos = latest["video_count"] if latest else 0
                views = latest["views_24h"] if latest else 0
                comments = latest["comments_24h"] if latest else 0
                shares = latest["shares_24h"] if latest else 0
                followers_delta = (followers - prev["followers"]) if (latest and prev) else None

                total_followers += followers or 0
                total_likes += likes or 0
                total_videos += videos or 0
                total_views += views or 0
                total_comments += comments or 0
                total_shares += shares or 0

                account_details.append({
                    "username": username,
                    "account_type": acc["account_type"],
                    "followers": followers,
                    "total_likes": likes,
                    "video_count": videos,
                    "followers_delta_24h": followers_delta,
                })

    artist["created_at"] = str(artist["created_at"])
    return jsonify({
        "artist": artist,
        "totals": {
            "followers": total_followers,
            "total_likes": total_likes,
            "video_count": total_videos,
            "account_count": len(accounts),
            "total_views": total_views,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "total_engagements": total_likes + total_comments + total_shares,
        },
        "accounts": account_details,
    })


@artists_bp.route("/api/artists/<int:artist_id>/history")
def roster_artist_history(artist_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT username FROM roster_accounts WHERE artist_id=%s", (artist_id,))
            usernames = [r["username"] for r in c.fetchall()]
            if not usernames:
                return jsonify({"dates": []})

            c.execute("""
                SELECT date,
                       SUM(followers) as followers,
                       SUM(video_count) as creates,
                       SUM(views_24h) as views
                FROM roster_stats
                WHERE username = ANY(%s)
                GROUP BY date
                ORDER BY date ASC
            """, (usernames,))
            rows = [dict(r) for r in c.fetchall()]

    for r in rows:
        r["date"] = str(r["date"])
    return jsonify({"dates": rows})


@artists_bp.route("/artists")
def artists_page():
    return render_template_string(open("artists.html").read())


@artists_bp.route("/artists/<int:artist_id>")
def artist_detail_page(artist_id):
    return render_template_string(open("artist_detail.html").read())