"""
routes_campaigns.py — the Campaigns feature (marketing efforts, not organic tracking).

A Campaign is a time-boxed marketing push, optionally referencing a Song
(via song_id) and/or directly tracking manually-pasted posts and an
attached sound (the older, pre-Songs-restructure mechanism, kept for
backward compatibility).
"""

import re
from flask import Blueprint, jsonify, request, render_template_string
from ingestion import api as ingestion
from db import db

campaigns_bp = Blueprint("campaigns", __name__)


def fetch_single_post(post_id, username=None):
    return ingestion.ingest_post(db, post_id, username)


def pull_sound_into_campaign(campaign_id, sound_id, max_results=30, sound_db_id=None):
    return ingestion.ingest_campaign_sound(db, campaign_id, sound_id, max_results=max_results, sound_db_id=sound_db_id)


@campaigns_bp.route("/api/campaigns", methods=["GET", "POST"])
def campaigns():
    if request.method == "POST":
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Request body must be valid JSON with Content-Type: application/json"}), 400
        name = str(data.get("name", "")).strip()
        artist = str(data.get("artist", "")).strip()
        release_type = str(data.get("release_type", "single")).strip()
        if release_type not in ("single", "album"):
            release_type = "single"
        if not name:
            return jsonify({"error": "Campaign name required"}), 400

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
                    INSERT INTO campaigns (name, artist, campaign_artist_id, release_type)
                    VALUES (%s,%s,%s,%s) RETURNING id
                """, (name, artist, campaign_artist_id, release_type))
                new_id = c.fetchone()["id"]
            conn.commit()
        return jsonify({"ok": True, "id": new_id})

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            rows = [dict(r) for r in c.fetchall()]
    for r in rows:
        r["start_date"] = str(r["start_date"])
        r["created_at"] = str(r["created_at"])
    return jsonify(rows)

@campaigns_bp.route("/api/campaigns/<int:campaign_id>", methods=["PATCH"])
def update_campaign(campaign_id):
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Invalid JSON"}), 400
    song_id = data.get("song_id")
    if song_id:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE campaigns SET song_id=%s WHERE id=%s", (song_id, campaign_id))
            conn.commit()
    return jsonify({"ok": True})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>", methods=["DELETE"])
def delete_campaign(campaign_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM campaigns WHERE id=%s", (campaign_id,))
            if not c.fetchone():
                return jsonify({"error": f"Campaign {campaign_id} doesn't exist"}), 404
            c.execute("DELETE FROM campaigns WHERE id=%s", (campaign_id,))
        conn.commit()
    return jsonify({"ok": True})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/add_post", methods=["POST"])
def add_post_to_campaign(campaign_id):
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON with Content-Type: application/json"}), 400
    url = str(data.get("url", "")).strip()
    username, post_id = ingestion.parse_video_url(url)
    if not username or not post_id:
        return jsonify({"error": "Couldn't parse that as a TikTok video URL"}), 400

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM campaigns WHERE id=%s", (campaign_id,))
            if not c.fetchone():
                return jsonify({"error": f"Campaign {campaign_id} doesn't exist"}), 404

    ok = fetch_single_post(post_id, username)
    if not ok:
        return jsonify({"error": "Couldn't fetch that post from TikTok — it may be private, deleted, or the link is wrong"}), 400

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO campaign_links (campaign_id, post_url, post_id, username)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (campaign_id, post_id) DO NOTHING
            """, (campaign_id, url, post_id, username))
            c.execute("UPDATE posts SET campaign_id=%s WHERE post_id=%s", (campaign_id, post_id))
        conn.commit()
    return jsonify({"ok": True, "username": username, "post_id": post_id})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/posts")
def campaign_posts(campaign_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
            campaign_row = c.fetchone()
            if not campaign_row:
                return jsonify({"error": f"Campaign {campaign_id} doesn't exist"}), 404
            campaign = dict(campaign_row)

            c.execute("""
                SELECT p.* FROM posts p
                WHERE p.campaign_id = %s
                ORDER BY p.views DESC
            """, (campaign_id,))
            rows = [dict(r) for r in c.fetchall()]
            c.execute("SELECT * FROM campaign_links WHERE campaign_id=%s", (campaign_id,))
            links = [dict(r) for r in c.fetchall()]
    for r in rows:
        r["date"] = str(r["date"])
    for l in links:
        l["added_at"] = str(l["added_at"])
    campaign["start_date"] = str(campaign["start_date"])
    campaign["created_at"] = str(campaign["created_at"])
    return jsonify({"campaign": campaign, "posts": rows, "links": links})


@campaigns_bp.route("/campaign/<int:campaign_id>")
def campaign_page(campaign_id):
    return render_template_string(open("campaign.html").read())


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/status", methods=["POST"])
def update_campaign_status(campaign_id):
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400
    status = str(data.get("status", "")).strip()
    if status not in ("In Progress", "Wrapped"):
        return jsonify({"error": "Status must be 'In Progress' or 'Wrapped'"}), 400
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM campaigns WHERE id=%s", (campaign_id,))
            if not c.fetchone():
                return jsonify({"error": f"Campaign {campaign_id} doesn't exist"}), 404
            c.execute("UPDATE campaigns SET status=%s WHERE id=%s", (status, campaign_id))
        conn.commit()
    return jsonify({"ok": True})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/posts/<post_id>", methods=["DELETE"])
def remove_post_from_campaign(campaign_id, post_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM campaign_links WHERE campaign_id=%s AND post_id=%s", (campaign_id, post_id))
            c.execute("UPDATE posts SET campaign_id=NULL WHERE post_id=%s AND campaign_id=%s", (post_id, campaign_id))
        conn.commit()
    return jsonify({"ok": True})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/attach_sound", methods=["POST"])
def attach_sound_to_campaign(campaign_id):
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400
    query = str(data.get("query", "")).strip()
    if not query:
        return jsonify({"error": "Enter a sound ID or TikTok link"}), 400

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT 1 FROM campaigns WHERE id=%s", (campaign_id,))
            if not c.fetchone():
                return jsonify({"error": f"Campaign {campaign_id} doesn't exist"}), 404

    sound_id = None
    sound_title = None
    if "tiktok.com" in query:
        m = re.search(r"tiktok\.com/music/[\w\-]*-(\d+)", query)
        if m:
            sound_id = m.group(1)
        else:
            username, post_id = ingestion.parse_video_url(query)
            if not post_id:
                return jsonify({"error": "Couldn't parse that as a TikTok link"}), 400
            sound_id, sound_title = ingestion.get_sound_id_from_post(post_id)
            if not sound_id:
                return jsonify({"error": "Couldn't find a sound on that video"}), 400
            sound_id = str(sound_id)
    else:
        sound_id = query

    info = ingestion.get_sound_info(sound_id)
    if not info:
        return jsonify({"error": f"Couldn't find a sound with ID {sound_id}"}), 400
    title = info.get("title", sound_title or "Unknown sound")

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE campaigns SET attached_sound_id=%s, attached_sound_title=%s WHERE id=%s
            """, (sound_id, title, campaign_id))
        conn.commit()

    added = pull_sound_into_campaign(campaign_id, sound_id, max_results=30)
    return jsonify({"ok": True, "sound_id": sound_id, "title": title, "videos_pulled": added})


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/detach_sound", methods=["POST"])
def detach_sound_from_campaign(campaign_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE campaigns SET attached_sound_id=NULL, attached_sound_title=NULL WHERE id=%s", (campaign_id,))
        conn.commit()
    return jsonify({"ok": True})


@campaigns_bp.route("/campaigns")
def campaigns_page():
    return render_template_string(open("index.html").read())


@campaigns_bp.route("/api/campaigns/<int:campaign_id>/songs")
def campaign_songs(campaign_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT s.*,
                    COUNT(DISTINCT p.post_id) as post_count,
                    COALESCE(SUM(p.views), 0) as total_views
                FROM campaigns c
                JOIN songs s ON s.id = c.song_id
                LEFT JOIN sounds snd ON snd.song_id = s.id
                LEFT JOIN posts p ON p.sound_db_id = snd.id
                WHERE c.id = %s
                GROUP BY s.id
            """, (campaign_id,))
            songs = [dict(r) for r in c.fetchall()]
            for song in songs:
                song["created_at"] = str(song["created_at"])
    return jsonify(songs)