"""
routes_sounds.py — standalone Sound Lookup tool.

Lets anyone paste a sound ID, sound link, or video link and see the top
videos using that sound — independent of Songs or Campaigns. This is the
original "search any sound" feature, kept simple on purpose.
"""

import re
from flask import Blueprint, jsonify, request, render_template_string
import ingestion
from db import db

sounds_bp = Blueprint("sounds", __name__)

parse_tiktok_url = ingestion.parse_tiktok_url
get_sound_id_from_post = ingestion.get_sound_id_from_post
fetch_sound_info = ingestion.fetch_sound_info
fetch_sound_posts = ingestion.fetch_sound_posts
search_sounds_by_name = ingestion.search_sounds_by_name


def get_sound_dashboard(sound_id, fallback_title=None):
    """Given a sound ID, fetch its info + posts in one bundle."""
    info = fetch_sound_info(sound_id)
    if not info:
        return None

    music_info = info.get("musicInfo", info)
    music = music_info.get("music", {})
    stats = music_info.get("stats", {})

    posts = fetch_sound_posts(sound_id, max_results=60)

    return {
        "sound_id": sound_id,
        "title": music.get("title", fallback_title or "Unknown sound"),
        "author": music.get("authorName", ""),
        "video_count": stats.get("videoCount"),
        "posts": posts,
    }


@sounds_bp.route("/api/sound_search", methods=["POST"])
def sound_search():
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400

    query = str(data.get("query", "")).strip()
    if not query:
        return jsonify({"error": "Enter a song name to search"}), 400

    sounds = search_sounds_by_name(query)
    return jsonify({"sounds": sounds})


@sounds_bp.route("/api/sound/<sound_id>")
def sound_by_id(sound_id):
    result = get_sound_dashboard(sound_id)
    if not result:
        return jsonify({"error": f"Couldn't find a sound with ID {sound_id}"}), 400
    return jsonify(result)


@sounds_bp.route("/sounds/<sound_id>")
def sound_page(sound_id):
    return render_template_string(open("sound_detail.html").read())


@sounds_bp.route("/api/sound_lookup", methods=["POST"])
def sound_lookup():
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400

    query = str(data.get("query", "")).strip()
    if not query:
        return jsonify({"error": "Enter a sound ID or a TikTok video link"}), 400

    sound_id = None
    sound_title = None

    if "tiktok.com" in query:
        m = re.search(r"tiktok\.com/music/[\w\-]*-(\d+)", query)
        if m:
            sound_id = m.group(1)
        else:
            username, post_id = parse_tiktok_url(query)
            if not post_id:
                return jsonify({"error": "Couldn't parse that as a TikTok link"}), 400
            sound_id, sound_title = get_sound_id_from_post(post_id)
            if not sound_id:
                return jsonify({"error": "Couldn't find a sound on that video — it may be private or deleted"}), 400
            sound_id = str(sound_id)
    else:
        sound_id = query

    result = get_sound_dashboard(sound_id, fallback_title=sound_title)
    if not result:
        return jsonify({"error": f"Couldn't find a sound with ID {sound_id}"}), 400
    return jsonify(result)


@sounds_bp.route("/sounds")
def sounds_page():
    return render_template_string(open("sounds.html").read())