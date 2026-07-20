"""
routes_fan_tracker.py - the original, simplest tracking feature.

A flat list of TikTok usernames (no artist grouping), each with daily
follower/likes/video snapshots and recent posts. This predates the
Artist Roster feature (routes_artists.py) and is kept separate since
some data and routes still depend on this simpler shape.

Now has a real page at /fanpages - global, not tied to any campaign,
since a fan page keeps posting regardless of which specific campaign is
currently active. That's a different, account-centric tracking unit
from Songs/Sounds (post-centric), so it lives as its own top-level tab.
"""

from flask import Blueprint, jsonify, request, render_template_string
from ingestion import api as ingestion
from db import db

fan_tracker_bp = Blueprint("fan_tracker", __name__)


def fetch(username):
    return ingestion.ingest_fan_account(db, username)


@fan_tracker_bp.route("/api/add", methods=["POST"])
def add():
    u = request.json.get("username", "").strip().lstrip("@").lower()
    if not u:
        return jsonify({"error": "No username"}), 400
    with db() as conn:
        with conn.cursor() as c:
            c.execute("INSERT INTO artists (username) VALUES (%s) ON CONFLICT DO NOTHING", (u,))
        conn.commit()
    if not fetch(u):
        return jsonify({"error": f"Could not fetch @{u} - double check the username"}), 400
    return jsonify({"ok": True})


@fan_tracker_bp.route("/api/remove", methods=["POST"])
def remove():
    u = request.json.get("username", "")
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM artists WHERE username=%s", (u,))
        conn.commit()
    return jsonify({"ok": True})


@fan_tracker_bp.route("/api/refresh", methods=["POST"])
def refresh():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT username FROM artists")
            artists = [r["username"] for r in c.fetchall()]
    for a in artists:
        fetch(a)
    return jsonify({"ok": True})


@fan_tracker_bp.route("/api/data")
def data():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT username FROM artists ORDER BY username")
            artists = [r["username"] for r in c.fetchall()]

            if not artists:
                return jsonify({"stats": [], "posts": [], "artists": []})

            # IMPORTANT: `posts` (and to a lesser extent `stats`) are shared
            # tables — campaigns and songs/sounds also write into `posts`,
            # distinguished only by which foreign key is populated (see the
            # table-ownership review flagged earlier in this project). An
            # unfiltered "ORDER BY views DESC LIMIT 200" here was silently
            # getting crowded out entirely by unrelated song/campaign posts
            # with far higher view counts — meaning fan-tracked accounts'
            # own posts never made it into the result at all, even though
            # ingest_fan_account was writing them successfully. Filtering by
            # username here scopes both queries strictly to accounts this
            # feature actually tracks, so tracked accounts' data can never
            # be pushed out by unrelated rows in the shared tables.
            c.execute("""
                SELECT * FROM stats
                WHERE username = ANY(%s)
                ORDER BY date DESC
                LIMIT 500
            """, (artists,))
            stats = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT * FROM posts
                WHERE username = ANY(%s)
                ORDER BY date DESC, views DESC
                LIMIT 200
            """, (artists,))
            posts = [dict(r) for r in c.fetchall()]
    for row in stats:
        row["date"] = str(row["date"])
    for row in posts:
        row["date"] = str(row["date"])
    return jsonify({"stats": stats, "posts": posts, "artists": artists})


@fan_tracker_bp.route("/fanpages")
def fan_pages_page():
    return render_template_string(open("fan_pages.html").read())