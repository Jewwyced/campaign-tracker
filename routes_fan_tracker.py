"""
routes_fan_tracker.py — the original, simplest tracking feature.

A flat list of TikTok usernames (no artist grouping), each with daily
follower/likes/video snapshots and recent posts. This predates the
Artist Roster feature (routes_artists.py) and is kept separate since
some data and routes still depend on this simpler shape.
"""

from flask import Blueprint, jsonify, request
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
        return jsonify({"error": f"Could not fetch @{u} — double check the username"}), 400
    return jsonify({"ok": True})


@fan_tracker_bp.route("/api/remove", methods=["POST"])
def remove():
    u = request.json.get("username", "")
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM artists WHERE username=%s", (u,))
        conn.commit()
    return jsonify({"ok": True})


@fan_tracker_bp.route("/api/data")
def data():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM stats ORDER BY date DESC LIMIT 500")
            stats = [dict(r) for r in c.fetchall()]
            c.execute("SELECT * FROM posts ORDER BY date DESC, views DESC LIMIT 200")
            posts = [dict(r) for r in c.fetchall()]
            c.execute("SELECT username FROM artists ORDER BY username")
            artists = [r["username"] for r in c.fetchall()]
    for row in stats:
        row["date"] = str(row["date"])
    for row in posts:
        row["date"] = str(row["date"])
    return jsonify({"stats": stats, "posts": posts, "artists": artists})