"""
routes_refresh.py — the single endpoint the hourly cron hits.

This is intentionally cross-cutting: it re-ingests fan accounts, campaign
posts, roster accounts, legacy campaign-attached sounds, and every tracked
Song's sounds, in one pass. Kept separate from the other route files since
it genuinely spans every feature rather than belonging to one of them.
"""

from flask import Blueprint, jsonify
from ingestion import api as ingestion
from db import db

refresh_bp = Blueprint("refresh", __name__)


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT username FROM artists")
            artists = [r["username"] for r in c.fetchall()]
            c.execute("SELECT DISTINCT post_id, username FROM campaign_links")
            campaign_posts = [(r["post_id"], r["username"]) for r in c.fetchall()]
            c.execute("SELECT username FROM roster_accounts")
            roster_usernames = [r["username"] for r in c.fetchall()]
            c.execute("SELECT id, attached_sound_id FROM campaigns WHERE attached_sound_id IS NOT NULL")
            attached_sounds = [(r["id"], r["attached_sound_id"]) for r in c.fetchall()]
            c.execute("SELECT id, song_id, sound_id FROM sounds WHERE status='approved'")
            song_sounds = [(r["id"], r["song_id"], r["sound_id"]) for r in c.fetchall()]

    for username in artists:
        ingestion.ingest_fan_account(db, username)
    for post_id, username in campaign_posts:
        ingestion.ingest_single_post(db, post_id, username)
    for username in roster_usernames:
        ingestion.ingest_roster_account(db, username)
    for campaign_id, sound_id in attached_sounds:
        ingestion.ingest_campaign_attached_sound(db, campaign_id, sound_id, max_results=30)
    for sound_db_id, song_id, tiktok_sound_id in song_sounds:
        ingestion.ingest_sound(db, song_id, sound_db_id, tiktok_sound_id, max_results=30)

    return jsonify({"ok": True})