"""
app.py — application setup, schema management, and blueprint registration.

This file should stay small. Routes live in routes_*.py files, grouped by
feature (artists, songs, campaigns, sounds, dashboard, fan_tracker, refresh).
All TikTok/TikAPI access lives in ingestion.py — this file never imports
requests for that purpose and never sees the API key.
"""

import os
from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException
import traceback

from db import db
from ingestion import api as ingestion

app = Flask(__name__)
ingestion.set_quota_db_factory(db)


@app.errorhandler(Exception)
def handle_unexpected_error(e):
    # If it's a real HTTP error (404, 400, 415, etc), pass through its actual code and message
    if isinstance(e, HTTPException):
        return jsonify({"error": e.description}), e.code
    # Otherwise it's a genuine unexpected crash — log the FULL traceback so we can find the real cause
    print("  UNEXPECTED ERROR — full traceback:")
    traceback.print_exc()
    return jsonify({"error": "Something went wrong on our end. Check server logs."}), 500


def create_schema():
    """Creates every table if it doesn't already exist. Safe to run on every
    startup — CREATE TABLE IF NOT EXISTS is a no-op against an existing table,
    so this never touches data, only ensures the shape exists."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS roster_artists (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS roster_accounts (
                    username TEXT PRIMARY KEY,
                    artist_id INT REFERENCES roster_artists(id) ON DELETE CASCADE,
                    account_type TEXT DEFAULT 'Fan Account'
                );
                CREATE TABLE IF NOT EXISTS roster_stats (
                    username TEXT, date DATE,
                    followers INT, total_likes BIGINT, video_count INT,
                    views_24h BIGINT, likes_24h BIGINT, comments_24h BIGINT, shares_24h BIGINT,
                    PRIMARY KEY (username, date)
                );
                CREATE TABLE IF NOT EXISTS artists (username TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS stats (
                    username TEXT, date DATE, followers INT, likes BIGINT, videos INT,
                    PRIMARY KEY (username, date)
                );
                CREATE TABLE IF NOT EXISTS campaign_artists (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS songs (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    artist TEXT,
                    match_key TEXT,
                    spotify_id TEXT,
                    isrc TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS campaigns (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    artist TEXT,
                    campaign_artist_id INT REFERENCES campaign_artists(id) ON DELETE SET NULL,
                    song_id INT REFERENCES songs(id) ON DELETE SET NULL,
                    release_type TEXT DEFAULT 'single',
                    status TEXT DEFAULT 'In Progress',
                    start_date DATE DEFAULT CURRENT_DATE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    attached_sound_id TEXT,
                    attached_sound_title TEXT
                );
                CREATE TABLE IF NOT EXISTS sounds (
                    id SERIAL PRIMARY KEY,
                    song_id INT REFERENCES songs(id) ON DELETE CASCADE,
                    sound_id TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    status TEXT DEFAULT 'approved',
                    current_video_count INT,
                    discovered_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(song_id, sound_id)
                );
                CREATE TABLE IF NOT EXISTS song_stats (
                    id SERIAL PRIMARY KEY,
                    sound_id INT REFERENCES sounds(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    video_count INT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(sound_id, date)
                );
                CREATE TABLE IF NOT EXISTS posts (
                    post_id TEXT PRIMARY KEY, date DATE, username TEXT,
                    description TEXT, views INT, likes INT, comments INT, saves INT, created_at BIGINT,
                    followers_at_post INT,
                    campaign_id INT REFERENCES campaigns(id) ON DELETE SET NULL,
                    sound_db_id INT REFERENCES sounds(id) ON DELETE SET NULL,
                    source TEXT DEFAULT 'manual'
                );
                CREATE TABLE IF NOT EXISTS campaign_links (
                    id SERIAL PRIMARY KEY,
                    campaign_id INT REFERENCES campaigns(id) ON DELETE CASCADE,
                    post_url TEXT NOT NULL,
                    post_id TEXT,
                    username TEXT,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(campaign_id, post_id)
                );
                CREATE TABLE IF NOT EXISTS quota_usage (
                    date DATE NOT NULL,
                    endpoint TEXT NOT NULL,
                    request_count INT DEFAULT 0,
                    PRIMARY KEY (date, endpoint)
                );
                CREATE TABLE IF NOT EXISTS ingestion_jobs (
                    id SERIAL PRIMARY KEY,
                    song_id INT REFERENCES songs(id) ON DELETE CASCADE,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    started_at TIMESTAMP DEFAULT NOW(),
                    completed_at TIMESTAMP,
                    sounds_found INT,
                    sounds_ingested INT,
                    posts_added INT,
                    error TEXT
                );
            """)
        conn.commit()


def run_schema_migrations():
    """Adds columns to tables that already existed before that column was
    introduced. CREATE TABLE IF NOT EXISTS skips the whole statement if the
    table is already there, so new columns need this explicit ALTER step —
    safe to run every time, ADD COLUMN IF NOT EXISTS is a no-op once applied."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS campaign_id INT REFERENCES campaigns(id) ON DELETE SET NULL;
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual';
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS thumbnail TEXT;
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS shares INT DEFAULT 0;
                ALTER TABLE posts ADD COLUMN IF NOT EXISTS sound_db_id INT REFERENCES sounds(id) ON DELETE SET NULL;
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS attached_sound_id TEXT;
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS attached_sound_title TEXT;
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS campaign_artist_id INT REFERENCES campaign_artists(id) ON DELETE SET NULL;
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS release_type TEXT DEFAULT 'single';
                ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS song_id INT REFERENCES songs(id) ON DELETE SET NULL;
                ALTER TABLE songs ADD COLUMN IF NOT EXISTS artist TEXT;
                ALTER TABLE songs ADD COLUMN IF NOT EXISTS match_key TEXT;
                ALTER TABLE songs ADD COLUMN IF NOT EXISTS spotify_id TEXT;
                ALTER TABLE songs ADD COLUMN IF NOT EXISTS isrc TEXT;
                ALTER TABLE songs DROP COLUMN IF EXISTS campaign_artist_id;
                ALTER TABLE sounds ADD COLUMN IF NOT EXISTS current_video_count INT;
                ALTER TABLE sounds ADD COLUMN IF NOT EXISTS last_ingested_at TIMESTAMP;
            """)
        conn.commit()


def run_data_migrations():
    """One-off data transformations that move/transform existing rows when
    the schema's *meaning* changes, not just its shape. Each migration here
    should be idempotent (safe to run repeatedly) and only act on rows that
    haven't already been migrated."""
    with db() as conn:
        # PHASE 1 MIGRATION: promote songs from "owned by a campaign" to "independent,
        # referenced by a campaign". This only matters if songs.campaign_id still exists
        # from the old schema direction (pre-restructure databases).
        with conn.cursor() as c:
            c.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='songs' AND column_name='campaign_id'
            """)
            old_column_exists = c.fetchone() is not None

        if old_column_exists:
            with conn.cursor() as c:
                c.execute("""
                    SELECT s.id as song_id, s.campaign_id, c.name as campaign_name, c.artist
                    FROM songs s
                    JOIN campaigns c ON s.campaign_id = c.id
                    WHERE s.campaign_id IS NOT NULL
                """)
                old_links = [dict(r) for r in c.fetchall()]

            for link in old_links:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE songs SET artist=%s
                        WHERE id=%s AND artist IS NULL
                    """, (link["artist"], link["song_id"]))
                    c.execute("""
                        UPDATE campaigns SET song_id=%s WHERE id=%s AND song_id IS NULL
                    """, (link["song_id"], link["campaign_id"]))
                conn.commit()
                print(f"  ✓ promoted song (id={link['song_id']}) to independent, campaign '{link['campaign_name']}' now references it")

            with conn.cursor() as c:
                c.execute("ALTER TABLE songs DROP COLUMN IF EXISTS campaign_id")
            conn.commit()
            print("  ✓ dropped songs.campaign_id — migration to independent Songs complete")

        # Migrate plain-text campaigns.artist into the new campaign_artists table,
        # creating one artist row per distinct name and linking campaigns to it.
        with conn.cursor() as c:
            c.execute("""
                SELECT DISTINCT artist FROM campaigns
                WHERE artist IS NOT NULL AND artist != '' AND campaign_artist_id IS NULL
            """)
            distinct_artists = [r["artist"] for r in c.fetchall()]

        for artist_name in distinct_artists:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO campaign_artists (name) VALUES (%s)
                    ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name
                    RETURNING id
                """, (artist_name,))
                artist_id = c.fetchone()["id"]
                c.execute("""
                    UPDATE campaigns SET campaign_artist_id=%s
                    WHERE artist=%s AND campaign_artist_id IS NULL
                """, (artist_id, artist_name))
            conn.commit()
            print(f"  ✓ linked campaigns with artist '{artist_name}' to campaign_artists")

        # Backfill match_key for songs created before this column existed.
        # Done in Python (not SQL) since the normalization logic lives in
        # services/song_identity.py and should only exist in one place.
        from services.song_identity import generate_match_key
        with conn.cursor() as c:
            c.execute("SELECT id, name, artist FROM songs WHERE match_key IS NULL")
            songs_needing_keys = [dict(r) for r in c.fetchall()]

        for song in songs_needing_keys:
            key = generate_match_key(song["name"], song["artist"] or "")
            with conn.cursor() as c:
                # If another song already has this key (a real pre-existing duplicate,
                # like the "Earrings" entries from tonight's testing), don't silently
                # collide — append the song's own id to keep the key unique, and log it
                # so it's visible rather than hidden.
                c.execute("SELECT id FROM songs WHERE match_key=%s", (key,))
                collision = c.fetchone()
                if collision:
                    key = f"{key}#{song['id']}"
                    print(f"  ⚠ song id={song['id']} ('{song['name']}') has the same match_key as an existing song — "
                          f"made unique as '{key}'. These look like duplicates worth reviewing manually.")
                c.execute("UPDATE songs SET match_key=%s WHERE id=%s", (key, song["id"]))
            conn.commit()

        if songs_needing_keys:
            print(f"  ✓ backfilled match_key for {len(songs_needing_keys)} existing song(s)")

        # Now that every song has a match_key, enforce uniqueness at the database level.
        with conn.cursor() as c:
            c.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'songs_match_key_unique'
                    ) THEN
                        ALTER TABLE songs ADD CONSTRAINT songs_match_key_unique UNIQUE (match_key);
                    END IF;
                END $$;
            """)
        conn.commit()


def setup():
    create_schema()
    run_schema_migrations()
    run_data_migrations()


# ── Register all feature blueprints ───────────────────────────────────────────
from routes_fan_tracker import fan_tracker_bp
from routes_artists import artists_bp
from routes_songs import songs_bp
from routes_campaigns import campaigns_bp
from routes_sounds import sounds_bp
from routes_dashboard import dashboard_bp
from routes_refresh import refresh_bp

app.register_blueprint(fan_tracker_bp)
app.register_blueprint(artists_bp)
app.register_blueprint(songs_bp)
app.register_blueprint(campaigns_bp)
app.register_blueprint(sounds_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(refresh_bp)


@app.route("/api/debug/hierarchy")
def debug_hierarchy():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM campaign_artists ORDER BY id")
            campaign_artists = [dict(r) for r in c.fetchall()]
            c.execute("SELECT id, name, artist, campaign_artist_id, release_type, song_id FROM campaigns ORDER BY id")
            campaigns_summary = [dict(r) for r in c.fetchall()]
            c.execute("SELECT * FROM songs ORDER BY id")
            songs = [dict(r) for r in c.fetchall()]
            c.execute("SELECT * FROM sounds ORDER BY id")
            sounds = [dict(r) for r in c.fetchall()]
            c.execute("SELECT COUNT(*) as n FROM posts WHERE sound_db_id IS NOT NULL")
            linked_posts = c.fetchone()["n"]
    for a in campaign_artists:
        a["created_at"] = str(a["created_at"])
    for s in songs:
        s["created_at"] = str(s["created_at"])
    for s in sounds:
        s["discovered_at"] = str(s["discovered_at"])
    return jsonify({
        "campaign_artists": campaign_artists,
        "campaigns": campaigns_summary,
        "songs": songs,
        "sounds": sounds,
        "posts_linked_to_sounds": linked_posts
    })



setup()
   