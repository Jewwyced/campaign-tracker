"""
services/song_catalog.py — the business operation layer for Songs.

This is what routes call. It owns "find or create a Song" as a single
operation: check identity (via song_identity.py), create the Song record if
it's new, and trigger ingestion (via ingestion.py) to go collect its data.

Kept separate from song_identity.py on purpose — that module answers "how do
we identify a song?" (pure normalization, no database, no side effects).
This module answers "what happens when someone searches for a song?" (a real
business operation with database writes and external calls). And it's kept
separate from ingestion.py: this module decides WHAT to ingest (which Song,
what title/artist to search with), ingestion.py decides HOW (which TikAPI
calls to make, pagination, parsing). We never hand ingestion a raw search
string — we tell it the song_id and title/artist, and it builds its own
search strategy internally.

The caller never sees a database connection — db is an implementation detail
of this module, imported directly here rather than passed in. A route calling
discover_song() doesn't even know Neon exists.
"""

import ingestion
from db import db
from services.song_identity import generate_match_key


def _find_existing_song(match_key):
    """Step 3: check if a Song with this identity already exists."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM songs WHERE match_key=%s", (match_key,))
            return c.fetchone()


def _build_existing_song_result(song_id):
    """Step 4: shape the response when an existing Song was found — no new
    ingestion runs, we just report what's already there."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM sounds WHERE song_id=%s", (song_id,))
            sound_count = len(c.fetchall())
    return {
        "song_id": song_id,
        "existing": True,
        "sounds_found": sound_count,
        "message": "This song is already being tracked — returning existing data instead of creating a duplicate.",
    }


def _create_song(title, artist, match_key):
    """Step 5: create the new Song record. This is the one spot that will
    grow a 5a/5b when a DSP (Spotify, Apple Music) is added later — query
    the DSP catalog, store its dsp_id + ISRC — without anything else in the
    application needing to know the DSP exists.

    Artist is stored as plain text here on purpose, not linked to
    campaign_artists — Songs are independent of Campaigns and shouldn't know
    that concept exists. Once Spotify integration lands, a real `artists`
    table backed by stable Spotify artist IDs should replace this, with
    both songs and campaigns referencing it independently — not invented now,
    since there's no stable identity to anchor it on yet."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO songs (name, artist, match_key) VALUES (%s,%s,%s) RETURNING id
            """, (title, artist, match_key))
            song_id = c.fetchone()["id"]
        conn.commit()
    return song_id


def _trigger_ingestion(song_id, title, artist):
    """Step 6: trigger ingestion to go collect this song's data. We tell
    ingestion WHAT to ingest (this song_id, with this title/artist to search
    for) — we never tell it HOW. It decides search strategy, TikAPI calls,
    pagination, sound creation, post pulling. Nothing here needs to change
    when a second TikTok collection method, or a second platform entirely,
    is added — discover_song_sounds's job is just "go ingest," not "how."""
    results = ingestion.discover_song_sounds(db, song_id, title, artist)
    return [{"sound_id": r["sound_id"], "title": r["title"], "author": r["author"]} for r in results]


def discover_song(title, artist=""):
    """
    The one entry point every new Song in the platform should go through.

    1. Normalize title + artist
    2. Build match_key
    3. Check existing Song
    4. Return existing Song if found
    5. Create new Song (future: 5a query DSP catalog, 5b store dsp_id/ISRC)
    6. Trigger ingestion
    7. Return completed Song
    """
    # 1. Normalize
    title = (title or "").strip()
    artist = (artist or "").strip()

    # 2. Build match_key
    match_key = generate_match_key(title, artist)

    # 3. Check existing Song
    existing = _find_existing_song(match_key)

    # 4. Return existing Song if found
    if existing:
        return _build_existing_song_result(existing["id"])

    # 5. Create new Song
    song_id = _create_song(title, artist, match_key)

    # 6. Trigger ingestion
    sounds_found = _trigger_ingestion(song_id, title, artist)

    # 7. Return completed Song
    return {
        "song_id": song_id,
        "existing": False,
        "sounds_found": len(sounds_found),
        "sounds": sounds_found,
    }