"""
services/song_identity.py — owns one concept and one concept only: what a Song is.

A Song is the permanent, platform-independent entity at the center of the
system. It is never defined by a TikTok sound ID, Spotify ID, or any other
platform-specific identifier — those are metadata attached to the Song over
time as ingestion discovers them (songs.spotify_id and songs.isrc already
exist as columns, reserved for when that integration happens, even though
nothing populates them yet).

For v1, the system generates a deterministic `match_key` from a normalized
version of the song title and primary artist. This key exists solely to
recognize duplicate Song records before richer identifiers like Spotify IDs
or ISRCs are available — it is an internal implementation detail, not the
canonical identity of the musical work itself.

The matching logic is intentionally conservative and deterministic, not
fuzzy. Common variations that don't change the underlying song — "(Official
Audio)", "(Lyrics)", punctuation, capitalization, extra whitespace, "feat./
ft./featuring" suffixes — normalize to the same match_key. Aggressive fuzzy
matching is deliberately avoided: incorrectly merging two different songs
would silently corrupt data, whereas missing a duplicate just leaves an
extra Song record that can be cleaned up later. This prioritizes data
integrity over completeness, and gives a stable foundation that can evolve
naturally as Spotify, ISRCs, and other sources get integrated.
"""

import re

# Suffixes commonly appended to song titles that don't change the underlying
# song — stripped before matching so "Earrings (Official Audio)" and
# "Earrings" resolve to the same key.
_STRIP_PATTERNS = [
    r"\(official audio\)",
    r"\(official video\)",
    r"\(official music video\)",
    r"\(visualizer\)",
    r"\(lyrics?\)",
    r"\[lyrics?\]",
    r"\(audio\)",
    r"\(video\)",
    r"\(lyric video\)",
]

# "feat. X", "ft. X", "featuring X" and everything after it gets stripped —
# these describe a guest artist on the same underlying song, not a different song.
_FEATURING_PATTERN = r"\b(feat\.?|ft\.?|featuring)\b.*$"


def _clean_text(text):
    """Lowercase, strip known noise patterns, remove punctuation, collapse whitespace."""
    text = (text or "").lower().strip()
    for pattern in _STRIP_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    text = re.sub(_FEATURING_PATTERN, "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^\w\s]", "", text)  # remove punctuation
    text = re.sub(r"\s+", " ", text)     # collapse multiple spaces
    return text.strip()


def generate_match_key(title, artist=""):
    """Builds the deterministic key used to detect duplicate Song searches.
    Same (title, artist) pair, regardless of casing/punctuation/common
    suffixes, always produces the same key."""
    clean_title = _clean_text(title)
    clean_artist = _clean_text(artist)
    return f"{clean_title}|{clean_artist}"