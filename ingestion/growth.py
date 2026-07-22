"""
ingestion/growth.py — the Growth boundary.

MOVED (provider-boundary refactor, growth-boundary step) from
ingestion/service.py — pure relocation, no behavior changes. Both
functions here are byte-for-byte identical to their previous versions.

Moved slightly out of the originally planned order: ingest_sound (in
ingestion.py) calls _update_sound_velocity directly, and left in
service.py, that created exactly the circular-import problem _shared.py
was built to avoid — service.py needs to import FROM ingestion.py (to
re-export ingest_sound etc.), so ingestion.py can't import back from
service.py. Extracting Growth now (rather than after Orchestration, as
originally sequenced) resolves this permanently instead of patching
around it.

Growth's job: derive and store growth/velocity metrics from data
ingestion already collected (song_stats snapshots). Nothing here
fetches from the provider or decides what to ingest.
"""

from datetime import date
from ._shared import _log


def _update_sound_velocity(db_conn_factory, sound_db_id):
    """Calculate 24h/7d growth in a sound's TOTAL video count, using the
    song_stats daily snapshots _update_sound_video_count already writes —
    NOT a count of posts in our own sampled `posts` table.

    IMPORTANT: this replaces an earlier version that counted rows in the
    local `posts` table filtered by created_at. That measured something
    fundamentally different and much smaller — we only ever ingest a
    capped sample of a sound's posts, so "posts created in the last 7
    days" among that tiny sample had no real relationship to the sound's
    actual growth across every video using it on TikTok. A sound with
    280K total videos showing "23 this week" was measuring our own
    sample size, not the sound's real momentum. song_stats already
    tracks the sound's true total video count once per calendar day;
    this just diffs those snapshots instead.
    """
    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT date, video_count FROM song_stats
                WHERE sound_id = %s
                ORDER BY date DESC
                LIMIT 14
            """, (sound_db_id,))
            rows = c.fetchall()

    if not rows:
        return 0

    current = rows[0]["video_count"] or 0

    # Closest snapshot at least 1 calendar day old, for 24h growth.
    growth_24h = 0
    for r in rows[1:]:
        if (today - r["date"]).days >= 1:
            growth_24h = max(current - (r["video_count"] or 0), 0)
            break

    # Closest snapshot at least 7 days old, for 7-day growth. If we don't
    # have 7 days of history yet, fall back to the oldest snapshot on file
    # as a partial-period baseline rather than reporting no growth at all.
    growth_7d = 0
    for r in rows[1:]:
        if (today - r["date"]).days >= 7:
            growth_7d = max(current - (r["video_count"] or 0), 0)
            break
    else:
        oldest = rows[-1]
        if oldest["date"] != rows[0]["date"]:
            growth_7d = max(current - (oldest["video_count"] or 0), 0)

    # velocity = 24h growth as a fraction of total video count — same
    # ratio Chartex displays as "24h % Growth" (e.g. 0.79%, 5.92%).
    velocity = round(growth_24h / current, 4) if current > 0 else 0

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE sounds SET posts_24h=%s, posts_7d=%s, velocity=%s
                WHERE id=%s
            """, (growth_24h, growth_7d, velocity, sound_db_id))
        conn.commit()

    _log(f"sound {sound_db_id} growth: +{growth_24h} videos/24h, +{growth_7d} videos/7d, velocity={velocity}")
    return velocity


def recompute_sound_growth(db_conn_factory, sound_db_id):
    """Public entry point for re-running the 24h/7d growth calc against a
    sound's EXISTING song_stats history — no TikAPI/TikLiveAPI call, no
    quota cost. Useful as a one-time backfill after a fix to the growth
    math itself (like the posts-count -> video-count-diff rewrite this
    accompanies): the daily snapshots were already being written correctly
    all along, they just weren't being read back correctly, so most sounds
    have plenty of real history sitting in song_stats already — this just
    re-derives posts_24h/posts_7d/velocity from what's already there,
    without waiting for each sound's turn in the normal refresh rotation.
    """
    return _update_sound_velocity(db_conn_factory, sound_db_id)