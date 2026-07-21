"""
ingestion/service.py — Layer 3: service / orchestration.

Owns all business logic and database writes. Calls parsers to get clean
data, then decides what to write to Neon and what to return.

Known deferred improvements:
  - song_id parameter on ingest_sound() is now unused — legacy, kept for
    backward compatibility with existing callers
  - Cache freshness only implemented for sounds — roster accounts, fan
    accounts, and single posts still always hit the provider pipeline
  - Result objects are not yet standardized across all functions
"""

import os
from datetime import date, datetime, timezone
from .providers import default_provider as provider
from .tiklive_provider import TikLiveAPIProvider as _TikLiveProvider
_tiklive = _TikLiveProvider()
from . import fingerprint as _fingerprint
from services import ai_service as _ai_service
from .parsers import (
    parse_sounds_from_search,
    parse_sound_info,
    parse_posts_from_music_page,
    parse_account_stats,
    parse_posts_from_user_feed,
    parse_single_post,
)

# Shared low-level helpers — moved to ._shared (discovery-boundary refactor)
# specifically to avoid a circular import: this file needs to import
# discovery functions back (see below), and _shared.py has no dependency
# on either side, so both can depend on it without a cycle. Same
# function bodies as before, just relocated — see _shared.py's docstring.
from ._shared import _log, _normalize_str, _score_sound, _artist_signal

# Discovery boundary — moved to .discovery (discovery-boundary refactor).
# Re-exported here, unchanged, so every existing reference keeps working:
# api.py imports discover_sounds/create_sound/discover_song_sounds
# directly from .service; routes_refresh.py calls
# ingestion_service.discover_via_creator_graph(...); initialize_song and
# find_new_sounds_for_song (still in this file) call discover_song_sounds
# and discover_community_sounds_for_song internally. All of that keeps
# working unchanged via this import.
from .discovery import (
    discover_sounds,
    discover_sounds_from_videos,
    discover_sounds_from_challenge,
    discover_via_creator_graph,
    discover_song_sounds,
    _promote_top_sounds,
    discover_community_sounds_for_song,
    create_sound,
    get_or_create_sound,
    _is_plausible_candidate,
    _adapt_challenge_video,
    _community_engagement_score,
    MAX_DISCOVERY_CANDIDATES,
)

# ── Constants ─────────────────────────────────────────────────────────────────

SOUND_FRESHNESS_HOURS = 6

# ── Coverage Engine tuning parameters ───────────────────────────────────────
# These are tuning hypotheses, not fixed truths — expect to adjust them once
# you've seen real coverage metrics (see determine_coverage_plan's logging)
# across a range of actual songs. Deliberately kept as named constants, not
# buried inline, so future tuning only ever touches this one spot.
COVERAGE_TIER_A_VIDEO_THRESHOLD = 10_000   # video_count above this -> Tier A
COVERAGE_TIER_B_VIDEO_THRESHOLD = 50        # video_count above this -> Tier B, else Tier C
                                             # LOWERED 7/18 from 500: real production data showed
                                             # most approved sounds live in the 1-500 range, and a
                                             # sound with 486 real videos (someone's single biggest
                                             # asset for a song) was defaulting to Tier C (30 posts)
                                             # purely because 486 < 500 — an arbitrary line, not a
                                             # real distinction. Confirmed: that sound only had 42
                                             # posts actually stored despite 486 real videos existing.
                                             # Tier A's 10,000 threshold is left as-is for now —
                                             # no real evidence yet on what a genuinely viral approved
                                             # sound looks like in this dataset to recalibrate it.
COVERAGE_TIER_A_TARGET_POSTS = 1500  # RAISED 7/20 from 300 — now safe because ingestion runs on
COVERAGE_TIER_B_TARGET_POSTS = 400   # RAISED 7/20 from 100 — a separate worker service (see
                                      # HANDOFF), not the main UI. This is the real fix for
                                      # "we approved big sounds but only ended up tracking a
                                      # few hundred of their real posts" — real production
                                      # scale, not a demo-sized sample. Real cost: Tier A now
                                      # means up to ~100 real /music-posts/ pages per sound
                                      # (1500 * 2 fetch_multiplier / 30 per page) — genuinely
                                      # substantial time and API calls per big sound, only
                                      # safe now that it can't freeze the live site.
COVERAGE_TIER_C_TARGET_POSTS = 30          # matches original pre-Coverage-Engine behavior
COVERAGE_TIER_A_FETCH_MULTIPLIER = 2       # paginate to 2x target before ranking/trimming
COVERAGE_TIER_B_FETCH_MULTIPLIER = 2
COVERAGE_TIER_C_FETCH_MULTIPLIER = 1       # no deep pagination for small sounds
COVERAGE_TOP_POST_RATIO = 0.70             # of target_posts, kept by views (the "biggest videos")
COVERAGE_RECENT_POST_RATIO = 0.30          # of target_posts, kept by recency (freshest activity)

SOURCE_CACHE    = "cache"
SOURCE_TIKAPI   = "tikapi"
SOURCE_FALLBACK = "fallback"

# Minimum proportion of an author string that the artist name must make
# up for _artist_signal to count it as a real match, not just a
# coincidental substring. Short/common artist names (e.g. "Yeat") can
# appear inside unrelated fan-account handles ("bells_yeat") that mention
# the artist without being any kind of official confirmation — a bare
# substring check can't tell "PlaqueBoyMax Clips" (a real match, artist
# name is 67% of the string) apart from "bells_yeat" (a fan handle, artist
# name is only 44% of the string). 0.5 cleanly separates every case seen
# so far — tune if new false positives/negatives turn up.

# Max number of pending sounds to actually hit the provider for in a single
# qualify_pending_sounds_for_song() call. A song can easily have 200-400
# pending candidates after discovery (common titles + hashtag/challenge
# crawling produce huge candidate lists). Calling get_sound_info() for
# EVERY pending sound inside one synchronous HTTP request doesn't scale —
# gunicorn's worker timeout will kill the request partway through (and
# crash the worker in the process), leaving the song half-approved,
# half-pending.
#
# Raised from 5 to 20: with each provider call taking up to ~15s worst
# case (5s connect / 10s read timeout, one retry), 20 sequential calls
# stays comfortably under a typical 30s+ gunicorn worker timeout while
# processing 4x as many candidates per click — meaningfully shrinking how
# many times "Find New Sounds" / requalify has to be clicked to clear a
# backlog, without touching the actual matching bar at all.
QUALIFY_BATCH_SIZE = 20

# Max plausible candidates to persist per discovery run. Even after the
# no-API-call plausibility filter (_could_possibly_qualify), a common
# title can still leave more "technically eligible" generic uploads than
# are worth tracking — but the previous cap of 30 was tuned for "avoid
# database bloat," not "give a reviewer a real pool to work through."
#
# Raised from 30 to 75, then 75 to 300 (7/20) — this second raise pairs
# with search_sounds() now digging much deeper per query (loosened
# low-yield tolerance, more pages). Finding more raw candidates but still
# capping storage at 75 would have thrown most of that new depth away —
# this matches the actual ambition (hundreds of real candidates per scan,
# not dozens). Still a real cap, not "store everything" — a song with
# more than 300 technically-plausible hits still gets bounded to the
# top-scored 300, ranked by the same scoring function as always.

# REMOVED 7/18 (see HANDOFF_state_machine_migration.md, "Discovery
# Roadmap: Stage 3"): EARLY_STOP_CANDIDATE_THRESHOLD used to stop calling
# further search sources once "enough" plausible candidates were found.
# That was the wrong question once fingerprinting became the validator —
# discovery's job isn't to decide it found enough, it's to exhaust every
# source and let fingerprinting sort out what's real. Every source in
# discover_song_sounds now always runs, unconditionally, no threshold.




# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_sound_fresh(db_conn_factory, sound_db_id):
    """Check whether a sound's cached data is fresh enough to skip re-ingestion."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("SELECT last_ingested_at FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()

    if not row or not row["last_ingested_at"]:
        return False, None

    last_ingested = row["last_ingested_at"]
    if last_ingested.tzinfo is None:
        last_ingested = last_ingested.replace(tzinfo=timezone.utc)

    age_hours = (datetime.now(timezone.utc) - last_ingested).total_seconds() / 3600
    return age_hours < SOUND_FRESHNESS_HOURS, age_hours


def _touch_sound_ingested(db_conn_factory, sound_db_id):
    """Update last_ingested_at to now. Called after every successful ingest."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE sounds SET last_ingested_at=NOW() WHERE id=%s", (sound_db_id,))
        conn.commit()


import re as _re
import unicodedata as _unicodedata


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


def _could_possibly_qualify(title, author, song_name, song_artist, discovered_via):
    """Cheap, NO-API-CALL pre-check using only the title/author already
    stored from discovery. Returns False only when we're CERTAIN qualify
    would reject this candidate regardless of video_count — used to bulk-
    reject candidates before spending a provider call on them.

    SOURCE-AWARE, matching _is_plausible_candidate's logic exactly. This
    function used to apply one universal strict-ish bar regardless of
    where a candidate came from — which quietly defeated the whole point
    of making discovery source-aware: discovery would happily store 75
    generic "original sound" uploads from a highly-trusted title_artist
    search, only for THIS function to immediately bulk-reject most of
    them for "zero textual relation" before they ever got a video_count
    check or a fingerprint. Two filters, only one had been updated — this
    fixes that inconsistency.

      - title_artist: TikTok's own search relevance already matched our
        exact query. Trust it — don't bulk-reject on text grounds at all;
        let it through to get a real video_count check and, eventually,
        an actual audio fingerprint, which is the real verifier now.
      - title_only / hashtag: light filtering, same single-significant-
        word bar. hashtag promoted here 7/18 based on real production
        evidence (see _is_plausible_candidate's docstring for the full
        finding) — it's proving to be the most productive source on real
        songs, not the riskiest, and the original "low trust" assumption
        was based on the challenge crawl specifically, a different
        mechanism.
      - challenge: keeps the existing stricter OR check (title match
        alone or artist match alone) — this is the source with the
        actual documented failure case (e.g. "Way Down We Go", "La
        Muchachita" pulled into a Griddle search).
    """
    title_norm = _normalize_str(song_name)
    s_title = _normalize_str(title)

    if not title_norm:
        return False

    if discovered_via == "title_artist":
        # Very high trust — mirrors _is_plausible_candidate: don't
        # re-litigate this with a text check, just confirm a real title
        # came back at all.
        return bool(s_title)

    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_author = _normalize_str(author)

    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword_strict = len(sig_words) >= 2 and title_words_matched >= 2
    title_multiword_light = len(sig_words) >= 1 and title_words_matched >= 1
    strong_title_possible = title_exact or title_contains or title_multiword_strict
    light_title_possible = title_exact or title_contains or title_multiword_light

    if discovered_via in ("title_only", "hashtag"):
        # High trust, light filtering — same bar as
        # _is_plausible_candidate's title_only/hashtag branch.
        if light_title_possible:
            return True
        if artist_norm and _artist_signal(author, artist_norm):
            return True
        return False

    if not artist_norm:
        # No artist on file — title match alone would be the deciding
        # factor, and we already know the title now (no API call needed
        # to learn it — video_count doesn't change whether a title matches).
        return strong_title_possible

    # challenge (and any future/unknown source) — low trust. Loosened
    # from a strict AND to an OR previously: either signal alone (title
    # match, or artist match) is enough to warrant the real check;
    # _classify_sound_match downstream still requires both together to
    # actually approve anything, so nothing new can get auto-approved
    # from this — it only means more real candidates reach the pending
    # queue (and now, fingerprinting) for a proper look.
    if _artist_signal(author, artist_norm) or strong_title_possible:
        return True

    return False








def _classify_sound_match(sound_title, sound_author, song_title, song_artist, video_count=0, discovered_via=None):
    """Relevance check used to GATE approval at qualify time.

    Confirmed artist match AND some real title relation -> approve.
    Everything else -> reject.

    Tier 2 (popularity-only approval for generic uploads with no artist
    signal) has been removed TWICE now, both times after confirming real
    false positives:
      1st removal — Griddle's challenge/hashtag crawl pulled in dozens of
      wildly popular, completely unrelated sounds via a dance-trend
      collision.
      2nd removal (this one) — even restricted to the supposedly-trusted
      'title_artist' discovery source, Back Home approved "TikTok
      Advertiser" and a Tyler the Creator fan account purely on video
      count. TikTok's search relevance for a combined title+artist query
      does not reliably require both terms to genuinely co-occur in a
      meaningful way — it can still surface generally popular content
      that only loosely matches. Popularity is not a safe substitute for
      a real artist match, and there's no source restriction that's
      proven reliable enough to justify the risk twice in a row.

    Returns True (approve) or False (reject) — no partial credit.
    """
    title_norm = _normalize_str(song_title)
    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_title = _normalize_str(sound_title)
    s_author = _normalize_str(sound_author)

    if not title_norm:
        return False  # no real song title to compare against — never approve blind

    # --- Title strength ---
    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword = len(sig_words) >= 2 and title_words_matched >= 2

    # A derivative marker downgrades a title match — a "remix"/"sped up"
    # version is NOT proof this is the original campaign sound.
    derivative_markers = [
        "sped up", "slowed", "remix", "instrumental", "reverb", "cover",
        "nightcore", "bass boosted", "8d", "phonk", "edit audio", "mashup",
        "loop", "extended", "sped-up", "slow reverb", "lyrics"
    ]
    is_derivative = any(m in s_title for m in derivative_markers)

    strong_title = (title_exact or title_contains) and not is_derivative

    if not artist_norm:
        # No artist on file to check against at all — fall back to title alone.
        return strong_title

    # --- Confirmed artist match AND some real title relation -> approve ──
    # Artist match ALONE is not enough — an artist can have many songs
    # (e.g. PlaqueBoyMax has "Thong Song" AND "Pink Dreads" AND others).
    if _artist_signal(sound_author, artist_norm) and (strong_title or title_multiword):
        return True

    # --- Everything else -> reject ─────────────────────────────────────────
    return False


def _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id):
    """Pull a sound's true total video count and write today's snapshot."""
    raw = provider.get_sound_info(tiktok_sound_id)
    info = parse_sound_info(raw)
    if not info:
        return {"video_count_updated": False, "error": "music/info call failed"}

    video_count = info.get("video_count")
    if video_count is None:
        return {"video_count_updated": False, "error": "music/info succeeded but had no videoCount field"}

    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO song_stats (sound_id, date, video_count)
                VALUES (%s,%s,%s)
                ON CONFLICT (sound_id, date) DO UPDATE SET video_count=EXCLUDED.video_count
            """, (sound_db_id, today, video_count))
            c.execute("UPDATE sounds SET current_video_count=%s WHERE id=%s", (video_count, sound_db_id))
        conn.commit()
    return {"video_count_updated": True, "video_count": video_count, "error": None}


def determine_coverage_plan(video_count):
    """The Coverage Engine's planning layer — Phase 1 of it (see
    HANDOFF_state_machine_migration.md; the full engine eventually also
    owns refresh cadence, API budget allocation across sounds, and its
    own coverage metrics — this function owns tier + pagination + post
    selection strategy only, for now).

    Decides HOW MUCH of a sound's real activity to try to capture, based
    on how big it actually is, instead of the flat 30-post cap every
    sound used to get regardless of size. Returns a plan dict — the
    execution layer (_ingest_sound_posts) reads this plan and executes
    it; it doesn't know what a "tier" is at all, so tuning coverage
    behavior only ever means touching this one function (and the
    constants above it), never the fetch/write logic itself.

    Confirmed via real production data (7/18): a sound with hundreds or
    thousands of real posts was only ever tracking ~15-30 of them,
    massively undercounting real reach — and confirmed via TikLive's own
    docs that /music-posts/ has no sort-by-views parameter, so finding
    the biggest videos requires paginating deeper into the (apparently
    newest-first) stream and sorting client-side ourselves, not just
    requesting them directly. Two other TikTok data providers (SociaVault
    among them) have the identical constraint on the equivalent endpoint
    — this is a platform-level limitation, not something to work around
    by switching vendors.
    """
    if video_count is None or video_count <= COVERAGE_TIER_B_VIDEO_THRESHOLD:
        tier = "C"
        target_posts = COVERAGE_TIER_C_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_C_FETCH_MULTIPLIER
    elif video_count <= COVERAGE_TIER_A_VIDEO_THRESHOLD:
        tier = "B"
        target_posts = COVERAGE_TIER_B_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_B_FETCH_MULTIPLIER
    else:
        tier = "A"
        target_posts = COVERAGE_TIER_A_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_A_FETCH_MULTIPLIER

    return {
        "tier": tier,
        "video_count": video_count,
        "target_posts": target_posts,
        "fetch_target": target_posts * fetch_multiplier,
        "max_pages": (target_posts * fetch_multiplier + 29) // 30,  # count=30 per page
        "top_ratio": COVERAGE_TOP_POST_RATIO,
        "recent_ratio": COVERAGE_RECENT_POST_RATIO,
    }


def _ingest_sound_posts(db_conn_factory, sound_db_id, tiktok_sound_id, coverage_plan):
    """Pull posts for a sound by EXECUTING a coverage plan (see
    determine_coverage_plan) — this function deliberately knows nothing
    about tiers, thresholds, or ratios. It just reads target_posts /
    fetch_target / top_ratio / recent_ratio off the plan it's handed.
    Tuning coverage behavior should never require touching this
    function — only determine_coverage_plan and the constants above it.

    TikLive returns newest first, so plain pagination alone only ever
    gets recent activity. When fetch_target > target_posts, paginates
    deeper, then keeps a blend: the top `top_ratio` by actual views (the
    "biggest videos" the API can't sort for us server-side) plus the
    most recent `recent_ratio` not already included, so a sound doesn't
    lose fresh activity in favor of only ever showing its all-time hits.
    """
    target_posts = coverage_plan["target_posts"]
    fetch_target = coverage_plan["fetch_target"]

    all_posts = []
    cursor = 0
    pages_fetched = 0
    while len(all_posts) < fetch_target:
        raw = provider.get_sound_posts_page(tiktok_sound_id, cursor=cursor, count=30)
        pages_fetched += 1
        posts, has_more, next_cursor = parse_posts_from_music_page(raw)
        if not posts:
            break
        all_posts.extend(posts)
        if not has_more:
            break
        cursor = int(next_cursor) if next_cursor is not None else cursor + 35

    unique_posts = {p.get("post_id"): p for p in all_posts if p.get("post_id")}
    all_posts_unique = list(unique_posts.values())

    if fetch_target > target_posts and len(all_posts_unique) > target_posts:
        top_n = int(target_posts * coverage_plan["top_ratio"])
        recent_n = target_posts - top_n
        by_views = sorted(all_posts_unique, key=lambda p: p.get("views", 0) or 0, reverse=True)
        top_posts = by_views[:top_n]
        top_ids = {p.get("post_id") for p in top_posts}
        recent_posts = [p for p in all_posts_unique if p.get("post_id") not in top_ids][:recent_n]
        posts_to_write = top_posts + recent_posts
    else:
        posts_to_write = all_posts_unique[:target_posts]

    # ── Coverage instrumentation — so tuning is based on real evidence, ──
    # not guessing whether deeper pagination actually found anything.
    views_list = [p.get("views", 0) or 0 for p in all_posts_unique]
    top_view = max(views_list) if views_list else 0
    median_view = sorted(views_list)[len(views_list) // 2] if views_list else 0
    _log(
        f"coverage[sound_db_id={sound_db_id}] tier={coverage_plan['tier']} "
        f"video_count={coverage_plan['video_count']} pages_fetched={pages_fetched} "
        f"posts_downloaded={len(all_posts)} unique_posts={len(all_posts_unique)} "
        f"top_view={top_view:,} median_view={median_view:,} "
        f"stored={len(posts_to_write)}/{target_posts}"
    )

    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in posts_to_write:
                if not p.get("post_id") or not p.get("username"):
                    continue
                c.execute("""
                    INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, source, thumbnail, shares, sound_db_id, followers_at_post)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'sound_auto',%s,%s,%s,%s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                        thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                        shares=EXCLUDED.shares,
                        sound_db_id=COALESCE(EXCLUDED.sound_db_id, posts.sound_db_id),
                        followers_at_post=COALESCE(EXCLUDED.followers_at_post, posts.followers_at_post)
                """, (
                    p["post_id"], today, p["username"],
                    p.get("description", "")[:300],
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("saves", 0),
                    p.get("created_at"), p.get("thumbnail"), p.get("shares", 0),
                    sound_db_id, p.get("followers")
                ))
                # Daily snapshot — one row per post per day, upserted on every
                # refresh so same-day re-ingests just update today's numbers
                # rather than duplicating. This is what makes true
                # tier-crossing detection possible (comparing today's likes
                # to yesterday's), instead of only being able to say "this
                # post is new AND already high" without knowing whether it
                # crossed a threshold today specifically.
                c.execute("""
                    INSERT INTO post_snapshots (post_id, date, views, likes, comments, shares)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (post_id, date) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, shares=EXCLUDED.shares
                """, (
                    p["post_id"], today,
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("shares", 0)
                ))
                # Milestone events — a PERMANENT record of the first time
                # this post was ever observed at each engagement tier.
                # ON CONFLICT (post_id, tier) DO NOTHING means each row
                # can only ever be created once, ever — critical, because
                # the dashboard digest used to instead diff today's
                # snapshot against yesterday's, and treat a MISSING
                # yesterday snapshot as "just crossed." With hundreds of
                # approved sounds and only 25 refreshed per hour, many
                # posts go more than a day between refreshes, so
                # "yesterday's snapshot" was very often just absent — not
                # because the post was new, but because it wasn't touched
                # that specific day. That made long-established, months-
                # old viral posts perpetually resurface as if they'd
                # "just crossed" every single day. This table fixes that
                # structurally: a crossing is recorded exactly once, the
                # first time it's ever seen, and never again after.
                likes = p.get("likes", 0) or 0
                for tier in (1000, 5000, 10000):
                    if likes >= tier:
                        c.execute("""
                            INSERT INTO milestone_events (post_id, tier, crossed_date, views, likes)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT (post_id, tier) DO NOTHING
                        """, (p["post_id"], tier, today, p.get("views", 0), likes))
                added += 1
        conn.commit()
    return added


# ── Public service functions ──────────────────────────────────────────────────

def get_sound_info(tiktok_sound_id):
    """Fetch metadata for a single sound. Returns normalized dict or None."""
    raw = provider.get_sound_info(tiktok_sound_id)
    info = parse_sound_info(raw)
    return info


def ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id, max_results=30, force=False):
    """Refresh one Sound's posts and video-count snapshot.
    Checks cache first — skips the provider pipeline if data is fresh.
    Pass force=True to bypass the freshness cache entirely (e.g. for
    manual testing right after a Coverage Engine tuning change, or an
    urgent refresh that can't wait out SOUND_FRESHNESS_HOURS).

    COVERAGE ENGINE, Phase 1 (see HANDOFF_state_machine_migration.md —
    this is one piece of the eventual full engine, which will also own
    refresh cadence and cross-sound API budget allocation; not "done"
    yet, just tiered pagination + post selection for now): max_results
    is now only a fallback for when video_count can't be determined —
    the real target comes from determine_coverage_plan, called right
    after we fetch this sound's fresh video_count below. A sound with
    280,000 videos gets a meaningfully deeper, smarter pull than a
    remix with 19 — no longer a flat number for every sound regardless
    of scale.
    """
    if not force:
        is_fresh, age_hours = _is_sound_fresh(db_conn_factory, sound_db_id)
        if is_fresh:
            _log(f"sound {sound_db_id} is fresh ({age_hours:.1f}h old) — skipping provider pipeline")
            return {
                "sound_db_id": sound_db_id,
                "video_count_updated": False,
                "posts_added": 0,
                "error": None,
                "source": SOURCE_CACHE,
                "degraded": False,
            }

    # Cache miss or stale — continue through the provider pipeline
    result = {
        "sound_db_id": sound_db_id,
        "video_count_updated": False,
        "posts_added": 0,
        "error": None,
        "source": SOURCE_TIKAPI,
        "degraded": False,
    }
    stats_result = _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id)
    result.update(stats_result)

    coverage_plan = determine_coverage_plan(stats_result.get("video_count"))
    result["coverage_tier"] = coverage_plan["tier"]
    result["posts_added"] = _ingest_sound_posts(
        db_conn_factory, sound_db_id, tiktok_sound_id, coverage_plan
    )

    if not result.get("error"):
        _touch_sound_ingested(db_conn_factory, sound_db_id)
        _update_sound_velocity(db_conn_factory, sound_db_id)

        # If sound has no posts and no video count, mark inactive
        video_count = result.get("video_count", 0) or 0
        posts_added = result.get("posts_added", 0) or 0
        if video_count == 0 and posts_added == 0:
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds SET status='inactive'
                        WHERE id=%s AND status='approved'
                    """, (sound_db_id,))
                conn.commit()
            _log(f"sound {sound_db_id} marked inactive (0 posts, 0 video_count)")

    return result


def resurrect_unfingerprinted_rejects(db_conn_factory, song_id=None):
    """One-time (or occasional) retroactive audit — see
    HANDOFF_state_machine_migration.md. Real evidence found 7/19: a
    genuinely large sound ('boldnfl', 6,318 videos, matching Chartex's
    independent count almost exactly) was sitting status='inactive' with
    fingerprint_status='unchecked' — meaning it was rejected by the OLD,
    pre-fingerprint qualify logic (or a bug since fixed) and NEVER
    actually audio-verified. Since get_or_create_sound's conflict
    handling never touches `status` on an existing row, rediscovering
    the same sound_id again does NOT give it a second look — once
    inactive, always inactive, forever, regardless of how much the
    matching/fingerprinting pipeline has improved since.

    This finds every sound in exactly that state (rejected, never
    actually fingerprinted) and resets it back into the real pipeline —
    state='discovered', status='pending' — so process_sound_pipeline
    picks it up and gives it a real, evidence-based decision under
    today's logic instead of leaving it stuck on a pre-fingerprint call
    forever. Confirmed real count for one song alone (Back Home): 131.

    Scope with song_id for a single song, or leave None to resurrect
    across every active-campaign song at once — call this deliberately,
    not automatically, since it's a real batch state change (though a
    safe, additive one: everything resurrected still goes through the
    same real fingerprint check before anyone has to review it).
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            if song_id is not None:
                c.execute("""
                    UPDATE sounds
                    SET status='pending', state='discovered'
                    WHERE song_id=%s AND status='inactive' AND fingerprint_status='unchecked'
                    RETURNING id
                """, (song_id,))
            else:
                c.execute("""
                    UPDATE sounds snd
                    SET status='pending', state='discovered'
                    FROM songs sg
                    JOIN campaign_songs cs ON cs.song_id = sg.id
                    JOIN campaigns camp ON camp.id = cs.campaign_id
                    WHERE snd.song_id = sg.id
                      AND camp.status = 'In Progress'
                      AND snd.status='inactive' AND snd.fingerprint_status='unchecked'
                    RETURNING snd.id
                """)
            resurrected = c.fetchall()
        conn.commit()

    _log(f"resurrect_unfingerprinted_rejects: {len(resurrected)} sounds reset to pending/discovered "
         f"(song_id={song_id or 'ALL active-campaign songs'})")
    return {"resurrected": len(resurrected)}


def refresh_song_sounds(db_conn_factory, song_id):
    """Refresh every Sound already belonging to a Song (used by the hourly cron)."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id, sound_id FROM sounds WHERE song_id=%s AND status='approved'", (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

    posts_added = 0
    ingested = 0
    for s in sounds:
        result = ingest_sound(db_conn_factory, song_id, s["id"], s["sound_id"], max_results=30)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1
    return {"sounds_found": len(sounds), "sounds_ingested": ingested, "posts_added": posts_added}


def ingest_roster_account(db_conn_factory, username):
    """Pull current stats for one Artist roster account and save today's snapshot."""
    raw = provider.get_account(username)
    account = parse_account_stats(raw)
    if not account:
        return False

    today = date.today()
    views_sum = likes_sum = comments_sum = shares_sum = 0
    if account["sec_uid"]:
        raw_posts = provider.get_account_posts(account["sec_uid"], count=10)
        for p in parse_posts_from_user_feed(raw_posts):
            views_sum += p.get("views", 0)
            likes_sum += p.get("likes", 0)
            comments_sum += p.get("comments", 0)
            shares_sum += p.get("shares", 0)

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO roster_stats (username, date, followers, total_likes, video_count, views_24h, likes_24h, comments_24h, shares_24h)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, total_likes=EXCLUDED.total_likes,
                    video_count=EXCLUDED.video_count, views_24h=EXCLUDED.views_24h,
                    likes_24h=EXCLUDED.likes_24h, comments_24h=EXCLUDED.comments_24h,
                    shares_24h=EXCLUDED.shares_24h
            """, (username, today, account["followers"], account["total_likes"],
                  account["video_count"], views_sum, likes_sum, comments_sum, shares_sum))
        conn.commit()
    return True


def ingest_fan_account(db_conn_factory, username):
    """Pull stats for a plain (non-roster) tracked fan account, plus its
    recent posts.

    IMPORTANT: this deliberately does NOT go through provider.get_account
    / provider.get_account_posts (the multi-provider pipeline used
    elsewhere in this file, which tries TikLive first). That pipeline has
    a known gap for this specific use case: TikLiveAPIProvider's
    get_account_posts requires a NUMERIC user ID, while a standard TikTok
    secUid is a long alphanumeric string — so for most accounts, TikLive's
    posts call silently returns nothing, and the pipeline has to fall
    through to TikAPI as a backup. That fallback had never actually been
    verified end-to-end for a real account before this feature shipped.

    Instead, this calls TikAPI directly — the exact same endpoints,
    parameters, and field names as a separate, already-proven prototype
    tool that's been confirmed working in production. Rather than trust
    an abstraction layer for something this important, fan-account
    ingestion uses the exact logic already known to work.
    """
    import requests as _requests

    tikapi_key = os.environ.get("TIKAPI_KEY", "")
    headers = {
        "X-API-KEY": tikapi_key,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    r = _requests.get(
        "https://api.tikapi.io/public/check",
        params={"username": username}, headers=headers, timeout=15
    )
    _log(f"TikAPI {r.status_code} for @{username}")
    if r.status_code != 200:
        _log(f"  {r.text[:300]}")
        return False

    data = r.json()
    info = data.get("userInfo", {})
    user = info.get("user", {})
    s = info.get("statsV2", info.get("stats", {}))
    sec_uid = user.get("secUid")

    followers = int(s.get("followerCount", 0))
    total_likes = int(s.get("heartCount", 0))
    video_count = int(s.get("videoCount", 0))
    today = date.today()

    # DIAGNOSTIC: if we got a secUid (proving the response parsed at all)
    # but every stat came back zero, something about this response's
    # actual shape doesn't match what we're reading — log the raw
    # response so the NEXT time this happens we can see exactly what
    # TikAPI actually sent back, instead of guessing at field names.
    if sec_uid and followers == 0 and total_likes == 0 and video_count == 0:
        _log(f"⚠ @{username}: got secUid but all stats are zero. "
             f"info keys: {list(info.keys())}, user keys: {list(user.keys())}, "
             f"stats dict used: {s}, full statsV2: {info.get('statsV2')}, "
             f"full stats: {info.get('stats')}")

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO stats (username, date, followers, likes, videos)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, likes=EXCLUDED.likes, videos=EXCLUDED.videos
            """, (username, today, followers, total_likes, video_count))
        conn.commit()

    if sec_uid:
        try:
            r2 = _requests.get(
                "https://api.tikapi.io/public/posts",
                params={"secUid": sec_uid, "count": 10}, headers=headers, timeout=15
            )
            if r2.status_code == 200:
                items = r2.json().get("itemList") or r2.json().get("items") or []
                with db_conn_factory() as conn:
                    with conn.cursor() as c:
                        for item in items:
                            ps = item.get("stats", {})
                            post_id = item.get("id")
                            item_likes = ps.get("diggCount", 0) or 0
                            item_views = ps.get("playCount", 0) or 0
                            c.execute("""
                                INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (post_id) DO UPDATE SET
                                    views=EXCLUDED.views, likes=EXCLUDED.likes,
                                    comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                                    followers_at_post=EXCLUDED.followers_at_post
                            """, (
                                post_id, today, username,
                                item.get("desc", "")[:300],
                                item_views, item_likes,
                                ps.get("commentCount", 0), ps.get("collectCount", 0),
                                item.get("createTime"), followers
                            ))
                            # Same daily-snapshot + milestone-event writes
                            # sound-driven posts already get — this is what
                            # was missing, and why a fan page going viral
                            # never showed up in the "Crossed a Tier Today"
                            # dashboard digest. Without this, fan pages were
                            # completely invisible to the daily digest no
                            # matter how well they performed.
                            if post_id:
                                c.execute("""
                                    INSERT INTO post_snapshots (post_id, date, views, likes, comments, shares)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (post_id, date) DO UPDATE SET
                                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                                        comments=EXCLUDED.comments, shares=EXCLUDED.shares
                                """, (
                                    post_id, today,
                                    item_views, item_likes,
                                    ps.get("commentCount", 0), ps.get("shareCount", 0)
                                ))
                                for tier in (1000, 5000, 10000):
                                    if item_likes >= tier:
                                        c.execute("""
                                            INSERT INTO milestone_events (post_id, tier, crossed_date, views, likes)
                                            VALUES (%s,%s,%s,%s,%s)
                                            ON CONFLICT (post_id, tier) DO NOTHING
                                        """, (post_id, tier, today, item_views, item_likes))
                    conn.commit()
        except Exception as e:
            _log(f"get_account_posts failed for @{username}: {e} — skipping posts")

    _log(f"✓ ingested fan account @{username} — {followers:,} followers")
    return True


def ingest_single_post(db_conn_factory, post_id, username=None):
    """Pull/update one specific TikTok video by its post ID."""
    raw = provider.get_post(post_id)
    post = parse_single_post(raw, fallback_username=username)
    if not post:
        return False

    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post, thumbnail, shares)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (post_id) DO UPDATE SET
                    views=EXCLUDED.views, likes=EXCLUDED.likes,
                    comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                    thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                    shares=EXCLUDED.shares
            """, (
                post["post_id"], today, post["username"],
                post.get("description", "")[:300],
                post.get("views", 0), post.get("likes", 0),
                post.get("comments", 0), post.get("saves", 0),
                post.get("created_at"), post.get("followers"),
                post.get("thumbnail"), post.get("shares", 0)
            ))
        conn.commit()
    _log(f"✓ ingested post {post_id} by @{post['username']}")
    return True


def ingest_campaign_attached_sound(db_conn_factory, campaign_id, tiktok_sound_id, max_results=30, sound_db_id=None):
    """Legacy path: pull a sound's posts directly into a campaign."""
    all_posts = []
    cursor = 0
    while len(all_posts) < max_results:
        raw = provider.get_sound_posts_page(tiktok_sound_id, cursor=cursor, count=30)
        posts, has_more, next_cursor = parse_posts_from_music_page(raw)
        if not posts:
            break
        all_posts.extend(posts)
        if not has_more:
            break
        cursor = int(next_cursor) if next_cursor is not None else cursor + 30

    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in all_posts[:max_results]:
                if not p.get("post_id") or not p.get("username"):
                    continue
                c.execute("""
                    INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, campaign_id, source, thumbnail, shares, sound_db_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'sound_auto',%s,%s,%s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                        campaign_id=COALESCE(posts.campaign_id, EXCLUDED.campaign_id),
                        thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                        shares=EXCLUDED.shares,
                        sound_db_id=COALESCE(EXCLUDED.sound_db_id, posts.sound_db_id)
                """, (
                    p["post_id"], today, p["username"],
                    p.get("description", "")[:300],
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("saves", 0),
                    p.get("created_at"), campaign_id, p.get("thumbnail"),
                    p.get("shares", 0), sound_db_id
                ))
                added += 1
        conn.commit()
    return added
def qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=True):
    """Check up to QUALIFY_BATCH_SIZE pending sounds for one song — ranked
    by relevance score first — and evaluate each against
    _classify_sound_match. This is the SAME logic as /api/refresh/qualify,
    extracted here so both the cron route and the instant per-song
    pipeline share one implementation instead of drifting apart.

    auto_approve controls what happens to a candidate the classifier
    considers a real match:
      True  (default) — status becomes 'approved' immediately. Used for
             initial song creation and for requalify (re-judging existing
             candidates under corrected logic — a maintenance/correction
             action, not a "discover something brand new" action).
      False — status STAYS 'pending', awaiting a human's explicit
              approval via /api/sounds/<id>/approve. Used by
              find_new_sounds_for_song: a song's canonical set should
              never silently grow just because a later discovery pass
              found something. Clear junk is still auto-rejected to
              'inactive' either way — only genuine candidates require a
              human decision, so the review queue stays small.

    IMPORTANT: this used to process EVERY pending sound for a song in one
    call. A song can have 200-400+ pending candidates after discovery
    (common titles + hashtag/challenge crawling produce huge candidate
    lists), and calling the provider once per sound inside a single
    synchronous HTTP request doesn't scale — gunicorn's worker timeout
    killed the request partway through and crashed the worker.

    Fix: rank pending sounds by _score_sound (using the title/author
    already stored from discovery — no extra provider calls needed just
    to rank) and only make provider calls for the top QUALIFY_BATCH_SIZE
    candidates. Everything past the batch stays 'pending' and gets picked
    up by a future call.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       snd.fingerprint_status, snd.fingerprint_checked_at
                FROM sounds snd
                WHERE snd.song_id = %s AND snd.status = 'pending'
            """, (song_id,))
            pending = [dict(r) for r in c.fetchall()]

            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()

    song_name = song_row["name"] if song_row else ""
    song_artist = song_row["artist"] if song_row else ""

    # ── Pre-filter: bulk-reject candidates that provably can't qualify ──
    # This costs ZERO API calls — it only uses title/author already stored
    # from discovery. Discovery (especially the challenge/hashtag crawl)
    # pulls in a large volume of candidates with zero textual relation at
    # all (complete, real songs by unrelated artists, not just generic
    # uploads), and there's no need to fetch video_count to know those
    # can't possibly qualify. Only genuinely plausible candidates get
    # ranked and spend one of the QUALIFY_BATCH_SIZE provider calls.
    plausible = []
    bulk_rejected_ids = []
    for s in pending:
        if _could_possibly_qualify(s.get("title"), s.get("author"), song_name, song_artist, s.get("discovered_via")):
            plausible.append(s)
        else:
            bulk_rejected_ids.append(s["id"])

    if bulk_rejected_ids:
        with db_conn_factory() as conn:
            with conn.cursor() as c:
                c.execute(
                    "UPDATE sounds SET status='inactive' WHERE id = ANY(%s)",
                    (bulk_rejected_ids,)
                )
            conn.commit()
        _log(f"qualify_pending_sounds_for_song: song {song_id} — bulk-rejected "
             f"{len(bulk_rejected_ids)} candidates with zero textual relation "
             f"(no API calls spent), {len(plausible)} plausible candidates remain")

    def rank_key(s):
        return _score_sound({"title": s.get("title"), "author": s.get("author")}, song_name, song_artist)

    pending_sorted = sorted(plausible, key=rank_key, reverse=True)
    batch = pending_sorted[:QUALIFY_BATCH_SIZE]
    remaining = len(pending_sorted) - len(batch)

    _log(f"qualifying {len(pending_sorted)} plausible pending sounds for song {song_id} "
         f"(batch_size={QUALIFY_BATCH_SIZE}, auto_approve={auto_approve})")
    if remaining > 0:
        _log(f"qualify_pending_sounds_for_song: song {song_id} has {len(pending_sorted)} plausible pending — "
             f"processing top {len(batch)} this call, leaving {remaining} for next cron cycle")

    approved = 0
    awaiting_review = 0
    inactive = 0

    for s in batch:
        try:
            raw = provider.get_sound_info(s["sound_id"])
            if not raw:
                new_status, video_count, title, author = "inactive", 0, "", ""
            else:
                music_info = raw.get("musicInfo", {})
                if music_info:
                    music = music_info.get("music", {})
                    stats = music_info.get("stats", {})
                    video_count = stats.get("videoCount") or 0
                    title = music.get("title") or ""
                    author = music.get("authorName") or ""
                    play_url = music.get("playUrl") or ""
                else:
                    video_count = raw.get("video_count") or 0
                    title = raw.get("title") or ""
                    author = raw.get("author") or ""
                    play_url = raw.get("play") or ""

                if video_count == 0:
                    new_status = "inactive"
                else:
                    # NOTE: audio fingerprinting deliberately does NOT
                    # happen here anymore. It used to run inline, right in
                    # this loop — but that meant up to QUALIFY_BATCH_SIZE
                    # (20) synchronous audio-fetch + ACRCloud calls inside
                    # one web request, reintroducing exactly the timeout
                    # risk QUALIFY_BATCH_SIZE was built to prevent for the
                    # cheaper video_count check. Fingerprinting now happens
                    # exclusively via the async run_fingerprint_backlog
                    # worker (see below in this file), triggered on its
                    # own cron schedule — candidates land here as
                    # 'unchecked' and get picked up shortly after by that
                    # worker instead of blocking this request.
                    is_relevant = _classify_sound_match(
                        title, author, song_name, song_artist, video_count,
                        discovered_via=s.get("discovered_via")
                    )
                    if auto_approve:
                        new_status = "approved" if is_relevant else "inactive"
                    else:
                        # Find New Sounds — three buckets, not two:
                        #   Auto Reject  -> fails _is_plausible_candidate
                        #                   entirely (no title or artist
                        #                   relation at all) -> inactive,
                        #                   regardless of video_count. This
                        #                   is the SAME bar that already
                        #                   keeps discovery clean, reapplied
                        #                   here with fresh, API-verified
                        #                   title/author — also correctly
                        #                   re-rejects any stale pending
                        #                   rows left over from before that
                        #                   filter existed.
                        #   Needs Review -> passes the plausibility bar but
                        #                   doesn't clear qualify's strict
                        #                   Tier 1 bar -> pending, for a
                        #                   human to judge (remixes by
                        #                   unconfirmed uploaders, generic
                        #                   reposts with no artist
                        #                   confirmation, etc).
                        #   Auto Approve -> handled above when auto_approve
                        #                   is True; never reachable here.
                        if is_relevant:
                            new_status = "pending"
                        else:
                            # Sole gate for human review: the same
                            # plausibility bar discovery already uses.
                            # A video-count-based exception for zero-
                            # signal generic uploads was considered and
                            # deliberately NOT added — popularity is not
                            # evidence of identity (a viral "original
                            # sound" can be completely unrelated; a real
                            # repost might only have 2,000 videos). If
                            # measurement across a real sample of songs
                            # shows generic uploads are a meaningful
                            # source of missed matches, that's a real,
                            # data-backed reason to revisit this — not
                            # something to guess a threshold for now.
                            # The actual fix for "TikTok's metadata
                            # doesn't identify this audio" is a different
                            # source of truth entirely (audio
                            # fingerprinting), not more string heuristics.
                            is_plausible = _is_plausible_candidate(
                                title, author, song_name, song_artist, s.get("discovered_via")
                            )
                            new_status = "pending" if is_plausible else "inactive"
                    _log(f"  sound {s['id']} '{title}' by '{author}' video_count={video_count} "
                         f"discovered_via={s.get('discovered_via')} relevant={is_relevant} -> {new_status}")

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET status=%s, current_video_count=%s,
                            title=COALESCE(NULLIF(%s,''), title),
                            author=COALESCE(NULLIF(%s,''), author)
                        WHERE id=%s
                    """, (new_status, video_count, title, author, s["id"]))
                conn.commit()

            if new_status == "approved":
                approved += 1
            elif new_status == "pending":
                awaiting_review += 1
            else:
                inactive += 1
        except Exception as e:
            _log(f"qualify_pending_sounds_for_song: failed on sound {s['id']}: {e}")

    _log(f"qualify_pending_sounds_for_song: song {song_id} — {approved} approved, "
         f"{awaiting_review} awaiting review, {inactive} inactive, "
         f"{len(bulk_rejected_ids)} bulk-rejected (no API call), {remaining} still pending")
    return {
        "approved": approved,
        "awaiting_review": awaiting_review,
        "inactive": inactive,
        "bulk_rejected": len(bulk_rejected_ids),
        "checked": len(batch),
        "remaining_pending": remaining,
    }


def run_fingerprint_backlog(db_conn_factory, batch_size=40, time_budget_seconds=25, song_id=None):
    """The fingerprint worker — drains the backlog of pending sounds that
    haven't been audio-verified yet.

    By default (song_id=None) this pulls from the GLOBAL queue, oldest
    candidate first across every song — that's what the cron uses. This
    caused real confusion once discovery started finding way more
    candidates: clicking "Find New Sounds" on one song, then running this
    worker, would often process a COMPLETELY DIFFERENT, older song's
    leftover backlog instead, since the queue is pure FIFO by sound id,
    not "whichever song you just touched." Passing song_id scopes this
    run to just that one song's pending candidates, so you can actually
    verify what you just discovered on demand instead of guessing whether
    the global queue happened to reach it yet.

    SAFE TO RUN AUTOMATICALLY ON A SCHEDULE (the song_id=None case). Unlike
    the discover/qualify crons that were deliberately removed from
    routes_refresh.py (see that file's docstring), this function never
    touches sounds.status. It only writes to the fingerprint_* columns —
    pure verification data layered on top of candidates a human already
    explicitly created via "Find New Sounds". It cannot silently expand,
    approve, or change the canonical sound set, so it doesn't reintroduce
    the "why did these appear? the cron decided" problem this
    architecture exists to prevent.

    Time-boxed rather than purely count-boxed (unlike QUALIFY_BATCH_SIZE)
    because fingerprint latency is unpredictable — audio fetch + ACRCloud
    round trip per candidate, not a single fast metadata call. Stops at
    whichever limit — batch_size or time_budget_seconds — comes first, to
    stay well under a gunicorn worker timeout.

    NOTE: play_url is not stored anywhere (it's only ever fetched
    transiently, mid-request, during qualify). This worker runs
    independently and must re-fetch it via the provider before it can
    fingerprint anything — one extra provider call per candidate, on top
    of the ACRCloud call itself.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.status = 'pending'
                  AND snd.fingerprint_status = 'unchecked'
                  {song_filter_sql}
                  AND snd.song_id IN (
                      SELECT cs.song_id FROM campaign_songs cs
                      JOIN campaigns camp ON camp.id = cs.campaign_id
                      WHERE camp.status = 'In Progress'
                  )
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            batch = [dict(r) for r in c.fetchall()]

    _log(f"run_fingerprint_backlog: {len(batch)} candidates pulled (batch_size={batch_size})")

    checked = matched = mismatched = inconclusive = errors = 0

    for s in batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"run_fingerprint_backlog: time budget ({time_budget_seconds}s) reached, "
                 f"stopping early after {checked}/{len(batch)}")
            break

        try:
            info = provider.get_sound_info(s["sound_id"])
            parsed = parse_sound_info(info)
            play_url = (parsed or {}).get("play_url")

            fp_result = _fingerprint.fingerprint_sound(play_url, s["song_name"], s["song_artist"])

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET fingerprint_status=%s,
                            fingerprint_recording_id=%s,
                            fingerprint_title=%s,
                            fingerprint_artist=%s,
                            fingerprint_confidence=%s,
                            fingerprint_checked_at=now()
                        WHERE id=%s
                    """, (
                        fp_result.get("status"),
                        fp_result.get("recording_id"),
                        fp_result.get("title"),
                        fp_result.get("artist"),
                        fp_result.get("confidence"),
                        s["id"],
                    ))
                conn.commit()

            checked += 1
            status = fp_result.get("status")
            if status == "matched":
                matched += 1
            elif status == "mismatched":
                mismatched += 1
            elif status == "inconclusive":
                inconclusive += 1
            else:
                errors += 1

        except Exception as e:
            _log(f"run_fingerprint_backlog: failed on sound {s['id']}: {e}")
            errors += 1

    _log(f"run_fingerprint_backlog: {checked} checked — {matched} matched, "
         f"{mismatched} mismatched, {inconclusive} inconclusive, {errors} errors")

    return {
        "pulled": len(batch),
        "checked": checked,
        "matched": matched,
        "mismatched": mismatched,
        "inconclusive": inconclusive,
        "errors": errors,
    }


def _compute_recommendation(fingerprint_status, fingerprint_confidence):
    """Turns the raw fingerprint result into a clean, human-facing
    recommendation — 'approve' / 'reject' / 'review' — instead of making
    a reviewer interpret internal fingerprint_status values themselves.
    This is intentionally simple right now (a direct mapping); it's the
    natural place to fold in additional signals (text match strength,
    video count) later without changing anything that reads
    `recommendation` downstream.
    """
    if fingerprint_status == "matched":
        return "approve"
    if fingerprint_status == "mismatched":
        return "reject"
    return "review"  # inconclusive or error — no confident automatic answer


def run_ai_review_backlog(db_conn_factory, batch_size=15, time_budget_seconds=25, song_id=None):
    """The AI sound-review worker — the 'final stamp' after fingerprinting,
    run specifically on candidates fingerprinting already checked and did
    NOT confirm as a match to the master recording (fingerprint_status
    'mismatched' or 'inconclusive'). That is NOT the same as "definitely
    unrelated" — it just means there's no official recording to match
    against, which is exactly and only true of remixes, reposts, and
    derivative/background-audio use. Those are the candidates a human
    currently has to eyeball one at a time in the pending review queue;
    this looks at the same sample video thumbnails a human would and
    renders the same kind of judgment, with reasoning attached.

    Mirrors run_fingerprint_backlog's shape deliberately: same song_id
    scoping (None = global FIFO queue for the cron; a song_id = "verify
    what I just discovered on this song, on demand"), same time-boxing
    (vision calls + a thumbnail-fetch round trip per candidate have
    unpredictable latency, same reasoning as the fingerprint worker).

    NEVER touches sounds.status — writes only to ai_review_* columns,
    same "verification layer, not a lifecycle transition" boundary the
    fingerprint worker holds. Auto-approve/reject based on this is a
    deliberate FUTURE decision once accuracy is proven out over time —
    today this only informs the human reviewer, it doesn't act on its
    own recommendation.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.status = 'pending'
                  AND snd.fingerprint_status IN ('mismatched', 'inconclusive')
                  AND snd.ai_review_status = 'unchecked'
                  {song_filter_sql}
                  AND snd.song_id IN (
                      SELECT cs.song_id FROM campaign_songs cs
                      JOIN campaigns camp ON camp.id = cs.campaign_id
                      WHERE camp.status = 'In Progress'
                  )
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            batch = [dict(r) for r in c.fetchall()]

    _log(f"run_ai_review_backlog: {len(batch)} candidates pulled (batch_size={batch_size})")

    checked = approved = rejected = needs_human = errors = 0

    for s in batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"run_ai_review_backlog: time budget ({time_budget_seconds}s) reached, "
                 f"stopping early after {checked}/{len(batch)}")
            break

        try:
            raw = provider.get_sound_posts_page(s["sound_id"], cursor=0, count=6)
            posts, _, _ = parse_posts_from_music_page(raw)
            sample_posts = [
                {
                    "username": p.get("username"),
                    "thumbnail": p.get("thumbnail"),
                    "description": p.get("description"),
                    "views": p.get("views", 0),
                }
                for p in posts[:5] if p.get("thumbnail")
            ]

            result = _ai_service.review_sound_candidate(
                s["song_name"], s["song_artist"], s["title"], s["author"],
                s.get("discovered_via"), sample_posts
            )

            if result is None:
                with db_conn_factory() as conn:
                    with conn.cursor() as c:
                        c.execute("""
                            UPDATE sounds SET ai_review_status='error', ai_review_checked_at=now()
                            WHERE id=%s
                        """, (s["id"],))
                    conn.commit()
                errors += 1
                continue

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET ai_review_status='reviewed',
                            ai_review_confidence=%s,
                            ai_review_recommendation=%s,
                            ai_review_reasoning=%s,
                            ai_review_checked_at=now()
                        WHERE id=%s
                    """, (
                        result["confidence"], result["recommendation"],
                        result["reasoning"], s["id"],
                    ))
                conn.commit()

            checked += 1
            if result["recommendation"] == "approve":
                approved += 1
            elif result["recommendation"] == "reject":
                rejected += 1
            else:
                needs_human += 1

        except Exception as e:
            _log(f"run_ai_review_backlog: failed on sound {s['id']}: {e}")
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds SET ai_review_status='error', ai_review_checked_at=now()
                        WHERE id=%s
                    """, (s["id"],))
                conn.commit()
            errors += 1

    _log(f"run_ai_review_backlog: {checked} checked — {approved} approve, "
         f"{rejected} reject, {needs_human} needs_human, {errors} errors")

    return {
        "pulled": len(batch),
        "checked": checked,
        "approved": approved,
        "rejected": rejected,
        "needs_human": needs_human,
        "errors": errors,
    }


def process_sound_pipeline(db_conn_factory, batch_size=40, time_budget_seconds=25, song_id=None):
    """THE state machine worker (state machine migration, step 4 — see
    HANDOFF_state_machine_migration.md). One transition, not two:

        DISCOVERED -> FINGERPRINTING -> AWAITING_REVIEW

    There is no separate VERIFIED state. Fingerprinting IS the
    verification — the moment the audio check comes back, we already have
    everything needed (title, artist, confidence, recommendation) to land
    the sound in the review queue. "Verified" would have been a lifecycle
    stage with nothing left to do in it — those are attributes of the
    sound (fingerprint_status, fingerprint_confidence, recommendation),
    not a distinct place in its journey. Splitting it into two states and
    two query passes was one state too many.

    song_id=None means "process everything, across every active-campaign
    song" — this is the one function every caller uses, the same way,
    with no duplicate logic:
        - manual button on a song page  -> song_id=<that song>
        - the eventual single cron      -> song_id=None
        - a CLI / test script           -> either, same function

    CURRENT SAFETY CAVEAT, not a limitation of the function itself: right
    now every sound gets BOTH status='pending' (old system) AND
    state='discovered' (new system) from the dual-write in create_sound/
    get_or_create_sound. Calling this with song_id=None today would
    re-fingerprint (and re-pay for) candidates the OLD run_fingerprint_
    backlog worker has already checked, since both draw from largely the
    same underlying rows during the migration's transition period. The
    route calling this currently requires song_id for that reason alone
    — once the old worker/qualify endpoints are retired (migration step
    6), that restriction goes away and this runs globally on a cron,
    exactly as designed.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.state = 'discovered'
                  {song_filter_sql}
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            discovered_batch = [dict(r) for r in c.fetchall()]

    _log(f"process_sound_pipeline: {len(discovered_batch)} DISCOVERED candidates pulled")

    fingerprinted = approved_rec = rejected_rec = review_rec = bulk_rejected = errors = 0

    for s in discovered_batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"process_sound_pipeline: time budget reached, "
                 f"stopping after {fingerprinted}/{len(discovered_batch)}")
            break

        # Cheap, FREE pre-filter before spending a real ACRCloud call —
        # same _could_possibly_qualify check the old qualify step used to
        # bulk-reject obvious zero-textual-relation garbage at no cost.
        # Without this, every discovered candidate (including the ones
        # discovery deliberately casts a wide net to include) would get a
        # paid fingerprint check, even ones we're already certain would
        # fail. This preserves that cost-saving without reintroducing it
        # as a separate "qualify" concept the caller has to know about.
        if not _could_possibly_qualify(s["title"], s["author"], s["song_name"], s["song_artist"], s["discovered_via"]):
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET state='rejected', recommendation='reject',
                            fingerprint_status='not_checked_bulk_rejected'
                        WHERE id=%s
                    """, (s["id"],))
                conn.commit()
            bulk_rejected += 1
            continue

        try:
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE sounds SET state='fingerprinting' WHERE id=%s", (s["id"],))
                conn.commit()

            info = provider.get_sound_info(s["sound_id"])
            parsed = parse_sound_info(info)
            play_url = (parsed or {}).get("play_url")
            # FIXED 7/19: video_count was already sitting in `parsed`
            # (parse_sound_info returns it) but never actually stored —
            # meaning every candidate showed "0 videos" in the review UI
            # regardless of its real size, since current_video_count was
            # never populated for anything that hadn't already been
            # through the OLD ingest_sound path (only approved sounds go
            # through that). This was hiding real size information from
            # reviewers on every pending candidate.
            video_count = (parsed or {}).get("video_count")

            fp_result = _fingerprint.fingerprint_sound(play_url, s["song_name"], s["song_artist"])
            recommendation = _compute_recommendation(fp_result.get("status"), fp_result.get("confidence"))

            # One write, straight to the final pre-decision state — no
            # intermediate VERIFIED row, no second query pass to find it
            # again a moment later.
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET fingerprint_status=%s,
                            fingerprint_recording_id=%s,
                            fingerprint_title=%s,
                            fingerprint_artist=%s,
                            fingerprint_confidence=%s,
                            fingerprint_checked_at=now(),
                            recommendation=%s,
                            current_video_count=COALESCE(%s, current_video_count),
                            state='awaiting_review'
                        WHERE id=%s
                    """, (
                        fp_result.get("status"),
                        fp_result.get("recording_id"),
                        fp_result.get("title"),
                        fp_result.get("artist"),
                        fp_result.get("confidence"),
                        recommendation,
                        video_count,
                        s["id"],
                    ))
                conn.commit()

            fingerprinted += 1
            if recommendation == "approve":
                approved_rec += 1
            elif recommendation == "reject":
                rejected_rec += 1
            else:
                review_rec += 1

        except Exception as e:
            _log(f"process_sound_pipeline: failed on sound {s['id']}: {e}")
            errors += 1

    _log(f"process_sound_pipeline: {bulk_rejected} bulk-rejected for free (no API call), "
         f"{fingerprinted} fingerprinted and moved to awaiting_review "
         f"({approved_rec} recommend approve, {rejected_rec} recommend reject, "
         f"{review_rec} recommend review, {errors} errors)")

    return {
        "discovered_pulled": len(discovered_batch),
        "bulk_rejected": bulk_rejected,
        "fingerprinted": fingerprinted,
        "recommend_approve": approved_rec,
        "recommend_reject": rejected_rec,
        "recommend_review": review_rec,
        "errors": errors,
    }


def run_nightly_discovery(db_conn_factory):
    """Discovery cron — runs once per night, loops every song attached to
    an active campaign, and runs discover -> process_sound_pipeline for
    each, landing results directly in state='awaiting_review' with a real
    recommendation attached, exactly as if a human had clicked "Find New
    Sounds" themselves.

    THE ONE PIPELINE, LOCKED IN: this now calls the same
    process_sound_pipeline used by find_new_sounds_v2 — there is no
    longer a separate old (text-only qualify) and new (fingerprint)
    system running in parallel. Nothing auto-approves: process_sound_
    pipeline only ever lands sounds in awaiting_review with a
    recommendation attached; a human still makes every final call. This
    directly matches the design already documented in routes_refresh.py,
    which explains why the OLD automatic discovery cron was removed (it
    called an un-capped legacy discovery function AND defaulted
    auto-approve to on) — this function reuses today's capped,
    plausibility-filtered discover_song_sounds + the single fingerprint-
    and-recommend pipeline, just triggered on a timer instead of a click.

    Intended schedule: once nightly (e.g. 3am), NOT hourly — re-running
    full discovery on the same songs repeatedly finds little new each
    time; once a day is enough to have a fresh, already-verified queue by
    morning, without paying the discovery cost on a tighter loop for no
    benefit.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT DISTINCT sg.id, sg.name, sg.artist
                FROM songs sg
                JOIN campaign_songs cs ON cs.song_id = sg.id
                JOIN campaigns camp ON camp.id = cs.campaign_id
                WHERE camp.status = 'In Progress'
            """)
            songs = [dict(r) for r in c.fetchall()]

    _log(f"run_nightly_discovery: {len(songs)} active songs")

    results = []
    for song in songs:
        try:
            discover_result = discover_song_sounds(
                db_conn_factory, song["id"], song["name"], song["artist"] or ""
            )
            pipeline_result = process_sound_pipeline(
                db_conn_factory, song_id=song["id"]
            )
            results.append({
                "song_id": song["id"],
                "song_name": song["name"],
                "discovered": discover_result,
                "pipeline": pipeline_result,
            })
        except Exception as e:
            _log(f"run_nightly_discovery: failed on song {song['id']} ('{song['name']}'): {e}")
            results.append({"song_id": song["id"], "song_name": song["name"], "error": str(e)})

    total_fingerprinted = sum(r.get("pipeline", {}).get("fingerprinted", 0) for r in results)
    _log(f"run_nightly_discovery: complete — {total_fingerprinted} sounds fingerprinted and moved "
         f"to awaiting_review across {len(songs)} songs")

    return {
        "songs_processed": len(songs),
        "total_fingerprinted": total_fingerprinted,
        "per_song": results,
    }


def ingest_approved_sounds_for_song(db_conn_factory, song_id, max_results=30):
    """Fetch posts/videos for every approved sound belonging to one song, right now.
    Unlike the global monitor cron (which only takes the top 25 stale sounds across
    ALL campaigns), this targets exactly the sounds that matter for a just-added
    song, so a demo doesn't have to wait for the next cron cycle."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id as sound_db_id, sound_id as tiktok_sound_id
                FROM sounds
                WHERE song_id = %s AND status = 'approved'
            """, (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

    posts_added = 0
    ingested = 0
    for s in sounds:
        result = ingest_sound(db_conn_factory, song_id, s["sound_db_id"], s["tiktok_sound_id"], max_results=max_results)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1

    _log(f"ingest_approved_sounds_for_song: song {song_id} — {ingested}/{len(sounds)} sounds ingested, {posts_added} posts added")
    return {"sounds_found": len(sounds), "sounds_ingested": ingested, "posts_added": posts_added}


def initialize_song(db_conn_factory, song_id, name, artist=""):
    """Runs ONCE per song, at creation time: discover -> qualify (auto-
    approving high-confidence matches) -> ingest. This is what establishes
    a song's initial canonical sound set and makes 'add song, it just
    appears' true.

    Auto-approval is appropriate here specifically because a brand new
    song has zero canonical sounds yet — it needs SOME starting set to be
    useful at all. Contrast with find_new_sounds_for_song, which expands
    an EXISTING canonical set and deliberately does NOT auto-approve (see
    that function for why).

    IMPORTANT: this must NEVER be called by a routine refresh action.
    Discovery is expensive (dozens of search API calls); refresh should
    only touch a song's already-approved (canonical) sounds — see
    refresh_approved_sounds_for_song.
    """
    discovered = discover_song_sounds(db_conn_factory, song_id, name, artist or "")

    # Second discovery sensor — see discover_community_sounds_for_song's
    # module notes for why this exists and what it was validated against.
    # Deliberately best-effort: a Community Discovery failure (rate limit,
    # provider outage, etc.) should never block song creation or title
    # search's results — this is additive, not load-bearing.
    try:
        community_discovered = discover_community_sounds_for_song(db_conn_factory, song_id, name, artist or "")
    except Exception as e:
        _log(f"initialize_song: community discovery failed, continuing without it: {e}")
        community_discovered = []

    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=True)
    ingest_result = ingest_approved_sounds_for_song(db_conn_factory, song_id)

    return {
        "sounds_discovered": len(discovered),
        "community_sounds_discovered": len(community_discovered),
        "qualify": qualify_result,
        "ingest": ingest_result,
    }


def refresh_approved_sounds_for_song(db_conn_factory, song_id, batch_size=15):
    """Routine refresh — operates EXCLUSIVELY on a song's already-approved
    (canonical) sounds. Updates posts, view/like counts, and creator data.
    Never discovers new candidates, never re-runs qualify. This is the
    ONLY thing a 'Refresh' button should ever do.

    Capped to the `batch_size` stalest approved sounds per call (oldest
    last_ingested_at first) — a song can have many approved sounds, and
    processing all of them synchronously risks the same worker-timeout
    crashes discovery caused. Repeated refresh calls naturally rotate
    through every approved sound over time. ingest_sound's own freshness
    cache means sounds refreshed recently are skipped cheaply (a DB read,
    no network call) rather than needlessly re-fetched.

    COVERAGE ENGINE NOTE: batch_size (a sound COUNT) is no longer a
    sufficient safety bound on its own — since ingest_sound now paginates
    much deeper for Tier A/B sounds (up to ~20 pages vs. 1 before), 15
    Tier A sounds in one batch could take far longer than 15 small ones
    used to. Added an explicit TIME_BUDGET_SECONDS below, same pattern as
    the fingerprint worker, so this stops safely partway through a batch
    rather than risking a timeout — remaining sounds just get picked up
    on the next refresh call, same as they already do today when
    batch_size itself doesn't cover everything.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id, sound_id
                FROM sounds
                WHERE song_id=%s AND status='approved'
                ORDER BY last_ingested_at ASC NULLS FIRST
                LIMIT %s
            """, (song_id, batch_size))
            sounds = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT COUNT(*) as total FROM sounds
                WHERE song_id=%s AND status='approved'
            """, (song_id,))
            total_approved = c.fetchone()["total"]

    posts_added = 0
    ingested = 0
    import time as _time
    start = _time.monotonic()
    TIME_BUDGET_SECONDS = 250  # RAISED 7/20 from 40 — safe now that this only ever runs on the
                                # separate worker service (see HANDOFF), never the main UI. Tier A
                                # sounds now genuinely take much longer to fetch their real 1500-post
                                # target (~100 pages) — 40s was nowhere near enough for even one big
                                # sound. Kept under the 300s gunicorn --timeout with real margin.
    for s in sounds:
        if _time.monotonic() - start > TIME_BUDGET_SECONDS:
            _log(f"refresh_approved_sounds_for_song: time budget reached after {ingested}/{len(sounds)}")
            break
        result = ingest_sound(db_conn_factory, song_id, s["id"], s["sound_id"], max_results=35)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1

    remaining = max(total_approved - len(sounds), 0)

    _log(f"refresh_approved_sounds_for_song: song {song_id} — {ingested}/{len(sounds)} sounds refreshed, "
         f"{posts_added} posts added, {remaining} still stale (of {total_approved} total approved)")

    return {
        "sounds_refreshed": len(sounds),
        "sounds_ingested": ingested,
        "posts_added": posts_added,
        "total_approved_sounds": total_approved,
        "remaining_stale_sounds": remaining,
    }


def find_new_sounds_for_song(db_conn_factory, song_id, name, artist=""):
    """The explicit 'Find New Sounds' action — deliberately, separately
    triggered by the user, never automatic. Runs discovery again (won't
    duplicate existing sound rows) and evaluates any newly-found pending
    candidates against the classifier, but does NOT auto-approve them —
    a song's canonical (approved) sound set should not silently grow or
    change just because a later discovery pass turned something up.

    Clear junk still gets auto-rejected to 'inactive' (no point making a
    human wade through hundreds of obvious non-matches), but anything the
    classifier considers a real match is left 'pending', waiting for a
    human to explicitly approve it via /api/sounds/<id>/approve. This is
    the design decision from the "should new discoveries silently expand
    the canonical set" question — they don't, ever, without a human click.

    Existing approved sounds are never touched by this function — new
    candidates are additive to the pending pool only.
    """
    discovered = discover_song_sounds(db_conn_factory, song_id, name, artist or "")
    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=False)

    return {
        "sounds_discovered": len(discovered),
        "qualify": qualify_result,
    }