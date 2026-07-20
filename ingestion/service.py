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
from .parsers import (
    parse_sounds_from_search,
    parse_sound_info,
    parse_posts_from_music_page,
    parse_account_stats,
    parse_posts_from_user_feed,
    parse_single_post,
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
COVERAGE_TIER_A_TARGET_POSTS = 300
COVERAGE_TIER_B_TARGET_POSTS = 100
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
ARTIST_SIGNAL_MIN_RATIO = 0.5

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
MAX_DISCOVERY_CANDIDATES = 300

# REMOVED 7/18 (see HANDOFF_state_machine_migration.md, "Discovery
# Roadmap: Stage 3"): EARLY_STOP_CANDIDATE_THRESHOLD used to stop calling
# further search sources once "enough" plausible candidates were found.
# That was the wrong question once fingerprinting became the validator —
# discovery's job isn't to decide it found enough, it's to exhaust every
# source and let fingerprinting sort out what's real. Every source in
# discover_song_sounds now always runs, unconditionally, no threshold.


def _log(msg):
    print(f"  [ingestion] {msg}", flush=True)


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

def _normalize_str(s):
    """Lowercase, transliterate accented/stylized characters to their plain
    equivalent, remove remaining punctuation, and collapse extra spaces.

    IMPORTANT: this used to just delete any character outside [a-z0-9 ],
    which silently mangled stylized titles instead of normalizing them.
    Many artists (Yeat especially — "Gët Busy", "Monëy so big", "Griddlë")
    use accented characters as stylization on otherwise plain song titles.
    Deleting 'ë' outright turned "Griddlë" into "griddl" (missing the final
    letter) or left a gap ("Monëy so big" -> "mon y so big"), so it could
    never exactly match the plain song title on file ("Griddle" ->
    "griddle") even though they're clearly the same song. This was
    silently breaking exact-title matching for a large share of this
    artist's own official sound titles specifically because of how he
    stylizes them — not a discovery or classifier bug, a normalization bug
    underneath both.

    Fix: NFKD-decompose first (splits 'ë' into 'e' + a combining diaeresis
    mark), then drop the combining marks, THEN strip remaining punctuation.
    'ë' becomes 'e' instead of vanishing.
    """
    s = s or ""
    s = _unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not _unicodedata.combining(c))
    return _re.sub(r'[^a-z0-9 ]', ' ', s.lower()).strip()

def _update_sound_velocity(db_conn_factory, sound_db_id):
    """Calculate and store velocity metrics for a sound.
    velocity = posts_24h / posts_7d — higher means rising faster."""
    import time
    now = int(time.time())
    day_ago = now - 86400
    week_ago = now - 7 * 86400

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE created_at >= %s) as posts_24h,
                    COUNT(*) FILTER (WHERE created_at >= %s) as posts_7d
                FROM posts
                WHERE sound_db_id = %s
                AND created_at IS NOT NULL
            """, (day_ago, week_ago, sound_db_id))
            row = c.fetchone()
            posts_24h = row["posts_24h"] or 0
            posts_7d = row["posts_7d"] or 0
            velocity = round(posts_24h / posts_7d, 3) if posts_7d > 0 else 0

            c.execute("""
                UPDATE sounds SET posts_24h=%s, posts_7d=%s, velocity=%s
                WHERE id=%s
            """, (posts_24h, posts_7d, velocity, sound_db_id))
        conn.commit()

    _log(f"sound {sound_db_id} velocity: {posts_24h} posts/24h, {posts_7d} posts/7d, ratio={velocity}")
    return velocity


def _is_plausible_candidate(title, author, song_name, song_artist, discovered_via):
    """Discovery-time filter, deliberately kept SEPARATE from
    _could_possibly_qualify (used by qualify's bulk pre-filter), even
    though the underlying signals look similar today. These answer two
    different questions:
      - Discovery asks: "Is this even worth storing?" — a cheap,
        PERMISSIVE junk filter.
      - Qualification asks: "Is this actually the song?" — the real,
        final verification (see _classify_sound_match), correctly
        stricter than this.

    SOURCE-AWARE, not a single bar for everything: the real question
    discovery should ask isn't "is this the right song" — it's "how much
    do I trust the search that produced this candidate."
      - title_artist: TikTok's OWN search relevance already ranked this
        as a top result for our exact "{title} {artist}" query. Trust
        that almost entirely — re-applying our own strict text check on
        top just throws away real matches with stylized, foreign-
        language, or generic "original sound" titles that TikTok's
        search already correctly surfaced but our regex can't confirm.
      - title_only: still a direct search on the correct title, slightly
        weaker evidence than title_artist (no artist term to help TikTok
        disambiguate) — light filtering, not none.
      - hashtag: REVISED 7/18 based on real production evidence, not the
        original assumption. Observed repeatedly on a real song: plain
        phrase-text search (title_artist, title_only) returned ZERO raw
        results, while hashtag search returned 200 raw candidates and,
        even under the OLD strict filter, correctly surfaced the actual
        official sound at top score with clean precision (18/200). The
        original "low trust" assumption was based on the CHALLENGE crawl
        specifically pulling in "Way Down We Go"/"La Muchachita" for
        Griddle — that's a different mechanism (the broader
        hashtag/challenge-page crawl, not a direct hashtag video search)
        and doesn't generalize to this source. Promoted to the same
        light-filtering tier as title_only.
      - challenge: exploratory, engagement-driven feeds with NO relevance
        guarantee at all — this is where the real "Way Down We Go" /
        "La Muchachita" collision happened. Keeps the original, strict
        independent textual check, alone now in this tier.

    Audio fingerprinting is now the universal verifier downstream of all
    of this — that's what makes trusting the stronger sources more here
    safe: a wrong candidate that gets stored still can't become canonical
    without either the strict Tier 1 text match or a human explicitly
    approving it, and now also gets an independent audio check before
    anyone has to look at it.
    """
    title_norm = _normalize_str(song_name)
    if not title_norm:
        return False

    s_title = _normalize_str(title)

    if discovered_via == "title_artist":
        # Very high trust — TikTok's own search already matched our exact
        # query. Keep almost everything; just confirm a real title came
        # back at all (guards against a malformed/empty API response,
        # not against the song itself).
        return bool(s_title)

    s_author = _normalize_str(author)
    artist_norm = _normalize_str(song_artist) if song_artist else ""

    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword_strict = len(sig_words) >= 2 and title_words_matched >= 2
    title_multiword_light = len(sig_words) >= 1 and title_words_matched >= 1
    plausible_title_strict = title_exact or title_contains or title_multiword_strict
    plausible_title_light = title_exact or title_contains or title_multiword_light

    if discovered_via in ("title_only", "hashtag"):
        # High trust, light filtering — still a direct search on the
        # correct title/tag, so a single significant word match (not the
        # stricter 2-word bar below) or any artist signal is enough.
        if plausible_title_light:
            return True
        if artist_norm and _artist_signal(author, artist_norm):
            return True
        return False

    # challenge (and any future/unknown source) — low trust, exploratory
    # feeds with no search-relevance guarantee. Keep the stricter bar:
    # 2+ significant words, or an exact/contains match, or a confirmed
    # artist signal.
    if not artist_norm:
        return plausible_title_strict
    if plausible_title_strict:
        return True
    if _artist_signal(author, artist_norm):
        return True
    return False


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


def _score_sound(sound, title, artist):
    """Score a sound candidate by relevance to the song title and artist.
    Higher score = better match. Used to RANK sounds — both during
    discovery (deciding which candidates are worth storing/logging first)
    and now also at qualify time (deciding which pending sounds are worth
    an actual provider call this batch, see QUALIFY_BATCH_SIZE). This is
    NOT used to gate approval — see _classify_sound_match() for the
    pass/fail decision.

    NOTE: video_count is NOT scored here because search APIs don't return it.
    It is fetched later via _update_sound_video_count() after sounds are selected.

    Scoring priority:
    1. Exact title match
    2. Title contained in sound title
    3. Multiple significant words match (2+ words, avoids single-word false positives)
    4. Verified artist match (normalized, punctuation-stripped)
    5. Official/original sound bonus
    6. Penalties for derivative versions
    """
    score = 0
    sound_title = _normalize_str(sound.get("title"))
    sound_author = _normalize_str(sound.get("author"))
    title_norm = _normalize_str(title)
    artist_norm = _normalize_str(artist) if artist else ""

    # Title matching
    if sound_title == title_norm:
        score += 150  # exact match
    elif title_norm in sound_title:
        score += 100  # title contained in sound title
    else:
        # Require 2+ significant words to match (avoids single common word false positives)
        sig_words = [w for w in title_norm.split() if len(w) > 3]
        matches = sum(1 for w in sig_words if w in sound_title)
        if len(sig_words) >= 2 and matches >= 2:
            score += 40

    # Artist matching — normalize punctuation before comparing
    if artist_norm:
        author_words = set(sound_author.split())
        artist_words = set(artist_norm.split())
        if sound_author == artist_norm:
            score += 100  # exact match
        elif artist_words.issubset(author_words):
            score += 75   # all artist words present in author
        elif any(w in author_words for w in artist_words):
            score += 30   # partial match only

    # Official/original sound bonus
    if sound.get("is_original"):
        score += 50

    # Penalties for derivative versions
    penalties = [
        "sped up", "slowed", "remix", "instrumental", "reverb", "cover",
        "nightcore", "bass boosted", "8d", "phonk", "edit audio", "mashup",
        "loop", "extended", "sped-up", "slow reverb", "lyrics"
    ]
    for word in penalties:
        if word in sound_title:
            score -= 30

    return score


import re as _collab_re_module
_COLLAB_SEPARATOR_RE = _collab_re_module.compile(r'&|,|\bx\b|\band\b|\bfeat\.?\b|\bft\.?\b', _collab_re_module.IGNORECASE)


def _artist_signal(raw_author, artist_norm):
    """Match an artist name against a sound's author field, handling two
    genuinely different situations that both shrink a naive length ratio:
      - A real multi-artist credit ("Yeat & Don Toliver") — the artist
        name is a small fraction of the FULL string, but 100% of its own
        segment once split on the collab separator.
      - A fan handle that merely mentions the artist ("bells_yeat") — no
        real separator, the artist name is genuinely just embedded in an
        unrelated compound word.

    A single whole-string ratio can't tell these apart — "Yeat" is a
    small fraction of "Yeat & Don Toliver" for the same arithmetic reason
    it's a small fraction of "bells_yeat" character-count-wise. The fix:
    split the RAW (pre-normalization — normalization destroys the '&')
    author string on real collab separators first, then check the ratio
    against each resulting segment individually. A short artist name
    still passes cleanly when it IS its own credited segment; it still
    fails when it's just embedded in one longer unrelated word.
    """
    if not artist_norm or not raw_author:
        return False
    artist_nospace = artist_norm.replace(" ", "")
    if not artist_nospace:
        return False

    segments = _COLLAB_SEPARATOR_RE.split(raw_author) or [raw_author]
    for seg in segments:
        seg_norm = _normalize_str(seg)
        seg_nospace = seg_norm.replace(" ", "")
        if not seg_nospace:
            continue
        contains = (artist_norm in seg_norm) or (artist_nospace in seg_nospace)
        if not contains:
            continue
        ratio = len(artist_nospace) / len(seg_nospace) if seg_nospace else 0
        if ratio >= ARTIST_SIGNAL_MIN_RATIO:
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


def discover_sounds(query):
    """Search TikTok for sounds matching a query. No database writes.
    Uses TikLive search-video with publish_time=7 to find sounds from this week."""
    raw = provider.search_sounds(query)
    sounds = parse_sounds_from_search(raw)
    _log(f"search '{query}' found {len(sounds)} distinct sounds")
    return sounds


def discover_sounds_from_videos(query, publish_time=7):
    """Workflow B: discover sounds by searching recent videos and extracting music IDs.
    This finds NEW sounds being used this week, not historically relevant sounds.
    Returns list of dicts with sound_id, title, author, frequency (how many videos used it),
    plus discovery_query (Discovery Memory — see HANDOFF_state_machine_migration.md's
    "Discovery Memory" section) so future analysis can answer "which specific query
    finds the biggest sounds" instead of only knowing the broad source category.

    NOTE: sort mode is NOT a parameter here — tiklive_provider.py's
    search_sounds() hardcodes sort_by=1 (like count) internally, not
    something this function controls. discovery_sort_by below records
    that TRUE fixed value for Discovery Memory purposes, rather than
    implying this function can vary it (it can't, today).
    """
    raw = provider.search_sounds(query)
    if not raw:
        return []

    # parse_sounds_from_search returns deduplicated sounds
    # but we want frequency counts — re-parse manually
    data = raw if isinstance(raw, dict) else {}
    items = data.get("data", [])

    # Count frequency of each music ID
    music_counts = {}
    music_meta = {}
    for entry in items:
        item = entry.get("item", {})
        music = item.get("music", {})
        mid = str(music.get("id", ""))
        if not mid:
            continue
        music_counts[mid] = music_counts.get(mid, 0) + 1
        if mid not in music_meta:
            music_meta[mid] = {
                "sound_id": mid,
                "title": music.get("title", "Unknown"),
                "author": music.get("authorName", ""),
                "frequency": 0,
                "discovery_query": query,
                "discovery_sort_by": "likes",  # true fixed value — see note above
            }
        music_meta[mid]["frequency"] = music_counts[mid]

    # Sort by frequency (most used sounds this week first)
    results = sorted(music_meta.values(), key=lambda x: x["frequency"], reverse=True)
    _log(f"discover_sounds_from_videos: found {len(results)} unique sounds from {len(items)} videos")
    return results


def create_sound(db_conn_factory, song_id, sound):
    """Persist one discovered sound. Returns new db id, or None if already existed.

    DUAL-WRITE (state machine migration, in progress — see
    HANDOFF_state_machine_migration.md): writes the new `state` column
    alongside the existing `status` column. Both are kept in sync during
    the transition so the current UI (which reads `status`) keeps working
    unmodified while the new state-based pipeline gets built and proven
    in parallel. Do not remove the `status` write until the new
    endpoints are confirmed working and status/fingerprint_status are
    formally retired (migration step 6).

    DISCOVERY MEMORY (7/19 — see HANDOFF_state_machine_migration.md's
    "Discovery Memory" section): also stores discovery_query/
    discovery_sort_by — the literal search string (or hashtag, or
    'creator_graph:<username>') that actually surfaced this candidate,
    not just the broad discovered_via category. This is what eventually
    lets you ask "which specific queries find the biggest sounds" instead
    of only knowing which category of source worked.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status, state, discovered_via, discovery_query, discovery_sort_by)
                VALUES (%s,%s,%s,%s,'pending','discovered',%s,%s,%s)
                ON CONFLICT (song_id, sound_id) DO NOTHING
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"], sound.get("discovered_via"),
                  sound.get("discovery_query"), sound.get("discovery_sort_by")))
            row = c.fetchone()
        conn.commit()
    return row["id"] if row else None


def get_or_create_sound(db_conn_factory, song_id, sound):
    """Get existing sound or create new one. Always returns a db id.
    New sounds get status='pending' — monitor decides when to ingest them.
    Existing sounds keep their current status.

    discovered_via is only ever set, never overwritten: if a sound was
    already tagged from an earlier discovery pass, a later rediscovery of
    the same sound_id keeps the original value — this is a historical
    record of how we first found it, not something that should shift
    around on every rediscovery. Same treatment for discovery_query/
    discovery_sort_by (Discovery Memory — see
    HANDOFF_state_machine_migration.md).

    FIXED 7/19: this is the function discover_song_sounds's final storage
    loop actually calls — Discovery Memory's discovery_query/
    discovery_sort_by were only added to create_sound (a different,
    less-used function) earlier, so they were silently staying NULL for
    every real discovery run despite the code "working."

    DUAL-WRITE (state machine migration, in progress — see
    HANDOFF_state_machine_migration.md): writes `state='discovered'`
    alongside `status='pending'` on initial insert only — an existing
    sound's `state` is NOT touched on conflict, same as `status` isn't,
    since it may have already progressed further in the new pipeline.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status, state, discovered_via, discovery_query, discovery_sort_by)
                VALUES (%s,%s,%s,%s,'pending','discovered',%s,%s,%s)
                ON CONFLICT (song_id, sound_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    author=EXCLUDED.author,
                    discovered_via=COALESCE(sounds.discovered_via, EXCLUDED.discovered_via),
                    discovery_query=COALESCE(sounds.discovery_query, EXCLUDED.discovery_query),
                    discovery_sort_by=COALESCE(sounds.discovery_sort_by, EXCLUDED.discovery_sort_by)
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"], sound.get("discovered_via"),
                  sound.get("discovery_query"), sound.get("discovery_sort_by")))
            row = c.fetchone()
        conn.commit()
    return row["id"] if row else None


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


def discover_sounds_from_challenge(title, artist=""):
    """Discover sounds by searching hashtags and fetching challenge posts.
    This finds far more sounds than video search because it crawls the full
    hashtag graph instead of relying on search sampling."""
    title_clean = title.strip().lower()
    artist_clean = artist.strip().lower().split(',')[0].split('&')[0].strip() if artist else ""

    # Generate hashtag variants to search
    hashtag_queries = list(dict.fromkeys(filter(None, [
        title_clean.replace(" ", ""),
        f"{title_clean.replace(' ', '')}{artist_clean.replace(' ', '')}",
        artist_clean.replace(" ", ""),
    ])))

    seen_sound_ids = set()
    all_sounds = []

    for hashtag in hashtag_queries:
        # Step 1: Find challenge ID for this hashtag
        challenges = _tiklive.search_challenge(hashtag)
        if not challenges:
            _log(f"no challenge found for #{hashtag}")
            continue

        # Pick challenge with most users (most relevant)
        challenge = max(challenges, key=lambda c: c.get("user_count", 0))
        challenge_id = challenge.get("id")
        cha_name = challenge.get("cha_name", "")
        user_count = challenge.get("user_count", 0)
        _log(f"best match: #{cha_name} (id={challenge_id}, users={user_count})")
        _log(f"challenge #{cha_name} (id={challenge_id}) — crawling posts")

        # Step 2: Paginate through challenge posts
        cursor = 0
        pages = 0
        new_per_page_low = 0

        while pages < 10:
            videos, has_more, next_cursor = _tiklive.get_challenge_posts(challenge_id, cursor=cursor)
            new_this_page = 0

            for v in videos:
                music_info = v.get("music_info", {})
                music_id = music_info.get("id") if music_info else None
                if not music_id or music_id in seen_sound_ids:
                    continue
                seen_sound_ids.add(music_id)
                new_this_page += 1
                # FIXED 7/19 — this used to append the raw nested TikTok
                # shape ({"item": {"music": {...}}}), which _ingest_results
                # in discover_song_sounds can't read (it expects flat
                # dicts with s.get("sound_id") etc). That meant
                # sid = s.get("sound_id") was silently None for EVERY
                # challenge candidate, ever — challenge has been
                # contributing zero real candidates to any song, always,
                # not because it found nothing new but because of this
                # shape mismatch. Confirmed by re-checking the "275 raw ->
                # 0 new unique" Discovery Report line from earlier tonight
                # — that wasn't redundancy, it was this bug. Now flat,
                # matching discover_sounds_from_videos's shape, plus
                # discovery_query tagged with the actual hashtag (Discovery
                # Memory — see HANDOFF_state_machine_migration.md).
                all_sounds.append({
                    "sound_id": music_id,
                    "title": (music_info.get("title") or "")[:50],
                    "author": music_info.get("author") or "",
                    "frequency": 1,
                    "discovery_query": f"#{hashtag}",
                    "discovery_sort_by": "challenge_crawl",
                })

            _log(f"  #{cha_name} page {pages+1}: {len(videos)} videos, {new_this_page} new sounds")

            if not has_more or not videos:
                break
            cursor = next_cursor
            pages += 1

    _log(f"discover_sounds_from_challenge: {len(all_sounds)} unique sounds from {len(hashtag_queries)} hashtags")
    return all_sounds


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


# ── Discovery Engine: creator-graph traversal ───────────────────────────────
# Tuning/safety constants — see determine_coverage_plan's pattern above for
# why these live here by name instead of buried inline.
CREATOR_GRAPH_MAX_CREATORS_PER_RUN = 30   # raised 7/18 from 15 — see below
CREATOR_GRAPH_POSTS_PER_CREATOR = 10      # how many of a creator's recent posts to check
CREATOR_GRAPH_TIME_BUDGET_SECONDS = 30


def discover_via_creator_graph(db_conn_factory, song_id, song_name, song_artist=""):
    """Discovery Engine, new source: creator_graph (see
    HANDOFF_state_machine_migration.md — validated 7/18 with real data
    before building: one creator's /user-posts/ call surfaced a real
    Chartex-confirmed sound our search-based sources had never found,
    for free, bundled in the same call).

    STRUCTURALLY DIFFERENT from every other discovery source: those all
    work from a blank slate via search. This one requires at least one
    already-approved sound to seed from — it traverses song -> known
    posters of that sound -> those creators' OTHER posts -> whatever
    OTHER sounds they've used, which search can't see at all (their
    other posts' captions may never mention this song or artist).

    REVISED 7/18 after real evidence showed high variance run-to-run: the
    first-ever test (a single essentially-random poster) found a real,
    Chartex-confirmed big sound immediately; a follow-up test (the
    top-15-by-views posters, same 15 every time) found nothing across
    multiple runs. This ISN'T "random creators don't work, need curated
    fan pages instead" — it's that a fixed top-15 sample, re-checked
    identically every run, can only ever explore 15 creators total, ever.
    The real fix is NEVER RE-CHECKING THE SAME CREATOR TWICE for a given
    song — tracked in the creator_graph_checked table (see schema note
    below) — so every run explores genuinely new ground instead of
    burning budget re-confirming the same 15 people found nothing new.

    REQUIRES this table (run once in Neon, additive, safe):
        CREATE TABLE IF NOT EXISTS creator_graph_checked (
            song_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (song_id, username)
        );

    Feeds discovered candidates through the exact same
    _is_plausible_candidate filter as every other source
    (discovered_via='creator_graph', so it's measured separately in the
    funnel-tracking views built earlier tonight) — no special-casing
    downstream, same fingerprint verification as anything else.
    """
    import time as _time
    start = _time.monotonic()

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.sound_id AS known_sound_id
                FROM sounds snd
                WHERE snd.song_id = %s
            """, (song_id,))
            known_sound_ids = {r["known_sound_id"] for r in c.fetchall()}

            # Broadened + never repeats: previously ORDER BY max_views DESC
            # LIMIT 15 meant every single run checked the exact same 15
            # biggest-viewed posters, forever. Now excludes anyone already
            # checked for this song (creator_graph_checked), so repeated
            # runs genuinely explore new creators instead of re-confirming
            # the same handful found nothing.
            c.execute("""
                SELECT p.username, MAX(p.views) AS max_views
                FROM posts p
                JOIN sounds snd ON snd.id = p.sound_db_id
                WHERE snd.song_id = %s AND snd.status = 'approved'
                  AND p.username NOT IN (
                      SELECT username FROM creator_graph_checked WHERE song_id = %s
                  )
                GROUP BY p.username
                ORDER BY max_views DESC
                LIMIT %s
            """, (song_id, song_id, CREATOR_GRAPH_MAX_CREATORS_PER_RUN))
            creators = [r["username"] for r in c.fetchall()]

    _log(f"discover_via_creator_graph: song {song_id} — {len(creators)} NEW (never-checked) creators to check "
         f"(from {len(known_sound_ids)} already-known sounds)")

    new_candidates = []
    new_sound_ids_seen = 0
    rejected_examples = []
    creators_checked = 0
    for username in creators:
        if _time.monotonic() - start > CREATOR_GRAPH_TIME_BUDGET_SECONDS:
            _log(f"discover_via_creator_graph: time budget reached after {creators_checked}/{len(creators)} creators")
            break
        try:
            # Mark checked FIRST, regardless of what happens below — a
            # private/deleted account or an API failure is a durable fact
            # about that creator, not worth re-attempting every future run.
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO creator_graph_checked (song_id, username)
                        VALUES (%s, %s)
                        ON CONFLICT (song_id, username) DO NOTHING
                    """, (song_id, username))
                conn.commit()

            account_raw = provider.get_account(username)
            if not account_raw:
                continue
            # NUMERIC id required by get_account_posts, NOT sec_uid — see
            # this function's docstring and the pre-existing bug noted in
            # ingest_roster_account for why this distinction matters.
            numeric_id = account_raw.get("userInfo", {}).get("user", {}).get("id")
            if not numeric_id:
                continue

            posts_raw = provider.get_account_posts(numeric_id, count=CREATOR_GRAPH_POSTS_PER_CREATOR)
            creators_checked += 1
            if not posts_raw:
                continue

            for item in posts_raw.get("itemList", []):
                music = item.get("music") or {}
                candidate_sound_id = music.get("id")
                if not candidate_sound_id or candidate_sound_id in known_sound_ids:
                    continue
                known_sound_ids.add(candidate_sound_id)  # dedupe within this run too
                new_sound_ids_seen += 1

                candidate = {
                    "sound_id": candidate_sound_id,
                    "title": music.get("title") or "",
                    "author": music.get("authorName") or "",
                    "discovered_via": "creator_graph",
                    "discovery_query": f"creator:{username}",
                    "discovery_sort_by": "creator_graph",
                }
                if _is_plausible_candidate(
                    candidate["title"], candidate["author"], song_name, song_artist, "creator_graph"
                ):
                    new_candidates.append(candidate)
                elif len(rejected_examples) < 10:
                    # Diagnostic only — so a 0-candidates run tells us WHY
                    # (nothing new found at all, vs. new sound_ids found
                    # but the plausibility filter rejected every one of
                    # them) instead of leaving us guessing.
                    rejected_examples.append((candidate["title"], candidate["author"]))

        except Exception as e:
            _log(f"discover_via_creator_graph: failed on creator @{username}: {e}")

    stored = 0
    for candidate in new_candidates:
        new_id = create_sound(db_conn_factory, song_id, candidate)
        if new_id:
            stored += 1

    _log(f"discover_via_creator_graph: song {song_id} — {creators_checked} creators checked, "
         f"{new_sound_ids_seen} new sound_ids seen (never encountered before), "
         f"{len(new_candidates)} passed the plausibility filter, {stored} new sounds stored")
    if rejected_examples:
        _log(f"discover_via_creator_graph: sample of rejected candidates (title, author): {rejected_examples}")

    return {
        "creators_checked": creators_checked,
        "new_sound_ids_seen": new_sound_ids_seen,
        "candidates_found": len(new_candidates),
        "stored": stored,
    }


def discover_song_sounds(db_conn_factory, song_id, title, artist=""):
    """Discovery: search for candidate sounds via EVERY available source,
    merge everything into one pool, filter out obvious junk, then rank
    and cap what actually gets persisted.

    EARLY STOPPING REMOVED (7/18 — see HANDOFF_state_machine_migration.md,
    "Discovery Roadmap: Stage 3"). This function used to stop calling
    further search sources once EARLY_STOP_CANDIDATE_THRESHOLD plausible
    candidates were found — that made sense when text matching was the
    final validator and more candidates just meant more manual review
    work. Now that fingerprinting verifies everything downstream, that
    logic was backwards: stopping early doesn't save real cost, it risks
    silently preventing discovery of the exact sounds you're looking for
    — you don't know in advance which source will find the missing
    10,000-video sound, and a fast source succeeding first shouldn't
    prevent a slower one from ever getting a chance to run.

    Every source still runs and every candidate still gets tagged with
    discovered_via, a permanent record of which method surfaced it:
      'title_artist' — the combined "{title} {artist}" search. Requires
                        TikTok's own search relevance to match BOTH terms
                        together — the strongest evidence.
      'title_only'   — title alone, or a sped-up/slowed/remix variant of
                        the title. Weaker: a common title can collide with
                        unrelated content.
      'hashtag'      — title or artist hashtag search via video search.
      'challenge'    — the challenge/hashtag crawl. Weakest and most
                        expensive (up to 10 pages per hashtag) — this is
                        exactly where "Griddle" collided with an unrelated
                        dance trend and pulled in dozens of wildly popular,
                        completely unrelated sounds.

    COST NOTE: since every source now always runs, a single discovery
    call is real-time and real-API-call more expensive than before,
    including the pricier challenge crawl every time, not just when
    cheaper sources came up short. This is the deliberate tradeoff of
    treating fingerprinting (cheap, ~$0.003/check) as the validator
    instead of the text filter — worth knowing if discovery starts
    feeling noticeably slower per click.

    Every candidate is still checked with _is_plausible_candidate before
    it's even added to the working set — candidates with zero textual
    relation to the song never get past this point, so they're never
    persisted to the database at all. This is a historical record ("how
    did we find this") plus a junk filter, not a recomputed confidence
    score — the classifier at qualify time always re-derives its own
    confidence from today's matching logic using fresh video_count data.
    """
    title_clean = title.strip()
    artist_raw = artist.strip() if artist else ""
    artist_clean = artist_raw.split(',')[0].split('&')[0].split('feat')[0].split('ft.')[0].strip()

    title_hashtag = "#" + title_clean.lower().replace(" ", "")
    artist_hashtag = "#" + artist_clean.lower().replace(" ", "") if artist_clean else ""

    targeted_query = f"{title_clean} {artist_clean}".strip() if artist_clean else ""

    title_only_queries = list(dict.fromkeys(filter(None, [
        title_clean,
        f"{title_clean} sped up",
        f"{title_clean} slowed",
        f"{title_clean} remix",
    ])))

    hashtag_queries = list(dict.fromkeys(filter(None, [
        title_hashtag,
        artist_hashtag,
    ])))

    seen_ids = set()
    all_sounds = []       # everything seen, for logging/dedup purposes
    plausible_sounds = [] # only candidates that passed the junk filter

    # ── Discovery Report (per design discussion 7/18) — three-tier per
    # source, not just plausible counts: raw (what the API actually
    # returned), new_unique (post-dedup against every earlier source in
    # this same run, pre-filter), and plausible (post-filter). Distinguishing
    # these tells you WHY a source shows 0 — genuinely found nothing, vs.
    # found things another source already claimed, vs. found real new
    # things that the filter rejected. Three very different situations
    # that used to look identical in the logs.
    per_source_raw = {}
    per_source_new_unique = {}
    per_source_plausible = {}

    def _ingest_results(sounds, source_tag):
        """Tag, dedupe, and junk-filter one search's results."""
        per_source_raw[source_tag] = per_source_raw.get(source_tag, 0) + len(sounds)
        new_unique_this_source = 0
        found_this_source = 0
        for s in sounds:
            sid = s.get("sound_id")
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            new_unique_this_source += 1
            s["discovered_via"] = source_tag
            all_sounds.append(s)
            if _is_plausible_candidate(s.get("title"), s.get("author"), title, artist, source_tag):
                plausible_sounds.append(s)
                found_this_source += 1
        per_source_new_unique[source_tag] = per_source_new_unique.get(source_tag, 0) + new_unique_this_source
        per_source_plausible[source_tag] = per_source_plausible.get(source_tag, 0) + found_this_source

    # 'title_artist' — strongest evidence
    if targeted_query:
        sounds = discover_sounds_from_videos(targeted_query)
        _ingest_results(sounds, "title_artist")

    # 'title_only' — title alone or a derivative-title variant
    for query in title_only_queries:
        sounds = discover_sounds_from_videos(query)
        _ingest_results(sounds, "title_only")

    # 'hashtag' — hashtag search via video search endpoint
    for query in hashtag_queries:
        sounds = discover_sounds_from_videos(query)
        _ingest_results(sounds, "hashtag")

    # 'challenge' — the expensive, weakest-evidence crawl. Now runs every
    # time, same as every other source — see cost note in docstring.
    challenge_sounds = discover_sounds_from_challenge(title, artist)
    _ingest_results(challenge_sounds, "challenge")

    _log(f"discover_song_sounds: {len(all_sounds)} unique sounds seen, "
         f"{len(plausible_sounds)} passed the plausibility filter (rest discarded, never persisted)")
    _log("discover_song_sounds: Discovery Report —")
    for source in ("title_artist", "title_only", "hashtag", "challenge"):
        raw = per_source_raw.get(source, 0)
        new_unique = per_source_new_unique.get(source, 0)
        plausible = per_source_plausible.get(source, 0)
        _log(f"  {source:14s} searched -> {raw:4d} raw -> {new_unique:4d} new unique -> {plausible:4d} plausible")

    # Score for ranking, then cap to a small, plausible ceiling — even
    # after filtering, a common title can still have more "technically
    # plausible" generic uploads than are worth tracking. A song should
    # have a handful of real candidates, not dozens.
    def score(s):
        base = _score_sound(s, title, artist)
        freq_bonus = min(s.get("frequency", 0) * 5, 50)
        return base + freq_bonus

    ranked_sounds = sorted(plausible_sounds, key=score, reverse=True)
    to_store = ranked_sounds[:MAX_DISCOVERY_CANDIDATES]

    _log(f"discover_song_sounds: storing top {len(to_store)} of {len(ranked_sounds)} plausible candidates "
         f"(capped at {MAX_DISCOVERY_CANDIDATES})")
    for i, s in enumerate(to_store[:5]):
        _log(f"  #{i+1} '{s.get('title')}' score={score(s)} freq={s.get('frequency',0)}")

    stored = 0
    for s in to_store:
        try:
            sound_db_id = get_or_create_sound(db_conn_factory, song_id, s)
            if sound_db_id:
                stored += 1
        except Exception as e:
            _log(f"EXCEPTION storing sound {s.get('sound_id')} for song {song_id}: {e}")

    _log(f"discover_song_sounds: stored {stored} sounds as pending — run /qualify to promote")
    return to_store[:stored]


def _promote_top_sounds(db_conn_factory, song_id, sounds):
    """Promote top-scored sounds to approved status so monitor ingests them."""
    if not sounds:
        return
    sound_ids = [s["sound_id"] for s in sounds if s.get("sound_id")]
    if not sound_ids:
        return
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE sounds SET status='approved'
                WHERE song_id=%s AND sound_id = ANY(%s)
            """, (song_id, sound_ids))
        conn.commit()


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
# ── New: consolidated per-song pipeline (discover -> qualify -> ingest posts) ──

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
    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=True)
    ingest_result = ingest_approved_sounds_for_song(db_conn_factory, song_id)

    return {
        "sounds_discovered": len(discovered),
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
    TIME_BUDGET_SECONDS = 40  # see note above — count alone no longer bounds worst-case time
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