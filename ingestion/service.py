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

from datetime import date, datetime, timezone
from .providers import default_provider as provider
from .tiklive_provider import TikLiveAPIProvider as _TikLiveProvider
_tiklive = _TikLiveProvider()
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
# Set to 5, not a larger "safety margin" number, because that's the actual
# MVP goal: the top 5 highest-confidence sounds, not "as many as we can
# squeeze in before timing out." Even 30 sequential provider calls can
# still blow the worker timeout if the upstream API has a few slow
# responses (each call has a 5s connect / 10s read timeout with one retry,
# so a handful of slow calls alone can eat 30+ seconds) — cutting to 5
# both matches the actual product goal AND gives a much bigger, more
# reliable safety margin against timeouts than trimming the count alone.
QUALIFY_BATCH_SIZE = 5

# Max plausible candidates to persist per discovery run. Even after the
# no-API-call plausibility filter (_could_possibly_qualify), a common
# title can still leave more "technically eligible" generic uploads than
# are worth tracking — a song should end up with a handful of real
# candidates, not dozens sitting in the pending queue. This is the actual
# fix for "494 pending sounds for one song": discovery should return a
# small, plausible set, not everything it could possibly find.
MAX_DISCOVERY_CANDIDATES = 30

# Once discovery has found this many plausible candidates, stop searching
# entirely — no reason to keep crawling hashtags/challenges once there's
# already a healthy pool to qualify from. Saves API calls, time, database
# writes, and downstream qualification work. Deliberately lower than
# MAX_DISCOVERY_CANDIDATES: this is "enough to stop looking," not "the
# most we'll ever keep" — the persist-time cap still applies on top.
EARLY_STOP_CANDIDATE_THRESHOLD = 15


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
        PERMISSIVE junk filter. A title match alone (even with an
        unconfirmed/unrelated-looking uploader) or an artist match alone
        is enough to be worth storing — e.g. "Griddle Remix" by some
        random uploader is plausible enough to check further, even though
        we can't yet confirm the uploader has anything to do with the
        artist. Qualify will do the real, stricter verification later
        with fresh video_count data.
      - Qualification asks: "Is this actually the song?" — the real,
        final verification, and it correctly requires BOTH a title AND
        artist match together (see _classify_sound_match) — that's where
        "Griddle Remix" by an unrelated uploader would likely get
        rejected once actually checked, not here.
    Keeping these as separate functions (and separately TUNED — this one
    is intentionally looser) means discovery's net can stay wide while
    qualification's bar stays strict, without the two silently becoming
    the same function in disguise.
    """
    title_norm = _normalize_str(song_name)
    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_title = _normalize_str(title)
    s_author = _normalize_str(author)

    if not title_norm:
        return False

    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword = len(sig_words) >= 2 and title_words_matched >= 2
    plausible_title = title_exact or title_contains or title_multiword

    if not artist_norm:
        return plausible_title

    # Permissive OR, not qualify's stricter AND: either signal alone is
    # enough to be worth storing and letting qualify look at properly.
    if plausible_title:
        return True
    if _artist_signal(author, artist_norm):
        return True
    # NOTE: previously also kept generic "original sound" uploads from the
    # 'title_artist' source as plausible, on the theory that qualify's
    # Tier 2 might approve them on popularity. Tier 2 has been removed
    # (see _classify_sound_match) after producing real false positives
    # twice, so that path no longer leads to approval — no reason to keep
    # these as plausible here either.

    return False


def _could_possibly_qualify(title, author, song_name, song_artist, discovered_via):
    """Cheap, NO-API-CALL pre-check using only the title/author already
    stored from discovery. Returns False only when we're CERTAIN qualify
    would reject this candidate regardless of video_count — used to bulk-
    reject candidates before spending a provider call on them.

    Why this exists: discovery (especially the challenge/hashtag crawl)
    pulls in a large volume of candidates with zero textual relation to
    the song at all — not just generic "original sound" uploads, but
    complete, real songs by unrelated artists (e.g. Griddle's crawl
    surfacing "Way down We Go" by KELAO, "La Muchachita" by Anthony
    Santos). We already know enough from discovery-time title/author to
    be certain these can never pass — spending a network call and a
    qualify "slot" just to prove what's already obvious from the text is
    wasted work, and it's why qualify was churning through 5-at-a-time
    for hundreds of rounds on something that's actually free to filter.

    The only thing genuinely gated behind an API call is video_count,
    which only matters for the narrow Tier 2 path (generic upload +
    discovered_via == 'title_artist' + popularity). Everything else this
    function checks mirrors _classify_sound_match's title/artist logic
    exactly, just without the video_count-dependent branch.
    """
    title_norm = _normalize_str(song_name)
    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_title = _normalize_str(title)
    s_author = _normalize_str(author)

    if not title_norm:
        return False

    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword = len(sig_words) >= 2 and title_words_matched >= 2
    strong_title_possible = title_exact or title_contains or title_multiword

    if not artist_norm:
        # No artist on file — title match alone would be the deciding
        # factor, and we already know the title now (no API call needed
        # to learn it — video_count doesn't change whether a title matches).
        return strong_title_possible

    if _artist_signal(author, artist_norm) and strong_title_possible:
        return True  # Tier 1 already provable from stored data alone

    # NOTE: previously kept generic uploads from the 'title_artist' source
    # as "possibly qualifiable" pending a video_count check, since Tier 2
    # might have approved them. Tier 2 has been removed (see
    # _classify_sound_match) — nothing left downstream can approve a
    # generic upload with no artist signal, so there's no reason to spend
    # an API call finding that out. Bulk-reject it here for free instead.

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


def _ingest_sound_posts(db_conn_factory, sound_db_id, tiktok_sound_id, max_results):
    """Pull newest posts for a sound. TikLive returns newest first so
    we get the most recent activity on every refresh."""
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
        cursor = int(next_cursor) if next_cursor is not None else cursor + 35

    _log(f"fetch_sound_posts id={tiktok_sound_id} -> got {len(all_posts)} posts")
    posts_to_write = all_posts[:max_results]
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
    Returns list of dicts with sound_id, title, author, frequency (how many videos used it)."""
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
            }
        music_meta[mid]["frequency"] = music_counts[mid]

    # Sort by frequency (most used sounds this week first)
    results = sorted(music_meta.values(), key=lambda x: x["frequency"], reverse=True)
    _log(f"discover_sounds_from_videos: found {len(results)} unique sounds from {len(items)} videos")
    return results


def create_sound(db_conn_factory, song_id, sound):
    """Persist one discovered sound. Returns new db id, or None if already existed."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status, discovered_via)
                VALUES (%s,%s,%s,%s,'pending',%s)
                ON CONFLICT (song_id, sound_id) DO NOTHING
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"], sound.get("discovered_via")))
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
    around on every rediscovery.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status, discovered_via)
                VALUES (%s,%s,%s,%s,'pending',%s)
                ON CONFLICT (song_id, sound_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    author=EXCLUDED.author,
                    discovered_via=COALESCE(sounds.discovered_via, EXCLUDED.discovered_via)
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"], sound.get("discovered_via")))
            row = c.fetchone()
        conn.commit()
    return row["id"] if row else None


def ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id, max_results=30):
    """Refresh one Sound's posts and video-count snapshot.
    Checks cache first — skips the provider pipeline if data is fresh."""
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
    result["posts_added"] = _ingest_sound_posts(db_conn_factory, sound_db_id, tiktok_sound_id, max_results)

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
                all_sounds.append({
                    "item": {
                        "music": {
                            "id": music_id,
                            "title": music_info.get("title", "")[:50],
                            "authorName": music_info.get("author", ""),
                        }
                    }
                })

            _log(f"  #{cha_name} page {pages+1}: {len(videos)} videos, {new_this_page} new sounds")

            if not has_more or not videos:
                break
            cursor = next_cursor
            pages += 1

    _log(f"discover_sounds_from_challenge: {len(all_sounds)} unique sounds from {len(hashtag_queries)} hashtags")
    return all_sounds


def discover_song_sounds(db_conn_factory, song_id, title, artist=""):
    """Discovery: search for candidate sounds, filter out obvious junk
    BEFORE persisting anything, and stop searching early once enough
    plausible candidates are found.

    Each candidate is tagged with discovered_via, a permanent record of
    which search method first surfaced it:
      'title_artist' — the combined "{title} {artist}" search. Requires
                        TikTok's own search relevance to match BOTH terms
                        together — the strongest evidence, tried first.
      'title_only'   — title alone, or a sped-up/slowed/remix variant of
                        the title. Weaker: a common title can collide with
                        unrelated content.
      'hashtag'      — title or artist hashtag search via video search.
      'challenge'    — the challenge/hashtag crawl. Weakest and most
                        expensive (up to 10 pages per hashtag) — this is
                        exactly where "Griddle" collided with an unrelated
                        dance trend and pulled in dozens of wildly popular,
                        completely unrelated sounds. Only run if cheaper
                        sources haven't already found enough.

    Search steps run in order from strongest to weakest evidence, and stop
    as soon as EARLY_STOP_CANDIDATE_THRESHOLD plausible candidates have
    been found — no reason to keep crawling hashtags once a healthy pool
    already exists to qualify from. This saves API calls, time, and
    avoids ever generating (let alone filtering) large amounts of noise
    in the first place.

    Every candidate is checked with _is_plausible_candidate before it's
    even added to the working set — candidates with zero textual relation
    to the song never get past this point, so they're never persisted to
    the database at all. This is a historical record ("how did we find
    this") plus a junk filter, not a recomputed confidence score — the
    classifier at qualify time always re-derives its own confidence from
    today's matching logic using fresh video_count data.
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

    def _plausible_count():
        return len(plausible_sounds)

    def _ingest_results(sounds, source_tag):
        """Tag, dedupe, and junk-filter one search's results. Returns True
        if the early-stop threshold is now met."""
        for s in sounds:
            sid = s.get("sound_id")
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            s["discovered_via"] = source_tag
            all_sounds.append(s)
            if _is_plausible_candidate(s.get("title"), s.get("author"), title, artist, source_tag):
                plausible_sounds.append(s)
        return _plausible_count() >= EARLY_STOP_CANDIDATE_THRESHOLD

    stopped_early_at = None

    # 'title_artist' — strongest evidence, tried first
    if targeted_query:
        sounds = discover_sounds_from_videos(targeted_query)
        if _ingest_results(sounds, "title_artist"):
            stopped_early_at = "title_artist"

    # 'title_only' — title alone or a derivative-title variant
    if not stopped_early_at:
        for query in title_only_queries:
            sounds = discover_sounds_from_videos(query)
            if _ingest_results(sounds, "title_only"):
                stopped_early_at = "title_only"
                break

    # 'hashtag' — hashtag search via video search endpoint
    if not stopped_early_at:
        for query in hashtag_queries:
            sounds = discover_sounds_from_videos(query)
            if _ingest_results(sounds, "hashtag"):
                stopped_early_at = "hashtag"
                break

    # 'challenge' — the expensive, weakest-evidence crawl. Only run if
    # cheaper sources haven't already found enough.
    if not stopped_early_at:
        challenge_sounds = discover_sounds_from_challenge(title, artist)
        if _ingest_results(challenge_sounds, "challenge"):
            stopped_early_at = "challenge"

    if stopped_early_at:
        _log(f"discover_song_sounds: stopped early after '{stopped_early_at}' — "
             f"{_plausible_count()} plausible candidates already found "
             f"(threshold={EARLY_STOP_CANDIDATE_THRESHOLD}), skipping remaining search sources")

    _log(f"discover_song_sounds: {len(all_sounds)} unique sounds seen, "
         f"{len(plausible_sounds)} passed the plausibility filter (rest discarded, never persisted)")

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
    """Pull stats for a plain (non-roster) tracked fan account, plus its recent posts."""
    raw = provider.get_account(username)
    account = parse_account_stats(raw)
    if not account:
        return False

    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO stats (username, date, followers, likes, videos)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, likes=EXCLUDED.likes, videos=EXCLUDED.videos
            """, (username, today, account["followers"], account["total_likes"], account["video_count"]))
        conn.commit()

    if account["sec_uid"]:
        try:
            raw_posts = provider.get_account_posts(account["sec_uid"], count=10)
            posts = parse_posts_from_user_feed(raw_posts)
            if posts:
                with db_conn_factory() as conn:
                    with conn.cursor() as c:
                        for p in posts:
                            c.execute("""
                                INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (post_id) DO UPDATE SET
                                    views=EXCLUDED.views, likes=EXCLUDED.likes,
                                    comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                                    followers_at_post=EXCLUDED.followers_at_post
                            """, (
                                p["post_id"], today, username,
                                p.get("description", "")[:300],
                                p.get("views", 0), p.get("likes", 0),
                                p.get("comments", 0), p.get("saves", 0),
                                p.get("created_at"), account["followers"]
                            ))
                    conn.commit()
        except Exception as e:
            _log(f"get_account_posts failed for @{username}: {e} — skipping posts")
    _log(f"✓ ingested fan account @{username} — {account['followers']:,} followers")
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
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via
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
                else:
                    video_count = raw.get("video_count") or 0
                    title = raw.get("title") or ""
                    author = raw.get("author") or ""

                if video_count == 0:
                    new_status = "inactive"
                else:
                    is_relevant = _classify_sound_match(
                        title, author, song_name, song_artist, video_count,
                        discovered_via=s.get("discovered_via")
                    )
                    if is_relevant:
                        new_status = "approved" if auto_approve else "pending"
                    else:
                        new_status = "inactive"
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
    for s in sounds:
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