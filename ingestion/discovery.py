"""
ingestion/discovery.py — the Discovery boundary.

MOVED (provider-boundary refactor, discovery-boundary step) from
ingestion/service.py — pure relocation, no behavior changes. Every
function here is byte-for-byte identical to its previous version in
service.py; only the file location and imports changed (shared helpers
now come from ._shared instead of being defined locally, to avoid a
circular import with service.py — see _shared.py's docstring).

Discovery's job, and ONLY job: produce candidate sound rows. Nothing
here fingerprints, qualifies, approves, or ingests posts — every
candidate lands in the same 'pending'/'discovered' queue regardless of
which sensor found it, and gets picked up by qualification (still in
service.py today) exactly the same way.

Sensors in this file, in the order a new song's discovery actually
uses them (see initialize_song / discover_song_sounds):
  - discover_song_sounds        — orchestrates the two below
      - discover_sounds_from_videos     (search-based)
      - discover_sounds_from_challenge  (song's OWN hashtags, e.g. #backhome)
  - discover_community_sounds_for_song  — FIXED generic derivative-genre
    hashtags (#slowedreverb, #nightcore, etc.) — a DIFFERENT idea from
    discover_sounds_from_challenge above; don't conflate the two, see
    conversation/handoff notes on why they're deliberately separate.
  - discover_via_creator_graph  — BUILT, has its own route
    (/api/refresh/creator_graph), but not called by initialize_song or
    find_new_sounds_for_song — dormant today, worth evaluating before
    wiring in, not wired in as part of this refactor.

Also contains:
  - discover_sounds             — thin top-level search wrapper
  - create_sound / get_or_create_sound — candidate persistence, used by
    every sensor above
  - _is_plausible_candidate     — discovery-time filter (NOT the same as
    _could_possibly_qualify, which stays in service.py/qualification —
    different jobs, deliberately separate, see its own docstring)
  - _promote_top_sounds         — UNUSED by anything today, internal or
    external. Likely superseded by qualify_pending_sounds_for_song's own
    approval logic once that was built. Moved as-is, not deleted, not
    wired to anything — flagging here rather than silently carrying it
    forward with no note.
"""

from .providers import default_provider as provider
from .tiklive_provider import TikLiveAPIProvider as _TikLiveProvider
_tiklive = _TikLiveProvider()
from .parsers import parse_sounds_from_search, parse_sounds_from_post_feed
from ._shared import _log, _normalize_str, _score_sound, _artist_signal

# Max plausible candidates to persist per discovery run. Even after the
# no-API-call plausibility filter (_could_possibly_qualify, in
# service.py), a common title can still leave more "technically
# eligible" generic uploads than are worth tracking — but the previous
# cap of 30 was tuned for "avoid database bloat," not "give a reviewer
# a real pool to work through."
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






# ── Community Discovery — second discovery sensor ─────────────────────────
# Validated via real experiment (4 hashtags: slowedreverb, nightcore, spedup,
# phonk — ~300 posts sampled each) before being wired in here. Findings that
# shaped this implementation:
#   - Title/artist search structurally misses this content: derivative-audio
#     edits routinely get uploaded under an unrelated "original sound -
#     randomuser" name with zero textual connection to the real song.
#   - Frequency is NOT a useful ranking signal here — ~98% of sounds in
#     every hashtag tested appeared exactly once, so "which sound repeats
#     most" barely discriminates at all.
#   - Engagement (views/likes/comments/shares) DOES work — consistently
#     surfaced real, coherent, identifiable songs at the top across all 4
#     hashtags tested, unlike frequency.
#   - Some accounts post many DISTINCT sounds within one hashtag (hub
#     accounts) — worth noting for a future sensor, but this function does
#     NOT persist a hub-accounts table yet, per the "ship the simple
#     version this week" call — that becomes valuable once this sensor is
#     actually running, not before.
#
# Architecture: this is ONE independent sensor among several (title
# search, this, eventually creator graph / other sensors). It does ONLY
# ONE job — produce candidate `sounds` rows — using the exact same
# get_or_create_sound() storage path and discovered_via/discovery_query
# Discovery Memory fields discover_song_sounds already uses. It never
# calls fingerprinting itself and never auto-approves anything: every
# candidate lands in the same 'pending' queue and gets picked up by the
# existing fingerprint backlog worker exactly like a title-search
# candidate would, regardless of which sensor found it.
COMMUNITY_DISCOVERY_HASHTAGS = ["slowedreverb", "nightcore", "spedup", "phonk"]
COMMUNITY_DISCOVERY_MAX_PAGES = 20       # ~300-360 posts/hashtag at count=35/page — the exact scale validated
COMMUNITY_DISCOVERY_MAX_CANDIDATES_PER_HASHTAG = 15  # top-by-engagement only; NOT every unique sound found


def _adapt_challenge_video(v):
    """Normalizes one raw TikLive challenge-post video into the
    itemStruct-item shape parse_sounds_from_post_feed expects. Same
    adapter validated in the diagnostic experiment — kept local to this
    module rather than changing tiklive_provider.py's get_challenge_posts
    return shape, since that's a larger interface-standardization change
    deliberately deferred until after this sensor proves itself in
    production (see conversation/handoff notes)."""
    music_info = v.get("music_info") or {}
    author = v.get("author", {}) if isinstance(v.get("author"), dict) else {}
    return {
        "id": v.get("video_id"),
        "desc": (v.get("title") or "")[:300],
        "createTime": v.get("create_time"),
        "stats": {
            "playCount": int(v.get("play_count") or 0),
            "diggCount": int(v.get("digg_count") or 0),
            "commentCount": int(v.get("comment_count") or 0),
            "shareCount": int(v.get("share_count") or 0),
        },
        "author": {"uniqueId": author.get("unique_id", "")},
        "music": {
            "id": music_info.get("id"),
            "title": music_info.get("title"),
            "authorName": music_info.get("author"),
        },
    }


def _community_engagement_score(e):
    """First-pass weighting — comments/shares weighted higher than raw
    views, matching the diagnostic experiment. Not tuned against real
    fingerprint-match outcomes yet; worth revisiting once there's
    production data to check it against."""
    return e["total_views"] + e["total_likes"] * 3 + e["total_comments"] * 5 + e["total_shares"] * 5


def discover_community_sounds_for_song(db_conn_factory, song_id, name, artist=""):
    """Community Discovery sensor — scans a fixed set of derivative-audio
    genre hashtags for sound candidates, completely independent of
    whether they textually match this song's title/artist. This is what
    reaches the blind spot title search structurally cannot (see module
    notes above). Calls TikLiveAPIProvider directly rather than through
    BaseProvider/ProviderPipeline — that interface standardization is
    deliberately deferred to Phase B, after this sensor proves out in
    production, not before.

    Returns the list of new/touched sound db ids (for logging/reporting
    only — callers should NOT branch on this beyond counting).
    """
    from .tiklive_provider import TikLiveAPIProvider
    from .parsers import parse_sounds_from_post_feed
    from collections import defaultdict

    provider = TikLiveAPIProvider()
    touched_ids = []

    for hashtag in COMMUNITY_DISCOVERY_HASHTAGS:
        challenges = provider.search_challenge(hashtag)
        if not challenges:
            _log(f"community discovery: no challenge found for #{hashtag}, skipping")
            continue

        exact = next((c for c in challenges if c.get("cha_name", "").lower() == hashtag.lower()), None)
        chosen = exact or challenges[0]
        challenge_id = chosen.get("id")

        sound_tally = defaultdict(lambda: {"title": None, "author": None,
                                            "total_views": 0, "total_likes": 0,
                                            "total_comments": 0, "total_shares": 0})
        cursor = 0
        for _page in range(COMMUNITY_DISCOVERY_MAX_PAGES):
            videos, has_more, next_cursor = provider.get_challenge_posts(challenge_id, cursor=cursor, count=35)
            if not videos:
                break

            adapted = [_adapt_challenge_video(v) for v in videos]
            feed = {"itemStruct": {"itemList": adapted}}
            for s in parse_sounds_from_post_feed(feed):
                entry = sound_tally[s["sound_id"]]
                entry["title"] = s["sound_title"]
                entry["author"] = s["sound_author"]
                entry["total_views"] += s["views"]
                entry["total_likes"] += s["likes"]
                entry["total_comments"] += s["comments"]
                entry["total_shares"] += s["shares"]

            if not has_more:
                break
            cursor = next_cursor

        ranked = sorted(sound_tally.items(), key=lambda kv: _community_engagement_score(kv[1]), reverse=True)
        top = ranked[:COMMUNITY_DISCOVERY_MAX_CANDIDATES_PER_HASHTAG]

        for sound_id, e in top:
            sound_db_id = get_or_create_sound(db_conn_factory, song_id, {
                "sound_id": sound_id,
                "title": e["title"],
                "author": e["author"],
                "discovered_via": "community_hashtag",
                "discovery_query": f"#{hashtag}",
                "discovery_sort_by": "engagement",
            })
            if sound_db_id:
                touched_ids.append(sound_db_id)

        _log(f"community discovery #{hashtag}: {len(sound_tally)} unique sounds seen, "
             f"top {len(top)} persisted as pending candidates")

    return touched_ids