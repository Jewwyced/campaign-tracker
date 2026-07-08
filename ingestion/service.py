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

def _normalize_str(s):
    """Lowercase, remove punctuation and extra spaces for comparison."""
    return _re.sub(r'[^a-z0-9 ]', ' ', (s or "").lower()).strip()

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


def _score_sound(sound, title, artist):
    """Score a sound candidate by relevance to the song title and artist.
    Higher score = better match. Used to rank sounds before taking the top 5,
    and also used at qualify-time to gate approval (see RELEVANCE_THRESHOLD
    in qualify_pending_sounds_for_song).

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
                INSERT INTO sounds (song_id, sound_id, title, author, status)
                VALUES (%s,%s,%s,%s,'pending')
                ON CONFLICT (song_id, sound_id) DO NOTHING
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"]))
            row = c.fetchone()
        conn.commit()
    return row["id"] if row else None


def get_or_create_sound(db_conn_factory, song_id, sound):
    """Get existing sound or create new one. Always returns a db id.
    New sounds get status='pending' — monitor decides when to ingest them.
    Existing sounds keep their current status."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status)
                VALUES (%s,%s,%s,%s,'pending')
                ON CONFLICT (song_id, sound_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    author=EXCLUDED.author
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"]))
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
    """Aggressive sound discovery using multiple search queries and pagination.
    Goal: find as many legitimate sounds as possible, store all of them.
    Monitoring will decide which ones to track frequently.
    """
    # Multiple search queries to maximize coverage
    title_clean = title.strip()
    # Clean artist — remove featured artists, take only primary artist
    artist_raw = artist.strip() if artist else ""
    artist_clean = artist_raw.split(',')[0].split('&')[0].split('feat')[0].split('ft.')[0].strip()

    # Generate hashtag variants
    title_hashtag = "#" + title_clean.lower().replace(" ", "")
    artist_hashtag = "#" + artist_clean.lower().replace(" ", "") if artist_clean else ""

    queries = list(dict.fromkeys(filter(None, [
        f"{title_clean} {artist_clean}".strip(),
        title_clean,
        title_hashtag,
        artist_hashtag,
        f"{title_clean} sped up",
        f"{title_clean} slowed",
        f"{title_clean} remix",
    ])))

    seen_ids = set()
    all_sounds = []

    # Method 1: Video search
    for query in queries:
        sounds = discover_sounds_from_videos(query)
        for s in sounds:
            sid = s.get("sound_id")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                all_sounds.append(s)

    # Method 2: Challenge/hashtag crawl (finds many more sounds)
    challenge_sounds = discover_sounds_from_challenge(title, artist)
    for s in challenge_sounds:
        sid = s.get("sound_id")
        if sid and sid not in seen_ids:
            seen_ids.add(sid)
            all_sounds.append(s)

    _log(f"discover_song_sounds: {len(all_sounds)} unique sounds from search + challenge crawl")

    # Score for ranking/logging but store ALL sounds — don't filter aggressively
    def score(s):
        base = _score_sound(s, title, artist)
        freq_bonus = min(s.get("frequency", 0) * 5, 50)
        return base + freq_bonus

    # Sort by score but keep all of them
    ranked_sounds = sorted(all_sounds, key=score, reverse=True)

    _log(f"discover_song_sounds: storing all {len(ranked_sounds)} sounds")
    for i, s in enumerate(ranked_sounds[:5]):
        _log(f"  #{i+1} '{s.get('title')}' score={score(s)} freq={s.get('frequency',0)}")

    # Store ALL sounds as pending — qualify endpoint will promote based on video_count
    stored = 0
    for s in ranked_sounds:
        try:
            sound_db_id = get_or_create_sound(db_conn_factory, song_id, s)
            if sound_db_id:
                stored += 1
        except Exception as e:
            _log(f"EXCEPTION storing sound {s.get('sound_id')} for song {song_id}: {e}")

    _log(f"discover_song_sounds: stored {stored} sounds as pending — run /qualify to promote")
    return ranked_sounds[:stored]


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

def qualify_pending_sounds_for_song(db_conn_factory, song_id):
    """Check every pending sound for one song and promote to approved/inactive
    based on video_count + a relevance SCORE (via _score_sound), not just
    loose keyword substring checks. This is the SAME logic as
    /api/refresh/qualify, extracted here so both the cron route and the
    instant per-song pipeline share one implementation instead of drifting
    apart.

    IMPORTANT: previously this used a substring-based `is_relevant` check
    that included `title_lower == ""` and `"original" in title_lower` as
    automatic passes. Since almost every generic TikTok clip is titled
    "original sound - username", that second condition alone approved
    huge numbers of completely unrelated sounds regardless of actual
    match to the song. Both catch-alls have been removed in favor of a
    real relevance score with a minimum threshold.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.id, snd.sound_id
                FROM sounds snd
                WHERE snd.song_id = %s AND snd.status = 'pending'
            """, (song_id,))
            pending = [dict(r) for r in c.fetchall()]

            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()

    song_name = song_row["name"] if song_row else ""
    song_artist = song_row["artist"] if song_row else ""

    # Minimum score required to approve. _score_sound gives:
    #   150 exact title match, 100 title contained, 40 multi-word match
    #   100 exact artist match, 75 artist words subset, 30 partial artist
    #   50 original-sound bonus, -30 per derivative-version penalty
    # A threshold of 100 means a sound needs a real title OR artist signal —
    # the "original sound" bonus (50) alone is no longer sufficient by itself.
    RELEVANCE_THRESHOLD = 100

    approved = 0
    inactive = 0

    for s in pending:
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
                    is_original = "original sound" in title.lower()
                    score = _score_sound(
                        {"title": title, "author": author, "is_original": is_original},
                        song_name, song_artist
                    )
                    new_status = "approved" if score >= RELEVANCE_THRESHOLD else "inactive"
                    _log(f"  sound {s['id']} '{title}' by '{author}' score={score} -> {new_status}")

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
            else:
                inactive += 1
        except Exception as e:
            _log(f"qualify_pending_sounds_for_song: failed on sound {s['id']}: {e}")

    _log(f"qualify_pending_sounds_for_song: song {song_id} — {approved} approved, {inactive} inactive")
    return {"approved": approved, "inactive": inactive, "checked": len(pending)}


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


def run_full_pipeline_for_song(db_conn_factory, song_id, name, artist=""):
    """The one call the frontend should make right after adding a song:
    discover -> qualify -> ingest posts, all synchronously, all scoped to
    this one song. This is what makes 'add song, it just appears' true."""
    discovered = discover_song_sounds(db_conn_factory, song_id, name, artist or "")
    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id)
    ingest_result = ingest_approved_sounds_for_song(db_conn_factory, song_id)

    return {
        "sounds_discovered": len(discovered),
        "qualify": qualify_result,
        "ingest": ingest_result,
    }