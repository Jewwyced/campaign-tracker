"""
ingestion/service.py — Layer 3: service / orchestration.

Owns all business logic and database writes. Calls parsers to get clean
data, then decides what to write to Neon and what to return.

This file decides WHAT happens — which rows to insert/upsert, what to
return to callers, what counts as success or failure. It does not know
or care how providers work internally (that's providers.py) or how to
parse their responses (that's parsers.py).

Public functions:
  - discover_sounds(query)
  - create_sound(db_conn_factory, song_id, sound)
  - ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id)
  - discover_song_sounds(db_conn_factory, song_id, title, artist)
  - refresh_song_sounds(db_conn_factory, song_id)
  - ingest_roster_account(db_conn_factory, username)
  - ingest_fan_account(db_conn_factory, username)
  - ingest_single_post(db_conn_factory, post_id, username)
  - ingest_campaign_attached_sound(db_conn_factory, campaign_id, tiktok_sound_id)

Known deferred improvements:
  - song_id parameter on ingest_sound() is now unused — legacy, kept for
    backward compatibility with existing callers
  - _update_sound_video_count() and _ingest_sound_posts() still combine
    fetching, parsing, and persistence — acceptable for now, worth splitting
    when they grow more complex
  - Cache freshness only implemented for sounds — roster accounts, fan
    accounts, and single posts still always hit the provider pipeline
  - Result objects are not yet standardized across all functions — ingest_sound
    returns a rich dict, other functions return True/False or a list
"""

from datetime import date, datetime, timezone
from .providers import default_provider as provider
from .parsers import (
    parse_sounds_from_search,
    parse_sound_info,
    parse_posts_from_music_page,
    parse_account_stats,
    parse_posts_from_user_feed,
    parse_single_post,
)

# ── Constants ─────────────────────────────────────────────────────────────────

SOUND_FRESHNESS_HOURS = 6  # skip re-ingestion if sound was ingested within this window

SOURCE_CACHE    = "cache"
SOURCE_TIKAPI   = "tikapi"
SOURCE_FALLBACK = "fallback"


def _log(msg):
    print(f"  [ingestion] {msg}", flush=True)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_sound_fresh(db_conn_factory, sound_db_id):
    """Check whether a sound's cached data is fresh enough to skip re-ingestion.
    Returns (is_fresh, age_hours) — age_hours is None if never ingested."""
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


def _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id):
    """Pull a sound's true total video count and write today's snapshot."""
    raw = provider.get_sound_info(tiktok_sound_id)
    info = parse_sound_info(raw)
    if not info:
        return {"video_count_updated": False, "error": "music/info call failed — see logs above for status code"}

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
    """Pull a curated sample of posts for a sound and save them to Neon."""
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

    _log(f"fetch_sound_posts id={tiktok_sound_id} -> got {len(all_posts)} posts (requested {max_results})")
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

def discover_sounds(query):
    """Search TikTok for sounds matching a query. No database writes."""
    raw = provider.search_sounds(query)
    sounds = parse_sounds_from_search(raw)
    _log(f"search '{query}' found {len(sounds)} distinct sounds")
    return sounds


def create_sound(db_conn_factory, song_id, sound):
    """Persist one discovered sound. Returns new db id, or None if already existed."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO sounds (song_id, sound_id, title, author, status)
                VALUES (%s,%s,%s,%s,'approved')
                ON CONFLICT (song_id, sound_id) DO NOTHING
                RETURNING id
            """, (song_id, sound["sound_id"], sound["title"], sound["author"]))
            row = c.fetchone()
        conn.commit()
    return row["id"] if row else None


def ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id, max_results=30):
    """Refresh one Sound's posts and video-count snapshot.
    Checks cache first — skips the provider pipeline entirely if data is fresh.

    Note: song_id is kept for backward compatibility but is currently unused.
    """
    is_fresh, age_hours = _is_sound_fresh(db_conn_factory, sound_db_id)
    is_fresh, age_hours = _is_sound_fresh(db_conn_factory, sound_db_id)
    _log(f"cache check sound={sound_db_id} fresh={is_fresh} age={age_hours}")
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

    # Update timestamp so the next call can use the cache
    if not result.get("error"):
        _touch_sound_ingested(db_conn_factory, sound_db_id)

    return result


def discover_song_sounds(db_conn_factory, song_id, title, artist=""):
    """Find every TikTok sound for a Song and ingest each one."""
    query = f"{title} {artist}".strip()
    found_sounds = discover_sounds(query)[:5]  # top 5 sounds only
    results = []
    for s in found_sounds:
        try:
            sound_db_id = create_sound(db_conn_factory, song_id, s)
            if sound_db_id:
                ingest_result = ingest_sound(db_conn_factory, song_id, sound_db_id, s["sound_id"], max_results=30)
                results.append({**s, **ingest_result})
        except Exception as e:
            _log(f"EXCEPTION ingesting sound {s.get('sound_id')} for song {song_id}: {e}")
    return results


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