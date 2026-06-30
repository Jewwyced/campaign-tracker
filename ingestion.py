"""
ingestion.py — the ONLY file in this app that talks to TikAPI.

This file owns TIKAPI_KEY and every external HTTP call to TikTok's data.
It is the only "writer" in the system: it pulls data and saves it to Neon.
app.py and every frontend page are read-only consumers of what this file writes —
they never call TikAPI directly, and the key never appears outside this file.

This file does NOT own Song identity or creation — that's services/song_catalog.py
(business operations) and services/song_identity.py (pure identity logic).
song_catalog.py decides WHAT to ingest (which Song, what title/artist to search
with); this file decides HOW (which TikAPI calls to make, how to parse them,
what to write to Neon). Callers tell this file what they want ingested — they
never need to know it works by searching, paginating, etc.

Main entry points:
  - discover_song_sounds(song_id, title, artist) → thin orchestrator: search,
    create, ingest — composed from the three functions below
  - discover_sounds(title, artist) → search TikTok only, no database writes
  - create_sound(song_id, sound) → persist one discovered sound, no search,
    no ingestion
  - ingest_sound(song_id, sound_db_id, tiktok_sound_id) → refresh one Sound's
    posts and video-count snapshot
  - refresh_song_sounds(song_id) → refresh every Sound already belonging to a
    Song (used by the hourly cron)
  - ingest_roster_account(username) → pull profile + recent post stats for an Artist roster account
  - ingest_fan_account(username) → pull stats + posts for a simple (non-roster) tracked account
  - ingest_single_post(post_id, username=None) → pull/update one specific TikTok video
  - ingest_campaign_attached_sound(campaign_id, sound_id) → legacy campaign-level sound pull

Every function logs exactly what it called and what came back (status code,
counts), so a failure is visible in one place instead of bleeding through as a
mysteriously empty page somewhere else in the app.
"""

import os
import re
import requests
from datetime import date

TIKAPI_KEY = os.environ.get("TIKAPI_KEY", "")

def _headers():
    return {
        "X-API-KEY": TIKAPI_KEY,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

def _log(msg):
    print(f"  [ingestion] {msg}", flush=True)


# ── Quota tracking ────────────────────────────────────────────────────────────
# TikAPI's daily cap is shared across every call this app makes. We track our
# own usage in Neon (not in-memory, since Render restarts the process on every
# deploy) so the dashboard can show "X/300 used today" before you hit a 429,
# instead of finding out only after a request fails.

def _record_quota_usage(db_conn_factory, endpoint):
    """Increments today's request counter. Safe to call even if the table doesn't exist yet —
    callers should ensure it's created in app setup; this just no-ops on failure rather than
    blocking the actual TikAPI call."""
    try:
        with db_conn_factory() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO quota_usage (date, endpoint, request_count)
                    VALUES (CURRENT_DATE, %s, 1)
                    ON CONFLICT (date, endpoint) DO UPDATE SET request_count = quota_usage.request_count + 1
                """, (endpoint,))
            conn.commit()
    except Exception as e:
        _log(f"quota tracking failed (non-fatal): {e}")


_quota_db_factory = None

def set_quota_db_factory(db_conn_factory):
    """Called once from app.py at startup so ingestion.py can record usage
    without needing its own database import — keeps db.py as the single
    source of truth for connections."""
    global _quota_db_factory
    _quota_db_factory = db_conn_factory


def _tracked_get(url, params, timeout, endpoint_label):
    """Single choke point for every outbound TikAPI call: makes the request,
    logs the result, and records quota usage — so every endpoint gets this
    behavior automatically instead of each function reimplementing it."""
    r = requests.get(url, params=params, headers=_headers(), timeout=timeout)
    _log(f"{endpoint_label} -> {r.status_code}")
    if r.status_code != 200:
        _log(f"  body: {r.text[:200]}")
    if _quota_db_factory:
        _record_quota_usage(_quota_db_factory, endpoint_label.split()[0])
    return r


# ── Low-level TikAPI calls ────────────────────────────────────────────────────

def _get_check(username):
    r = _tracked_get("https://api.tikapi.io/public/check",
                      {"username": username}, 15, f"check @{username}")
    if r.status_code != 200:
        return None
    return r.json()

def _get_video(post_id):
    r = _tracked_get("https://api.tikapi.io/public/video",
                      {"id": post_id}, 15, f"video id={post_id}")
    if r.status_code != 200:
        return None
    return r.json()

def _get_posts_by_secuid(sec_uid, count=10):
    r = _tracked_get("https://api.tikapi.io/public/posts",
                      {"secUid": sec_uid, "count": count}, 15, f"posts secUid={sec_uid[:12]}...")
    if r.status_code != 200:
        return None
    return r.json()

def _get_music_info(sound_id):
    r = _tracked_get("https://api.tikapi.io/public/music/info",
                      {"id": sound_id}, 30, f"music/info id={sound_id}")
    if r.status_code != 200:
        return None
    return r.json()

def _get_music_posts_page(sound_id, cursor=0, count=30):
    r = _tracked_get("https://api.tikapi.io/public/music",
                      {"id": sound_id, "count": count, "cursor": cursor}, 30,
                      f"music/posts id={sound_id} cursor={cursor}")
    if r.status_code != 200:
        return None
    return r.json()

def _get_search_general(query):
    r = _tracked_get("https://api.tikapi.io/public/search/general",
                      {"query": query}, 20, f"search/general '{query}'")
    if r.status_code != 200:
        return None
    return r.json()


# ── Mid-level parsing helpers ─────────────────────────────────────────────────

def parse_tiktok_url(url):
    """Extract username and post_id from a TikTok video URL."""
    m = re.search(r"tiktok\.com/@([\w.\-]+)/video/(\d+)", url)
    if not m:
        return None, None
    return m.group(1).lower(), m.group(2)

def get_sound_id_from_post(post_id):
    """Given a video post ID, look up the sound/music ID it uses."""
    data = _get_video(post_id)
    if not data:
        return None, None
    item = data.get("itemInfo", {}).get("itemStruct", {})
    music = item.get("music", {})
    music_id = music.get("id")
    return (str(music_id) if music_id is not None else None), music.get("title")

def search_sounds_by_name(query):
    """Search TikTok for videos matching a song name, return distinct sounds found."""
    data = _get_search_general(query)
    if not data or data.get("status") == "error" or "data" not in data:
        return []

    seen_ids = set()
    sounds = []
    for entry in data.get("data", []):
        item = entry.get("item", {})
        music = item.get("music", {})
        sound_id = music.get("id")
        if not sound_id or sound_id in seen_ids:
            continue
        seen_ids.add(sound_id)
        sounds.append({
            "sound_id": str(sound_id),
            "title": music.get("title", "Unknown sound"),
            "author": music.get("authorName", ""),
        })
    _log(f"search '{query}' found {len(sounds)} distinct sounds")
    return sounds

def fetch_sound_info(sound_id):
    """Get metadata about a sound — title, author, true total video count."""
    data = _get_music_info(sound_id)
    if not data:
        return None
    return data

def fetch_sound_posts(sound_id, max_results=30):
    """Get a curated sample of videos using a given sound, up to max_results."""
    posts = []
    cursor = 0
    while len(posts) < max_results:
        data = _get_music_posts_page(sound_id, cursor=cursor, count=30)
        if not data:
            break
        item_struct = data.get("itemStruct", data)
        items = item_struct.get("itemList", [])
        if not items:
            break
        for item in items:
            s = item.get("stats", {})
            author = item.get("author", {})
            author_stats = item.get("authorStats", {})
            video_info = item.get("video", {})
            posts.append({
                "post_id": item.get("id"),
                "username": author.get("uniqueId", ""),
                "description": item.get("desc", "")[:300],
                "views": s.get("playCount", 0),
                "likes": s.get("diggCount", 0),
                "comments": s.get("commentCount", 0),
                "saves": s.get("collectCount", 0),
                "shares": s.get("shareCount", 0),
                "created_at": item.get("createTime"),
                "thumbnail": video_info.get("cover"),
                "followers": author_stats.get("followerCount"),
            })
        if not item_struct.get("hasMore"):
            break
        next_cursor = item_struct.get("cursor")
        cursor = int(next_cursor) if next_cursor is not None else cursor + 30
    _log(f"fetch_sound_posts id={sound_id} -> got {len(posts)} posts (requested {max_results})")
    return posts[:max_results]


# ── High-level ingestion functions — these are what callers should use ───────

def _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id):
    """
    Pull a sound's true total video count (cheap, authoritative TikAPI call —
    used for growth tracking, not a sample) and write today's snapshot.

    Single responsibility: the video-count side of refreshing a sound. Does
    not touch posts at all. Returns a dict describing what happened, so the
    orchestrator can merge it into the overall result.
    """
    info = fetch_sound_info(tiktok_sound_id)
    if not info:
        return {"video_count_updated": False, "error": "music/info call failed — see logs above for status code"}

    music_info = info.get("musicInfo", info)
    stats = music_info.get("stats", {})
    video_count = stats.get("videoCount")
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
    """
    Pull a curated sample of posts for a sound and save them to Neon.

    Single responsibility: the posts side of refreshing a sound. Does not
    touch video-count/song_stats at all. Returns how many posts were added.
    """
    posts = fetch_sound_posts(tiktok_sound_id, max_results=max_results)
    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in posts:
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
                    p.get("created_at"), p.get("thumbnail"), p.get("shares", 0), sound_db_id, p.get("followers")
                ))
                added += 1
        conn.commit()
    return added


def ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id, max_results=30):
    """
    Refresh one Sound's posts and video-count snapshot.

    Orchestrator only — composes two single-purpose steps:
      1. _update_sound_video_count() — the growth-tracking snapshot
      2. _ingest_sound_posts() — the curated post sample for display

    Returns a dict summarizing what happened, so the caller can log/report it.
    """
    result = {"sound_db_id": sound_db_id, "video_count_updated": False, "posts_added": 0, "error": None}

    stats_result = _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id)
    result.update(stats_result)

    result["posts_added"] = _ingest_sound_posts(db_conn_factory, sound_db_id, tiktok_sound_id, max_results)

    return result


def discover_sounds(query):
    """
    Search TikTok for sounds matching a query string.

    Single responsibility: execute a search, return candidates. Does not
    touch the database, does not ingest anything, and does not construct
    the query itself — that's the caller's job (song_catalog.py knows about
    titles and artists; this function just knows how to search TikTok with
    whatever string it's given).
    """
    return search_sounds_by_name(query)


def create_sound(db_conn_factory, song_id, sound):
    """
    Create one Sound row under a Song, if it doesn't already exist.

    Single responsibility: persist a discovered sound. Does not search
    TikTok, does not ingest posts — just the one INSERT. Returns the new
    sound's database id, or None if it already existed (ON CONFLICT DO
    NOTHING means no row, and we shouldn't re-ingest something already
    being tracked just because it showed up in a search again).
    """
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


def discover_song_sounds(db_conn_factory, song_id, title, artist=""):
    """
    Find every TikTok sound for a Song and ingest each one.

    A thin orchestrator over three single-purpose steps:
      1. discover_sounds(query) — search TikTok, find candidates
      2. create_sound() — persist each new one
      3. ingest_sound() — pull its posts and stats

    The caller (song_catalog.py) tells us WHAT to ingest — a song_id, plus
    its title/artist so we know what to search for. This orchestrator is
    where title/artist becomes a search query — discover_sounds() itself
    just executes whatever query string it's handed, it doesn't know what
    a "title" or "artist" is. That keeps query construction here, one level
    up from raw search execution, while still keeping song_catalog.py
    completely unaware that TikTok search even works by query string.

    Returns the list of sounds that were successfully created + ingested.
    """
    query = f"{title} {artist}".strip()
    found_sounds = discover_sounds(query)
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
    """
    Refresh every Sound already belonging to a Song — used by the hourly
    cron. Unlike discover_song_sounds, this doesn't search TikTok again, it
    just re-pulls fresh stats/posts for sounds we already know about.
    """
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
    data = _get_check(username)
    if not data:
        return False

    info = data.get("userInfo", {})
    stats = info.get("statsV2", info.get("stats", {}))
    sec_uid = info.get("user", {}).get("secUid")
    followers = int(stats.get("followerCount", 0))
    total_likes = int(stats.get("heartCount", 0))
    video_count = int(stats.get("videoCount", 0))
    today = date.today()

    views_sum = likes_sum = comments_sum = shares_sum = 0
    if sec_uid:
        posts_data = _get_posts_by_secuid(sec_uid, count=10)
        if posts_data:
            items = posts_data.get("itemList") or posts_data.get("items") or []
            for item in items:
                s = item.get("stats", {})
                views_sum += s.get("playCount", 0)
                likes_sum += s.get("diggCount", 0)
                comments_sum += s.get("commentCount", 0)
                shares_sum += s.get("shareCount", 0)

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO roster_stats (username, date, followers, total_likes, video_count, views_24h, likes_24h, comments_24h, shares_24h)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, total_likes=EXCLUDED.total_likes,
                    video_count=EXCLUDED.video_count, views_24h=EXCLUDED.views_24h,
                    likes_24h=EXCLUDED.likes_24h, comments_24h=EXCLUDED.comments_24h, shares_24h=EXCLUDED.shares_24h
            """, (username, today, followers, total_likes, video_count, views_sum, likes_sum, comments_sum, shares_sum))
        conn.commit()
    return True

def ingest_fan_account(db_conn_factory, username):
    """Pull stats for a plain (non-roster) tracked fan account, plus its recent posts —
    used by the original artists/stats tables (the fan-tracker-style feature)."""
    data = _get_check(username)
    if not data:
        return False
    info = data.get("userInfo", {})
    user = info.get("user", {})
    stats = info.get("statsV2", info.get("stats", {}))
    sec_uid = user.get("secUid")
    followers = int(stats.get("followerCount", 0))
    likes = int(stats.get("heartCount", 0))
    videos = int(stats.get("videoCount", 0))
    today = date.today()

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO stats (username, date, followers, likes, videos)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, likes=EXCLUDED.likes, videos=EXCLUDED.videos
            """, (username, today, followers, likes, videos))
        conn.commit()

    if sec_uid:
        posts_data = _get_posts_by_secuid(sec_uid, count=10)
        if posts_data:
            items = posts_data.get("itemList") or posts_data.get("items") or []
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    for item in items:
                        ps = item.get("stats", {})
                        c.execute("""
                            INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (post_id) DO UPDATE SET
                                views=EXCLUDED.views, likes=EXCLUDED.likes,
                                comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                                followers_at_post=EXCLUDED.followers_at_post
                        """, (
                            item.get("id"), today, username,
                            item.get("desc","")[:300],
                            ps.get("playCount",0), ps.get("diggCount",0),
                            ps.get("commentCount",0), ps.get("collectCount",0),
                            item.get("createTime"), followers
                        ))
                conn.commit()
    _log(f"✓ ingested fan account @{username} — {followers:,} followers")
    return True

def ingest_single_post(db_conn_factory, post_id, username=None):
    """Pull/update one specific TikTok video by its post ID (used for manually-pasted links)."""
    data = _get_video(post_id)
    if not data:
        return False
    item = data.get("itemInfo", {}).get("itemStruct", {})
    if not item:
        return False

    s = item.get("stats", {})
    author = item.get("author", {})
    author_stats = item.get("authorStats", {})
    video_info = item.get("video", {})
    resolved_username = author.get("uniqueId", username)
    followers = author_stats.get("followerCount")
    thumbnail = video_info.get("cover")
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
                post_id, today, resolved_username,
                item.get("desc","")[:300],
                s.get("playCount",0), s.get("diggCount",0),
                s.get("commentCount",0), s.get("collectCount",0),
                item.get("createTime"), followers, thumbnail, s.get("shareCount", 0)
            ))
        conn.commit()
    _log(f"✓ ingested post {post_id} by @{resolved_username}")
    return True

def ingest_campaign_attached_sound(db_conn_factory, campaign_id, tiktok_sound_id, max_results=30, sound_db_id=None):
    """Legacy path: pull a sound's posts directly into a campaign (pre-Songs-restructure)."""
    posts = fetch_sound_posts(tiktok_sound_id, max_results=max_results)
    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in posts:
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
                    p.get("created_at"), campaign_id, p.get("thumbnail"), p.get("shares", 0), sound_db_id
                ))
                added += 1
        conn.commit()
    return added