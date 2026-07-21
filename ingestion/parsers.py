"""
ingestion_parsers.py — Layer 2: parsing and normalization.

Pure transformation functions only. Every function in this file:
  - Takes raw JSON (or a plain string) as input
  - Returns clean, normalized Python objects as output
  - Never calls the network
  - Never imports from ingestion_client
  - Has zero side effects

This file knows TikAPI's response shapes (which fields exist, what they're
called), but it does NOT know or care where the data came from. Whether the
raw JSON arrived from TikAPI, a scraper, a cache, or a test fixture makes
no difference here. That's the point — when a second provider gets added,
the parsers don't change, only the client layer does.

The service layer (ingestion_service.py) owns the fetch → parse → store
flow. It calls the client to get raw JSON, passes that JSON to these
functions to get clean objects, then writes those objects to Neon.
"""

import re


def parse_tiktok_url(url):
    """Extract username and post_id from a TikTok video URL.
    Pure string parsing — no network calls."""
    m = re.search(r"tiktok\.com/@([\w.\-]+)/video/(\d+)", url)
    if not m:
        return None, None
    return m.group(1).lower(), m.group(2)


def parse_sound_id_from_video(raw_video_response):
    """Given raw JSON from a video endpoint, extract the sound/music ID it uses.
    Caller is responsible for fetching the raw response; this just extracts."""
    if not raw_video_response:
        return None, None
    item = raw_video_response.get("itemInfo", {}).get("itemStruct", {})
    music = item.get("music", {})
    music_id = music.get("id")
    return (str(music_id) if music_id is not None else None), music.get("title")


def parse_sounds_from_search(raw_search_response):
    """Given raw JSON from a search/general endpoint, return distinct sounds found.
    Caller is responsible for fetching the raw response; this just normalizes."""
    if not raw_search_response:
        return []
    if raw_search_response.get("status") == "error" or "data" not in raw_search_response:
        return []

    seen_ids = set()
    sounds = []
    for entry in raw_search_response.get("data", []):
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
    return sounds


def parse_sound_info(raw_music_info_response):
    if not raw_music_info_response:
        return None
    
    # TikLive flat format
    if "video_count" in raw_music_info_response or "id" in raw_music_info_response:
        return {
            "title": raw_music_info_response.get("title"),
            "author": raw_music_info_response.get("author"),
            "video_count": raw_music_info_response.get("video_count"),
            # Direct playable audio URL for the sound itself, if TikLive
            # returned one. Previously discarded here — this is the field
            # audio fingerprinting needs to actually fetch the sound.
            "play_url": raw_music_info_response.get("play"),
        }
    
    # TikAPI nested format
    music_info = raw_music_info_response.get("musicInfo", raw_music_info_response)
    music = music_info.get("music", {})
    stats = music_info.get("stats", {})
    return {
        "title": music.get("title"),
        "author": music.get("authorName"),
        "video_count": stats.get("videoCount"),
        "play_url": music.get("playUrl"),
    }


def parse_posts_from_music_page(raw_music_page_response):
    """Given raw JSON from one page of a music/posts endpoint, return a list
    of normalized post dicts. Returns (posts, has_more, next_cursor)."""
    if not raw_music_page_response:
        return [], False, None

    item_struct = raw_music_page_response.get("itemStruct", raw_music_page_response)
    items = item_struct.get("itemList", [])
    has_more = bool(item_struct.get("hasMore"))
    next_cursor = item_struct.get("cursor")

    posts = []
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
    return posts, has_more, next_cursor


def parse_sounds_from_post_feed(raw_feed_response):
    """Given a normalized post-feed response (itemStruct.itemList, or a bare
    itemList — both shapes already used elsewhere in this file), extract
    each post's sound alongside minimal post context.

    Shared by Community Discovery (challenge/hashtag posts) and Creator
    Graph (account posts) — both need the same underlying thing: which
    sound did THIS post use, not just its engagement stats. Neither
    parse_posts_from_music_page (built for pages where the sound is
    already known and never extracts one) nor parse_posts_from_user_feed
    (drops the music field entirely, despite get_account_posts already
    preserving it) currently do this — this is the one function that does,
    so both discovery engines produce identically-shaped sound candidates
    without duplicating extraction logic between them.

    Posts with no usable music data are skipped, not included as None
    entries — a post can genuinely have no sound (rare, but the API
    doesn't guarantee one), and callers tallying sound frequency across a
    feed shouldn't have to filter Nones out themselves.
    """
    if not raw_feed_response:
        return []

    item_struct = raw_feed_response.get("itemStruct", raw_feed_response)
    items = item_struct.get("itemList", [])

    results = []
    for item in items:
        music = item.get("music", {})
        sound_id = music.get("id")
        if not sound_id:
            continue
        author = item.get("author", {})
        s = item.get("stats", {})
        results.append({
            "sound_id": str(sound_id),
            "sound_title": music.get("title", "Unknown sound"),
            "sound_author": music.get("authorName", ""),
            "post_id": item.get("id"),
            "username": author.get("uniqueId", ""),
            "views": s.get("playCount", 0),
        })
    return results


def parse_account_stats(raw_check_response):
    """Given raw JSON from a user/check endpoint, return normalized account stats.
    Returns None if the response is missing or malformed."""
    if not raw_check_response:
        return None
    info = raw_check_response.get("userInfo", {})
    user = info.get("user", {})
    stats = info.get("statsV2", info.get("stats", {}))
    return {
        "sec_uid": user.get("secUid"),
        "followers": int(stats.get("followerCount", 0)),
        "total_likes": int(stats.get("heartCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
    }


def parse_posts_from_user_feed(raw_posts_response):
    """Given raw JSON from a user posts endpoint, return a list of normalized
    post engagement dicts."""
    if not raw_posts_response:
        return []
    items = raw_posts_response.get("itemList") or raw_posts_response.get("items") or []
    posts = []
    for item in items:
        s = item.get("stats", {})
        posts.append({
            "post_id": item.get("id"),
            "description": item.get("desc", "")[:300],
            "views": s.get("playCount", 0),
            "likes": s.get("diggCount", 0),
            "comments": s.get("commentCount", 0),
            "shares": s.get("shareCount", 0),
            "created_at": item.get("createTime"),
        })
    return posts


def parse_single_post(raw_video_response, fallback_username=None):
    """Given raw JSON from a video endpoint, return a normalized post dict.
    Returns None if the response is missing or the item struct is absent."""
    if not raw_video_response:
        return None
    item = raw_video_response.get("itemInfo", {}).get("itemStruct", {})
    if not item:
        return None
    s = item.get("stats", {})
    author = item.get("author", {})
    author_stats = item.get("authorStats", {})
    video_info = item.get("video", {})
    return {
        "post_id": item.get("id"),
        "username": author.get("uniqueId", fallback_username),
        "description": item.get("desc", "")[:300],
        "views": s.get("playCount", 0),
        "likes": s.get("diggCount", 0),
        "comments": s.get("commentCount", 0),
        "saves": s.get("collectCount", 0),
        "shares": s.get("shareCount", 0),
        "created_at": item.get("createTime"),
        "thumbnail": video_info.get("cover"),
        "followers": author_stats.get("followerCount"),
    }