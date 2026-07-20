"""
ingestion/tiklive_provider.py — TikLiveAPI provider implementation.

Implements BaseProvider using TikLiveAPI (tikliveapi.com) as the data source.
Designed as an additional provider for discovery operations and failover
when another provider is unavailable.

All field names verified against actual TikLiveAPI response examples.
"""

import os
import re
import json
import requests

TIKLIVEAPI_KEY = os.environ.get("TIKLIVEAPI_KEY", "")
BASE_URL = "https://api.tikliveapi.com"


def _log(msg):
    print(f"  [tiklive] {msg}", flush=True)


class TikLiveAPIProvider:
    """TikLiveAPI implementation of BaseProvider."""

    def _headers(self):
        return {
            "X-Api-Key": TIKLIVEAPI_KEY,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        }

    def _get(self, path, params):
        """Make a GET request with one retry on timeout.
        Returns parsed JSON or None on failure."""
        url = f"{BASE_URL}{path}"
        for attempt in range(2):
            try:
                r = requests.get(url, params=params, headers=self._headers(), timeout=(5, 10))
                _log(f"{path} -> {r.status_code}")
                if r.status_code == 429:
                    _log("  rate limited")
                    return None
                if r.status_code != 200:
                    _log(f"  error: {r.text[:200]}")
                    return None
                return r.json()
            except requests.Timeout:
                if attempt == 0:
                    _log(f"{path} timed out, retrying...")
                    continue
                _log(f"{path} timed out after retry")
                return None
            except Exception as e:
                _log(f"  exception: {e}")
                return None
        return None

    def get_sound_info(self, sound_id):
        """Returns raw TikAPI-compatible response for parse_sound_info."""
        data = self._get("/music-info/", {"music_id": sound_id})
        if not data:
            return None
        return {
            "musicInfo": {
                "music": {
                    "title": data.get("title"),
                    "authorName": data.get("author"),
                    "coverLarge": data.get("cover"),
                    "playUrl": data.get("play"),
                    "duration": data.get("duration"),
                },
                "stats": {
                    "videoCount": data.get("video_count"),
                }
            }
        }

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        """Returns one page of posts normalized to TikAPI itemStruct shape.
        Verified against actual TikLive /music-posts/ response structure."""
        data = self._get("/music-posts/", {
            "music_id": sound_id,
            "count": min(count, 30),
            "cursor": cursor,
        })
        if not data:
            return None

        videos = data.get("videos", [])
        _log(f"music-posts got {len(videos)} videos for sound {sound_id}")

        items = []
        for v in videos:
            author = v.get("author", {})
            items.append({
                "id": v.get("video_id"),
                # FIXED 7/18 — was v.get("title", "")[:300], which crashed
                # production's monitor cron: .get()'s default only fires
                # when the key is MISSING, not when TikTok returns the key
                # with an explicit null value. `(v.get("title") or "")`
                # safely handles both cases.
                "desc": (v.get("title") or "")[:300],
                "createTime": v.get("create_time"),
                "stats": {
                    "playCount": int(v.get("play_count") or 0),
                    "diggCount": int(v.get("digg_count") or 0),
                    "commentCount": int(v.get("comment_count") or 0),
                    "collectCount": int(v.get("collect_count") or 0),
                    "shareCount": int(v.get("share_count") or 0),
                },
                "author": {
                    "uniqueId": author.get("unique_id", "") if isinstance(author, dict) else "",
                },
                "authorStats": {},
                "video": {
                    "cover": v.get("cover") or "",
                },
            })

        # TikLive returns hasMore and cursor at top level
        has_more = bool(data.get("hasMore", False))
        next_cursor = data.get("cursor")

        return {
            "itemStruct": {
                "itemList": items,
                "hasMore": has_more,
                "cursor": next_cursor,
            }
        }


    def search_sounds(self, query, max_pages=30, min_new_per_page=3, low_yield_tolerance=6):
        """Search videos by keyword with adaptive pagination.
        Stops early when new sounds per page drops below min_new_per_page
        for `low_yield_tolerance` consecutive pages.
        Retries once on timeout to handle transient TikLive failures.

        LOOSENED 7/20 — this was a SECOND, deeper early-stop mechanism we
        hadn't touched when the outer discover_song_sounds loop's
        EARLY_STOP_CANDIDATE_THRESHOLD was removed earlier. Even with
        every source now running unconditionally, each INDIVIDUAL query
        was still quietly cutting itself short after just 2 consecutive
        low-yield pages — capping real depth well below what max_pages
        would otherwise allow, and very likely the actual reason a scan
        was surfacing ~50 candidates instead of hundreds. Raised
        low_yield_tolerance from a hardcoded 2 to 6, and max_pages default
        from 15 to 30 — same "verification is cheap now, worth digging
        deeper" economics already applied everywhere else tonight. Real
        cost note: at count=35/page, 30 pages is up to 1,050 raw results
        per single query — a real, meaningful increase in both API calls
        and per-click time, not free.
        """
        seen_ids = set()
        items = []
        cursor = 0
        page = 0
        consecutive_low_yield = 0

        while page < max_pages:
            data = self._get("/search-video/", {
                "keyword": query,
                "count": 35,
                "cursor": cursor,
                "publish_time": 0,   # All time for discovery — gets more sounds
                "sort_by": 1,        # Like count — surfaces high-engagement videos,
                                     # far more likely to be using a real, widely-used
                                     # sound than just whatever was posted most recently.
                                     # (TikLive sort_by values: 0=relevance, 1=like
                                     # count, 2=date posted — this was previously 2,
                                     # which explains why discovery kept surfacing only
                                     # low-volume, very-recent posts.)
            })

            if not data:
                break

            videos = data.get("videos", [])
            new_this_page = 0

            for v in videos:
                # Use music_info.id — the real music ID
                # NOT the music URL which contains the audio file ID (different!)
                music_info = v.get("music_info", {})
                music_id = music_info.get("id") if music_info else None

                if not music_id or music_id in seen_ids:
                    continue
                seen_ids.add(music_id)
                new_this_page += 1
                items.append({
                    "item": {
                        "music": {
                            "id": music_id,
                            # Same class of bug as get_sound_posts_page above —
                            # fixed 7/18.
                            "title": (music_info.get("title") or "")[:50],
                            "authorName": music_info.get("author") or "",
                        }
                    }
                })

            duplicates = len(videos) - new_this_page
            _log(f"search page {page+1}: {len(videos)} videos, {new_this_page} new, {duplicates} duplicates (total {len(items)})")

            # Adaptive stop — configurable minimum yield, configurable tolerance
            if new_this_page < min_new_per_page:
                consecutive_low_yield += 1
                if consecutive_low_yield >= low_yield_tolerance:
                    _log(f"stopping early — yield below {min_new_per_page} for "
                         f"{low_yield_tolerance} consecutive pages")
                    break
            else:
                consecutive_low_yield = 0

            has_more = bool(data.get("hasMore", False))
            if not has_more or not videos:
                break
            cursor = data.get("cursor", 0)
            page += 1

        _log(f"search_sounds '{query}': {len(items)} distinct sounds from {page+1} pages")
        return {"data": items}


    def search_challenge(self, keyword):
        """Search for a hashtag challenge by keyword. Returns list of challenges with IDs."""
        data = self._get("/search-challenge/", {"keyword": keyword, "count": 5})
        if not data:
            return []
        return data.get("challenge_list", [])

    def get_challenge_posts(self, challenge_id, cursor=0, count=35):
        """Get posts for a hashtag challenge. Returns videos with music_info."""
        data = self._get("/challenge-posts/", {
            "challenge_id": challenge_id,
            "count": min(count, 35),
            "cursor": cursor,
        })
        if not data:
            return [], False, 0
        videos = data.get("videos", [])
        has_more = bool(data.get("hasMore", False))
        next_cursor = data.get("cursor", 0)
        return videos, has_more, next_cursor

    def get_account(self, username):
        """Returns user profile normalized to TikAPI userInfo shape."""
        data = self._get("/userinfo-by-username/", {"username": username})
        if not data:
            return None

        user = data.get("user", {})
        stats = user.get("stats", {})

        return {
            "userInfo": {
                "user": {
                    "id": user.get("id"),
                    "secUid": user.get("secUid"),
                    "uniqueId": user.get("uniqueId"),
                },
                "statsV2": {
                    "followerCount": str(stats.get("followerCount", 0)),
                    "heartCount": str(stats.get("heartCount", 0)),
                    "videoCount": str(stats.get("videoCount", 0)),
                }
            }
        }

    def get_account_posts(self, sec_uid, count=10):
        """Returns recent posts normalized to TikAPI posts shape.

        # TODO:
        # TikLiveAPI requires a numeric user ID for get_account_posts(),
        # while BaseProvider currently passes secUid.
        # Redesign provider interface to support provider-specific account IDs.

        music_info is preserved per-video (id/title/author of the sound
        THAT post uses) — confirmed present in the raw /user-posts/
        response via direct testing 7/18. This is what creator-graph
        discovery needs: a creator's OTHER posts often use OTHER sounds
        entirely, and this comes bundled free in this same call, no
        extra per-video API cost. Previously this method silently
        dropped it, which would have made graph-based discovery
        impossible without realizing why.
        """
        if sec_uid and str(sec_uid).isdigit():
            data = self._get("/user-posts/", {"userid": sec_uid, "count": min(count, 30)})
        else:
            _log(f"get_account_posts: secUid is not numeric, skipping")
            return None

        if not data:
            return None

        videos = data.get("videos", [])
        items = []
        for v in videos:
            music_info = v.get("music_info") or {}
            items.append({
                "id": v.get("video_id"),
                "desc": v.get("title", ""),
                "createTime": v.get("create_time"),
                "stats": {
                    "playCount": v.get("play_count", 0),
                    "diggCount": v.get("digg_count", 0),
                    "commentCount": v.get("comment_count", 0),
                    "shareCount": v.get("share_count", 0),
                },
                "music": {
                    "id": music_info.get("id"),
                    "title": music_info.get("title"),
                    "authorName": music_info.get("author"),
                },
            })
        return {"itemList": items}

    def get_post(self, post_id):
        """Returns single post normalized to TikAPI video shape."""
        url = f"https://www.tiktok.com/@placeholder/video/{post_id}"
        data = self._get("/post-detail/", {"url": url})
        if not data:
            return None

        author = data.get("author", {})
        return {
            "itemInfo": {
                "itemStruct": {
                    "id": data.get("id"),
                    "desc": data.get("title", ""),
                    "createTime": data.get("create_time"),
                    "stats": {
                        "playCount": data.get("play_count", 0),
                        "diggCount": data.get("digg_count", 0),
                        "commentCount": data.get("comment_count", 0),
                        "collectCount": data.get("collect_count", 0),
                        "shareCount": data.get("share_count", 0),
                    },
                    "author": {
                        "uniqueId": author.get("unique_id", ""),
                    },
                    "authorStats": {},
                    "video": {
                        "cover": data.get("cover"),
                    },
                }
            }
        }