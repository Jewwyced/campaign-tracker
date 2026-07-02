"""
ingestion/tiklive_provider.py — TikLiveAPI provider implementation.

Implements BaseProvider using TikLiveAPI (tikliveapi.com) as the data source.
Designed as an additional provider for discovery operations and failover
when another provider is unavailable.

All field names verified against actual TikLiveAPI response examples.
"""

import os
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
        url = f"{BASE_URL}{path}"
        try:
            r = requests.get(url, params=params, headers=self._headers(), timeout=30)
            _log(f"{path} -> {r.status_code}")
            if r.status_code == 429:
                _log("  rate limited")
                return None
            if r.status_code != 200:
                _log(f"  error: {r.text[:200]}")
                return None
            return r.json()
        except Exception as e:
            _log(f"  exception: {e}")
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
        """Returns one page of posts normalized to TikAPI itemStruct shape."""
        data = self._get("/music-posts/", {
            "music_id": sound_id,
            "count": min(count, 35),
            "cursor": cursor,
        })
        if not data:
            return None

        # Debug: log raw response on first page only
        if cursor == 0:
            _log(json.dumps(data, indent=2)[:3000])

        videos = data.get("videos", [])
        items = []
        for v in videos:
            author = v.get("author", {})
            items.append({
                "id": v.get("video_id"),
                "desc": v.get("title", ""),
                "createTime": v.get("create_time"),
                "stats": {
                    "playCount": v.get("play_count", 0),
                    "diggCount": v.get("digg_count", 0),
                    "commentCount": v.get("comment_count", 0),
                    "collectCount": v.get("collect_count", 0),
                    "shareCount": v.get("share_count", 0),
                },
                "author": {
                    "uniqueId": author.get("unique_id", ""),
                },
                "authorStats": {},
                "video": {
                    "cover": v.get("cover"),
                },
            })

        return {
            "itemStruct": {
                "itemList": items,
                "hasMore": bool(data.get("hasMore")),
                "cursor": data.get("cursor"),
            }
        }

    def search_sounds(self, query):
        """Search videos by keyword, deduplicate by music id."""
        data = self._get("/search-video/", {"keyword": query, "count": 30})
        if not data:
            return None

        # Debug: log raw response
        _log(json.dumps(data, indent=2)[:3000])

        videos = data.get("videos", [])
        seen_ids = set()
        items = []
        for v in videos:
            music = v.get("music_info", {})
            music_id = music.get("id")
            if not music_id or music_id in seen_ids:
                continue
            seen_ids.add(music_id)
            items.append({
                "item": {
                    "music": {
                        "id": music_id,
                        "title": music.get("title", ""),
                        "authorName": music.get("author", ""),
                    }
                }
            })

        return {"data": items}

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
        """
        if sec_uid and str(sec_uid).isdigit():
            data = self._get("/user-posts/", {"userid": sec_uid, "count": min(count, 35)})
        else:
            _log(f"get_account_posts: secUid is not numeric, skipping")
            return None

        if not data:
            return None

        videos = data.get("videos", [])
        items = []
        for v in videos:
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