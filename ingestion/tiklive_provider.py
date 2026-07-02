"""
ingestion/tiklive_provider.py — TikLiveAPI provider implementation.

Implements BaseProvider using TikLiveAPI (tikliveapi.com) as the data source.
Designed as an additional provider for discovery operations and failover
when another provider is unavailable.

All field names verified against actual TikLiveAPI response examples.

Add to ProviderPipeline in providers.py:
    from .tiklive_provider import TikLiveAPIProvider
    default_provider = ProviderPipeline([
        TikAPIProvider(),
        TikLiveAPIProvider(),
    ])
"""

import os
import requests
from .providers import BaseProvider

TIKLIVEAPI_KEY = os.environ.get("d9ef39496d0711607eec376658918c06", "")
BASE_URL = "https://api.tikliveapi.com"


def _log(msg):
    print(f"  [tiklive] {msg}", flush=True)


class TikLiveAPIProvider(BaseProvider):
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
        # Normalize to TikAPI musicInfo shape
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
            "count": min(count, 35),  # TikLiveAPI max is 35
            "cursor": cursor,
        })
        if not data:
            return None

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
        """Search videos by keyword, deduplicate by music id, return TikAPI search shape."""
        data = self._get("/search-video/", {"keyword": query, "count": 30})
        if not data:
            return None

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
        """Returns user profile normalized to TikAPI userInfo shape.
        Verified against actual /userinfo-by-username/ response.
        Fields: user.id (numeric), user.secUid, user.uniqueId,
                user.stats.followerCount, heartCount, videoCount
        """
        data = self._get("/userinfo-by-username/", {"username": username})
        if not data:
            return None

        user = data.get("user", {})
        stats = user.get("stats", {})

        return {
            "userInfo": {
                "user": {
                    "id": user.get("id"),           # numeric ID for user-posts
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
        TikLiveAPI /user-posts/ requires numeric userid, not secUid.

        # TODO:
        # TikLiveAPI requires a numeric user ID for get_account_posts(),
        # while BaseProvider currently passes secUid.
        # Redesign provider interface to support provider-specific account IDs.
        The sec_uid parameter name is kept for BaseProvider compatibility —
        callers pass secUid but TikLiveAPI needs the numeric id.
        Since we can't convert secUid→userid without an extra API call,
        this method returns None (falls back to TikAPIProvider) unless
        the caller passes a numeric id disguised as sec_uid.
        Long term: get_account() should return userid and service layer
        should pass it through explicitly.
        """
        # If sec_uid looks numeric, use it directly as userid
        if sec_uid and str(sec_uid).isdigit():
            data = self._get("/user-posts/", {"userid": sec_uid, "count": min(count, 35)})
        else:
            # Can't convert secUid to userid without extra call — skip this provider
            _log(f"get_account_posts: secUid {sec_uid[:12]}... is not numeric, skipping")
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
        """Returns single post normalized to TikAPI video shape.
        TikLiveAPI /post-detail/ accepts a full TikTok URL, not a bare video ID.
        We construct a placeholder URL since we only have the post_id.
        Verified field names: id, title, create_time, play_count, digg_count,
        comment_count, collect_count, share_count, cover, author.unique_id
        """
        url = f"https://www.tiktok.com/@placeholder/video/{post_id}"
        data = self._get("/post-detail/", {"url": url})
        if not data:
            return None

        # post-detail returns the video object at the top level
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