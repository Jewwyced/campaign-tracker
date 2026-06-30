"""
ingestion_client.py — Layer 1: TikAPI HTTP client.

Pure HTTP wrappers only. This file knows:
  - TikAPI's endpoint paths
  - How to authenticate
  - How to log and track quota usage

Nothing in this file touches the database, parses responses into
business objects, or makes any decisions about what to do with data.
That's ingestion_parsers.py (clean up the response) and
ingestion_service.py (decide what to write to Neon).

When retry logic, backoff, or rate-limit handling gets added, it goes
here and only here — every other file benefits automatically.
"""

import os
import requests

TIKAPI_KEY = os.environ.get("TIKAPI_KEY", "")


def _log(msg):
    print(f"  [ingestion] {msg}", flush=True)


# ── Quota tracking ────────────────────────────────────────────────────────────

def _record_quota_usage(db_conn_factory, endpoint):
    """Increments today's request counter in Neon. Non-fatal — if it fails,
    the actual TikAPI call still proceeds."""
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
    """Called once from app.py at startup so the client can record quota
    usage without owning a database import itself."""
    global _quota_db_factory
    _quota_db_factory = db_conn_factory


# ── TikAPI Client ─────────────────────────────────────────────────────────────

class TikAPIClient:
    """
    Single choke point for every outbound TikAPI HTTP call.

    All endpoint paths, auth headers, timeouts, logging, and quota tracking
    live here. Nothing outside this class calls requests.get() directly.
    """

    def __init__(self, key):
        self.key = key

    def _headers(self):
        return {
            "X-API-KEY": self.key,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }

    def get(self, url, params, timeout, label):
        r = requests.get(url, params=params, headers=self._headers(), timeout=timeout)
        _log(f"{label} -> {r.status_code}")
        if r.status_code != 200:
            _log(f"  body: {r.text[:200]}")
        if _quota_db_factory:
            _record_quota_usage(_quota_db_factory, label.split()[0])
        return r

    def get_check(self, username):
        r = self.get("https://api.tikapi.io/public/check",
                     {"username": username}, 15, f"check @{username}")
        return r.json() if r.status_code == 200 else None

    def get_video(self, post_id):
        r = self.get("https://api.tikapi.io/public/video",
                     {"id": post_id}, 15, f"video id={post_id}")
        return r.json() if r.status_code == 200 else None

    def get_posts_by_secuid(self, sec_uid, count=10):
        r = self.get("https://api.tikapi.io/public/posts",
                     {"secUid": sec_uid, "count": count}, 15,
                     f"posts secUid={sec_uid[:12]}...")
        return r.json() if r.status_code == 200 else None

    def get_music_info(self, sound_id):
        r = self.get("https://api.tikapi.io/public/music/info",
                     {"id": sound_id}, 30, f"music/info id={sound_id}")
        return r.json() if r.status_code == 200 else None

    def get_music_posts_page(self, sound_id, cursor=0, count=30):
        r = self.get("https://api.tikapi.io/public/music",
                     {"id": sound_id, "count": count, "cursor": cursor}, 30,
                     f"music/posts id={sound_id} cursor={cursor}")
        return r.json() if r.status_code == 200 else None

    def get_search_general(self, query):
        r = self.get("https://api.tikapi.io/public/search/general",
                     {"query": query}, 20, f"search/general '{query}'")
        return r.json() if r.status_code == 200 else None


tikapi = TikAPIClient(TIKAPI_KEY)