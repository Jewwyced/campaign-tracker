"""
ingestion/providers/tikapi.py — TikAPI provider implementation.

MOVED (provider-boundary refactor) from the old flat ingestion/providers.py —
this class's methods are unchanged from that version. Only the file
location and the relative import path (one level deeper now) changed.
"""

from ..client import tikapi
from .base import BaseProvider


class TikAPIProvider(BaseProvider):
    """Fallback provider today (behind TikLive in the pipeline) — routes
    every call through TikAPI."""

    def get_sound_info(self, sound_id):
        return tikapi.get_music_info(sound_id)

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        return tikapi.get_music_posts_page(sound_id, cursor=cursor, count=count)

    def search_sounds(self, query, max_pages=None):
        # TikAPI's underlying search doesn't support a page cap — ignore it.
        return tikapi.get_search_general(query)

    def get_account(self, username):
        return tikapi.get_check(username)

    def get_account_posts(self, sec_uid, count=10):
        return tikapi.get_posts_by_secuid(sec_uid, count=count)

    def get_post(self, post_id):
        return tikapi.get_video(post_id)

    # search_challenge / get_challenge_posts: not supported by TikAPI —
    # inherits BaseProvider's default (empty) implementations, which is
    # exactly the "this provider doesn't do this, try the next one"
    # behavior ProviderPipeline expects.