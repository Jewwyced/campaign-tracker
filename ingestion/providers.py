"""
ingestion_providers.py — provider abstraction layer.

Defines the interface every data provider must implement, and the TikAPI
implementation of that interface. The service layer calls providers through
this interface rather than calling tikapi directly — so swapping, combining,
or falling back across providers never requires touching business logic.

Today there is one real provider (TikAPI) and one placeholder (FallbackProvider).
The structure is already in place for:
  - A scraper provider (when TikAPI rate-limits)
  - A cache provider (check Neon before hitting any external API)
  - A Spotify/Apple Music provider (for song/artist metadata)

How the service layer uses this:
  Instead of: raw = tikapi.get_music_info(sound_id)
  It becomes:  raw = provider.get_sound_info(sound_id)

The provider is injected at the call site, defaulting to the TikAPI provider.
Later, the service can try multiple providers in sequence without the caller
knowing anything changed.
"""

import os
from .client import tikapi
from .tiklive_provider import TikLiveAPIProvider


# ── Provider interface ────────────────────────────────────────────────────────
# Every provider must implement these methods with these exact signatures.
# Return None on failure — callers check for None before proceeding.

class BaseProvider:
    """Defines the contract every provider must fulfill.
    Subclasses override the methods they support."""

    def get_sound_info(self, sound_id):
        """Return raw sound metadata JSON or None."""
        raise NotImplementedError

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        """Return one page of raw posts for a sound, or None."""
        raise NotImplementedError

    def search_sounds(self, query):
        """Return raw search results JSON or None."""
        raise NotImplementedError

    def get_account(self, username):
        """Return raw account profile/stats JSON or None."""
        raise NotImplementedError

    def get_account_posts(self, sec_uid, count=10):
        """Return raw recent posts for an account, or None."""
        raise NotImplementedError

    def get_post(self, post_id):
        """Return raw single post JSON or None."""
        raise NotImplementedError


# ── TikAPI provider ───────────────────────────────────────────────────────────

class TikAPIProvider(BaseProvider):
    """Production provider — routes every call through TikAPI.
    This is the only real provider today."""

    def get_sound_info(self, sound_id):
        return tikapi.get_music_info(sound_id)

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        return tikapi.get_music_posts_page(sound_id, cursor=cursor, count=count)

    def search_sounds(self, query):
        return tikapi.get_search_general(query)

    def get_account(self, username):
        return tikapi.get_check(username)

    def get_account_posts(self, sec_uid, count=10):
        return tikapi.get_posts_by_secuid(sec_uid, count=count)

    def get_post(self, post_id):
        return tikapi.get_video(post_id)


# ── Fallback provider (placeholder) ──────────────────────────────────────────

class FallbackProvider(BaseProvider):
    """Placeholder for a future fallback — scraper, cache, or secondary API.
    Currently returns None for everything, which causes the service to skip
    gracefully. Replace individual methods as real fallbacks get built."""

    def get_sound_info(self, sound_id):
        return None  # Future: check Neon cache, then scraper

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        return None  # Future: scraper

    def search_sounds(self, query):
        return None  # Future: Spotify/Apple Music search

    def get_account(self, username):
        return None  # Future: scraper

    def get_account_posts(self, sec_uid, count=10):
        return None  # Future: scraper

    def get_post(self, post_id):
        return None  # Future: scraper


# ── Provider pipeline ─────────────────────────────────────────────────────────

class ProviderPipeline:
    """Tries providers in order, returning the first non-None result.
    This is the object the service layer actually uses — it never needs to
    know which provider succeeded, only that it got data back (or didn't).

    Today: [TikAPIProvider] — one provider, no fallback.
    Tomorrow: [TikAPIProvider, FallbackProvider] — tries TikAPI, falls back
              to scraper/cache on 429 or None response.
    """

    def __init__(self, providers):
        self.providers = providers

    def get_sound_info(self, sound_id):
        for p in self.providers:
            result = p.get_sound_info(sound_id)
            if result is not None:
                return result
        return None

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        for p in self.providers:
            result = p.get_sound_posts_page(sound_id, cursor=cursor, count=count)
            if result is not None:
                return result
        return None

    def search_sounds(self, query):
        for p in self.providers:
            result = p.search_sounds(query)
            if result is not None:
                return result
        return None

    def get_account(self, username):
        for p in self.providers:
            result = p.get_account(username)
            if result is not None:
                return result
        return None

    def get_account_posts(self, sec_uid, count=10):
        for p in self.providers:
            result = p.get_account_posts(sec_uid, count=count)
            if result is not None:
                return result
        return None

    def get_post(self, post_id):
        for p in self.providers:
            result = p.get_post(post_id)
            if result is not None:
                return result
        return None


# ── Default pipeline instance ─────────────────────────────────────────────────
# The service layer imports and uses this. To add a fallback later,
# just add it to this list — nothing else in the codebase changes.

default_provider = ProviderPipeline([
    TikAPIProvider(),
    TikLiveAPIProvider(),
    # FallbackProvider(),  ← uncomment when a real fallback exists
])