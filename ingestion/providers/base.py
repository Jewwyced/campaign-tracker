"""
ingestion/providers/base.py — the provider abstraction layer.

Defines the interface every data provider must implement, plus the
ProviderPipeline that tries them in order. Discovery/qualification/
ingestion code calls providers ONLY through this interface — never a
specific provider's own methods directly — so adding Apify, Bright Data,
or any future source never requires touching business logic anywhere
else in the codebase. That's the entire point of the provider boundary.

How calling code uses this:
  Instead of: raw = tikapi.get_music_info(sound_id)
  It becomes:  raw = provider.get_sound_info(sound_id)

MOVED (provider-boundary refactor): this file's contents used to live in
ingestion/providers.py as one flat module alongside TikAPIProvider
directly. Split out so each provider gets its own file
(providers/tiklive.py, providers/tikapi.py) and this file holds only the
shared contract — the interface itself, not any specific provider's
implementation of it.
"""


# ── Provider interface ────────────────────────────────────────────────────
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

    def search_sounds(self, query, max_pages=None):
        """Return raw search results JSON or None. max_pages is optional —
        providers that don't support pagination limits (e.g. TikAPIProvider)
        should ignore it."""
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

    def search_challenge(self, keyword):
        """Return a list of raw challenge/hashtag dicts, or [] if this
        provider doesn't support challenge search. PROMOTED to the shared
        interface as part of the provider-boundary refactor — previously
        only existed on TikLiveAPIProvider directly, called by Community
        Discovery bypassing this interface entirely. Deliberately kept the
        same "[] on failure" contract (not None) that TikLiveAPIProvider's
        original implementation already used — carrying behavior over
        unchanged rather than tightening it during a structural move."""
        return []

    def get_challenge_posts(self, challenge_id, cursor=0, count=35):
        """Return (videos, has_more, next_cursor) for one page of a
        challenge's posts. PROMOTED alongside search_challenge, same
        reasoning — kept the existing tuple-return shape (not the
        single-normalized-dict-or-None shape every other method here
        uses) rather than changing behavior mid-refactor. Worth revisiting
        once this is proven stable; not changed now on purpose."""
        return [], False, 0


# ── Fallback provider (placeholder) ──────────────────────────────────────────

class FallbackProvider(BaseProvider):
    """Placeholder for a future fallback — scraper, cache, or secondary API.
    Currently returns None/empty for everything, which causes the pipeline
    to move on to the next provider (or run out) gracefully. Replace
    individual methods as real fallbacks get built."""

    def get_sound_info(self, sound_id):
        return None  # Future: check Neon cache, then scraper

    def get_sound_posts_page(self, sound_id, cursor=0, count=30):
        return None  # Future: scraper

    def search_sounds(self, query, max_pages=None):
        return None  # Future: Spotify/Apple Music search

    def get_account(self, username):
        return None  # Future: scraper

    def get_account_posts(self, sec_uid, count=10):
        return None  # Future: scraper

    def get_post(self, post_id):
        return None  # Future: scraper

    def search_challenge(self, keyword):
        return []  # Future: scraper

    def get_challenge_posts(self, challenge_id, cursor=0, count=35):
        return [], False, 0  # Future: scraper


# ── Provider pipeline ─────────────────────────────────────────────────────────

class ProviderPipeline:
    """Tries providers in order, returning the first non-empty result.
    This is the object discovery/qualification/ingestion code actually
    uses — it never needs to know which provider succeeded, only that it
    got data back (or didn't).

    Today: [TikLiveAPIProvider, TikAPIProvider] — TikLive primary, TikAPI
    fallback. Adding Apify or Bright Data later is just adding another
    entry to this list — nothing else in the codebase changes.
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

    def search_sounds(self, query, max_pages=None):
        for p in self.providers:
            if max_pages is not None:
                result = p.search_sounds(query, max_pages=max_pages)
            else:
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

    def search_challenge(self, keyword):
        for p in self.providers:
            result = p.search_challenge(keyword)
            if result:
                return result
        return []

    def get_challenge_posts(self, challenge_id, cursor=0, count=35):
        for p in self.providers:
            videos, has_more, next_cursor = p.get_challenge_posts(challenge_id, cursor=cursor, count=count)
            if videos:
                return videos, has_more, next_cursor
        return [], False, 0