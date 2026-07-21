"""
ingestion/api.py — the stable public contract for the ingestion system.
This is the ONLY file Flask routes should import from. It exposes a clean,
intention-named interface that never changes shape even as the internals evolve.
"""
from .client import tikapi, set_quota_db_factory
from .parsers import (
    parse_tiktok_url,
    parse_sound_id_from_video,
    parse_sound_info,
    parse_posts_from_music_page,
)
from .service import (
    discover_sounds,
    create_sound,
    ingest_sound,
    discover_song_sounds,
    refresh_song_sounds,
    ingest_roster_account,
    ingest_fan_account,
    ingest_single_post,
    ingest_campaign_attached_sound,
    recompute_sound_growth as _recompute_sound_growth,
    run_ai_review_backlog as _run_ai_review_backlog,
    initialize_song as _initialize_song,
    get_sound_info as _get_sound_info_from_service,
)
from .providers import default_provider as _provider

# ── URL / Link utilities ──────────────────────────────────────────────────────
def parse_video_url(url):
    """Extract (username, post_id) from a TikTok video URL."""
    return parse_tiktok_url(url)

def get_sound_id_from_post(post_id):
    """Given a TikTok video post ID, return (sound_id, sound_title)."""
    raw = tikapi.get_video(post_id)
    return parse_sound_id_from_video(raw)

# ── Sound metadata ────────────────────────────────────────────────────────────
def get_sound_info(sound_id):
    """Get metadata about a TikTok sound using the provider pipeline (TikLive primary).
    Returns normalized dict with title, author, video_count or None."""
    raw = _provider.get_sound_info(sound_id)
    return parse_sound_info(raw)

def get_sound_posts(sound_id, max_results=30):
    """Get a curated sample of posts using a given sound."""
    posts = []
    cursor = 0
    while len(posts) < max_results:
        raw = tikapi.get_music_posts_page(sound_id, cursor=cursor, count=30)
        page_posts, has_more, next_cursor = parse_posts_from_music_page(raw)
        if not page_posts:
            break
        posts.extend(page_posts)
        if not has_more:
            break
        cursor = int(next_cursor) if next_cursor is not None else cursor + 30
    return posts[:max_results]

def search_sounds(query):
    """Search TikTok for sounds matching a query string."""
    return discover_sounds(query)

# ── Ingestion entry points ────────────────────────────────────────────────────
def ingest_song_sound(db, song_id, sound_db_id, tiktok_sound_id, max_results=30):
    """Refresh one Sound — pull fresh posts and video-count snapshot."""
    return ingest_sound(db, song_id, sound_db_id, tiktok_sound_id, max_results)

def ingest_song_sounds(db, song_id, title, artist=""):
    """Discover all TikTok sounds for a Song and ingest each one.
    discover_song_sounds returns a plain list of ranked sound candidates —
    this wrapper just passes it straight through so existing callers
    (which do len(results) to count sounds found) keep working unchanged."""
    return discover_song_sounds(db, song_id, title, artist)

def refresh_all_song_sounds(db, song_id):
    """Re-ingest every known Sound for a Song. Used by the hourly cron."""
    return refresh_song_sounds(db, song_id)

def recompute_sound_growth(db, sound_db_id):
    """Re-derive a sound's 24h/7d growth from its EXISTING song_stats
    history — pure DB read+write, no API call/quota cost. Use this to
    backfill growth numbers for many sounds at once without waiting for
    each one's turn in the normal refresh rotation."""
    return _recompute_sound_growth(db, sound_db_id)

def run_ai_review_backlog(db, batch_size=15, time_budget_seconds=25, song_id=None):
    """The AI sound-review 'final stamp' — runs on pending candidates
    fingerprinting already checked but couldn't confirm as a master-
    recording match (remixes/reposts/derivative use). Looks at real
    sample video thumbnails to judge genuine campaign relatedness, the
    same read a human reviewer does by eye. Informs the pending review
    queue only — never changes sounds.status on its own."""
    return _run_ai_review_backlog(db, batch_size=batch_size, time_budget_seconds=time_budget_seconds, song_id=song_id)

def initialize_song(db, song_id, name, artist=""):
    """Runs ONCE per song, right after creation: title-search discovery +
    Community Discovery (both independent sensors feeding the same
    pending queue) -> qualify (auto-approving high-confidence matches,
    appropriate here since a brand new song starts with zero canonical
    sounds) -> ingest. This establishes a song's initial sound set.

    IMPORTANT: this had ZERO callers anywhere in the routes layer before
    this was wired up — campaign.html has always called a
    /quick_refresh route that never existed, meaning newly created songs
    got no initial discovery at all via any live path. The new
    /api/songs/<id>/quick_refresh route is what actually calls this now."""
    return _initialize_song(db, song_id, name, artist)


def ingest_account(db, username, account_type="roster"):
    """Pull current stats for a tracked TikTok account."""
    if account_type == "roster":
        return ingest_roster_account(db, username)
    return ingest_fan_account(db, username)

def ingest_post(db, post_id, username=None):
    """Pull/update one specific TikTok video by post ID."""
    return ingest_single_post(db, post_id, username)

def ingest_campaign_sound(db, campaign_id, tiktok_sound_id, max_results=30, sound_db_id=None):
    """Legacy: pull a sound's posts directly into a campaign."""
    return ingest_campaign_attached_sound(db, campaign_id, tiktok_sound_id, max_results, sound_db_id)

# ── App startup ───────────────────────────────────────────────────────────────
def configure(db_conn_factory):
    """Call once at app startup to wire up quota tracking."""
    set_quota_db_factory(db_conn_factory)