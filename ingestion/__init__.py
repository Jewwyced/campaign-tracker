"""
ingestion/__init__.py — Public API layer (compatibility wrapper)

This file exposes a stable interface for the rest of the codebase.
It should always match the real function names inside client/parsers/service.

No business logic belongs here.
"""
from . import api

from .client import (
    TikAPIClient,
    tikapi,
    TIKAPI_KEY,
    set_quota_db_factory,
)

from .parsers import (
    parse_tiktok_url,
    parse_sound_id_from_video,
    parse_sounds_from_search,
    parse_sound_info,
    parse_posts_from_music_page,
    parse_account_stats,
    parse_posts_from_user_feed,
    parse_single_post,
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
)

# Backward compatibility alias (old name used elsewhere in codebase)
get_sound_id_from_post = parse_sound_id_from_video