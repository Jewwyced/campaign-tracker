"""
ingestion/service.py — Layer 3: service / orchestration (LEGACY PATH).

After the full boundary refactor (discovery / qualification / ingestion
/ growth / orchestration), this file is now ALMOST ENTIRELY re-exports.
Its only remaining job is backward compatibility: every existing
external import path (api.py's `from .service import X`, routes files'
`from ingestion import service as ingestion_service` then
`.some_function(...)`) continues to resolve exactly as before, without
any of those call sites needing to change.

Known deferred improvements (carried over, still accurate):
  - song_id parameter on ingest_sound() is now unused — legacy, kept for
    backward compatibility with existing callers
  - Cache freshness only implemented for sounds — roster accounts, fan
    accounts, and single posts still always hit the provider pipeline
  - Result objects are not yet standardized across all functions

Do not add new logic here — anything new belongs in one of the actual
boundary modules (discovery.py, qualification.py, ingestion.py,
growth.py, orchestration.py). This file's only reason to exist is that
deleting it would break every external import path at once.
"""


# Discovery boundary — moved to .discovery (discovery-boundary refactor).
# Re-exported here, unchanged, so every existing reference keeps working:
# api.py imports discover_sounds/create_sound/discover_song_sounds
# directly from .service; routes_refresh.py calls
# ingestion_service.discover_via_creator_graph(...); initialize_song and
# find_new_sounds_for_song (still in this file) call discover_song_sounds
# and discover_community_sounds_for_song internally. All of that keeps
# working unchanged via this import.
from .discovery import (
    discover_sounds,
    discover_sounds_from_videos,
    discover_sounds_from_challenge,
    discover_via_creator_graph,
    discover_song_sounds,
    _promote_top_sounds,
    discover_community_sounds_for_song,
    create_sound,
    get_or_create_sound,
    _is_plausible_candidate,
    _adapt_challenge_video,
    _community_engagement_score,
    MAX_DISCOVERY_CANDIDATES,
)

# Qualification boundary — moved to .qualification (qualification-boundary
# refactor). Re-exported here, unchanged, so every existing reference keeps
# working: api.py imports run_ai_review_backlog directly from .service;
# routes_songs.py imports _classify_sound_match directly from
# ingestion.service; routes_refresh.py calls
# ingestion_service.run_fingerprint_backlog(...) /
# .process_sound_pipeline(...) / .resurrect_unfingerprinted_rejects(...);
# initialize_song and find_new_sounds_for_song (still in this file) call
# qualify_pending_sounds_for_song internally. All of that keeps working
# unchanged via this import.
from .qualification import (
    _could_possibly_qualify,
    _classify_sound_match,
    resurrect_unfingerprinted_rejects,
    qualify_pending_sounds_for_song,
    run_fingerprint_backlog,
    _compute_recommendation,
    run_ai_review_backlog,
    process_sound_pipeline,
    QUALIFY_BATCH_SIZE,
)

# Ingestion boundary — moved to .ingestion (ingestion-boundary refactor).
# Re-exported here, unchanged, so every existing reference keeps working:
# api.py imports ingest_sound directly from .service; routes_songs.py and
# routes_refresh.py call ingestion_service.ingest_approved_sounds_for_song
# / .refresh_approved_sounds_for_song / .ingest_sound directly;
# initialize_song and find_new_sounds_for_song (still in this file) call
# several of these internally. All of that keeps working unchanged via
# this import.
from .ingestion import (
    _is_sound_fresh,
    _touch_sound_ingested,
    _update_sound_video_count,
    determine_coverage_plan,
    _ingest_sound_posts,
    get_sound_info,
    ingest_sound,
    refresh_song_sounds,
    ingest_roster_account,
    ingest_fan_account,
    ingest_single_post,
    ingest_campaign_attached_sound,
    ingest_approved_sounds_for_song,
    refresh_approved_sounds_for_song,
)

# Growth boundary — moved to .growth (growth-boundary refactor). Re-exported
# here, unchanged, so every existing reference keeps working: api.py imports
# recompute_sound_growth directly from .service.
from .growth import _update_sound_velocity, recompute_sound_growth

# Orchestration boundary — moved to .orchestration (final boundary step).
# Re-exported here, unchanged, so every existing reference keeps working:
# api.py imports initialize_song directly from .service; routes_songs.py
# calls ingestion_service.find_new_sounds_for_song(...); routes_refresh.py
# calls ingestion_service.run_nightly_discovery(...); the quick_refresh
# route calls ingestion.initialize_song(...) through api.py.
from .orchestration import run_nightly_discovery, initialize_song, find_new_sounds_for_song

# ── Constants ─────────────────────────────────────────────────────────────────

SOURCE_FALLBACK = "fallback"  # NOTE: unused anywhere in the codebase as of this
                              # audit — left as-is, not part of this refactor's scope.