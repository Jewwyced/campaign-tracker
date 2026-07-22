"""
ingestion/service.py — Layer 3: service / orchestration.

Owns all business logic and database writes. Calls parsers to get clean
data, then decides what to write to Neon and what to return.

Known deferred improvements:
  - song_id parameter on ingest_sound() is now unused — legacy, kept for
    backward compatibility with existing callers
  - Cache freshness only implemented for sounds — roster accounts, fan
    accounts, and single posts still always hit the provider pipeline
  - Result objects are not yet standardized across all functions

NOTE: after the discovery/qualification/ingestion/growth boundary
extractions, this file now holds only cross-boundary Orchestration
(run_nightly_discovery, initialize_song, find_new_sounds_for_song) —
plus re-exports from every extracted module so every existing external
import path keeps working unchanged. The final boundary step splits
Orchestration into orchestration.py; this file will then be almost
entirely re-exports.
"""

from .providers import default_provider as provider

# Shared low-level helpers — moved to ._shared (discovery-boundary refactor)
# specifically to avoid a circular import: this file needs to import
# discovery functions back (see below), and _shared.py has no dependency
# on either side, so both can depend on it without a cycle. Same
# function bodies as before, just relocated — see _shared.py's docstring.
from ._shared import _log, _normalize_str, _score_sound, _artist_signal

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

# ── Constants ─────────────────────────────────────────────────────────────────

SOURCE_FALLBACK = "fallback"  # NOTE: unused anywhere in the codebase as of this
                              # audit — left as-is, not part of this refactor's scope.


# ── Internal helpers ──────────────────────────────────────────────────────────

def run_nightly_discovery(db_conn_factory):
    """Discovery cron — runs once per night, loops every song attached to
    an active campaign, and runs discover -> process_sound_pipeline for
    each, landing results directly in state='awaiting_review' with a real
    recommendation attached, exactly as if a human had clicked "Find New
    Sounds" themselves.

    THE ONE PIPELINE, LOCKED IN: this now calls the same
    process_sound_pipeline used by find_new_sounds_v2 — there is no
    longer a separate old (text-only qualify) and new (fingerprint)
    system running in parallel. Nothing auto-approves: process_sound_
    pipeline only ever lands sounds in awaiting_review with a
    recommendation attached; a human still makes every final call. This
    directly matches the design already documented in routes_refresh.py,
    which explains why the OLD automatic discovery cron was removed (it
    called an un-capped legacy discovery function AND defaulted
    auto-approve to on) — this function reuses today's capped,
    plausibility-filtered discover_song_sounds + the single fingerprint-
    and-recommend pipeline, just triggered on a timer instead of a click.

    Intended schedule: once nightly (e.g. 3am), NOT hourly — re-running
    full discovery on the same songs repeatedly finds little new each
    time; once a day is enough to have a fresh, already-verified queue by
    morning, without paying the discovery cost on a tighter loop for no
    benefit.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT DISTINCT sg.id, sg.name, sg.artist
                FROM songs sg
                JOIN campaign_songs cs ON cs.song_id = sg.id
                JOIN campaigns camp ON camp.id = cs.campaign_id
                WHERE camp.status = 'In Progress'
            """)
            songs = [dict(r) for r in c.fetchall()]

    _log(f"run_nightly_discovery: {len(songs)} active songs")

    results = []
    for song in songs:
        try:
            discover_result = discover_song_sounds(
                db_conn_factory, song["id"], song["name"], song["artist"] or ""
            )
            pipeline_result = process_sound_pipeline(
                db_conn_factory, song_id=song["id"]
            )
            results.append({
                "song_id": song["id"],
                "song_name": song["name"],
                "discovered": discover_result,
                "pipeline": pipeline_result,
            })
        except Exception as e:
            _log(f"run_nightly_discovery: failed on song {song['id']} ('{song['name']}'): {e}")
            results.append({"song_id": song["id"], "song_name": song["name"], "error": str(e)})

    total_fingerprinted = sum(r.get("pipeline", {}).get("fingerprinted", 0) for r in results)
    _log(f"run_nightly_discovery: complete — {total_fingerprinted} sounds fingerprinted and moved "
         f"to awaiting_review across {len(songs)} songs")

    return {
        "songs_processed": len(songs),
        "total_fingerprinted": total_fingerprinted,
        "per_song": results,
    }


def initialize_song(db_conn_factory, song_id, name, artist=""):
    """Runs ONCE per song, at creation time: discover -> qualify (auto-
    approving high-confidence matches) -> ingest. This is what establishes
    a song's initial canonical sound set and makes 'add song, it just
    appears' true.

    Auto-approval is appropriate here specifically because a brand new
    song has zero canonical sounds yet — it needs SOME starting set to be
    useful at all. Contrast with find_new_sounds_for_song, which expands
    an EXISTING canonical set and deliberately does NOT auto-approve (see
    that function for why).

    IMPORTANT: this must NEVER be called by a routine refresh action.
    Discovery is expensive (dozens of search API calls); refresh should
    only touch a song's already-approved (canonical) sounds — see
    refresh_approved_sounds_for_song.
    """
    discovered = discover_song_sounds(db_conn_factory, song_id, name, artist or "")

    # Second discovery sensor — see discover_community_sounds_for_song's
    # module notes for why this exists and what it was validated against.
    # Deliberately best-effort: a Community Discovery failure (rate limit,
    # provider outage, etc.) should never block song creation or title
    # search's results — this is additive, not load-bearing.
    try:
        community_discovered = discover_community_sounds_for_song(db_conn_factory, song_id, name, artist or "")
    except Exception as e:
        _log(f"initialize_song: community discovery failed, continuing without it: {e}")
        community_discovered = []

    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=True)
    ingest_result = ingest_approved_sounds_for_song(db_conn_factory, song_id)

    return {
        "sounds_discovered": len(discovered),
        "community_sounds_discovered": len(community_discovered),
        "qualify": qualify_result,
        "ingest": ingest_result,
    }


def find_new_sounds_for_song(db_conn_factory, song_id, name, artist=""):
    """The explicit 'Find New Sounds' action — deliberately, separately
    triggered by the user, never automatic. Runs discovery again (won't
    duplicate existing sound rows) and evaluates any newly-found pending
    candidates against the classifier, but does NOT auto-approve them —
    a song's canonical (approved) sound set should not silently grow or
    change just because a later discovery pass turned something up.

    Clear junk still gets auto-rejected to 'inactive' (no point making a
    human wade through hundreds of obvious non-matches), but anything the
    classifier considers a real match is left 'pending', waiting for a
    human to explicitly approve it via /api/sounds/<id>/approve. This is
    the design decision from the "should new discoveries silently expand
    the canonical set" question — they don't, ever, without a human click.

    Existing approved sounds are never touched by this function — new
    candidates are additive to the pending pool only.
    """
    discovered = discover_song_sounds(db_conn_factory, song_id, name, artist or "")
    qualify_result = qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=False)

    return {
        "sounds_discovered": len(discovered),
        "qualify": qualify_result,
    }