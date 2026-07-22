"""
ingestion/orchestration.py — the Orchestration boundary.

MOVED (provider-boundary refactor, final boundary step) from
ingestion/service.py — pure relocation, no behavior changes. Every
function here is byte-for-byte identical to its previous version.

Orchestration's job: coordinate multiple boundaries together for a
specific real-world event (a song being created, a manual "find new
sounds" click, the nightly cron) — unlike every other module in this
package, which does exactly ONE job. This is the only boundary allowed
to call across Discovery + Qualification + Ingestion in the same
function, which is precisely why these three functions couldn't live in
any single one of those modules without misrepresenting what they do.

  - initialize_song            — runs ONCE per song, right after
    creation (via /quick_refresh — see routes_songs.py comments on why
    that route existed but was never wired to anything until this was
    found and fixed). Discovery (both sensors) -> qualify -> ingest.
  - find_new_sounds_for_song    — the repeatable "Find New Sounds"
    button. Discovery (title/hashtag only, NOT community — see its own
    docstring for why) -> qualify.
  - run_nightly_discovery       — the nightly cron
    (campaign-tracker-discover-nightly). Discovery -> process_sound_pipeline
    (the state-machine qualification path, not qualify_pending_sounds_for_song).
    This is the function that was silently calling a non-existent
    process_sound_pipeline every night until that bug was found and
    fixed during this same refactor — see qualification.py's docstring.

This module depends on discovery.py, qualification.py, and ingestion.py
directly (imports downward, never the reverse) — confirmed via a
two-directional grep before this extraction: none of those three modules
call anything defined here.
"""

from ._shared import _log
from .providers import default_provider as provider
from .discovery import discover_song_sounds, discover_community_sounds_for_song
from .qualification import qualify_pending_sounds_for_song, process_sound_pipeline
from .ingestion import ingest_approved_sounds_for_song, refresh_approved_sounds_for_song


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