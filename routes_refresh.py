"""
routes_refresh.py — the automatic refresh cron and per-song manual actions.

ARCHITECTURE (finalized): discovery and refresh are fully separate
responsibilities, and only ONE of them is allowed to run automatically.

  Cron (automatic, hourly) — /api/refresh/monitor
    Refreshes posts/stats for sounds ALREADY approved. Never searches for
    new sounds, never adds pending candidates, never auto-approves
    anything. This is the only cron job left, deliberately, so it's safe
    to leave running 24/7 without worrying it will silently expand or
    alter the canonical sound set while nobody's watching.

  Find New Sounds (manual, per-song) — /api/songs/<id>/find_new_sounds
    (see routes_songs.py) — the ONLY place discovery happens after a
    song's initial creation. Never auto-approves — genuine matches land
    in a pending review queue for a human to explicitly approve or
    reject.

  Initialize Song (one-time, at creation) — /api/songs/<id>/quick_refresh
    (this file) — discover -> qualify (auto-approving) -> ingest. This is
    the only place auto-approval happens, and only because a brand new
    song needs SOME starting canonical set to be useful at all.

REMOVED from this file: /api/refresh/discover and /api/refresh/qualify.
Both used to run automatically and BOTH violated the "discovery is
explicit, human-approved" principle above:
  - /api/refresh/discover called the OLD, un-capped, unfiltered legacy
    discovery function on every active song on a schedule — exactly the
    bug that caused hundreds of unrelated candidates to flood in per song
    before tonight's rebuild.
  - /api/refresh/qualify auto-qualified EVERY pending sound across all
    active campaigns on a schedule, with auto-approve defaulting to on.
    Any sound sitting in a "Find New Sounds" review queue would have
    silently become canonical the next time this cron fired — the exact
    "why did these appear? the cron decided" problem this architecture
    exists to prevent.

If either of these routes is still configured as a scheduled job
somewhere (Render Cron Job, external scheduler, etc.), that schedule
needs to be deleted too — removing the route here means it'll just start
404ing on schedule instead of running, which isn't the same as it being
safely disabled.
"""

import logging
from flask import Blueprint, jsonify, request
from ingestion import api as ingestion
from ingestion import service as ingestion_service
from ingestion.providers import default_provider as _provider
from ingestion.parsers import parse_sound_info
from db import db

refresh_bp = Blueprint("refresh", __name__)

LOCK_TIMEOUT_MINUTES = 30

# NOTE: ingestion_lock is currently a single global lock (row id=1). All
# background jobs (refresh, discovery, fingerprint worker) are
# serialized. This is intentional for now to prevent concurrent writes
# and API contention. When throughput becomes a bottleneck, migrate to
# per-job or per-song locks.


def _acquire_lock(lock_name='refresh'):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE ingestion_lock
                SET locked = TRUE, locked_at = NOW(), locked_by = %s
                WHERE id = 1
                  AND (locked = FALSE OR locked_at < NOW() - (%s * INTERVAL '1 minute'))
            """, (lock_name, LOCK_TIMEOUT_MINUTES))
            acquired = c.rowcount == 1
        conn.commit()
    return acquired


def _release_lock():
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE ingestion_lock
                SET locked = FALSE, locked_at = NULL, locked_by = NULL
                WHERE id = 1
            """)
        conn.commit()


def _get_active_sounds():
    """Get top 25 approved sounds by priority score (recent activity weighted).
    Only returns stale sounds not refreshed in last 3 hours."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.id as sound_db_id, snd.song_id,
                       snd.sound_id as tiktok_sound_id,
                       COALESCE(snd.posts_7d, 0) as posts_7d,
                       COALESCE(snd.posts_24h, 0) as posts_24h
                FROM sounds snd
                WHERE snd.status = 'approved'
                AND snd.song_id IN (
                    SELECT cs.song_id FROM campaign_songs cs
                    JOIN campaigns c ON c.id = cs.campaign_id
                    WHERE c.status = 'In Progress'
                )
                AND (snd.last_ingested_at IS NULL
                     OR snd.last_ingested_at < NOW() - INTERVAL '3 hours')
                ORDER BY
                    (snd.last_ingested_at IS NULL) DESC,
                    (COALESCE(snd.posts_24h, 0) * 10 +
                     COALESCE(snd.posts_7d, 0) * 3) DESC,
                    snd.last_ingested_at ASC NULLS FIRST
                LIMIT 25
            """)
            return [dict(r) for r in c.fetchall()]


@refresh_bp.route("/api/refresh/monitor", methods=["POST"])
def refresh_monitor():
    """The ONLY automatic cron job left. Refreshes posts for sounds that
    are ALREADY approved — never searches for new sounds, never adds
    pending candidates, never auto-approves anything. Safe to run hourly,
    24/7, indefinitely, without risk of silently changing the canonical
    sound set.

    Run every hour via cron: 0 * * * *
    """
    if not _acquire_lock('monitor'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        song_sounds = _get_active_sounds()
        logging.info(f"[monitor] refreshing {len(song_sounds)} sounds")

        skipped = 0
        ingested = 0
        for s in song_sounds:
            result = ingestion.ingest_sound(
                db, s["song_id"], s["sound_db_id"], s["tiktok_sound_id"], max_results=30
            )
            if result.get("source") == "cache":
                skipped += 1
            else:
                ingested += 1

        logging.info(f"[monitor] {len(song_sounds)} sounds: {skipped} fresh, {ingested} refreshed")
        return jsonify({
            "ok": True,
            "sounds_checked": len(song_sounds),
            "skipped": skipped,
            "ingested": ingested,
        })

    except Exception as e:
        logging.exception("Monitor scan failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/songs/<int:song_id>/quick_refresh", methods=["POST"])
def quick_refresh_song(song_id):
    """Initialize Song — runs ONCE, right after a song is created: discover
    -> qualify (auto-approving) -> ingest posts. This is the only place
    auto-approval happens, and only because a brand new song needs SOME
    starting canonical set to be useful. Never call this as a routine
    refresh — see refresh_monitor above and refresh_song in
    routes_songs.py for that.
    """
    if not _acquire_lock(f'quick_refresh_song_{song_id}'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT id, name, artist FROM songs WHERE id=%s", (song_id,))
                song = c.fetchone()

        if not song:
            return jsonify({"ok": False, "error": f"song {song_id} not found"}), 404

        logging.info(f"[quick_refresh] starting full pipeline for song {song_id} ('{song['name']}')")
        result = ingestion_service.initialize_song(
            db, song_id, song["name"], song["artist"] or ""
        )
        logging.info(f"[quick_refresh] song {song_id} complete: {result}")

        return jsonify({"ok": True, "song_id": song_id, **result})

    except Exception as e:
        logging.exception(f"Quick refresh failed for song {song_id}:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/songs/<int:song_id>/ingest_only", methods=["POST"])
def ingest_only_song(song_id):
    """Fast, targeted ingest for ONE song's already-approved sounds — no
    discovery, no qualify. Use this when a song's sounds are already
    correctly approved (e.g. approved manually) and you just need to pull
    posts in without re-running the full discover -> qualify -> ingest
    pipeline via quick_refresh."""
    if not _acquire_lock(f'ingest_only_song_{song_id}'):
        return jsonify({"ok": False, "reason": "ingestion already running"}), 429

    try:
        result = ingestion_service.ingest_approved_sounds_for_song(db, song_id)
        return jsonify({"ok": True, "song_id": song_id, **result})
    except Exception as e:
        logging.exception(f"Ingest-only failed for song {song_id}:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh/fingerprint", methods=["POST"])
def refresh_fingerprint():
    """The fingerprint backlog worker — SAFE to run automatically on a
    schedule, unlike the removed /api/refresh/discover and
    /api/refresh/qualify crons described at the top of this file.

    Why this one's different: it never touches sounds.status. It only
    writes audio-verification data (fingerprint_status, matched
    title/artist/confidence) onto sounds a human already explicitly put
    into the pending queue via "Find New Sounds". It can't silently
    expand, approve, or reject anything — purely annotation, not a
    decision. So running it hourly (or more often — it's cheap, roughly
    $0.003/check) doesn't reintroduce the "why did these appear? the cron
    decided" problem the rest of this file's architecture exists to
    prevent.

    Optional ?song_id=<id> scopes this run to just that song, instead of
    the default global FIFO queue — see run_fingerprint_backlog's
    docstring for why that matters: without this, running the worker
    right after "Find New Sounds" on one song often ends up processing a
    completely different, older song's backlog instead.

    Suggested schedule (no song_id — the global drain): every 10-15
    minutes, more frequent than the hourly monitor scan, since draining
    the fingerprint backlog quickly is what makes "Find New Sounds" feel
    responsive even though fingerprinting itself runs out-of-band.
    """
    song_id = request.args.get('song_id', type=int)
    lock_name = f'fingerprint_backlog_song_{song_id}' if song_id else 'fingerprint_backlog'
    if not _acquire_lock(lock_name):
        return jsonify({"ok": False, "reason": "fingerprint backlog already running"}), 429

    try:
        result = ingestion_service.run_fingerprint_backlog(db, song_id=song_id)
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception("Fingerprint backlog run failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh/discover_nightly", methods=["POST"])
def refresh_discover_nightly():
    """Discovery cron — reinstates automatic discovery, DELIBERATELY, with
    the one guarantee that makes it safe: auto_approve is hardcoded False
    inside ingestion_service.run_nightly_discovery and is NOT a parameter
    here or anywhere in that call chain. Nothing this route touches can
    become canonical without a human explicitly approving it in the
    pending review queue — same guarantee "Find New Sounds" already
    provides, just on a timer instead of a click.

    This is NOT the same as the old /api/refresh/discover this file's
    docstring describes removing. That one called an un-capped legacy
    discovery function with auto-approve defaulted on. This one reuses
    today's capped, plausibility-filtered discover_song_sounds /
    qualify_pending_sounds_for_song exactly as they already exist for
    manual use.

    Suggested schedule: once nightly (e.g. 3am) — see
    run_nightly_discovery's docstring for why not more often.
    """
    if not _acquire_lock('discover_nightly'):
        return jsonify({"ok": False, "reason": "discovery already running"}), 429

    try:
        result = ingestion_service.run_nightly_discovery(db)
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception("Nightly discovery run failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh/process_pipeline", methods=["POST"])
def refresh_process_pipeline():
    """THE state machine worker (migration step 6 — see
    HANDOFF_state_machine_migration.md). Runs process_sound_pipeline(),
    which moves sounds DISCOVERED -> AWAITING_REVIEW in one step —
    fingerprinting IS the verification, there's no separate VERIFIED
    stage to pass through.

    ONE PIPELINE NOW, NOT TWO: the old /api/refresh/fingerprint route
    (run_fingerprint_backlog) is retired — delete its Render Cron Job and
    point that schedule at this route instead, with NO song_id, so it
    drains the global backlog exactly as originally designed. The
    earlier caution about scoping every call to one song_id existed only
    because the old worker was running in parallel and would have
    double-billed against the same candidates — once that cron is gone,
    running this unscoped hourly (or however often you scheduled the old
    one) is correct and safe.

    song_id=<id> still works for the per-song manual "Fingerprint
    Pending" button on a song page — same function, same code path,
    no duplicate logic either way.
    """
    song_id = request.args.get('song_id', type=int)
    lock_name = f'process_pipeline_song_{song_id}' if song_id else 'process_pipeline'
    if not _acquire_lock(lock_name):
        return jsonify({"ok": False, "reason": "pipeline already running"}), 429

    try:
        result = ingestion_service.process_sound_pipeline(db, song_id=song_id)
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception("process_sound_pipeline run failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/refresh/creator_graph", methods=["POST"])
def refresh_creator_graph():
    """Discovery Engine, new source (see HANDOFF_state_machine_migration.md
    — validated 7/18 with real data before building). Traverses the
    posters of a song's already-approved sounds to find sound_ids search
    could never surface. Requires at least one approved sound to seed
    from — a chicken-and-egg source, structurally different from the
    others, so this is its own explicit route rather than folded into
    "Find New Sounds".
    """
    song_id = request.args.get('song_id', type=int)
    if not song_id:
        return jsonify({"ok": False, "error": "song_id is required"}), 400

    lock_name = f'creator_graph_song_{song_id}'
    if not _acquire_lock(lock_name):
        return jsonify({"ok": False, "reason": "creator graph discovery already running"}), 429

    try:
        with db() as conn:
            with conn.cursor() as c:
                c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
                row = c.fetchone()
                if not row:
                    return jsonify({"ok": False, "error": "song not found"}), 404

        result = ingestion_service.discover_via_creator_graph(db, song_id, row["name"], row["artist"] or "")
        return jsonify({"ok": True, **result})
    except Exception as e:
        logging.exception("discover_via_creator_graph run failed:")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _release_lock()


@refresh_bp.route("/api/sounds/<int:sound_db_id>/force_refresh", methods=["POST"])
def force_refresh_sound(sound_db_id):
    """Manually force-refresh ONE sound, bypassing the freshness cache
    entirely. Needed for testing right after a Coverage Engine tuning
    change (e.g. adjusting tier thresholds) — without this, you'd have
    to wait out SOUND_FRESHNESS_HOURS before a real re-fetch happens,
    even though clicking 'Refresh' looks like it should have worked.
    """
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT song_id, sound_id, status FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"ok": False, "error": "sound not found"}), 404

    result = ingestion_service.ingest_sound(
        db, row["song_id"], sound_db_id, row["sound_id"], force=True
    )
    return jsonify({"ok": True, **result})


@refresh_bp.route("/api/refresh", methods=["POST"])
def refresh():
    """Legacy endpoint — runs monitor scan only."""
    return refresh_monitor()