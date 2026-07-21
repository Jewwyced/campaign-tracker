"""
ingestion/qualification.py — the Qualification boundary.

MOVED (provider-boundary refactor, qualification-boundary step) from
ingestion/service.py — pure relocation, no behavior changes. Every
function here is byte-for-byte identical to its previous version in
service.py; only the file location and imports changed (shared helpers
now come from ._shared instead of being defined locally, same reasoning
as discovery.py — see _shared.py's docstring).

Qualification's job: decide whether a candidate discovery already found
is legitimate. Nothing here discovers new candidates or ingests posts —
it only reads/updates sounds that are already sitting in the pending
queue, regardless of which discovery sensor put them there.

Three parallel verification mechanisms currently coexist here — found
during the pre-refactor audit, not something this move changes or
resolves:
  - run_fingerprint_backlog     — the OLDER system (status/fingerprint_status)
  - run_ai_review_backlog       — the AI vision "final stamp" layer
  - process_sound_pipeline      — a NEWER state-machine system (state
    column: discovered -> fingerprinting -> awaiting_review), intended
    per its own docstring to eventually RETIRE run_fingerprint_backlog's
    cron — that migration never actually happened; both crons are live
    simultaneously in production today (confirmed via Render dashboard).
    process_sound_pipeline was ALSO found completely disconnected during
    this audit (its `def` line had been accidentally deleted) and has
    been restored — see the conversation/handoff notes for how that
    happened and what it means for the nightly cron.

resurrect_unfingerprinted_rejects is a queue-reset utility, not a
discovery or verification function itself — it just moves old
rejected sounds back into the pending queue so the functions above can
give them a real, up-to-date decision. Kept here since it exists purely
to feed this boundary's input, not because it decides anything itself.

NOTE: run_nightly_discovery is NOT moved here, even though it calls
process_sound_pipeline — it also calls discover_song_sounds (Discovery
boundary), making it a cross-boundary orchestrator like initialize_song
and find_new_sounds_for_song. It stays in service.py for now and will
move to a future orchestration.py module, not this one.
"""

from .providers import default_provider as provider
from .parsers import parse_sound_info, parse_posts_from_music_page
from . import fingerprint as _fingerprint
from services import ai_service as _ai_service
from ._shared import _log, _normalize_str, _score_sound, _artist_signal

# Max number of pending sounds to actually hit the provider for in a single
# qualify_pending_sounds_for_song() call. A song can easily have 200-400
# pending candidates after discovery (common titles + hashtag/challenge
# crawling produce huge candidate lists). Calling get_sound_info() for
# EVERY pending sound inside one synchronous HTTP request doesn't scale —
# gunicorn's worker timeout will kill the request partway through (and
# crash the worker in the process), leaving the song half-approved,
# half-pending.
#
# Raised from 5 to 20: with each provider call taking up to ~15s worst
# case (5s connect / 10s read timeout, one retry), 20 sequential calls
# stays comfortably under a typical 30s+ gunicorn worker timeout while
# processing 4x as many candidates per click — meaningfully shrinking how
# many times "Find New Sounds" / requalify has to be clicked to clear a
# backlog, without touching the actual matching bar at all.
QUALIFY_BATCH_SIZE = 20


def _could_possibly_qualify(title, author, song_name, song_artist, discovered_via):
    """Cheap, NO-API-CALL pre-check using only the title/author already
    stored from discovery. Returns False only when we're CERTAIN qualify
    would reject this candidate regardless of video_count — used to bulk-
    reject candidates before spending a provider call on them.

    SOURCE-AWARE, matching _is_plausible_candidate's logic exactly. This
    function used to apply one universal strict-ish bar regardless of
    where a candidate came from — which quietly defeated the whole point
    of making discovery source-aware: discovery would happily store 75
    generic "original sound" uploads from a highly-trusted title_artist
    search, only for THIS function to immediately bulk-reject most of
    them for "zero textual relation" before they ever got a video_count
    check or a fingerprint. Two filters, only one had been updated — this
    fixes that inconsistency.

      - title_artist: TikTok's own search relevance already matched our
        exact query. Trust it — don't bulk-reject on text grounds at all;
        let it through to get a real video_count check and, eventually,
        an actual audio fingerprint, which is the real verifier now.
      - title_only / hashtag: light filtering, same single-significant-
        word bar. hashtag promoted here 7/18 based on real production
        evidence (see _is_plausible_candidate's docstring for the full
        finding) — it's proving to be the most productive source on real
        songs, not the riskiest, and the original "low trust" assumption
        was based on the challenge crawl specifically, a different
        mechanism.
      - challenge: keeps the existing stricter OR check (title match
        alone or artist match alone) — this is the source with the
        actual documented failure case (e.g. "Way Down We Go", "La
        Muchachita" pulled into a Griddle search).
    """
    title_norm = _normalize_str(song_name)
    s_title = _normalize_str(title)

    if not title_norm:
        return False

    if discovered_via == "title_artist":
        # Very high trust — mirrors _is_plausible_candidate: don't
        # re-litigate this with a text check, just confirm a real title
        # came back at all.
        return bool(s_title)

    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_author = _normalize_str(author)

    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword_strict = len(sig_words) >= 2 and title_words_matched >= 2
    title_multiword_light = len(sig_words) >= 1 and title_words_matched >= 1
    strong_title_possible = title_exact or title_contains or title_multiword_strict
    light_title_possible = title_exact or title_contains or title_multiword_light

    if discovered_via in ("title_only", "hashtag"):
        # High trust, light filtering — same bar as
        # _is_plausible_candidate's title_only/hashtag branch.
        if light_title_possible:
            return True
        if artist_norm and _artist_signal(author, artist_norm):
            return True
        return False

    if not artist_norm:
        # No artist on file — title match alone would be the deciding
        # factor, and we already know the title now (no API call needed
        # to learn it — video_count doesn't change whether a title matches).
        return strong_title_possible

    # challenge (and any future/unknown source) — low trust. Loosened
    # from a strict AND to an OR previously: either signal alone (title
    # match, or artist match) is enough to warrant the real check;
    # _classify_sound_match downstream still requires both together to
    # actually approve anything, so nothing new can get auto-approved
    # from this — it only means more real candidates reach the pending
    # queue (and now, fingerprinting) for a proper look.
    if _artist_signal(author, artist_norm) or strong_title_possible:
        return True

    return False










def _classify_sound_match(sound_title, sound_author, song_title, song_artist, video_count=0, discovered_via=None):
    """Relevance check used to GATE approval at qualify time.

    Confirmed artist match AND some real title relation -> approve.
    Everything else -> reject.

    Tier 2 (popularity-only approval for generic uploads with no artist
    signal) has been removed TWICE now, both times after confirming real
    false positives:
      1st removal — Griddle's challenge/hashtag crawl pulled in dozens of
      wildly popular, completely unrelated sounds via a dance-trend
      collision.
      2nd removal (this one) — even restricted to the supposedly-trusted
      'title_artist' discovery source, Back Home approved "TikTok
      Advertiser" and a Tyler the Creator fan account purely on video
      count. TikTok's search relevance for a combined title+artist query
      does not reliably require both terms to genuinely co-occur in a
      meaningful way — it can still surface generally popular content
      that only loosely matches. Popularity is not a safe substitute for
      a real artist match, and there's no source restriction that's
      proven reliable enough to justify the risk twice in a row.

    Returns True (approve) or False (reject) — no partial credit.
    """
    title_norm = _normalize_str(song_title)
    artist_norm = _normalize_str(song_artist) if song_artist else ""
    s_title = _normalize_str(sound_title)
    s_author = _normalize_str(sound_author)

    if not title_norm:
        return False  # no real song title to compare against — never approve blind

    # --- Title strength ---
    title_exact = (s_title == title_norm)
    title_contains = (title_norm in s_title) and not title_exact
    sig_words = [w for w in title_norm.split() if len(w) > 3]
    title_words_matched = sum(1 for w in sig_words if w in s_title)
    title_multiword = len(sig_words) >= 2 and title_words_matched >= 2

    # A derivative marker downgrades a title match — a "remix"/"sped up"
    # version is NOT proof this is the original campaign sound.
    derivative_markers = [
        "sped up", "slowed", "remix", "instrumental", "reverb", "cover",
        "nightcore", "bass boosted", "8d", "phonk", "edit audio", "mashup",
        "loop", "extended", "sped-up", "slow reverb", "lyrics"
    ]
    is_derivative = any(m in s_title for m in derivative_markers)

    strong_title = (title_exact or title_contains) and not is_derivative

    if not artist_norm:
        # No artist on file to check against at all — fall back to title alone.
        return strong_title

    # --- Confirmed artist match AND some real title relation -> approve ──
    # Artist match ALONE is not enough — an artist can have many songs
    # (e.g. PlaqueBoyMax has "Thong Song" AND "Pink Dreads" AND others).
    if _artist_signal(sound_author, artist_norm) and (strong_title or title_multiword):
        return True

    # --- Everything else -> reject ─────────────────────────────────────────
    return False




def resurrect_unfingerprinted_rejects(db_conn_factory, song_id=None):
    """One-time (or occasional) retroactive audit — see
    HANDOFF_state_machine_migration.md. Real evidence found 7/19: a
    genuinely large sound ('boldnfl', 6,318 videos, matching Chartex's
    independent count almost exactly) was sitting status='inactive' with
    fingerprint_status='unchecked' — meaning it was rejected by the OLD,
    pre-fingerprint qualify logic (or a bug since fixed) and NEVER
    actually audio-verified. Since get_or_create_sound's conflict
    handling never touches `status` on an existing row, rediscovering
    the same sound_id again does NOT give it a second look — once
    inactive, always inactive, forever, regardless of how much the
    matching/fingerprinting pipeline has improved since.

    This finds every sound in exactly that state (rejected, never
    actually fingerprinted) and resets it back into the real pipeline —
    state='discovered', status='pending' — so process_sound_pipeline
    picks it up and gives it a real, evidence-based decision under
    today's logic instead of leaving it stuck on a pre-fingerprint call
    forever. Confirmed real count for one song alone (Back Home): 131.

    Scope with song_id for a single song, or leave None to resurrect
    across every active-campaign song at once — call this deliberately,
    not automatically, since it's a real batch state change (though a
    safe, additive one: everything resurrected still goes through the
    same real fingerprint check before anyone has to review it).
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            if song_id is not None:
                c.execute("""
                    UPDATE sounds
                    SET status='pending', state='discovered'
                    WHERE song_id=%s AND status='inactive' AND fingerprint_status='unchecked'
                    RETURNING id
                """, (song_id,))
            else:
                c.execute("""
                    UPDATE sounds snd
                    SET status='pending', state='discovered'
                    FROM songs sg
                    JOIN campaign_songs cs ON cs.song_id = sg.id
                    JOIN campaigns camp ON camp.id = cs.campaign_id
                    WHERE snd.song_id = sg.id
                      AND camp.status = 'In Progress'
                      AND snd.status='inactive' AND snd.fingerprint_status='unchecked'
                    RETURNING snd.id
                """)
            resurrected = c.fetchall()
        conn.commit()

    _log(f"resurrect_unfingerprinted_rejects: {len(resurrected)} sounds reset to pending/discovered "
         f"(song_id={song_id or 'ALL active-campaign songs'})")
    return {"resurrected": len(resurrected)}




def qualify_pending_sounds_for_song(db_conn_factory, song_id, auto_approve=True):
    """Check up to QUALIFY_BATCH_SIZE pending sounds for one song — ranked
    by relevance score first — and evaluate each against
    _classify_sound_match. This is the SAME logic as /api/refresh/qualify,
    extracted here so both the cron route and the instant per-song
    pipeline share one implementation instead of drifting apart.

    auto_approve controls what happens to a candidate the classifier
    considers a real match:
      True  (default) — status becomes 'approved' immediately. Used for
             initial song creation and for requalify (re-judging existing
             candidates under corrected logic — a maintenance/correction
             action, not a "discover something brand new" action).
      False — status STAYS 'pending', awaiting a human's explicit
              approval via /api/sounds/<id>/approve. Used by
              find_new_sounds_for_song: a song's canonical set should
              never silently grow just because a later discovery pass
              found something. Clear junk is still auto-rejected to
              'inactive' either way — only genuine candidates require a
              human decision, so the review queue stays small.

    IMPORTANT: this used to process EVERY pending sound for a song in one
    call. A song can have 200-400+ pending candidates after discovery
    (common titles + hashtag/challenge crawling produce huge candidate
    lists), and calling the provider once per sound inside a single
    synchronous HTTP request doesn't scale — gunicorn's worker timeout
    killed the request partway through and crashed the worker.

    Fix: rank pending sounds by _score_sound (using the title/author
    already stored from discovery — no extra provider calls needed just
    to rank) and only make provider calls for the top QUALIFY_BATCH_SIZE
    candidates. Everything past the batch stays 'pending' and gets picked
    up by a future call.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       snd.fingerprint_status, snd.fingerprint_checked_at
                FROM sounds snd
                WHERE snd.song_id = %s AND snd.status = 'pending'
            """, (song_id,))
            pending = [dict(r) for r in c.fetchall()]

            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()

    song_name = song_row["name"] if song_row else ""
    song_artist = song_row["artist"] if song_row else ""

    # ── Pre-filter: bulk-reject candidates that provably can't qualify ──
    # This costs ZERO API calls — it only uses title/author already stored
    # from discovery. Discovery (especially the challenge/hashtag crawl)
    # pulls in a large volume of candidates with zero textual relation at
    # all (complete, real songs by unrelated artists, not just generic
    # uploads), and there's no need to fetch video_count to know those
    # can't possibly qualify. Only genuinely plausible candidates get
    # ranked and spend one of the QUALIFY_BATCH_SIZE provider calls.
    plausible = []
    bulk_rejected_ids = []
    for s in pending:
        if _could_possibly_qualify(s.get("title"), s.get("author"), song_name, song_artist, s.get("discovered_via")):
            plausible.append(s)
        else:
            bulk_rejected_ids.append(s["id"])

    if bulk_rejected_ids:
        with db_conn_factory() as conn:
            with conn.cursor() as c:
                c.execute(
                    "UPDATE sounds SET status='inactive' WHERE id = ANY(%s)",
                    (bulk_rejected_ids,)
                )
            conn.commit()
        _log(f"qualify_pending_sounds_for_song: song {song_id} — bulk-rejected "
             f"{len(bulk_rejected_ids)} candidates with zero textual relation "
             f"(no API calls spent), {len(plausible)} plausible candidates remain")

    def rank_key(s):
        return _score_sound({"title": s.get("title"), "author": s.get("author")}, song_name, song_artist)

    pending_sorted = sorted(plausible, key=rank_key, reverse=True)
    batch = pending_sorted[:QUALIFY_BATCH_SIZE]
    remaining = len(pending_sorted) - len(batch)

    _log(f"qualifying {len(pending_sorted)} plausible pending sounds for song {song_id} "
         f"(batch_size={QUALIFY_BATCH_SIZE}, auto_approve={auto_approve})")
    if remaining > 0:
        _log(f"qualify_pending_sounds_for_song: song {song_id} has {len(pending_sorted)} plausible pending — "
             f"processing top {len(batch)} this call, leaving {remaining} for next cron cycle")

    approved = 0
    awaiting_review = 0
    inactive = 0

    for s in batch:
        try:
            raw = provider.get_sound_info(s["sound_id"])
            if not raw:
                new_status, video_count, title, author = "inactive", 0, "", ""
            else:
                music_info = raw.get("musicInfo", {})
                if music_info:
                    music = music_info.get("music", {})
                    stats = music_info.get("stats", {})
                    video_count = stats.get("videoCount") or 0
                    title = music.get("title") or ""
                    author = music.get("authorName") or ""
                    play_url = music.get("playUrl") or ""
                else:
                    video_count = raw.get("video_count") or 0
                    title = raw.get("title") or ""
                    author = raw.get("author") or ""
                    play_url = raw.get("play") or ""

                if video_count == 0:
                    new_status = "inactive"
                else:
                    # NOTE: audio fingerprinting deliberately does NOT
                    # happen here anymore. It used to run inline, right in
                    # this loop — but that meant up to QUALIFY_BATCH_SIZE
                    # (20) synchronous audio-fetch + ACRCloud calls inside
                    # one web request, reintroducing exactly the timeout
                    # risk QUALIFY_BATCH_SIZE was built to prevent for the
                    # cheaper video_count check. Fingerprinting now happens
                    # exclusively via the async run_fingerprint_backlog
                    # worker (see below in this file), triggered on its
                    # own cron schedule — candidates land here as
                    # 'unchecked' and get picked up shortly after by that
                    # worker instead of blocking this request.
                    is_relevant = _classify_sound_match(
                        title, author, song_name, song_artist, video_count,
                        discovered_via=s.get("discovered_via")
                    )
                    if auto_approve:
                        new_status = "approved" if is_relevant else "inactive"
                    else:
                        # Find New Sounds — three buckets, not two:
                        #   Auto Reject  -> fails _is_plausible_candidate
                        #                   entirely (no title or artist
                        #                   relation at all) -> inactive,
                        #                   regardless of video_count. This
                        #                   is the SAME bar that already
                        #                   keeps discovery clean, reapplied
                        #                   here with fresh, API-verified
                        #                   title/author — also correctly
                        #                   re-rejects any stale pending
                        #                   rows left over from before that
                        #                   filter existed.
                        #   Needs Review -> passes the plausibility bar but
                        #                   doesn't clear qualify's strict
                        #                   Tier 1 bar -> pending, for a
                        #                   human to judge (remixes by
                        #                   unconfirmed uploaders, generic
                        #                   reposts with no artist
                        #                   confirmation, etc).
                        #   Auto Approve -> handled above when auto_approve
                        #                   is True; never reachable here.
                        if is_relevant:
                            new_status = "pending"
                        else:
                            # Sole gate for human review: the same
                            # plausibility bar discovery already uses.
                            # A video-count-based exception for zero-
                            # signal generic uploads was considered and
                            # deliberately NOT added — popularity is not
                            # evidence of identity (a viral "original
                            # sound" can be completely unrelated; a real
                            # repost might only have 2,000 videos). If
                            # measurement across a real sample of songs
                            # shows generic uploads are a meaningful
                            # source of missed matches, that's a real,
                            # data-backed reason to revisit this — not
                            # something to guess a threshold for now.
                            # The actual fix for "TikTok's metadata
                            # doesn't identify this audio" is a different
                            # source of truth entirely (audio
                            # fingerprinting), not more string heuristics.
                            is_plausible = _is_plausible_candidate(
                                title, author, song_name, song_artist, s.get("discovered_via")
                            )
                            new_status = "pending" if is_plausible else "inactive"
                    _log(f"  sound {s['id']} '{title}' by '{author}' video_count={video_count} "
                         f"discovered_via={s.get('discovered_via')} relevant={is_relevant} -> {new_status}")

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET status=%s, current_video_count=%s,
                            title=COALESCE(NULLIF(%s,''), title),
                            author=COALESCE(NULLIF(%s,''), author)
                        WHERE id=%s
                    """, (new_status, video_count, title, author, s["id"]))
                conn.commit()

            if new_status == "approved":
                approved += 1
            elif new_status == "pending":
                awaiting_review += 1
            else:
                inactive += 1
        except Exception as e:
            _log(f"qualify_pending_sounds_for_song: failed on sound {s['id']}: {e}")

    _log(f"qualify_pending_sounds_for_song: song {song_id} — {approved} approved, "
         f"{awaiting_review} awaiting review, {inactive} inactive, "
         f"{len(bulk_rejected_ids)} bulk-rejected (no API call), {remaining} still pending")
    return {
        "approved": approved,
        "awaiting_review": awaiting_review,
        "inactive": inactive,
        "bulk_rejected": len(bulk_rejected_ids),
        "checked": len(batch),
        "remaining_pending": remaining,
    }




def run_fingerprint_backlog(db_conn_factory, batch_size=40, time_budget_seconds=25, song_id=None):
    """The fingerprint worker — drains the backlog of pending sounds that
    haven't been audio-verified yet.

    By default (song_id=None) this pulls from the GLOBAL queue, oldest
    candidate first across every song — that's what the cron uses. This
    caused real confusion once discovery started finding way more
    candidates: clicking "Find New Sounds" on one song, then running this
    worker, would often process a COMPLETELY DIFFERENT, older song's
    leftover backlog instead, since the queue is pure FIFO by sound id,
    not "whichever song you just touched." Passing song_id scopes this
    run to just that one song's pending candidates, so you can actually
    verify what you just discovered on demand instead of guessing whether
    the global queue happened to reach it yet.

    SAFE TO RUN AUTOMATICALLY ON A SCHEDULE (the song_id=None case). Unlike
    the discover/qualify crons that were deliberately removed from
    routes_refresh.py (see that file's docstring), this function never
    touches sounds.status. It only writes to the fingerprint_* columns —
    pure verification data layered on top of candidates a human already
    explicitly created via "Find New Sounds". It cannot silently expand,
    approve, or change the canonical sound set, so it doesn't reintroduce
    the "why did these appear? the cron decided" problem this
    architecture exists to prevent.

    Time-boxed rather than purely count-boxed (unlike QUALIFY_BATCH_SIZE)
    because fingerprint latency is unpredictable — audio fetch + ACRCloud
    round trip per candidate, not a single fast metadata call. Stops at
    whichever limit — batch_size or time_budget_seconds — comes first, to
    stay well under a gunicorn worker timeout.

    NOTE: play_url is not stored anywhere (it's only ever fetched
    transiently, mid-request, during qualify). This worker runs
    independently and must re-fetch it via the provider before it can
    fingerprint anything — one extra provider call per candidate, on top
    of the ACRCloud call itself.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.status = 'pending'
                  AND snd.fingerprint_status = 'unchecked'
                  {song_filter_sql}
                  AND snd.song_id IN (
                      SELECT cs.song_id FROM campaign_songs cs
                      JOIN campaigns camp ON camp.id = cs.campaign_id
                      WHERE camp.status = 'In Progress'
                  )
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            batch = [dict(r) for r in c.fetchall()]

    _log(f"run_fingerprint_backlog: {len(batch)} candidates pulled (batch_size={batch_size})")

    checked = matched = mismatched = inconclusive = errors = 0

    for s in batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"run_fingerprint_backlog: time budget ({time_budget_seconds}s) reached, "
                 f"stopping early after {checked}/{len(batch)}")
            break

        try:
            info = provider.get_sound_info(s["sound_id"])
            parsed = parse_sound_info(info)
            play_url = (parsed or {}).get("play_url")

            fp_result = _fingerprint.fingerprint_sound(play_url, s["song_name"], s["song_artist"])

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET fingerprint_status=%s,
                            fingerprint_recording_id=%s,
                            fingerprint_title=%s,
                            fingerprint_artist=%s,
                            fingerprint_confidence=%s,
                            fingerprint_checked_at=now()
                        WHERE id=%s
                    """, (
                        fp_result.get("status"),
                        fp_result.get("recording_id"),
                        fp_result.get("title"),
                        fp_result.get("artist"),
                        fp_result.get("confidence"),
                        s["id"],
                    ))
                conn.commit()

            checked += 1
            status = fp_result.get("status")
            if status == "matched":
                matched += 1
            elif status == "mismatched":
                mismatched += 1
            elif status == "inconclusive":
                inconclusive += 1
            else:
                errors += 1

        except Exception as e:
            _log(f"run_fingerprint_backlog: failed on sound {s['id']}: {e}")
            errors += 1

    _log(f"run_fingerprint_backlog: {checked} checked — {matched} matched, "
         f"{mismatched} mismatched, {inconclusive} inconclusive, {errors} errors")

    return {
        "pulled": len(batch),
        "checked": checked,
        "matched": matched,
        "mismatched": mismatched,
        "inconclusive": inconclusive,
        "errors": errors,
    }




def _compute_recommendation(fingerprint_status, fingerprint_confidence):
    """Turns the raw fingerprint result into a clean, human-facing
    recommendation — 'approve' / 'reject' / 'review' — instead of making
    a reviewer interpret internal fingerprint_status values themselves.
    This is intentionally simple right now (a direct mapping); it's the
    natural place to fold in additional signals (text match strength,
    video count) later without changing anything that reads
    `recommendation` downstream.
    """
    if fingerprint_status == "matched":
        return "approve"
    if fingerprint_status == "mismatched":
        return "reject"
    return "review"  # inconclusive or error — no confident automatic answer




def run_ai_review_backlog(db_conn_factory, batch_size=15, time_budget_seconds=25, song_id=None):
    """The AI sound-review worker — the 'final stamp' after fingerprinting,
    run specifically on candidates fingerprinting already checked and did
    NOT confirm as a match to the master recording (fingerprint_status
    'mismatched' or 'inconclusive'). That is NOT the same as "definitely
    unrelated" — it just means there's no official recording to match
    against, which is exactly and only true of remixes, reposts, and
    derivative/background-audio use. Those are the candidates a human
    currently has to eyeball one at a time in the pending review queue;
    this looks at the same sample video thumbnails a human would and
    renders the same kind of judgment, with reasoning attached.

    Mirrors run_fingerprint_backlog's shape deliberately: same song_id
    scoping (None = global FIFO queue for the cron; a song_id = "verify
    what I just discovered on this song, on demand"), same time-boxing
    (vision calls + a thumbnail-fetch round trip per candidate have
    unpredictable latency, same reasoning as the fingerprint worker).

    NEVER touches sounds.status — writes only to ai_review_* columns,
    same "verification layer, not a lifecycle transition" boundary the
    fingerprint worker holds. Auto-approve/reject based on this is a
    deliberate FUTURE decision once accuracy is proven out over time —
    today this only informs the human reviewer, it doesn't act on its
    own recommendation.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.status = 'pending'
                  AND snd.fingerprint_status IN ('mismatched', 'inconclusive')
                  AND snd.ai_review_status = 'unchecked'
                  {song_filter_sql}
                  AND snd.song_id IN (
                      SELECT cs.song_id FROM campaign_songs cs
                      JOIN campaigns camp ON camp.id = cs.campaign_id
                      WHERE camp.status = 'In Progress'
                  )
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            batch = [dict(r) for r in c.fetchall()]

    _log(f"run_ai_review_backlog: {len(batch)} candidates pulled (batch_size={batch_size})")

    checked = approved = rejected = needs_human = errors = 0

    for s in batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"run_ai_review_backlog: time budget ({time_budget_seconds}s) reached, "
                 f"stopping early after {checked}/{len(batch)}")
            break

        try:
            raw = provider.get_sound_posts_page(s["sound_id"], cursor=0, count=6)
            posts, _, _ = parse_posts_from_music_page(raw)
            sample_posts = [
                {
                    "username": p.get("username"),
                    "thumbnail": p.get("thumbnail"),
                    "description": p.get("description"),
                    "views": p.get("views", 0),
                }
                for p in posts[:5] if p.get("thumbnail")
            ]

            result = _ai_service.review_sound_candidate(
                s["song_name"], s["song_artist"], s["title"], s["author"],
                s.get("discovered_via"), sample_posts
            )

            if result is None:
                with db_conn_factory() as conn:
                    with conn.cursor() as c:
                        c.execute("""
                            UPDATE sounds SET ai_review_status='error', ai_review_checked_at=now()
                            WHERE id=%s
                        """, (s["id"],))
                    conn.commit()
                errors += 1
                continue

            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET ai_review_status='reviewed',
                            ai_review_confidence=%s,
                            ai_review_recommendation=%s,
                            ai_review_reasoning=%s,
                            ai_review_checked_at=now()
                        WHERE id=%s
                    """, (
                        result["confidence"], result["recommendation"],
                        result["reasoning"], s["id"],
                    ))
                conn.commit()

            checked += 1
            if result["recommendation"] == "approve":
                approved += 1
            elif result["recommendation"] == "reject":
                rejected += 1
            else:
                needs_human += 1

        except Exception as e:
            _log(f"run_ai_review_backlog: failed on sound {s['id']}: {e}")
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds SET ai_review_status='error', ai_review_checked_at=now()
                        WHERE id=%s
                    """, (s["id"],))
                conn.commit()
            errors += 1

    _log(f"run_ai_review_backlog: {checked} checked — {approved} approve, "
         f"{rejected} reject, {needs_human} needs_human, {errors} errors")

    return {
        "pulled": len(batch),
        "checked": checked,
        "approved": approved,
        "rejected": rejected,
        "needs_human": needs_human,
        "errors": errors,
    }




def process_sound_pipeline(db_conn_factory, batch_size=40, time_budget_seconds=25, song_id=None):
    """THE state machine worker (state machine migration, step 4 — see
    HANDOFF_state_machine_migration.md). One transition, not two:

        DISCOVERED -> FINGERPRINTING -> AWAITING_REVIEW

    There is no separate VERIFIED state. Fingerprinting IS the
    verification — the moment the audio check comes back, we already have
    everything needed (title, artist, confidence, recommendation) to land
    the sound in the review queue. "Verified" would have been a lifecycle
    stage with nothing left to do in it — those are attributes of the
    sound (fingerprint_status, fingerprint_confidence, recommendation),
    not a distinct place in its journey. Splitting it into two states and
    two query passes was one state too many.

    song_id=None means "process everything, across every active-campaign
    song" — this is the one function every caller uses, the same way,
    with no duplicate logic:
        - manual button on a song page  -> song_id=<that song>
        - the eventual single cron      -> song_id=None
        - a CLI / test script           -> either, same function

    CURRENT SAFETY CAVEAT, not a limitation of the function itself: right
    now every sound gets BOTH status='pending' (old system) AND
    state='discovered' (new system) from the dual-write in create_sound/
    get_or_create_sound. Calling this with song_id=None today would
    re-fingerprint (and re-pay for) candidates the OLD run_fingerprint_
    backlog worker has already checked, since both draw from largely the
    same underlying rows during the migration's transition period. The
    route calling this currently requires song_id for that reason alone
    — once the old worker/qualify endpoints are retired (migration step
    6), that restriction goes away and this runs globally on a cron,
    exactly as designed.
    """
    import time as _time
    start = _time.monotonic()

    song_filter_sql = "AND snd.song_id = %s" if song_id is not None else ""
    query_params = ([song_id] if song_id is not None else []) + [batch_size]

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT snd.id, snd.sound_id, snd.title, snd.author, snd.discovered_via,
                       sg.name AS song_name, sg.artist AS song_artist
                FROM sounds snd
                JOIN songs sg ON sg.id = snd.song_id
                WHERE snd.state = 'discovered'
                  {song_filter_sql}
                ORDER BY snd.id ASC
                LIMIT %s
            """, query_params)
            discovered_batch = [dict(r) for r in c.fetchall()]

    _log(f"process_sound_pipeline: {len(discovered_batch)} DISCOVERED candidates pulled")

    fingerprinted = approved_rec = rejected_rec = review_rec = bulk_rejected = errors = 0

    for s in discovered_batch:
        if _time.monotonic() - start > time_budget_seconds:
            _log(f"process_sound_pipeline: time budget reached, "
                 f"stopping after {fingerprinted}/{len(discovered_batch)}")
            break

        # Cheap, FREE pre-filter before spending a real ACRCloud call —
        # same _could_possibly_qualify check the old qualify step used to
        # bulk-reject obvious zero-textual-relation garbage at no cost.
        # Without this, every discovered candidate (including the ones
        # discovery deliberately casts a wide net to include) would get a
        # paid fingerprint check, even ones we're already certain would
        # fail. This preserves that cost-saving without reintroducing it
        # as a separate "qualify" concept the caller has to know about.
        if not _could_possibly_qualify(s["title"], s["author"], s["song_name"], s["song_artist"], s["discovered_via"]):
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET state='rejected', recommendation='reject',
                            fingerprint_status='not_checked_bulk_rejected'
                        WHERE id=%s
                    """, (s["id"],))
                conn.commit()
            bulk_rejected += 1
            continue

        try:
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE sounds SET state='fingerprinting' WHERE id=%s", (s["id"],))
                conn.commit()

            info = provider.get_sound_info(s["sound_id"])
            parsed = parse_sound_info(info)
            play_url = (parsed or {}).get("play_url")
            # FIXED 7/19: video_count was already sitting in `parsed`
            # (parse_sound_info returns it) but never actually stored —
            # meaning every candidate showed "0 videos" in the review UI
            # regardless of its real size, since current_video_count was
            # never populated for anything that hadn't already been
            # through the OLD ingest_sound path (only approved sounds go
            # through that). This was hiding real size information from
            # reviewers on every pending candidate.
            video_count = (parsed or {}).get("video_count")

            fp_result = _fingerprint.fingerprint_sound(play_url, s["song_name"], s["song_artist"])
            recommendation = _compute_recommendation(fp_result.get("status"), fp_result.get("confidence"))

            # One write, straight to the final pre-decision state — no
            # intermediate VERIFIED row, no second query pass to find it
            # again a moment later.
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds
                        SET fingerprint_status=%s,
                            fingerprint_recording_id=%s,
                            fingerprint_title=%s,
                            fingerprint_artist=%s,
                            fingerprint_confidence=%s,
                            fingerprint_checked_at=now(),
                            recommendation=%s,
                            current_video_count=COALESCE(%s, current_video_count),
                            state='awaiting_review'
                        WHERE id=%s
                    """, (
                        fp_result.get("status"),
                        fp_result.get("recording_id"),
                        fp_result.get("title"),
                        fp_result.get("artist"),
                        fp_result.get("confidence"),
                        recommendation,
                        video_count,
                        s["id"],
                    ))
                conn.commit()

            fingerprinted += 1
            if recommendation == "approve":
                approved_rec += 1
            elif recommendation == "reject":
                rejected_rec += 1
            else:
                review_rec += 1

        except Exception as e:
            _log(f"process_sound_pipeline: failed on sound {s['id']}: {e}")
            errors += 1

    _log(f"process_sound_pipeline: {bulk_rejected} bulk-rejected for free (no API call), "
         f"{fingerprinted} fingerprinted and moved to awaiting_review "
         f"({approved_rec} recommend approve, {rejected_rec} recommend reject, "
         f"{review_rec} recommend review, {errors} errors)")

    return {
        "discovered_pulled": len(discovered_batch),
        "bulk_rejected": bulk_rejected,
        "fingerprinted": fingerprinted,
        "recommend_approve": approved_rec,
        "recommend_reject": rejected_rec,
        "recommend_review": review_rec,
        "errors": errors,
    }