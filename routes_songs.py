"""
routes_songs.py — the Songs dashboard (Chartex-style monitoring).

A Song is independent of any Campaign — it's tracked forever once added,
regardless of whether there's an active marketing push behind it. Each Song
has many Sounds (Original, Sped Up, Remix, etc), each Sound has many Posts.
"""

from flask import Blueprint, jsonify, request, render_template_string, Response
from ingestion import api as ingestion
from db import db
import requests

songs_bp = Blueprint("songs", __name__)

# Max number of a song's approved sounds to actually refresh (network calls)
# in one call to /api/songs/<id>/refresh. Standalone Songs (not attached to
# any in-progress Campaign) have NO cron safety net — this manual refresh
# is the ONLY path their sounds ever get updated through, unlike campaign
# sounds which also get picked up by the hourly monitor cron. So instead of
# capping and leaving a remainder to "get picked up automatically" (there's
# nothing to pick it up), this orders by staleness (oldest last_ingested_at
# first) and only processes the top N — repeated manual refreshes then
# naturally rotate through every approved sound over time, each call bounded
# and safe, rather than one call trying to force-refresh everything at once.
SONG_REFRESH_BATCH_SIZE = 15

# How many of a song's posts /api/songs/<id>/detail embeds for the Creators
# panel rollup (which sums views per creator to rank top 20). The main Posts
# grid no longer reads from here — it calls /api/songs/<id>/posts directly,
# which queries+paginates the full posts table server-side per filter (see
# that route for why: this views-DESC sample structurally excludes recent
# posts, so it's wrong for anything time-window-based). 150 is plenty for a
# views-based creator rollup, which is naturally dominated by top-viewed
# posts anyway.
TOP_POSTS_LIMIT = 150


@songs_bp.route("/api/songs", methods=["GET", "POST"])
def songs_collection():
    if request.method == "POST":
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Request body must be valid JSON"}), 400
        name = str(data.get("name", "")).strip()
        artist = str(data.get("artist", "")).strip()
        if not name:
            return jsonify({"error": "Song name required"}), 400

        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO songs (name, artist) VALUES (%s,%s) RETURNING id
                """, (name, artist))
                song_id = c.fetchone()["id"]
            conn.commit()

        # IMPORTANT: this used to also call ingestion.ingest_song_sounds()
        # (a separate, older discovery implementation living in
        # ingestion/api.py, never touched by this session's rebuild) right
        # here at creation time. That ran in addition to, and before, the
        # new discover_song_sounds() pipeline that quick_refresh triggers
        # right after — meaning every new song was silently discovered
        # TWICE, once by old un-capped/unfiltered logic and once by the
        # new capped/filtered one. That's confirmed by the numbers not
        # adding up: Griddle showed 38 total persisted sounds despite
        # discover_song_sounds reporting only 30 discovered; Back Home
        # showed 48 vs 30. Per the discovery/refresh/find_new_sounds
        # architecture this session settled on, song CREATION should not
        # discover anything at all — only initialize_song (triggered via
        # quick_refresh) or find_new_sounds should ever call discovery.
        # The frontend is expected to call quick_refresh right after this
        # returns, same as it already does.
        return jsonify({
            "ok": True,
            "song_id": song_id,
        })

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT
                    s.id, s.name, s.artist, s.created_at,
                    COUNT(DISTINCT snd.id) as sound_count,
                    COUNT(DISTINCT p.post_id) as post_count,
                    COUNT(DISTINCT p.username) as creator_count,
                    COALESCE(SUM(p.views), 0) as total_views,
                    COALESCE(SUM(p.likes), 0) as total_likes
                FROM songs s
                LEFT JOIN sounds snd ON snd.song_id = s.id
                LEFT JOIN posts p ON p.sound_db_id = snd.id
                GROUP BY s.id
                ORDER BY total_views DESC
            """)
            rows = [dict(r) for r in c.fetchall()]
    for r in rows:
        r["created_at"] = str(r["created_at"])
    return jsonify(rows)


@songs_bp.route("/api/songs/<int:song_id>/quick_refresh", methods=["POST"])
def quick_refresh(song_id):
    """Runs ONCE right after a new song is created — campaign.html has
    always called this route immediately after POST /api/songs, but it
    never actually existed until now (confirmed: no route in this file
    called initialize_song, meaning every newly created song got ZERO
    initial discovery via any live path — a real, separate bug found
    while wiring in Community Discovery, not something this route
    introduces).

    Calls initialize_song, which now runs BOTH discovery sensors (title
    search + Community Discovery) before qualifying/ingesting — see
    initialize_song and discover_community_sounds_for_song in
    ingestion/service.py.
    """
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Song not found"}), 404
            name = row["name"]
            artist = row["artist"] or ""

    result = ingestion.initialize_song(db, song_id, name, artist)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/api/songs/<int:song_id>/detail")
def song_detail(song_id):
    window = request.args.get("window", "all")

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()
            if not song_row:
                return jsonify({"error": "Song not found"}), 404
            song = dict(song_row)
            song["created_at"] = str(song["created_at"])

            c.execute("""
                SELECT id, sound_id, title, author, status, current_video_count,
                       posts_24h, posts_7d, velocity,
                       -- How many of this sound's posts we've actually
                       -- collected into our own posts table -- the numerator
                       -- for a real coverage ratio (collected / current_video_count).
                       -- Scalar subquery so it isn't affected by anything
                       -- else joined against sounds elsewhere on this page.
                       (SELECT COUNT(*) FROM posts p WHERE p.sound_db_id = sounds.id) as posts_collected
                FROM sounds WHERE song_id=%s AND status='approved'
                ORDER BY velocity DESC NULLS LAST, current_video_count DESC NULLS LAST
            """, (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT
                    COUNT(DISTINCT p.post_id) as post_count,
                    COUNT(DISTINCT p.username) as creator_count,
                    COALESCE(SUM(p.views), 0) as views,
                    COALESCE(SUM(p.likes), 0) as likes,
                    COUNT(DISTINCT s.id) as sound_count
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
            """, (song_id,))
            stats_row = dict(c.fetchone())

            # Previously this was a UNION of "top 20 by views" + "top 20 by
            # recency", capping out around ~40 posts total no matter what —
            # which meant the frontend's filters/sort (Most Recent, This
            # Week, 48h) were often working off a stale, views-biased subset
            # that didn't actually contain the right posts for those windows.
            # Since TikTok's own API doesn't support server-side sort-by-views
            # either, sorting/filtering is a client-side job — so instead this
            # pulls one much larger set (capped at TOP_POSTS_LIMIT, ordered by
            # views) and lets the frontend paginate/sort/filter over the full
            # set in JS.
            c.execute("""
                SELECT p.post_id, p.username, p.views, p.likes, p.comments,
                       p.saves, p.shares, p.thumbnail, p.description,
                       p.created_at, p.date
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
                ORDER BY p.views DESC NULLS LAST
                LIMIT %s
            """, (song_id, TOP_POSTS_LIMIT))
            top_posts = [dict(r) for r in c.fetchall()]
            for p in top_posts:
                p["created_at"] = str(p["created_at"]) if p["created_at"] else None
                p["date"] = str(p["date"]) if p["date"] else None

            c.execute("""
                SELECT p.username,
                       COUNT(DISTINCT p.post_id) as post_count,
                       COALESCE(SUM(p.views), 0) as total_views,
                       COALESCE(SUM(p.likes), 0) as total_likes
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
                GROUP BY p.username
                ORDER BY total_views DESC
                LIMIT 10
            """, (song_id,))
            top_creators = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT p.date, COALESCE(SUM(p.views), 0) as views
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved'
                GROUP BY p.date
                ORDER BY p.date ASC
            """, (song_id,))
            trend = [{"date": str(r["date"]), "views": r["views"]} for r in c.fetchall()]

    return jsonify({
        "song": song,
        "sounds": sounds,
        "header_stats": {
            "post_count": stats_row["post_count"],
            "creator_count": stats_row["creator_count"],
            "views": stats_row["views"],
            "likes": stats_row["likes"],
            "sound_count": stats_row["sound_count"],
        },
        "top_posts": top_posts,
        "top_creators": top_creators,
        "trend": trend,
        "window": window,
    })


@songs_bp.route("/api/songs/<int:song_id>/insight")
def song_insight(song_id):
    from services.ai_service import generate_song_insight
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"insight": None}), 404
            song = dict(row)

            c.execute("""
                SELECT p.username, COUNT(*) as post_count, SUM(p.views) as total_views
                FROM posts p JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s
                GROUP BY p.username ORDER BY total_views DESC LIMIT 5
            """, (song_id,))
            top_creators = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT description FROM posts p JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND description IS NOT NULL AND description != ''
                LIMIT 20
            """, (song_id,))
            descriptions = [r["description"] for r in c.fetchall()]

            c.execute("""
                SELECT COUNT(DISTINCT p.post_id) as post_count,
                       COUNT(DISTINCT p.username) as creator_count,
                       COALESCE(SUM(p.views), 0) as views,
                       COALESCE(SUM(p.likes), 0) as likes,
                       COUNT(DISTINCT s.id) as sound_count
                FROM sounds s LEFT JOIN posts p ON p.sound_db_id = s.id
                WHERE s.song_id = %s
            """, (song_id,))
            stats = dict(c.fetchone())

    insight = generate_song_insight(song["name"], song["artist"], stats, top_creators, descriptions)
    return jsonify({"insight": insight})


@songs_bp.route("/api/songs/<int:song_id>", methods=["DELETE"])
def delete_song(song_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM sounds WHERE song_id=%s", (song_id,))
            c.execute("DELETE FROM songs WHERE id=%s", (song_id,))
        conn.commit()
    return jsonify({"ok": True})


@songs_bp.route("/api/sounds/<int:sound_db_id>/preview_posts")
def preview_sound_posts(sound_db_id):
    """Fetch a handful of sample posts for a PENDING sound directly from
    the provider — read-only, nothing written to the database, not part
    of the ingest pipeline. This exists specifically so a human reviewing
    the Find New Sounds queue can actually watch/listen to real videos
    using this sound before approving or rejecting it, instead of judging
    from title/author text alone. Normal ingestion only pulls posts for
    sounds that are ALREADY approved, so without this, a pending
    candidate has zero videos attached to look at."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT sound_id FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Sound not found"}), 404

    from ingestion.providers import default_provider as provider
    from ingestion.parsers import parse_posts_from_music_page

    try:
        raw = provider.get_sound_posts_page(row["sound_id"], cursor=0, count=6)
        posts, has_more, next_cursor = parse_posts_from_music_page(raw)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    preview = [
        {
            "post_id": p.get("post_id"),
            "username": p.get("username"),
            "thumbnail": p.get("thumbnail"),
            "views": p.get("views", 0),
        }
        for p in posts[:6] if p.get("post_id") and p.get("username")
    ]
    return jsonify(preview)


@songs_bp.route("/api/thumbnail_proxy")
def thumbnail_proxy():
    """Fetches a thumbnail image server-side and streams it back through
    our own domain, instead of the frontend hotlinking TikTok's CDN URL
    directly in an <img src>.

    WHY THIS EXISTS: pending-review thumbnails were rendering as flat
    gray boxes with no visible error — TikTok's CDN checks the browser's
    Referer/Origin header on image requests and silently refuses ones
    that don't come from tiktok.com itself, returning nothing usable
    rather than a loud 403. A server-to-server fetch (this route, running
    on Render) doesn't send a browser Referer at all, so it isn't subject
    to that check — the browser then loads the image from OUR domain,
    which was never blocked in the first place.

    Only proxies tiktokcdn.com-family hosts — deliberately not a generic
    open proxy for arbitrary URLs (that would let this route be used to
    fetch/relay anything, a real SSRF-style risk for a public route).
    """
    url = request.args.get("url", "")
    if not url or "tiktokcdn" not in url:
        return jsonify({"error": "Invalid or disallowed URL"}), 400

    try:
        resp = requests.get(
            url,
            timeout=10,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                # Some of these signed CDN URLs return non-200 with NO
                # referer at all (not just a wrong one) — confirmed via
                # real 502s in production logs on a subset of thumbnails,
                # inconsistent with a blanket "block all non-tiktok
                # referers" policy. Sending one that looks like a genuine
                # TikTok page load fixes the ones that specifically
                # require SOME referer to be present.
                "Referer": "https://www.tiktok.com/",
            },
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Upstream returned {resp.status_code}"}), 502
        return Response(
            resp.content,
            mimetype=resp.headers.get("Content-Type", "image/jpeg"),
            headers={"Cache-Control": "public, max-age=3600"},  # thumbnails don't change; safe to cache an hour
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@songs_bp.route("/api/sounds/<int:sound_db_id>", methods=["DELETE"])
def delete_sound(sound_db_id):
    with db() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM sounds WHERE id=%s", (sound_db_id,))
        conn.commit()
    return jsonify({"ok": True})


@songs_bp.route("/api/sounds/<int:sound_db_id>/approve", methods=["POST"])
def approve_sound(sound_db_id):
    """Manually approve a pending sound — the human-in-the-loop step for
    candidates find_new_sounds found but deliberately left pending rather
    than auto-approving. Immediately ingests posts for it too, so it
    starts showing real data right away instead of waiting for the next
    refresh cycle."""
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id, song_id, sound_id FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Sound not found"}), 404
            c.execute("UPDATE sounds SET status='approved' WHERE id=%s", (sound_db_id,))
        conn.commit()

    result = ingestion_service.ingest_sound(db, row["song_id"], row["id"], row["sound_id"], max_results=35)
    return jsonify({"ok": True, "sound_id": sound_db_id, "posts_added": result.get("posts_added", 0)})


@songs_bp.route("/api/sounds/<int:sound_db_id>/reject", methods=["POST"])
def reject_sound(sound_db_id):
    """Manually reject a pending sound — explicit human 'no', distinct
    from DELETE (which removes the row entirely). Keeps the sound's
    history/title/author on record as 'inactive' rather than erasing it."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM sounds WHERE id=%s", (sound_db_id,))
            if not c.fetchone():
                return jsonify({"error": "Sound not found"}), 404
            c.execute("UPDATE sounds SET status='inactive' WHERE id=%s", (sound_db_id,))
        conn.commit()
    return jsonify({"ok": True, "sound_id": sound_db_id})


@songs_bp.route("/api/songs/<int:song_id>/pending_review")
def pending_review(song_id):
    """Lists sounds still sitting 'pending' for a song — the review queue
    a human works through after 'Find New Sounds' runs. Since qualify no
    longer uses its strict automatic-approval bar to decide who reaches
    this queue (see qualify_pending_sounds_for_song), it includes both
    confirmed-looking matches AND genuinely ambiguous candidates (remixes
    by unconfirmed uploaders, generic reposts with no textual artist
    confirmation) — anything with real traction, for a human to judge.

    Each row includes a `likely_match` flag, computed on-the-fly with the
    same classifier used for automatic approval — purely informational,
    to help you tell "the algorithm is confident, this is probably just a
    duplicate upload of the real sound" apart from "this genuinely needs
    you to go look at the video," not to gate anything.

    Sorted by video_count so the most active candidates surface first.
    """
    from ingestion.service import _classify_sound_match

    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            song_row = c.fetchone()
            song_name = song_row["name"] if song_row else ""
            song_artist = song_row["artist"] if song_row else ""

            c.execute("""
                SELECT id, sound_id, title, author, current_video_count, discovered_via,
                       fingerprint_status, ai_review_status, ai_review_confidence,
                       ai_review_recommendation, ai_review_reasoning
                FROM sounds
                WHERE song_id = %s AND status = 'pending'
                ORDER BY current_video_count DESC NULLS LAST
            """, (song_id,))
            pending = [dict(r) for r in c.fetchall()]

    for s in pending:
        s["likely_match"] = _classify_sound_match(
            s.get("title"), s.get("author"), song_name, song_artist,
            s.get("current_video_count") or 0, discovered_via=s.get("discovered_via")
        )

    return jsonify(pending)


@songs_bp.route("/api/songs/<int:song_id>/run_ai_review", methods=["POST"])
def run_ai_review(song_id):
    """Triggers the AI sound-review 'final stamp' for this song's pending
    candidates — only ones fingerprinting already checked and couldn't
    confirm as a master-recording match (see run_ai_review_backlog).
    Never changes sounds.status; only writes ai_review_* fields the
    pending review UI reads."""
    result = ingestion.run_ai_review_backlog(db, batch_size=15, song_id=song_id)
    return jsonify({"ok": True, **result})


@songs_bp.route("/api/songs/<int:song_id>/posts")
def song_posts(song_id):
    """Server-side filtered/sorted/paginated posts for a song's video
    statistics grid.

    IMPORTANT: this replaces a views-DESC-LIMIT-100 (and, before that, a
    views-DESC-LIMIT-300) query that the frontend then filtered/sorted
    CLIENT-SIDE for "Most Recent" / "This Week" / "48h". That was broken by
    construction: a song with thousands of posts has its top-N-by-views
    list dominated by old posts that have had months to accumulate views —
    a post from the last 48h essentially never has enough views yet to make
    a top-300 cut, so it never even reached the frontend. The "48h" filter
    was therefore filtering *within* an already-views-biased subset that
    structurally excluded almost everything recent, showing "No posts
    found" even when thousands of genuinely recent posts existed in the
    database. This queries the full table directly per filter instead, so
    each time window is actually correct regardless of how large `posts`
    gets.
    """
    filter_ = request.args.get("filter", "popular")
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    try:
        page_size = int(request.args.get("page_size", 20))
    except ValueError:
        page_size = 20
    page_size = max(1, min(page_size, 200))  # sane ceiling regardless of what's requested

    where_extra = ""
    params = [song_id]
    if filter_ == "today":
        where_extra = "AND p.created_at >= extract(epoch from now() - interval '48 hours')"
    elif filter_ == "week":
        where_extra = "AND p.created_at >= extract(epoch from now() - interval '7 days')"

    order_by = "p.created_at DESC NULLS LAST" if filter_ in ("recent", "today", "week") else "p.views DESC NULLS LAST"

    with db() as conn:
        with conn.cursor() as c:
            c.execute(f"""
                SELECT COUNT(*) as total
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved' {where_extra}
            """, params)
            total = c.fetchone()["total"]

            c.execute(f"""
                SELECT p.post_id, p.username, p.views, p.likes, p.comments,
                       p.saves, p.shares, p.thumbnail, p.description,
                       p.created_at, p.date
                FROM posts p
                JOIN sounds s ON s.id = p.sound_db_id
                WHERE s.song_id = %s AND s.status = 'approved' {where_extra}
                ORDER BY {order_by}
                LIMIT %s OFFSET %s
            """, params + [page_size, (page - 1) * page_size])
            posts = [dict(r) for r in c.fetchall()]

    for p in posts:
        p["date"] = str(p["date"]) if p["date"] else None
        p["created_at"] = str(p["created_at"]) if p["created_at"] else None

    return jsonify({
        "posts": posts,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, -(-total // page_size)),  # ceiling division
    })


@songs_bp.route("/api/songs/<int:song_id>/refresh", methods=["POST"])
def refresh_song(song_id):
    """Routine refresh — updates posts/stats for a song's already-approved
    (canonical) sounds ONLY. Never discovers new candidates.

    This used to also re-run discovery on every call. That made sense
    while the pipeline was still being built and discovery/refresh hadn't
    been split into distinct responsibilities yet — now that they have
    (see initialize_song, refresh_approved_sounds_for_song, and
    find_new_sounds_for_song in ingestion/service.py), refresh should only
    ever touch the canonical set, never expand it.
    """
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM songs WHERE id=%s", (song_id,))
            if not c.fetchone():
                return jsonify({"error": "Song not found"}), 404

    result = ingestion_service.refresh_approved_sounds_for_song(db, song_id, batch_size=SONG_REFRESH_BATCH_SIZE)
    return jsonify({"ok": True, **result})


@songs_bp.route("/api/songs/<int:song_id>/recompute_growth", methods=["POST"])
def recompute_growth(song_id):
    """One-time (or repeatable) backfill: re-derive 24h/7d growth for every
    approved sound of this song from EXISTING song_stats history, with no
    TikAPI/TikLiveAPI call and no quota cost. Needed after the growth-calc
    rewrite (posts-sample-count -> real video-count diffing) — the daily
    snapshots were already being recorded correctly all along, they just
    weren't being read back correctly, so most sounds already have real
    history to recompute from immediately, rather than waiting for each
    one's turn in the normal (API-costing) refresh rotation."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id FROM songs WHERE id=%s", (song_id,))
            if not c.fetchone():
                return jsonify({"error": "Song not found"}), 404
            c.execute("SELECT id FROM sounds WHERE song_id=%s AND status='approved'", (song_id,))
            sound_ids = [r["id"] for r in c.fetchall()]

    for sound_id in sound_ids:
        ingestion.recompute_sound_growth(db, sound_id)

    return jsonify({"ok": True, "recomputed": len(sound_ids)})


@songs_bp.route("/api/songs/<int:song_id>/find_new_sounds", methods=["POST"])
def find_new_sounds(song_id):
    """Explicit, user-triggered discovery — expands a song's canonical
    sound set beyond what was found initially. This is the ONLY place
    besides initial song creation where discovery should ever run. Capped
    the same way as everywhere else (qualify processes QUALIFY_BATCH_SIZE
    candidates per call) to avoid worker timeouts; a song with a large
    pending backlog may need this called more than once.
    """
    from ingestion import service as ingestion_service
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT name, artist FROM songs WHERE id=%s", (song_id,))
            row = c.fetchone()
            if not row:
                return jsonify({"error": "Song not found"}), 404
            name = row["name"]
            artist = row["artist"] or ""

    result = ingestion_service.find_new_sounds_for_song(db, song_id, name, artist)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/api/songs/<int:song_id>/requalify", methods=["POST"])
def requalify_song(song_id):
    """Re-run ONLY the qualify step for a song's pending sounds — no
    discovery (which hits many expensive search-video/challenge API calls
    and would re-add candidates rather than just re-judging existing ones),
    no ingest. Use this after fixing/tuning the matching logic to re-judge
    already-discovered candidates against the corrected rules, without
    paying for full re-discovery.

    Typical flow: reset wrongly-approved sounds back to 'pending' via SQL,
    then call this to re-classify them under the current rules. Capped to
    QUALIFY_BATCH_SIZE candidates per call (same cap as everywhere else),
    so a song with a large pending backlog may need this called more than
    once to fully clear.
    """
    from ingestion import service as ingestion_service
    result = ingestion_service.qualify_pending_sounds_for_song(db, song_id)
    return jsonify({"ok": True, "song_id": song_id, **result})


@songs_bp.route("/songs")
def songs_page():
    return render_template_string(open("songs.html").read())


@songs_bp.route("/song/<int:song_id>")
def song_page(song_id):
    return render_template_string(open("song.html").read())