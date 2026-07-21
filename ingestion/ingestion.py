"""
ingestion/ingestion.py — the Ingestion boundary.

MOVED (provider-boundary refactor, ingestion-boundary step) from
ingestion/service.py — pure relocation, no behavior changes. Every
function here is byte-for-byte identical to its previous version in
service.py; only the file location and imports changed.

Ingestion's job: pull posts/stats for sounds and accounts that are
already known (already-approved sounds, roster/fan accounts, single
posts, campaign-attached sounds). Nothing here discovers new
candidates or decides whether something is legitimate — it only
fetches and stores data for things Discovery/Qualification already
approved or that were added directly (manual post/roster additions).

Note: this file is named ingestion.py, distinct from the ingestion/
PACKAGE it lives inside (ingestion/ingestion.py) — matches the
provider-boundary refactor's existing pattern of one file per boundary
inside the ingestion package (discovery.py, qualification.py, this).
"""

import os
from datetime import date, datetime, timezone
from .providers import default_provider as provider
from .parsers import (
    parse_sound_info,
    parse_posts_from_music_page,
    parse_account_stats,
    parse_posts_from_user_feed,
    parse_single_post,
)
from ._shared import _log

SOUND_FRESHNESS_HOURS = 6

# ── Coverage Engine tuning parameters ───────────────────────────────────────
# These are tuning hypotheses, not fixed truths — expect to adjust them once
# you've seen real coverage metrics (see determine_coverage_plan's logging)
# across a range of actual songs. Deliberately kept as named constants, not
# buried inline, so future tuning only ever touches this one spot.
COVERAGE_TIER_A_VIDEO_THRESHOLD = 10_000   # video_count above this -> Tier A
COVERAGE_TIER_B_VIDEO_THRESHOLD = 50        # video_count above this -> Tier B, else Tier C
                                             # LOWERED 7/18 from 500: real production data showed
                                             # most approved sounds live in the 1-500 range, and a
                                             # sound with 486 real videos (someone's single biggest
                                             # asset for a song) was defaulting to Tier C (30 posts)
                                             # purely because 486 < 500 — an arbitrary line, not a
                                             # real distinction. Confirmed: that sound only had 42
                                             # posts actually stored despite 486 real videos existing.
                                             # Tier A's 10,000 threshold is left as-is for now —
                                             # no real evidence yet on what a genuinely viral approved
                                             # sound looks like in this dataset to recalibrate it.
COVERAGE_TIER_A_TARGET_POSTS = 1500  # RAISED 7/20 from 300 — now safe because ingestion runs on
COVERAGE_TIER_B_TARGET_POSTS = 400   # RAISED 7/20 from 100 — a separate worker service (see
                                      # HANDOFF), not the main UI. This is the real fix for
                                      # "we approved big sounds but only ended up tracking a
                                      # few hundred of their real posts" — real production
                                      # scale, not a demo-sized sample. Real cost: Tier A now
                                      # means up to ~100 real /music-posts/ pages per sound
                                      # (1500 * 2 fetch_multiplier / 30 per page) — genuinely
                                      # substantial time and API calls per big sound, only
                                      # safe now that it can't freeze the live site.
COVERAGE_TIER_C_TARGET_POSTS = 30          # matches original pre-Coverage-Engine behavior
COVERAGE_TIER_A_FETCH_MULTIPLIER = 2       # paginate to 2x target before ranking/trimming
COVERAGE_TIER_B_FETCH_MULTIPLIER = 2
COVERAGE_TIER_C_FETCH_MULTIPLIER = 1       # no deep pagination for small sounds
COVERAGE_TOP_POST_RATIO = 0.70             # of target_posts, kept by views (the "biggest videos")
COVERAGE_RECENT_POST_RATIO = 0.30          # of target_posts, kept by recency (freshest activity)

SOURCE_CACHE    = "cache"
SOURCE_TIKAPI   = "tikapi"


def _is_sound_fresh(db_conn_factory, sound_db_id):
    """Check whether a sound's cached data is fresh enough to skip re-ingestion."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("SELECT last_ingested_at FROM sounds WHERE id=%s", (sound_db_id,))
            row = c.fetchone()

    if not row or not row["last_ingested_at"]:
        return False, None

    last_ingested = row["last_ingested_at"]
    if last_ingested.tzinfo is None:
        last_ingested = last_ingested.replace(tzinfo=timezone.utc)

    age_hours = (datetime.now(timezone.utc) - last_ingested).total_seconds() / 3600
    return age_hours < SOUND_FRESHNESS_HOURS, age_hours


def _touch_sound_ingested(db_conn_factory, sound_db_id):
    """Update last_ingested_at to now. Called after every successful ingest."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE sounds SET last_ingested_at=NOW() WHERE id=%s", (sound_db_id,))
        conn.commit()


import re as _re
import unicodedata as _unicodedata




def _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id):
    """Pull a sound's true total video count and write today's snapshot."""
    raw = provider.get_sound_info(tiktok_sound_id)
    info = parse_sound_info(raw)
    if not info:
        return {"video_count_updated": False, "error": "music/info call failed"}

    video_count = info.get("video_count")
    if video_count is None:
        return {"video_count_updated": False, "error": "music/info succeeded but had no videoCount field"}

    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO song_stats (sound_id, date, video_count)
                VALUES (%s,%s,%s)
                ON CONFLICT (sound_id, date) DO UPDATE SET video_count=EXCLUDED.video_count
            """, (sound_db_id, today, video_count))
            c.execute("UPDATE sounds SET current_video_count=%s WHERE id=%s", (video_count, sound_db_id))
        conn.commit()
    return {"video_count_updated": True, "video_count": video_count, "error": None}


def determine_coverage_plan(video_count):
    """The Coverage Engine's planning layer — Phase 1 of it (see
    HANDOFF_state_machine_migration.md; the full engine eventually also
    owns refresh cadence, API budget allocation across sounds, and its
    own coverage metrics — this function owns tier + pagination + post
    selection strategy only, for now).

    Decides HOW MUCH of a sound's real activity to try to capture, based
    on how big it actually is, instead of the flat 30-post cap every
    sound used to get regardless of size. Returns a plan dict — the
    execution layer (_ingest_sound_posts) reads this plan and executes
    it; it doesn't know what a "tier" is at all, so tuning coverage
    behavior only ever means touching this one function (and the
    constants above it), never the fetch/write logic itself.

    Confirmed via real production data (7/18): a sound with hundreds or
    thousands of real posts was only ever tracking ~15-30 of them,
    massively undercounting real reach — and confirmed via TikLive's own
    docs that /music-posts/ has no sort-by-views parameter, so finding
    the biggest videos requires paginating deeper into the (apparently
    newest-first) stream and sorting client-side ourselves, not just
    requesting them directly. Two other TikTok data providers (SociaVault
    among them) have the identical constraint on the equivalent endpoint
    — this is a platform-level limitation, not something to work around
    by switching vendors.
    """
    if video_count is None or video_count <= COVERAGE_TIER_B_VIDEO_THRESHOLD:
        tier = "C"
        target_posts = COVERAGE_TIER_C_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_C_FETCH_MULTIPLIER
    elif video_count <= COVERAGE_TIER_A_VIDEO_THRESHOLD:
        tier = "B"
        target_posts = COVERAGE_TIER_B_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_B_FETCH_MULTIPLIER
    else:
        tier = "A"
        target_posts = COVERAGE_TIER_A_TARGET_POSTS
        fetch_multiplier = COVERAGE_TIER_A_FETCH_MULTIPLIER

    return {
        "tier": tier,
        "video_count": video_count,
        "target_posts": target_posts,
        "fetch_target": target_posts * fetch_multiplier,
        "max_pages": (target_posts * fetch_multiplier + 29) // 30,  # count=30 per page
        "top_ratio": COVERAGE_TOP_POST_RATIO,
        "recent_ratio": COVERAGE_RECENT_POST_RATIO,
    }


def _ingest_sound_posts(db_conn_factory, sound_db_id, tiktok_sound_id, coverage_plan):
    """Pull posts for a sound by EXECUTING a coverage plan (see
    determine_coverage_plan) — this function deliberately knows nothing
    about tiers, thresholds, or ratios. It just reads target_posts /
    fetch_target / top_ratio / recent_ratio off the plan it's handed.
    Tuning coverage behavior should never require touching this
    function — only determine_coverage_plan and the constants above it.

    TikLive returns newest first, so plain pagination alone only ever
    gets recent activity. When fetch_target > target_posts, paginates
    deeper, then keeps a blend: the top `top_ratio` by actual views (the
    "biggest videos" the API can't sort for us server-side) plus the
    most recent `recent_ratio` not already included, so a sound doesn't
    lose fresh activity in favor of only ever showing its all-time hits.
    """
    target_posts = coverage_plan["target_posts"]
    fetch_target = coverage_plan["fetch_target"]

    all_posts = []
    cursor = 0
    pages_fetched = 0
    while len(all_posts) < fetch_target:
        raw = provider.get_sound_posts_page(tiktok_sound_id, cursor=cursor, count=30)
        pages_fetched += 1
        posts, has_more, next_cursor = parse_posts_from_music_page(raw)
        if not posts:
            break
        all_posts.extend(posts)
        if not has_more:
            break
        cursor = int(next_cursor) if next_cursor is not None else cursor + 35

    unique_posts = {p.get("post_id"): p for p in all_posts if p.get("post_id")}
    all_posts_unique = list(unique_posts.values())

    if fetch_target > target_posts and len(all_posts_unique) > target_posts:
        top_n = int(target_posts * coverage_plan["top_ratio"])
        recent_n = target_posts - top_n
        by_views = sorted(all_posts_unique, key=lambda p: p.get("views", 0) or 0, reverse=True)
        top_posts = by_views[:top_n]
        top_ids = {p.get("post_id") for p in top_posts}
        recent_posts = [p for p in all_posts_unique if p.get("post_id") not in top_ids][:recent_n]
        posts_to_write = top_posts + recent_posts
    else:
        posts_to_write = all_posts_unique[:target_posts]

    # ── Coverage instrumentation — so tuning is based on real evidence, ──
    # not guessing whether deeper pagination actually found anything.
    views_list = [p.get("views", 0) or 0 for p in all_posts_unique]
    top_view = max(views_list) if views_list else 0
    median_view = sorted(views_list)[len(views_list) // 2] if views_list else 0
    _log(
        f"coverage[sound_db_id={sound_db_id}] tier={coverage_plan['tier']} "
        f"video_count={coverage_plan['video_count']} pages_fetched={pages_fetched} "
        f"posts_downloaded={len(all_posts)} unique_posts={len(all_posts_unique)} "
        f"top_view={top_view:,} median_view={median_view:,} "
        f"stored={len(posts_to_write)}/{target_posts}"
    )

    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in posts_to_write:
                if not p.get("post_id") or not p.get("username"):
                    continue
                c.execute("""
                    INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, source, thumbnail, shares, sound_db_id, followers_at_post)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'sound_auto',%s,%s,%s,%s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                        thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                        shares=EXCLUDED.shares,
                        sound_db_id=COALESCE(EXCLUDED.sound_db_id, posts.sound_db_id),
                        followers_at_post=COALESCE(EXCLUDED.followers_at_post, posts.followers_at_post)
                """, (
                    p["post_id"], today, p["username"],
                    p.get("description", "")[:300],
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("saves", 0),
                    p.get("created_at"), p.get("thumbnail"), p.get("shares", 0),
                    sound_db_id, p.get("followers")
                ))
                # Daily snapshot — one row per post per day, upserted on every
                # refresh so same-day re-ingests just update today's numbers
                # rather than duplicating. This is what makes true
                # tier-crossing detection possible (comparing today's likes
                # to yesterday's), instead of only being able to say "this
                # post is new AND already high" without knowing whether it
                # crossed a threshold today specifically.
                c.execute("""
                    INSERT INTO post_snapshots (post_id, date, views, likes, comments, shares)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (post_id, date) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, shares=EXCLUDED.shares
                """, (
                    p["post_id"], today,
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("shares", 0)
                ))
                # Milestone events — a PERMANENT record of the first time
                # this post was ever observed at each engagement tier.
                # ON CONFLICT (post_id, tier) DO NOTHING means each row
                # can only ever be created once, ever — critical, because
                # the dashboard digest used to instead diff today's
                # snapshot against yesterday's, and treat a MISSING
                # yesterday snapshot as "just crossed." With hundreds of
                # approved sounds and only 25 refreshed per hour, many
                # posts go more than a day between refreshes, so
                # "yesterday's snapshot" was very often just absent — not
                # because the post was new, but because it wasn't touched
                # that specific day. That made long-established, months-
                # old viral posts perpetually resurface as if they'd
                # "just crossed" every single day. This table fixes that
                # structurally: a crossing is recorded exactly once, the
                # first time it's ever seen, and never again after.
                likes = p.get("likes", 0) or 0
                for tier in (1000, 5000, 10000):
                    if likes >= tier:
                        c.execute("""
                            INSERT INTO milestone_events (post_id, tier, crossed_date, views, likes)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT (post_id, tier) DO NOTHING
                        """, (p["post_id"], tier, today, p.get("views", 0), likes))
                added += 1
        conn.commit()
    return added


# ── Public service functions ──────────────────────────────────────────────────



def get_sound_info(tiktok_sound_id):
    """Fetch metadata for a single sound. Returns normalized dict or None."""
    raw = provider.get_sound_info(tiktok_sound_id)
    info = parse_sound_info(raw)
    return info


def ingest_sound(db_conn_factory, song_id, sound_db_id, tiktok_sound_id, max_results=30, force=False):
    """Refresh one Sound's posts and video-count snapshot.
    Checks cache first — skips the provider pipeline if data is fresh.
    Pass force=True to bypass the freshness cache entirely (e.g. for
    manual testing right after a Coverage Engine tuning change, or an
    urgent refresh that can't wait out SOUND_FRESHNESS_HOURS).

    COVERAGE ENGINE, Phase 1 (see HANDOFF_state_machine_migration.md —
    this is one piece of the eventual full engine, which will also own
    refresh cadence and cross-sound API budget allocation; not "done"
    yet, just tiered pagination + post selection for now): max_results
    is now only a fallback for when video_count can't be determined —
    the real target comes from determine_coverage_plan, called right
    after we fetch this sound's fresh video_count below. A sound with
    280,000 videos gets a meaningfully deeper, smarter pull than a
    remix with 19 — no longer a flat number for every sound regardless
    of scale.
    """
    if not force:
        is_fresh, age_hours = _is_sound_fresh(db_conn_factory, sound_db_id)
        if is_fresh:
            _log(f"sound {sound_db_id} is fresh ({age_hours:.1f}h old) — skipping provider pipeline")
            return {
                "sound_db_id": sound_db_id,
                "video_count_updated": False,
                "posts_added": 0,
                "error": None,
                "source": SOURCE_CACHE,
                "degraded": False,
            }

    # Cache miss or stale — continue through the provider pipeline
    result = {
        "sound_db_id": sound_db_id,
        "video_count_updated": False,
        "posts_added": 0,
        "error": None,
        "source": SOURCE_TIKAPI,
        "degraded": False,
    }
    stats_result = _update_sound_video_count(db_conn_factory, sound_db_id, tiktok_sound_id)
    result.update(stats_result)

    coverage_plan = determine_coverage_plan(stats_result.get("video_count"))
    result["coverage_tier"] = coverage_plan["tier"]
    result["posts_added"] = _ingest_sound_posts(
        db_conn_factory, sound_db_id, tiktok_sound_id, coverage_plan
    )

    if not result.get("error"):
        _touch_sound_ingested(db_conn_factory, sound_db_id)
        _update_sound_velocity(db_conn_factory, sound_db_id)

        # If sound has no posts and no video count, mark inactive
        video_count = result.get("video_count", 0) or 0
        posts_added = result.get("posts_added", 0) or 0
        if video_count == 0 and posts_added == 0:
            with db_conn_factory() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE sounds SET status='inactive'
                        WHERE id=%s AND status='approved'
                    """, (sound_db_id,))
                conn.commit()
            _log(f"sound {sound_db_id} marked inactive (0 posts, 0 video_count)")

    return result




def refresh_song_sounds(db_conn_factory, song_id):
    """Refresh every Sound already belonging to a Song (used by the hourly cron)."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("SELECT id, sound_id FROM sounds WHERE song_id=%s AND status='approved'", (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

    posts_added = 0
    ingested = 0
    for s in sounds:
        result = ingest_sound(db_conn_factory, song_id, s["id"], s["sound_id"], max_results=30)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1
    return {"sounds_found": len(sounds), "sounds_ingested": ingested, "posts_added": posts_added}


def ingest_roster_account(db_conn_factory, username):
    """Pull current stats for one Artist roster account and save today's snapshot."""
    raw = provider.get_account(username)
    account = parse_account_stats(raw)
    if not account:
        return False

    today = date.today()
    views_sum = likes_sum = comments_sum = shares_sum = 0
    if account["sec_uid"]:
        raw_posts = provider.get_account_posts(account["sec_uid"], count=10)
        for p in parse_posts_from_user_feed(raw_posts):
            views_sum += p.get("views", 0)
            likes_sum += p.get("likes", 0)
            comments_sum += p.get("comments", 0)
            shares_sum += p.get("shares", 0)

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO roster_stats (username, date, followers, total_likes, video_count, views_24h, likes_24h, comments_24h, shares_24h)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, total_likes=EXCLUDED.total_likes,
                    video_count=EXCLUDED.video_count, views_24h=EXCLUDED.views_24h,
                    likes_24h=EXCLUDED.likes_24h, comments_24h=EXCLUDED.comments_24h,
                    shares_24h=EXCLUDED.shares_24h
            """, (username, today, account["followers"], account["total_likes"],
                  account["video_count"], views_sum, likes_sum, comments_sum, shares_sum))
        conn.commit()
    return True


def ingest_fan_account(db_conn_factory, username):
    """Pull stats for a plain (non-roster) tracked fan account, plus its
    recent posts.

    IMPORTANT: this deliberately does NOT go through provider.get_account
    / provider.get_account_posts (the multi-provider pipeline used
    elsewhere in this file, which tries TikLive first). That pipeline has
    a known gap for this specific use case: TikLiveAPIProvider's
    get_account_posts requires a NUMERIC user ID, while a standard TikTok
    secUid is a long alphanumeric string — so for most accounts, TikLive's
    posts call silently returns nothing, and the pipeline has to fall
    through to TikAPI as a backup. That fallback had never actually been
    verified end-to-end for a real account before this feature shipped.

    Instead, this calls TikAPI directly — the exact same endpoints,
    parameters, and field names as a separate, already-proven prototype
    tool that's been confirmed working in production. Rather than trust
    an abstraction layer for something this important, fan-account
    ingestion uses the exact logic already known to work.
    """
    import requests as _requests

    tikapi_key = os.environ.get("TIKAPI_KEY", "")
    headers = {
        "X-API-KEY": tikapi_key,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    r = _requests.get(
        "https://api.tikapi.io/public/check",
        params={"username": username}, headers=headers, timeout=15
    )
    _log(f"TikAPI {r.status_code} for @{username}")
    if r.status_code != 200:
        _log(f"  {r.text[:300]}")
        return False

    data = r.json()
    info = data.get("userInfo", {})
    user = info.get("user", {})
    s = info.get("statsV2", info.get("stats", {}))
    sec_uid = user.get("secUid")

    followers = int(s.get("followerCount", 0))
    total_likes = int(s.get("heartCount", 0))
    video_count = int(s.get("videoCount", 0))
    today = date.today()

    # DIAGNOSTIC: if we got a secUid (proving the response parsed at all)
    # but every stat came back zero, something about this response's
    # actual shape doesn't match what we're reading — log the raw
    # response so the NEXT time this happens we can see exactly what
    # TikAPI actually sent back, instead of guessing at field names.
    if sec_uid and followers == 0 and total_likes == 0 and video_count == 0:
        _log(f"⚠ @{username}: got secUid but all stats are zero. "
             f"info keys: {list(info.keys())}, user keys: {list(user.keys())}, "
             f"stats dict used: {s}, full statsV2: {info.get('statsV2')}, "
             f"full stats: {info.get('stats')}")

    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO stats (username, date, followers, likes, videos)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (username, date) DO UPDATE SET
                    followers=EXCLUDED.followers, likes=EXCLUDED.likes, videos=EXCLUDED.videos
            """, (username, today, followers, total_likes, video_count))
        conn.commit()

    if sec_uid:
        try:
            r2 = _requests.get(
                "https://api.tikapi.io/public/posts",
                params={"secUid": sec_uid, "count": 10}, headers=headers, timeout=15
            )
            if r2.status_code == 200:
                items = r2.json().get("itemList") or r2.json().get("items") or []
                with db_conn_factory() as conn:
                    with conn.cursor() as c:
                        for item in items:
                            ps = item.get("stats", {})
                            post_id = item.get("id")
                            item_likes = ps.get("diggCount", 0) or 0
                            item_views = ps.get("playCount", 0) or 0
                            c.execute("""
                                INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (post_id) DO UPDATE SET
                                    views=EXCLUDED.views, likes=EXCLUDED.likes,
                                    comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                                    followers_at_post=EXCLUDED.followers_at_post
                            """, (
                                post_id, today, username,
                                item.get("desc", "")[:300],
                                item_views, item_likes,
                                ps.get("commentCount", 0), ps.get("collectCount", 0),
                                item.get("createTime"), followers
                            ))
                            # Same daily-snapshot + milestone-event writes
                            # sound-driven posts already get — this is what
                            # was missing, and why a fan page going viral
                            # never showed up in the "Crossed a Tier Today"
                            # dashboard digest. Without this, fan pages were
                            # completely invisible to the daily digest no
                            # matter how well they performed.
                            if post_id:
                                c.execute("""
                                    INSERT INTO post_snapshots (post_id, date, views, likes, comments, shares)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (post_id, date) DO UPDATE SET
                                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                                        comments=EXCLUDED.comments, shares=EXCLUDED.shares
                                """, (
                                    post_id, today,
                                    item_views, item_likes,
                                    ps.get("commentCount", 0), ps.get("shareCount", 0)
                                ))
                                for tier in (1000, 5000, 10000):
                                    if item_likes >= tier:
                                        c.execute("""
                                            INSERT INTO milestone_events (post_id, tier, crossed_date, views, likes)
                                            VALUES (%s,%s,%s,%s,%s)
                                            ON CONFLICT (post_id, tier) DO NOTHING
                                        """, (post_id, tier, today, item_views, item_likes))
                    conn.commit()
        except Exception as e:
            _log(f"get_account_posts failed for @{username}: {e} — skipping posts")

    _log(f"✓ ingested fan account @{username} — {followers:,} followers")
    return True


def ingest_single_post(db_conn_factory, post_id, username=None):
    """Pull/update one specific TikTok video by its post ID."""
    raw = provider.get_post(post_id)
    post = parse_single_post(raw, fallback_username=username)
    if not post:
        return False

    today = date.today()
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, followers_at_post, thumbnail, shares)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (post_id) DO UPDATE SET
                    views=EXCLUDED.views, likes=EXCLUDED.likes,
                    comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                    thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                    shares=EXCLUDED.shares
            """, (
                post["post_id"], today, post["username"],
                post.get("description", "")[:300],
                post.get("views", 0), post.get("likes", 0),
                post.get("comments", 0), post.get("saves", 0),
                post.get("created_at"), post.get("followers"),
                post.get("thumbnail"), post.get("shares", 0)
            ))
        conn.commit()
    _log(f"✓ ingested post {post_id} by @{post['username']}")
    return True


def ingest_campaign_attached_sound(db_conn_factory, campaign_id, tiktok_sound_id, max_results=30, sound_db_id=None):
    """Legacy path: pull a sound's posts directly into a campaign."""
    all_posts = []
    cursor = 0
    while len(all_posts) < max_results:
        raw = provider.get_sound_posts_page(tiktok_sound_id, cursor=cursor, count=30)
        posts, has_more, next_cursor = parse_posts_from_music_page(raw)
        if not posts:
            break
        all_posts.extend(posts)
        if not has_more:
            break
        cursor = int(next_cursor) if next_cursor is not None else cursor + 30

    today = date.today()
    added = 0
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            for p in all_posts[:max_results]:
                if not p.get("post_id") or not p.get("username"):
                    continue
                c.execute("""
                    INSERT INTO posts (post_id, date, username, description, views, likes, comments, saves, created_at, campaign_id, source, thumbnail, shares, sound_db_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'sound_auto',%s,%s,%s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        views=EXCLUDED.views, likes=EXCLUDED.likes,
                        comments=EXCLUDED.comments, saves=EXCLUDED.saves,
                        campaign_id=COALESCE(posts.campaign_id, EXCLUDED.campaign_id),
                        thumbnail=COALESCE(EXCLUDED.thumbnail, posts.thumbnail),
                        shares=EXCLUDED.shares,
                        sound_db_id=COALESCE(EXCLUDED.sound_db_id, posts.sound_db_id)
                """, (
                    p["post_id"], today, p["username"],
                    p.get("description", "")[:300],
                    p.get("views", 0), p.get("likes", 0),
                    p.get("comments", 0), p.get("saves", 0),
                    p.get("created_at"), campaign_id, p.get("thumbnail"),
                    p.get("shares", 0), sound_db_id
                ))
                added += 1
        conn.commit()
    return added


def ingest_approved_sounds_for_song(db_conn_factory, song_id, max_results=30):
    """Fetch posts/videos for every approved sound belonging to one song, right now.
    Unlike the global monitor cron (which only takes the top 25 stale sounds across
    ALL campaigns), this targets exactly the sounds that matter for a just-added
    song, so a demo doesn't have to wait for the next cron cycle."""
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id as sound_db_id, sound_id as tiktok_sound_id
                FROM sounds
                WHERE song_id = %s AND status = 'approved'
            """, (song_id,))
            sounds = [dict(r) for r in c.fetchall()]

    posts_added = 0
    ingested = 0
    for s in sounds:
        result = ingest_sound(db_conn_factory, song_id, s["sound_db_id"], s["tiktok_sound_id"], max_results=max_results)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1

    _log(f"ingest_approved_sounds_for_song: song {song_id} — {ingested}/{len(sounds)} sounds ingested, {posts_added} posts added")
    return {"sounds_found": len(sounds), "sounds_ingested": ingested, "posts_added": posts_added}




def refresh_approved_sounds_for_song(db_conn_factory, song_id, batch_size=15):
    """Routine refresh — operates EXCLUSIVELY on a song's already-approved
    (canonical) sounds. Updates posts, view/like counts, and creator data.
    Never discovers new candidates, never re-runs qualify. This is the
    ONLY thing a 'Refresh' button should ever do.

    Capped to the `batch_size` stalest approved sounds per call (oldest
    last_ingested_at first) — a song can have many approved sounds, and
    processing all of them synchronously risks the same worker-timeout
    crashes discovery caused. Repeated refresh calls naturally rotate
    through every approved sound over time. ingest_sound's own freshness
    cache means sounds refreshed recently are skipped cheaply (a DB read,
    no network call) rather than needlessly re-fetched.

    COVERAGE ENGINE NOTE: batch_size (a sound COUNT) is no longer a
    sufficient safety bound on its own — since ingest_sound now paginates
    much deeper for Tier A/B sounds (up to ~20 pages vs. 1 before), 15
    Tier A sounds in one batch could take far longer than 15 small ones
    used to. Added an explicit TIME_BUDGET_SECONDS below, same pattern as
    the fingerprint worker, so this stops safely partway through a batch
    rather than risking a timeout — remaining sounds just get picked up
    on the next refresh call, same as they already do today when
    batch_size itself doesn't cover everything.
    """
    with db_conn_factory() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT id, sound_id
                FROM sounds
                WHERE song_id=%s AND status='approved'
                ORDER BY last_ingested_at ASC NULLS FIRST
                LIMIT %s
            """, (song_id, batch_size))
            sounds = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT COUNT(*) as total FROM sounds
                WHERE song_id=%s AND status='approved'
            """, (song_id,))
            total_approved = c.fetchone()["total"]

    posts_added = 0
    ingested = 0
    import time as _time
    start = _time.monotonic()
    TIME_BUDGET_SECONDS = 250  # RAISED 7/20 from 40 — safe now that this only ever runs on the
                                # separate worker service (see HANDOFF), never the main UI. Tier A
                                # sounds now genuinely take much longer to fetch their real 1500-post
                                # target (~100 pages) — 40s was nowhere near enough for even one big
                                # sound. Kept under the 300s gunicorn --timeout with real margin.
    for s in sounds:
        if _time.monotonic() - start > TIME_BUDGET_SECONDS:
            _log(f"refresh_approved_sounds_for_song: time budget reached after {ingested}/{len(sounds)}")
            break
        result = ingest_sound(db_conn_factory, song_id, s["id"], s["sound_id"], max_results=35)
        posts_added += result.get("posts_added", 0)
        if not result.get("error"):
            ingested += 1

    remaining = max(total_approved - len(sounds), 0)

    _log(f"refresh_approved_sounds_for_song: song {song_id} — {ingested}/{len(sounds)} sounds refreshed, "
         f"{posts_added} posts added, {remaining} still stale (of {total_approved} total approved)")

    return {
        "sounds_refreshed": len(sounds),
        "sounds_ingested": ingested,
        "posts_added": posts_added,
        "total_approved_sounds": total_approved,
        "remaining_stale_sounds": remaining,
    }