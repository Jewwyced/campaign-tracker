"""
routes_fan_pages.py — Fan Page tracking, tied to a specific Campaign.

Separate concern from Sounds: a Sound answers "which audio are people
using," a Fan Page answers "how is this specific account doing over
time," regardless of which sound they're using this week. Both feed the
same goal (informing what to commission — a fan edit's style might
justify seeding an influencer, or vice versa) but they're genuinely
different tracking units: one is post-centric, one is account-centric.

Reuses ingest_fan_account() from ingestion/service.py, which already
writes into the real shared `stats` and `posts` tables — not a separate
schema. Fan-account posts land in the same `posts` table sound-driven
posts do, just with sound_db_id left NULL, which is what naturally
distinguishes them.

KNOWN GAP, flagged rather than silently left: ingest_fan_account does
NOT currently write to post_snapshots or milestone_events, so a fan
page's viral post won't show up in the "Crossed a Tier Today" dashboard
digest yet, even after this ships. That's a deliberate, separate next
step — wiring in a large new feature and a cross-cutting digest change
in one unreviewed pass is exactly the kind of thing that's caused
regressions all session.
"""

from flask import Blueprint, jsonify, request, render_template_string
from db import db

fan_pages_bp = Blueprint("fan_pages", __name__)


@fan_pages_bp.route("/api/campaigns/<int:campaign_id>/fan_pages", methods=["GET", "POST"])
def campaign_fan_pages(campaign_id):
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        username = str(data.get("username", "")).strip().lstrip("@").lower()
        if not username:
            return jsonify({"error": "Username required"}), 400

        with db() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO campaign_fan_pages (campaign_id, username)
                    VALUES (%s, %s)
                    ON CONFLICT (campaign_id, username) DO NOTHING
                """, (campaign_id, username))
            conn.commit()

        # Pull initial data right away so it doesn't sit empty until the
        # next manual refresh — same "add it, it just appears" pattern
        # used for songs.
        from ingestion import service as ingestion_service
        ok = ingestion_service.ingest_fan_account(db, username)
        if not ok:
            return jsonify({
                "ok": False,
                "error": f"Added @{username}, but could not fetch their data — double check the username."
            }), 200

        return jsonify({"ok": True, "username": username})

    # GET — list tracked fan pages for this campaign with latest stats
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT username FROM campaign_fan_pages
                WHERE campaign_id = %s
                ORDER BY added_at ASC
            """, (campaign_id,))
            usernames = [r["username"] for r in c.fetchall()]

            fan_pages = []
            for username in usernames:
                c.execute("""
                    SELECT date, followers, likes, videos
                    FROM stats
                    WHERE username = %s
                    ORDER BY date DESC
                    LIMIT 2
                """, (username,))
                stat_rows = [dict(r) for r in c.fetchall()]
                latest = stat_rows[0] if stat_rows else None
                previous = stat_rows[1] if len(stat_rows) > 1 else None
                followers_delta = None
                if latest and previous and latest["followers"] is not None and previous["followers"] is not None:
                    followers_delta = latest["followers"] - previous["followers"]

                c.execute("""
                    SELECT COUNT(*) as post_count,
                           COALESCE(AVG(views), 0) as avg_views,
                           COALESCE(SUM(views), 0) as total_views
                    FROM posts
                    WHERE username = %s AND sound_db_id IS NULL
                """, (username,))
                post_stats = dict(c.fetchone())

                fan_pages.append({
                    "username": username,
                    "followers": latest["followers"] if latest else None,
                    "followers_delta": followers_delta,
                    "total_likes": latest["likes"] if latest else None,
                    "videos": latest["videos"] if latest else None,
                    "post_count": post_stats["post_count"],
                    "avg_views": round(post_stats["avg_views"]) if post_stats["avg_views"] else 0,
                    "total_views": post_stats["total_views"],
                })

    return jsonify(fan_pages)


@fan_pages_bp.route("/api/campaigns/<int:campaign_id>/fan_pages/<username>", methods=["DELETE"])
def remove_fan_page(campaign_id, username):
    """Untrack a fan page for this campaign. Only removes the LINK, not
    the historical stats/posts data — same philosophy as elsewhere in
    this app: untracking isn't erasing history."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                DELETE FROM campaign_fan_pages
                WHERE campaign_id = %s AND username = %s
            """, (campaign_id, username))
        conn.commit()
    return jsonify({"ok": True})


@fan_pages_bp.route("/api/campaigns/<int:campaign_id>/fan_pages/refresh", methods=["POST"])
def refresh_fan_pages(campaign_id):
    """Manual refresh for all fan pages tracked under this campaign.
    Deliberately NOT wired into the automatic hourly cron yet — that's a
    reasonable next step (a tracked fan page is analogous to an approved
    sound: deliberately added by a human, safe to refresh automatically),
    but adding to the cron is a separate, considered decision, not a
    default to reach for while building the base feature."""
    from ingestion import service as ingestion_service

    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT username FROM campaign_fan_pages WHERE campaign_id = %s
            """, (campaign_id,))
            usernames = [r["username"] for r in c.fetchall()]

    refreshed = 0
    failed = []
    for username in usernames:
        ok = ingestion_service.ingest_fan_account(db, username)
        if ok:
            refreshed += 1
        else:
            failed.append(username)

    return jsonify({"ok": True, "refreshed": refreshed, "total": len(usernames), "failed": failed})


@fan_pages_bp.route("/api/campaigns/<int:campaign_id>/fan_pages/<username>/posts")
def fan_page_posts(campaign_id, username):
    """Recent posts for one tracked fan page — sound_db_id IS NULL is
    what distinguishes a fan-account-authored post from a sound-driven
    one in the shared posts table."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT post_id, description, views, likes, comments, saves,
                       created_at, followers_at_post, date
                FROM posts
                WHERE username = %s AND sound_db_id IS NULL
                ORDER BY created_at DESC NULLS LAST
                LIMIT 30
            """, (username,))
            posts = [dict(r) for r in c.fetchall()]
    for p in posts:
        p["date"] = str(p["date"]) if p["date"] else None
    return jsonify(posts)


@fan_pages_bp.route("/campaign/<int:campaign_id>/fanpages")
def fan_pages_page(campaign_id):
    return render_template_string(open("fan_pages.html").read())