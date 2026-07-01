"""
services/dashboard_service.py — owns the SQL and aggregation logic behind
the homepage and the campaign summary. Routes should just call these
functions and return the result; they should not contain raw SQL themselves.
"""

from db import db
from services.ai_service import generate_daily_campaign_summary


def get_campaign_summary():
    """Powers /api/summary: headline campaign stats plus an optional AI-written summary."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT COALESCE(SUM(p.views), 0) as total_views, COUNT(*) as total_posts
                FROM posts p
                WHERE p.campaign_id IS NOT NULL
            """)
            totals = dict(c.fetchone())

            c.execute("""
                SELECT COUNT(*) as new_posts
                FROM campaign_links
                WHERE added_at >= NOW() - INTERVAL '24 hours'
            """)
            new_posts = dict(c.fetchone())["new_posts"]

            c.execute("""
                SELECT c.id, c.name, c.artist,
                       COALESCE(SUM(p.views), 0) as views,
                       COUNT(p.post_id) as post_count
                FROM campaigns c
                LEFT JOIN posts p ON p.campaign_id = c.id
                GROUP BY c.id, c.name, c.artist
                ORDER BY views DESC
            """)
            campaign_breakdown = [dict(r) for r in c.fetchall()]

            c.execute("""
                SELECT p.username, p.description, p.views, c.name as campaign_name
                FROM posts p
                JOIN campaigns c ON p.campaign_id = c.id
                ORDER BY p.views DESC
                LIMIT 1
            """)
            top_row = c.fetchone()
            top_post = dict(top_row) if top_row else None

    stats = {
        "total_views": totals["total_views"],
        "total_posts": totals["total_posts"],
        "new_posts_24h": new_posts,
        "campaigns": campaign_breakdown,
        "top_post": top_post,
    }

    stats["ai_summary"] = generate_daily_campaign_summary(stats)
    return stats


def _get_top_artist_by_follower_growth(c):
    """Finds the roster artist with the largest combined 24h follower delta."""
    c.execute("SELECT id, name FROM roster_artists")
    artists = [dict(r) for r in c.fetchall()]
    top_artist = None
    best_delta = None
    for a in artists:
        c.execute("SELECT username FROM roster_accounts WHERE artist_id=%s", (a["id"],))
        usernames = [r["username"] for r in c.fetchall()]
        delta_sum = 0
        for u in usernames:
            c.execute("""
                SELECT followers FROM roster_stats WHERE username=%s
                ORDER BY date DESC LIMIT 2
            """, (u,))
            rows = [dict(r) for r in c.fetchall()]
            if len(rows) == 2:
                delta_sum += (rows[0]["followers"] or 0) - (rows[1]["followers"] or 0)
        if best_delta is None or delta_sum > best_delta:
            best_delta = delta_sum
            top_artist = {"id": a["id"], "name": a["name"], "followers_delta": delta_sum}
    return top_artist


def _get_standout_creators(c, limit=3):
    """Top creators by total views across all campaign posts, each with their best post."""
    c.execute("""
        SELECT username,
               SUM(views) as total_views,
               SUM(likes) as total_likes,
               COUNT(*) as video_count
        FROM posts
        WHERE campaign_id IS NOT NULL
        GROUP BY username
        ORDER BY total_views DESC
        LIMIT %s
    """, (limit,))
    standout_creators = [dict(r) for r in c.fetchall()]

    for creator in standout_creators:
        c.execute("""
            SELECT post_id, username, description, views
            FROM posts
            WHERE username=%s AND campaign_id IS NOT NULL
            ORDER BY views DESC LIMIT 1
        """, (creator["username"],))
        top_post_row = c.fetchone()
        creator["top_post"] = dict(top_post_row) if top_post_row else None

    return standout_creators


def get_homepage_dashboard():
    """Powers /api/dashboard: headline stats across Campaigns and Artists for the homepage."""
    with db() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) as n FROM campaigns WHERE status='In Progress'")
            active_campaigns = c.fetchone()["n"]

            c.execute("SELECT COUNT(*) as n FROM roster_artists")
            artist_count = c.fetchone()["n"]

            c.execute("""
                SELECT COUNT(*) as n FROM campaign_links
                WHERE added_at >= NOW() - INTERVAL '7 days'
            """)
            posts_added_7d = c.fetchone()["n"]

            c.execute("""
                SELECT COALESCE(SUM(views), 0) as total
                FROM posts WHERE campaign_id IS NOT NULL
            """)
            total_views = c.fetchone()["total"]

            c.execute("""
                SELECT c.id, c.name, COALESCE(SUM(p.views), 0) as views
                FROM campaigns c
                LEFT JOIN posts p ON p.campaign_id = c.id
                GROUP BY c.id, c.name
                ORDER BY views DESC LIMIT 1
            """)
            top_campaign_row = c.fetchone()
            top_campaign = dict(top_campaign_row) if top_campaign_row else None

            top_artist = _get_top_artist_by_follower_growth(c)
            standout_creators = _get_standout_creators(c, limit=3)

            c.execute("""
                SELECT date, COALESCE(SUM(views), 0) as views
                FROM posts
                WHERE campaign_id IS NOT NULL AND date >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY date ORDER BY date ASC
            """)
            trend = [{"date": str(r["date"]), "views": r["views"]} for r in c.fetchall()]

    return {
        "active_campaigns": active_campaigns,
        "artist_count": artist_count,
        "posts_added_7d": posts_added_7d,
        "total_views": total_views,
        "top_campaign": top_campaign,
        "top_artist": top_artist,
        "standout_creators": standout_creators,
        "trend": trend,
        # Mock data — no real payment tracking built yet
        "spend": {
            "this_week": 1840,
            "creators_paid": 6,
            "top_creator": {"username": "onlyftc", "amount": 400, "videos": 4},
            "is_mock": True,
        },
    }