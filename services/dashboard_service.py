"""
services/dashboard_service.py — owns the SQL and aggregation logic behind
the homepage and the campaign summary.
"""
from db import db
from services.ai_service import generate_daily_campaign_summary


def get_dashboard_stats():
    """Powers /api/dashboard: headline stats using the campaign_songs chain."""
    with db() as conn:
        with conn.cursor() as c:
            # Total views and posts across all campaigns via campaign_songs
            c.execute("""
                SELECT
                    COALESCE(SUM(p.views), 0) as total_views,
                    COUNT(DISTINCT p.post_id) as total_posts,
                    COUNT(DISTINCT p.username) as total_creators
                FROM campaign_songs cs
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
            """)
            totals = dict(c.fetchone())

            # Posts added in last 7 days (by TikTok post date)
            c.execute("""
                SELECT COUNT(DISTINCT p.post_id) as n
                FROM campaign_songs cs
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
                WHERE p.created_at >= extract(epoch from now() - interval '7 days')
            """)
            posts_7d = c.fetchone()["n"] or 0

            # Views this week
            c.execute("""
                SELECT COALESCE(SUM(p.views), 0) as views_7d
                FROM campaign_songs cs
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
                WHERE p.created_at >= extract(epoch from now() - interval '7 days')
            """)
            views_7d = c.fetchone()["views_7d"] or 0

            # Active campaigns
            c.execute("SELECT COUNT(*) as n FROM campaigns WHERE status='In Progress'")
            active_campaigns = c.fetchone()["n"] or 0

            # Top campaign by views
            c.execute("""
                SELECT c.id, c.name, c.artist,
                    COUNT(DISTINCT p.post_id) as post_count,
                    COALESCE(SUM(p.views), 0) as views
                FROM campaigns c
                JOIN campaign_songs cs ON cs.campaign_id = c.id
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
                GROUP BY c.id, c.name, c.artist
                ORDER BY views DESC
                LIMIT 1
            """)
            top_campaign_row = c.fetchone()
            top_campaign = dict(top_campaign_row) if top_campaign_row else None

            # Top creator
            c.execute("""
                SELECT p.username, COUNT(DISTINCT p.post_id) as posts,
                       COALESCE(SUM(p.views), 0) as views
                FROM campaign_songs cs
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
                GROUP BY p.username
                ORDER BY views DESC
                LIMIT 1
            """)
            top_creator_row = c.fetchone()
            top_creator = dict(top_creator_row) if top_creator_row else None

            # Campaign breakdown for AI summary
            c.execute("""
                SELECT c.name, c.artist,
                    COALESCE(SUM(p.views), 0) as views,
                    COUNT(DISTINCT p.post_id) as post_count
                FROM campaigns c
                JOIN campaign_songs cs ON cs.campaign_id = c.id
                JOIN sounds snd ON snd.song_id = cs.song_id
                JOIN posts p ON p.sound_db_id = snd.id
                GROUP BY c.id, c.name, c.artist
                ORDER BY views DESC
            """)
            campaign_breakdown = [dict(r) for r in c.fetchall()]

    # AI summary
    ai_stats = {
        "total_views": totals["total_views"],
        "new_posts_24h": posts_7d,
        "campaigns": [
            {"name": c["name"], "artist": c["artist"],
             "views": c["views"], "post_count": c["post_count"]}
            for c in campaign_breakdown
        ],
        "top_post": None,
    }
    ai_summary = generate_daily_campaign_summary(ai_stats)

    return {
        "total_views": totals["total_views"],
        "total_posts": totals["total_posts"],
        "total_creators": totals["total_creators"],
        "views_7d": views_7d,
        "posts_7d": posts_7d,
        "active_campaigns": active_campaigns,
        "top_campaign": top_campaign,
        "top_creator": top_creator,
        "campaign_breakdown": campaign_breakdown,
        "ai_summary": ai_summary,
    }


def get_campaign_summary():
    """Legacy function — redirects to get_dashboard_stats for backward compat."""
    return get_dashboard_stats()


def get_homepage_dashboard():
    """Alias for get_dashboard_stats — called by routes_dashboard.py."""
    return get_dashboard_stats()