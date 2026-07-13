"""
services/dashboard_service.py — owns the SQL and aggregation logic behind
the homepage and the campaign summary.
"""
from db import db
from services.ai_service import generate_daily_campaign_summary

# Engagement tiers for milestone detection, checked highest-first so a
# post that jumped straight from 200 to 15,000 likes overnight gets
# credited with the 10K crossing, not double-counted across all three.
MILESTONE_TIERS = [10000, 5000, 1000]


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

            # Total approved sounds
            c.execute("""
                SELECT COUNT(DISTINCT snd.id) as n
                FROM sounds snd
                JOIN campaign_songs cs ON cs.song_id = snd.song_id
                JOIN campaigns c ON c.id = cs.campaign_id
                WHERE snd.status = 'approved' AND c.status = 'In Progress'
            """)
            total_sounds = c.fetchone()["n"] or 0

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

    digest = get_daily_digest()

    return {
        "total_views": totals["total_views"],
        "total_posts": totals["total_posts"],
        "total_creators": totals["total_creators"],
        "views_7d": views_7d,
        "posts_7d": posts_7d,
        "active_campaigns": active_campaigns,
        "top_campaign": top_campaign,
        "top_creator": top_creator,
        "total_sounds": total_sounds,
        "campaign_breakdown": campaign_breakdown,
        "ai_summary": ai_summary,
        "milestone_crossings": digest["milestone_crossings"],
        "todays_activity": digest["todays_activity"],
        "weekly_trend": digest["weekly_trend"],
    }


def get_daily_digest():
    """The three sections of the daily digest, global across every
    active campaign (not scoped to whichever campaign happens to be
    open) — this is the 'open the app every morning' page, answering
    'what happened across our entire roster since yesterday.'

    1. milestone_crossings — posts that crossed a real engagement tier
       (1K/5K/10K likes) since yesterday, using post_snapshots for a true
       day-over-day comparison. A post with no snapshot from yesterday
       (brand new, or from before post_snapshots existed) is treated as
       having crossed today if it's already at/above the tier — this is
       the deliberate first-day fallback discussed when post_snapshots
       was built: history only starts accumulating once the cron
       actually runs, so there's no "yesterday" to compare against yet
       for anything that predates it.
    2. todays_activity — everything else posted/updated in the last 24h,
       ranked by performance, so posts gaining real momentum surface even
       before they cross a milestone.
    3. weekly_trend — simple posts-per-day and views-per-day for the last
       7 days, to answer "was this week bigger or smaller than last."
    """
    with db() as conn:
        with conn.cursor() as c:
            # ── Milestone crossings ──────────────────────────────────
            # CASE picks the HIGHEST tier crossed so a post that jumped
            # from 200 to 15,000 likes overnight is credited once, with
            # the 10K badge, not double-counted across all three tiers.
            c.execute("""
                WITH today_snap AS (
                    SELECT * FROM post_snapshots WHERE date = CURRENT_DATE
                ),
                yesterday_snap AS (
                    SELECT * FROM post_snapshots WHERE date = CURRENT_DATE - 1
                )
                SELECT
                    p.post_id, p.username, p.thumbnail,
                    t.views, t.likes,
                    snd.title as sound_title, sg.name as song_name, sg.id as song_id,
                    CASE
                        WHEN t.likes >= 10000 AND (y.likes IS NULL OR y.likes < 10000) THEN 10000
                        WHEN t.likes >= 5000 AND (y.likes IS NULL OR y.likes < 5000) THEN 5000
                        WHEN t.likes >= 1000 AND (y.likes IS NULL OR y.likes < 1000) THEN 1000
                    END as tier_crossed
                FROM today_snap t
                JOIN posts p ON p.post_id = t.post_id
                JOIN sounds snd ON snd.id = p.sound_db_id
                JOIN songs sg ON sg.id = snd.song_id
                JOIN campaign_songs cs ON cs.song_id = sg.id
                JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                LEFT JOIN yesterday_snap y ON y.post_id = t.post_id
                WHERE (
                    (t.likes >= 10000 AND (y.likes IS NULL OR y.likes < 10000)) OR
                    (t.likes >= 5000 AND (y.likes IS NULL OR y.likes < 5000)) OR
                    (t.likes >= 1000 AND (y.likes IS NULL OR y.likes < 1000))
                )
                ORDER BY tier_crossed DESC, t.likes DESC
                LIMIT 20
            """)
            milestone_crossings = [dict(r) for r in c.fetchall()]

            milestone_post_ids = [m["post_id"] for m in milestone_crossings]

            # ── Today's activity — everything else from the last 24h ──
            if milestone_post_ids:
                c.execute("""
                    SELECT
                        p.post_id, p.username, p.thumbnail, p.views, p.likes, p.created_at,
                        snd.title as sound_title, sg.name as song_name, sg.id as song_id
                    FROM posts p
                    JOIN sounds snd ON snd.id = p.sound_db_id
                    JOIN songs sg ON sg.id = snd.song_id
                    JOIN campaign_songs cs ON cs.song_id = sg.id
                    JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                    WHERE p.created_at >= extract(epoch from now() - interval '24 hours')
                    AND p.post_id != ALL(%s)
                    ORDER BY p.views DESC NULLS LAST
                    LIMIT 15
                """, (milestone_post_ids,))
            else:
                c.execute("""
                    SELECT
                        p.post_id, p.username, p.thumbnail, p.views, p.likes, p.created_at,
                        snd.title as sound_title, sg.name as song_name, sg.id as song_id
                    FROM posts p
                    JOIN sounds snd ON snd.id = p.sound_db_id
                    JOIN songs sg ON sg.id = snd.song_id
                    JOIN campaign_songs cs ON cs.song_id = sg.id
                    JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                    WHERE p.created_at >= extract(epoch from now() - interval '24 hours')
                    ORDER BY p.views DESC NULLS LAST
                    LIMIT 15
                """)
            todays_activity = [dict(r) for r in c.fetchall()]

            # ── Weekly trend — posts/views per day, last 7 days ────────
            c.execute("""
                SELECT
                    ps.date,
                    COUNT(DISTINCT ps.post_id) as posts,
                    COALESCE(SUM(ps.views), 0) as views
                FROM post_snapshots ps
                JOIN posts p ON p.post_id = ps.post_id
                JOIN sounds snd ON snd.id = p.sound_db_id
                JOIN campaign_songs cs ON cs.song_id = snd.song_id
                JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                WHERE ps.date >= CURRENT_DATE - 6
                GROUP BY ps.date
                ORDER BY ps.date ASC
            """)
            weekly_trend = [
                {"date": str(r["date"]), "posts": r["posts"], "views": r["views"]}
                for r in c.fetchall()
            ]

    return {
        "milestone_crossings": milestone_crossings,
        "todays_activity": todays_activity,
        "weekly_trend": weekly_trend,
    }


def get_campaign_summary():
    """Legacy function — redirects to get_dashboard_stats for backward compat."""
    return get_dashboard_stats()


def get_homepage_dashboard():
    """Alias for get_dashboard_stats — called by routes_dashboard.py."""
    return get_dashboard_stats()