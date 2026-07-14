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
    active campaign AND every tracked fan page.

    1. milestone_crossings — REAL, one-time crossing events from the
       milestone_events table, covering BOTH sound-driven posts (via
       campaign_songs) AND fan-page posts (via the `artists` table —
       see ingest_fan_account in ingestion/service.py, which now writes
       the same milestone_events rows fan pages were previously missing
       entirely from this digest).
    2. todays_activity — everything else posted/updated in the last 24h,
       from either source, ranked by performance.
    3. weekly_trend — posts-per-day and views-per-day for the last 7
       days, combined across both sources.

    Each row includes a `source_type` ('sound' or 'fan_page') so the UI
    can label them differently — a fan-page post has no song/sound to
    attribute, so `song_name`/`sound_title` are NULL for those rows.
    """
    with db() as conn:
        with conn.cursor() as c:
            # ── Milestone crossings — sound-driven UNION fan-page ──────
            # DISTINCT ON (post_id) + ORDER BY tier DESC keeps only the
            # HIGHEST tier per post — a post new to us but already very
            # popular could otherwise qualify for 1K, 5K, AND 10K at
            # once, showing as three redundant cards for one post.
            c.execute("""
                SELECT * FROM (
                    (
                        SELECT DISTINCT ON (me.post_id)
                            me.post_id, me.tier, me.views, me.likes, me.crossed_date,
                            p.username, p.thumbnail,
                            snd.title as sound_title, sg.name as song_name, sg.id as song_id,
                            'sound' as source_type
                        FROM milestone_events me
                        JOIN posts p ON p.post_id = me.post_id
                        JOIN sounds snd ON snd.id = p.sound_db_id
                        JOIN songs sg ON sg.id = snd.song_id
                        JOIN campaign_songs cs ON cs.song_id = sg.id
                        JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                        WHERE me.crossed_date = CURRENT_DATE
                        ORDER BY me.post_id, me.tier DESC
                    )

                    UNION ALL

                    (
                        SELECT DISTINCT ON (me.post_id)
                            me.post_id, me.tier, me.views, me.likes, me.crossed_date,
                            p.username, p.thumbnail,
                            NULL as sound_title, NULL as song_name, NULL as song_id,
                            'fan_page' as source_type
                        FROM milestone_events me
                        JOIN posts p ON p.post_id = me.post_id
                        JOIN artists a ON a.username = p.username
                        WHERE me.crossed_date = CURRENT_DATE
                        AND p.sound_db_id IS NULL
                        ORDER BY me.post_id, me.tier DESC
                    )
                ) sub
                ORDER BY tier DESC, likes DESC
                LIMIT 20
            """)
            milestone_rows = [dict(r) for r in c.fetchall()]
            milestone_crossings = [
                {**r, "tier_crossed": r["tier"]} for r in milestone_rows
            ]

            milestone_post_ids = [m["post_id"] for m in milestone_crossings]
            exclude_ids = milestone_post_ids if milestone_post_ids else ['__none__']

            # ── Today's activity — everything else, both sources ──────
            c.execute("""
                SELECT * FROM (
                    SELECT
                        p.post_id, p.username, p.thumbnail, p.views, p.likes, p.created_at,
                        snd.title as sound_title, sg.name as song_name, sg.id as song_id,
                        'sound' as source_type
                    FROM posts p
                    JOIN sounds snd ON snd.id = p.sound_db_id
                    JOIN songs sg ON sg.id = snd.song_id
                    JOIN campaign_songs cs ON cs.song_id = sg.id
                    JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                    WHERE p.created_at >= extract(epoch from now() - interval '24 hours')
                    AND p.post_id != ALL(%s)

                    UNION ALL

                    SELECT
                        p.post_id, p.username, p.thumbnail, p.views, p.likes, p.created_at,
                        NULL as sound_title, NULL as song_name, NULL as song_id,
                        'fan_page' as source_type
                    FROM posts p
                    JOIN artists a ON a.username = p.username
                    WHERE p.sound_db_id IS NULL
                    AND p.created_at >= extract(epoch from now() - interval '24 hours')
                    AND p.post_id != ALL(%s)
                ) sub
                ORDER BY views DESC NULLS LAST
                LIMIT 15
            """, (exclude_ids, exclude_ids))
            todays_activity = [dict(r) for r in c.fetchall()]

            # ── Weekly trend — posts/views per day, both sources ───────
            c.execute("""
                SELECT date, SUM(posts) as posts, SUM(views) as views
                FROM (
                    SELECT ps.date, COUNT(DISTINCT ps.post_id) as posts, COALESCE(SUM(ps.views), 0) as views
                    FROM post_snapshots ps
                    JOIN posts p ON p.post_id = ps.post_id
                    JOIN sounds snd ON snd.id = p.sound_db_id
                    JOIN campaign_songs cs ON cs.song_id = snd.song_id
                    JOIN campaigns camp ON camp.id = cs.campaign_id AND camp.status = 'In Progress'
                    WHERE ps.date >= CURRENT_DATE - 6
                    GROUP BY ps.date

                    UNION ALL

                    SELECT ps.date, COUNT(DISTINCT ps.post_id) as posts, COALESCE(SUM(ps.views), 0) as views
                    FROM post_snapshots ps
                    JOIN posts p ON p.post_id = ps.post_id
                    JOIN artists a ON a.username = p.username
                    WHERE p.sound_db_id IS NULL
                    AND ps.date >= CURRENT_DATE - 6
                    GROUP BY ps.date
                ) combined
                GROUP BY date
                ORDER BY date ASC
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