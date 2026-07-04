"""
services/ai_service.py — owns all calls to Anthropic's API for generated text.
Currently used for the optional daily campaign summary on the dashboard
and per-song TikTok performance insights.
"""
import os
import requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def generate_daily_campaign_summary(stats):
    """Given the dashboard summary stats dict, ask Claude for a short,
    plain-English daily summary. Returns None if no key is set or the call fails."""
    if not ANTHROPIC_API_KEY:
        return None
    if not stats["campaigns"]:
        return "No campaigns yet. Create one to start tracking."

    campaign_lines = "\n".join(
        f"- {c['name']} ({c['artist'] or 'unknown artist'}): {c['views']:,} views across {c['post_count']} posts"
        for c in stats["campaigns"]
    )
    top_post_line = ""
    if stats["top_post"]:
        tp = stats["top_post"]
        top_post_line = f"\nTop performing post: @{tp['username']} in '{tp['campaign_name']}' with {tp['views']:,} views — caption: \"{(tp['description'] or '')[:120]}\""

    prompt = f"""You are writing a short daily summary for a music marketing team's campaign dashboard.
Total views across all campaigns: {stats['total_views']:,}
New posts added in the last 24 hours: {stats['new_posts_24h']}
Per-campaign breakdown:
{campaign_lines}
{top_post_line}
Write a 2-3 sentence summary a marketing team would want to read first thing in the morning. Be specific and use real numbers. Plain, confident tone — no fluff, no exclamation points, no generic phrases like "great progress.\""""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  AI summary error: {resp.status_code} {resp.text[:200]}")
            return None
        data = resp.json()
        return data["content"][0]["text"].strip()
    except Exception as e:
        print(f"  AI summary exception: {e}")
        return None


def generate_song_insight(song_name, artist, stats, top_creators, top_descriptions):
    """Generate a 2-3 sentence insight about a song's TikTok performance.
    Returns None if no key is set or the call fails — treat as nice-to-have."""
    if not ANTHROPIC_API_KEY:
        return None

    creator_lines = "\n".join(
        f"- @{c['username']}: {c['total_views']:,} views across {c['post_count']} posts"
        for c in top_creators[:5]
    ) if top_creators else "No creator data yet."

    hashtag_sample = " ".join(top_descriptions[:8]) if top_descriptions else ""
    engagement_rate = round(stats['likes'] / stats['views'] * 100, 1) if stats.get('views', 0) > 0 else 0

    prompt = f"""You are writing a short insight for a music marketing team tracking TikTok performance.

Song: {song_name} by {artist}
Total views: {stats.get('views', 0):,}
Total posts: {stats.get('post_count', 0)}
Unique creators: {stats.get('creator_count', 0)}
Sounds tracked: {stats.get('sound_count', 0)}
Engagement rate: {engagement_rate}%

Top creators:
{creator_lines}

Sample hashtags/captions from posts:
{hashtag_sample[:300]}

Write 2-3 sentences a music marketing exec would want to read. Be specific with numbers. Identify what type of creators are using the sound based on the hashtags. Plain confident tone — no fluff, no exclamation points."""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5",
                "max_tokens": 150,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  AI insight error: {resp.status_code} {resp.text[:200]}")
            return None
        data = resp.json()
        return data["content"][0]["text"].strip()
    except Exception as e:
        print(f"  AI insight exception: {e}")
        return None


def generate_campaign_insight(campaign_name, artist, songs):
    """Generate a 2-3 sentence strategic recommendation for a campaign."""
    if not ANTHROPIC_API_KEY or not songs:
        return None

    song_lines = "\n".join(
        f"- {s.get('name','')}: {s.get('total_views',0):,} total views, {s.get('post_count',0)} posts, "
        f"+{s.get('posts_7d',0)} posts this week, {s.get('views_7d',0):,} views this week"
        for s in songs
    )

    prompt = f"""You are a music marketing analyst advising a record label.

Campaign: {campaign_name} by {artist}
Songs performance:
{song_lines}

Write 2-3 sentences with a specific strategic recommendation. Which song has the most momentum? What type of creator activity is driving results? What should the team do next? Be direct and specific — use real numbers. No fluff, no exclamation points."""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5",
                "max_tokens": 180,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data["content"][0]["text"].strip()
    except Exception as e:
        print(f"  AI campaign insight exception: {e}")
        return None