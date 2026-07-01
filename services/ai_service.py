"""
services/ai_service.py — owns all calls to Anthropic's API for generated text.

Currently used for the optional daily campaign summary on the dashboard.
Kept separate from dashboard_service.py because "ask Claude to write something"
is a distinct concern from "compute these numbers from Neon" — if a second
feature ever wants AI-generated text (e.g. a song-level insight), it belongs
here too, not duplicated into another service file.
"""

import os
import requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


def generate_daily_campaign_summary(stats):
    """Given the dashboard summary stats dict, ask Claude for a short,
    plain-English daily summary. Returns None if no key is set or the call fails —
    callers should treat this as a nice-to-have, never block on it."""
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
                "model": "claude-sonnet-4-6",
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