"""
routes_dashboard.py — the main landing page and its supporting summary endpoints.

This file should stay thin. All the actual SQL aggregation and AI-summary
logic lives in services/dashboard_service.py and services/ai_service.py —
these routes just call into them and return the result.
"""

import requests
from flask import Blueprint, jsonify, request, render_template_string
from services import dashboard_service

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/api/summary")
def summary():
    return jsonify(dashboard_service.get_campaign_summary())


@dashboard_bp.route("/api/dashboard")
def dashboard_data():
    return jsonify(dashboard_service.get_homepage_dashboard())


@dashboard_bp.route("/api/oembed")
def oembed_proxy():
    video_url = request.args.get("url", "").strip()
    if not video_url:
        return jsonify({"error": "Missing url parameter"}), 400
    try:
        r = requests.get(
            "https://www.tiktok.com/oembed",
            params={"url": video_url},
            timeout=10
        )
        if r.status_code != 200:
            return jsonify({"error": "Could not fetch embed"}), 400
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/")
def dashboard():
    return render_template_string(open("dashboard.html").read())