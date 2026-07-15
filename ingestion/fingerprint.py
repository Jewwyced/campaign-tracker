"""
ingestion/fingerprint.py — Layer 2/3 boundary: audio identity verification.

Wraps ACRCloud's identify API. Given a sound's play_url (already surfaced
by parsers.parse_sound_info), fetches the audio and asks ACRCloud what it
actually is — a second, independent signal from title/author text matching.

This module NEVER raises — every function returns a result dict with a
'status' field, even on total failure. A slow or down ACRCloud must never
break the qualify pipeline; worst case, a candidate is just marked
'error' and falls through to normal human review, exactly as if this
module didn't exist.

Credentials read from environment variables (same pattern as
TIKLIVEAPI_KEY elsewhere in this codebase):
    ACR_HOST, ACR_ACCESS_KEY, ACR_ACCESS_SECRET

CONFIDENCE_THRESHOLD is the bar above which a result is treated as
authoritative rather than inconclusive. Set conservatively (90) based on
real testing: every clean true/false-positive match we tested scored
exactly 100, while a genuine slowed/tempo-shifted edit of a correct song
scored only 49 (effectively noise, not "recognized but uncertain") — so
there's a wide, safe gap between "real match" and "not confidently
recognized" at this threshold. Anything below it should be treated as
inconclusive, not as evidence of a mismatch.
"""

import base64
import hashlib
import hmac
import os
import time

import requests

ACR_HOST = os.environ.get("ACR_HOST", "")
ACR_ACCESS_KEY = os.environ.get("ACR_ACCESS_KEY", "")
ACR_ACCESS_SECRET = os.environ.get("ACR_ACCESS_SECRET", "")

CONFIDENCE_THRESHOLD = 90

# Statuses stored in sounds.fingerprint_status:
#   'unchecked'   — never attempted (the column default)
#   'matched'     — confident (>= threshold) match against the song this
#                   sound is being evaluated for
#   'mismatched'  — confident (>= threshold) match, but against a
#                   DIFFERENT known song than expected
#   'inconclusive'— either no result, or below the confidence threshold
#                   (this is the expected outcome for slowed/sped-up
#                   edits — see module docstring)
#   'error'       — the audio fetch or ACRCloud call itself failed
#                   (network issue, missing credentials, no play_url,
#                   etc.) — distinct from 'inconclusive' so these can be
#                   retried later without re-counting as a real result


def _sign_request(timestamp):
    string_to_sign = "\n".join([
        "POST", "/v1/identify", ACR_ACCESS_KEY, "audio", "1", str(timestamp)
    ])
    return base64.b64encode(
        hmac.new(ACR_ACCESS_SECRET.encode("ascii"), string_to_sign.encode("ascii"),
                  digestmod=hashlib.sha1).digest()
    ).decode("ascii")


def _identify_audio_bytes(audio_bytes):
    """Send raw audio bytes to ACRCloud's identify endpoint. Returns the
    raw parsed JSON response, or None on any failure."""
    if not (ACR_HOST and ACR_ACCESS_KEY and ACR_ACCESS_SECRET):
        return None
    try:
        timestamp = time.time()
        signature = _sign_request(timestamp)
        files = {"sample": ("sample.mp3", audio_bytes, "audio/mpeg")}
        data = {
            "access_key": ACR_ACCESS_KEY,
            "sample_bytes": len(audio_bytes),
            "timestamp": str(timestamp),
            "signature": signature,
            "data_type": "audio",
            "signature_version": "1",
        }
        resp = requests.post(f"https://{ACR_HOST}/v1/identify", files=files, data=data, timeout=20)
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _fetch_audio_bytes(play_url):
    """Download a sound's audio from its play_url. Returns bytes, or None
    on any failure (network error, non-200, suspiciously small response)."""
    if not play_url:
        return None
    try:
        resp = requests.get(play_url, timeout=20)
        if resp.status_code != 200:
            return None
        if len(resp.content) < 5000:
            return None
        return resp.content
    except Exception:
        return None


def _names_roughly_match(a, b):
    """Loose, case/whitespace-insensitive comparison — used only to decide
    whether ACRCloud's answer agrees with what we expected, not as a
    matching algorithm in its own right."""
    if not a or not b:
        return False
    norm = lambda s: "".join(ch.lower() for ch in s if ch.isalnum())
    a, b = norm(a), norm(b)
    return a == b or a in b or b in a


def fingerprint_sound(play_url, expected_song_name, expected_song_artist):
    """Given a sound's play_url and the song it's being evaluated against,
    return a dict describing what ACRCloud actually thinks this audio is.

    Always returns a dict with at least a 'status' key. Never raises.
    """
    audio_bytes = _fetch_audio_bytes(play_url)
    if audio_bytes is None:
        return {"status": "error", "reason": "could not fetch audio"}

    result = _identify_audio_bytes(audio_bytes)
    if result is None:
        return {"status": "error", "reason": "ACRCloud call failed"}

    status = result.get("status", {})
    if status.get("code") != 0:
        return {"status": "inconclusive", "reason": status.get("msg", "no result")}

    musics = result.get("metadata", {}).get("music", [])
    if not musics:
        return {"status": "inconclusive", "reason": "empty result"}

    top = musics[0]
    title = top.get("title", "")
    artists = ", ".join(a.get("name", "") for a in top.get("artists", []))
    acrid = top.get("acrid", "")
    score = top.get("score", 0)

    base = {
        "recording_id": acrid,
        "title": title,
        "artist": artists,
        "confidence": score,
    }

    if score < CONFIDENCE_THRESHOLD:
        return {**base, "status": "inconclusive", "reason": f"below confidence threshold ({score})"}

    title_matches = _names_roughly_match(title, expected_song_name)
    artist_matches = _names_roughly_match(artists, expected_song_artist) if expected_song_artist else True

    if title_matches and artist_matches:
        return {**base, "status": "matched"}
    else:
        return {**base, "status": "mismatched"}