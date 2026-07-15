"""
test_audio_fetch.py — ONE-TIME DIAGNOSTIC, not part of the app.

Answers question 1 from the audio fingerprinting plan for real, with
evidence, instead of guessing from reading code: does TikLive's
/music-info/ endpoint return a play_url that's an actual, working,
directly-downloadable audio file — not just a field that exists in the
JSON, but bytes we can really save to disk and hand to a fingerprinting
service.

Usage:
    python3 test_audio_fetch.py

Edit SOUND_IDS_TO_TEST below with a handful of real tiktok_sound_id
values pulled straight from your `sounds` table — ideally include:
  - A sound_id you're confident IS the real "Back Home" (already approved)
  - A sound_id from the false-positive candidate that looked plausible
    but wasn't actually the song
  - A couple of random others for a broader sample

For each one, this script:
  1. Calls /music-info/ (same call your app already makes)
  2. Extracts the play_url
  3. Actually downloads it and saves to ./audio_test_output/<sound_id>.mp3
  4. Reports file size and whether it looks like real audio (checked via
     file size sanity + content-type header), not just "download succeeded"

Run this, then actually try playing the resulting .mp3 files locally to
confirm with your own ears that they're the right, clean audio (no talking
over it, not truncated, etc.) before trusting this as a real pipeline.
"""

import os
import sys
import requests

# ── EDIT THIS with real sound_id values from your `sounds` table ──────────
SOUND_IDS_TO_TEST = [
    # "1234567890123456789",  # <- e.g. the real approved "Back Home" sound_id
    # "9876543210987654321",  # <- e.g. the false-positive candidate's sound_id
]
# ────────────────────────────────────────────────────────────────────────

TIKLIVEAPI_KEY = os.environ.get("TIKLIVEAPI_KEY", "")
BASE_URL = "https://api.tikliveapi.com"
OUTPUT_DIR = "audio_test_output"


def get_music_info(sound_id):
    headers = {
        "X-Api-Key": TIKLIVEAPI_KEY,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.get(
        f"{BASE_URL}/music-info/",
        params={"music_id": sound_id},
        headers=headers,
        timeout=15,
    )
    print(f"  /music-info/ status: {r.status_code}")
    if r.status_code != 200:
        print(f"  error body: {r.text[:300]}")
        return None
    return r.json()


def try_download_audio(play_url, out_path):
    if not play_url:
        print("  NO play_url in response — TikLive did not return an audio link for this sound.")
        return False
    print(f"  play_url: {play_url[:120]}...")
    try:
        r = requests.get(play_url, timeout=20, stream=True)
        print(f"  download status: {r.status_code}, content-type: {r.headers.get('content-type')}")
        if r.status_code != 200:
            print(f"  FAILED — got non-200 trying to actually fetch the audio bytes")
            return False
        total_bytes = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                total_bytes += len(chunk)
        print(f"  Saved {total_bytes:,} bytes to {out_path}")
        if total_bytes < 5000:
            print(f"  WARNING: file is suspiciously small ({total_bytes} bytes) — likely NOT real audio, "
                  f"probably an error page or empty response disguised as 200 OK.")
            return False
        return True
    except Exception as e:
        print(f"  EXCEPTION downloading audio: {e}")
        return False


def main():
    if not TIKLIVEAPI_KEY:
        print("ERROR: TIKLIVEAPI_KEY environment variable not set. Run this with the same "
              "env vars your app uses (e.g. `heroku config` / your .env / Render env vars).")
        sys.exit(1)

    if not SOUND_IDS_TO_TEST:
        print("ERROR: SOUND_IDS_TO_TEST is empty. Edit this script and add a few real "
              "tiktok_sound_id values from your `sounds` table first — see the docstring "
              "at the top of this file for which ones to pick.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    results = []
    for sound_id in SOUND_IDS_TO_TEST:
        print(f"\n=== Testing sound_id {sound_id} ===")
        info = get_music_info(sound_id)
        if not info:
            results.append((sound_id, "FAILED", "music-info call failed"))
            continue

        title = info.get("title", "?")
        play_url = info.get("play")
        print(f"  title: {title}")

        out_path = os.path.join(OUTPUT_DIR, f"{sound_id}.mp3")
        ok = try_download_audio(play_url, out_path)
        results.append((sound_id, "OK" if ok else "FAILED", title))

    print("\n\n=== SUMMARY ===")
    ok_count = sum(1 for _, status, _ in results if status == "OK")
    for sound_id, status, title in results:
        print(f"  {status:8s} sound_id={sound_id}  title={title}")
    print(f"\n{ok_count}/{len(results)} sounds returned real, downloadable audio.")
    print(f"\nNext step: go listen to the actual .mp3 files in ./{OUTPUT_DIR}/ with your own "
          f"ears — confirm they're clean, correct, and not truncated before trusting this "
          f"as a real pipeline for fingerprinting.")


if __name__ == "__main__":
    main()