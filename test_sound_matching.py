"""
test_sound_matching.py — regression tests for _classify_sound_match().

WHY THIS FILE EXISTS:
This classifier has been fixed forward three times in one session — each
fix correctly handled the case that had just broken, but silently
reintroduced or left unguarded a DIFFERENT case that had already been
fixed before:

  1. Loose substring/"original sound" auto-pass -> approved huge amounts
     of unrelated junk.
  2. Summed score threshold -> fixed (1), but let a single strong signal
     (exact title match) approve regardless of artist. Broke on common
     titles: "Thong Song" approved Sisqo's original under a PlaqueBoyMax
     search.
  3. Mandatory artist match whenever an artist is on file -> fixed (2),
     but over-corrected: TikTok author metadata is often just an
     uploader's handle ("original sound - maxfanpage"), not a credited
     artist, so a real match got rejected for having no textual artist
     signal at all.
  4. Tiered check, artist match ALONE sufficient for Tier 1 -> fixed (3),
     but broke again: an artist can have multiple songs. A search for
     "Thong Song" by PlaqueBoyMax approved "Pink Dreads" by the same
     artist, because artist match alone doesn't say WHICH song this is.
  5. Current: Tier 1 requires artist match AND a real title relation.

Run this file (`python test_sound_matching.py`) before shipping ANY future
change to _classify_sound_match, _artist_signal, or _score_sound. It
checks every real case above simultaneously, so a fix for a new problem
can't silently re-break an old one without you finding out immediately.

Usage:
    python test_sound_matching.py

Exits 0 if all cases pass, 1 if any fail (with a printed diff of which
cases broke and why) — safe to wire into a pre-deploy check.
"""

import sys
import os

# Adjust this import path to match your actual project layout.
# Assumes this file sits at the project root, next to the `ingestion/` package.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.service import _classify_sound_match, TIER2_VIDEO_COUNT_THRESHOLD


# Each case: (sound_title, sound_author, song_title, song_artist, video_count,
#             expected, description)
CASES = [
    # ── Real, exact matches — should always approve ──────────────────────
    (
        "ski slopes", "Lex Amarni",
        "Ski Slopes", "Lex Amarni",
        5541, True,
        "Exact title + exact artist match — the original Ski Slopes bug case"
    ),
    (
        "Thong Song", "PlaqueBoyMax",
        "Thong Song", "PlaqueBoyMax",
        2000, True,
        "Exact title + exact artist match — the correct Thong Song sound"
    ),
    (
        "Pink Dreads", "DDG & PlaqueBoyMax",
        "Pink Dreads", "PlaqueBoyMax",
        138845, True,
        "Exact title + artist present in a multi-artist author field"
    ),

    # ── Same artist, DIFFERENT song — must reject (Tier 1 over-broad bug) ─
    (
        "pink dreads", "DDG & PlaqueBoyMax",
        "Thong Song", "PlaqueBoyMax",
        138845, False,
        "REGRESSION: Pink Dreads must NOT match a Thong Song search just "
        "because PlaqueBoyMax is in the author field — artist match alone "
        "doesn't say which song this is"
    ),

    # ── Same title, DIFFERENT credited artist — must reject (score-sum bug) ─
    (
        "Thong Song", "Sisqo",
        "Thong Song", "PlaqueBoyMax",
        4557, False,
        "REGRESSION: Sisqo's original Thong Song must NOT match a "
        "PlaqueBoyMax Thong Song search just because the title is identical"
    ),
    (
        "Thong Song (Re-Recorded)", "Sisqó",
        "Thong Song", "PlaqueBoyMax",
        558, False,
        "Same as above with a re-recorded/derivative variant"
    ),
    (
        "Thong Song (with Sisqo)", "Joezi & ADAME (US) & Sisqó",
        "Thong Song", "PlaqueBoyMax",
        90, False,
        "Collab title mentioning the song name but crediting different artists"
    ),

    # ── Generic uploads, no artist signal — Tier 2 depends on popularity ──
    (
        "original sound - maxfanpage", "maxfanpage",
        "Thong Song", "PlaqueBoyMax",
        TIER2_VIDEO_COUNT_THRESHOLD + 1, True,
        "REGRESSION: generic upload with no artist signal but huge traction "
        "and exact title match should be approved (Tier 2) — this was "
        "wrongly rejected when artist match was made mandatory"
    ),
    (
        "original sound - maxfanpage", "maxfanpage",
        "Thong Song", "PlaqueBoyMax",
        50, False,
        "Same generic upload but LOW traction — not enough evidence, reject"
    ),
    (
        "original sound - user382920", "user382920",
        "Thong Song", "PlaqueBoyMax",
        TIER2_VIDEO_COUNT_THRESHOLD, True,
        "Boundary check: video_count exactly AT threshold should pass"
    ),
    (
        "original sound - user382920", "user382920",
        "Thong Song", "PlaqueBoyMax",
        TIER2_VIDEO_COUNT_THRESHOLD - 1, False,
        "Boundary check: video_count one below threshold should fail"
    ),

    # ── Derivative versions — should never pass even with high popularity ─
    (
        "Thong Song Remix", "Yung Princey",
        "Thong Song", "PlaqueBoyMax",
        500000, False,
        "A remix by an unrelated uploader must not pass even with huge "
        "video_count — derivative marker should block Tier 2"
    ),
    (
        "Thong Song sped up", "randomclipz",
        "Thong Song", "PlaqueBoyMax",
        500000, False,
        "Sped-up derivative should also be blocked from Tier 2"
    ),

    # ── Artist name variants — substring matching (the Pink Dreads/Clips fix) ─
    (
        "Thong Song", "PlaqueBoyMax Clips",
        "Thong Song", "PlaqueBoyMax",
        1000, True,
        "Space-separated variant of the artist name in author field"
    ),
    (
        "Thong Song", "officialplaqueboymax",
        "Thong Song", "PlaqueBoyMax",
        1000, True,
        "Concatenated, no-space variant of the artist name in author field"
    ),
    (
        "Thong Song", "plaqueboymaxarchive",
        "Thong Song", "PlaqueBoyMax",
        1000, True,
        "Another concatenated no-space variant"
    ),

    # ── No artist on file at all — fall back to title-only matching ──────
    (
        "Ski Slopes", "some_random_uploader",
        "Ski Slopes", "",
        1000, True,
        "No artist on file for this song — exact title match alone should pass"
    ),
    (
        "Completely Different Song", "some_random_uploader",
        "Ski Slopes", "",
        1000, False,
        "No artist on file, and title doesn't match either — reject"
    ),

    # ── Edge cases ─────────────────────────────────────────────────────────
    (
        "Thong Song", "PlaqueBoyMax",
        "", "PlaqueBoyMax",
        1000, False,
        "Missing song title entirely — never approve blind, even with "
        "an artist match"
    ),

    # ── Stylized/accented titles — the Yeat diacritic bug ────────────────
    (
        "Griddlë", "Yeat & Don Toliver",
        "Griddle", "Yeat",
        2223, True,
        "REGRESSION: stylized title with a diacritic ('Griddlë') must "
        "still exact-match the plain song title on file ('Griddle') — "
        "_normalize_str was deleting accented characters outright instead "
        "of transliterating them, so 'Griddlë' normalized to 'griddl' "
        "(missing the final letter) and could never match 'griddle'"
    ),
    (
        "Monëy so big", "Yeat",
        "Money So Big", "Yeat",
        66233, True,
        "REGRESSION: 'Monëy so big' must exact-match 'Money So Big' — "
        "previously normalized to 'mon y so big' (gap where the accented "
        "character was deleted), breaking the match entirely"
    ),
    (
        "original sound - Gët Busy fan", "randomuser",
        "Gët Busy", "Yeat",
        50, False,
        "Low-traction generic upload should still correctly reject even "
        "when both sound and song titles share the same stylization"
    ),
]


def run():
    failures = []
    for i, (sound_title, sound_author, song_title, song_artist,
            video_count, expected, description) in enumerate(CASES, 1):
        actual = _classify_sound_match(
            sound_title, sound_author, song_title, song_artist, video_count
        )
        status = "PASS" if actual == expected else "FAIL"
        if actual != expected:
            failures.append((i, description, sound_title, sound_author,
                              song_title, song_artist, video_count,
                              expected, actual))
        print(f"[{status}] #{i}: {description}")

    print()
    if failures:
        print(f"{len(failures)} of {len(CASES)} cases FAILED:\n")
        for (i, description, sound_title, sound_author, song_title,
             song_artist, video_count, expected, actual) in failures:
            print(f"  #{i}: {description}")
            print(f"      sound_title={sound_title!r} sound_author={sound_author!r}")
            print(f"      song_title={song_title!r} song_artist={song_artist!r} video_count={video_count}")
            print(f"      expected={expected} actual={actual}\n")
        sys.exit(1)
    else:
        print(f"All {len(CASES)} cases passed.")
        sys.exit(0)


if __name__ == "__main__":
    run()