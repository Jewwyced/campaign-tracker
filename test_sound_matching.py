"""
test_sound_matching.py — regression tests for _classify_sound_match(),
_artist_signal(), and _is_plausible_candidate().

WHY THIS FILE EXISTS:
This classifier has been fixed forward many times in one session — each
fix correctly handled the case that had just broken, but several times
silently reintroduced or left unguarded a DIFFERENT case that had already
been fixed before. Full history:

  1. Loose substring/"original sound" auto-pass -> approved huge amounts
     of unrelated junk.
  2. Summed relevance SCORE with a threshold -> a single strong signal
     (exact title match) could approve regardless of artist. "Thong Song"
     approved Sisqo's original under a PlaqueBoyMax search.
  3. Mandatory artist match whenever an artist is on file -> over-corrected:
     TikTok author metadata is often just an uploader's handle
     ("original sound - maxfanpage"), not a credited artist, so real
     matches got rejected for having no textual artist signal at all.
  4. Tiered check, artist match ALONE sufficient for Tier 1 -> an artist
     can have multiple songs. "Thong Song" by PlaqueBoyMax approved
     "Pink Dreads" by the same artist, since artist match alone doesn't
     say WHICH song this is. Fixed: Tier 1 requires artist AND title
     together.
  5. Tier 2 added: popularity-only approval for generic uploads with no
     artist signal, gated to sounds discovered via a "broad" hashtag/
     challenge crawl vs a "targeted" search -> STILL broke: "Griddle"
     collided with an unrelated dance trend, and the challenge crawl
     pulled in 33 false positives with multi-million view counts.
  6. Tier 2 narrowed further: only trust popularity for candidates from
     discovered_via == 'title_artist' specifically (the combined title+
     artist search) -> STILL broke: Back Home approved "TikTok
     Advertiser" and a Tyler the Creator fan account purely on video
     count, proving even the "trusted" targeted search doesn't reliably
     require real co-occurrence on TikTok's end.
  7. Tier 2 REMOVED ENTIRELY (second time, this version) — confirmed
     unreliable across two different discovery sources; popularity is
     never trusted as a substitute for a real artist match, full stop.
  8. Separately: _artist_signal's plain substring check broke on short/
     common artist names. "Yeat" (4 chars) matched inside "bells_yeat"
     (a fan handle), incorrectly passing Tier 1. Fixed with a length-
     ratio requirement (ARTIST_SIGNAL_MIN_RATIO) — the artist name must
     make up at least 50% of the author string, not just appear in it.

Run this file (`python test_sound_matching.py`) before shipping ANY
future change to the matching/discovery-filter logic. It checks every
real case above simultaneously, so a fix for a new problem can't
silently re-break an old one without you finding out immediately.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.service import (
    _classify_sound_match,
    _artist_signal,
    _is_plausible_candidate,
    _normalize_str,
    ARTIST_SIGNAL_MIN_RATIO,
)


def _simulate_qualify_branch(title, author, song, artist, video_count, source):
    """Mirrors the actual three-bucket branch logic inside
    qualify_pending_sounds_for_song's auto_approve=False path, so this
    orchestration behavior gets the same regression protection as the
    pure classifier functions."""
    if video_count == 0:
        return "inactive"
    is_relevant = _classify_sound_match(title, author, song, artist, video_count, discovered_via=source)
    if is_relevant:
        return "pending"
    is_plausible = _is_plausible_candidate(title, author, song, artist, source)
    return "pending" if is_plausible else "inactive"


# ── Three-bucket qualify orchestration cases (Find New Sounds path) ──────
# (title, author, song, artist, video_count, discovered_via, expected, description)
QUALIFY_BUCKET_CASES = [
    ("Way down We Go", "KELAO", "Griddle", "Yeat", 4196939, "challenge", "inactive",
     "REGRESSION: real, unrelated song with huge popularity must auto-reject "
     "regardless of view count — popularity alone never earns a human review"),
    ("La Muchachita", "Anthony Santos", "Griddle", "Yeat", 23993, "title_only", "inactive",
     "Same principle — a real, different song by a real, different artist"),
    ("Griddle Remix", "randomuser123", "Griddle", "Yeat", 45000, "title_only", "pending",
     "Plausible title relation, unconfirmed uploader — worth a human's judgment"),
    ("original sound - john", "john", "Griddle", "Yeat", 5000, "title_only", "inactive",
     "Generic upload, zero signal, low popularity — auto-reject"),
    ("original sound - john", "john", "Griddle", "Yeat", 500000, "title_only", "inactive",
     "REGRESSION: same generic upload with NO textual signal, even at "
     "huge popularity, still auto-rejects. A popularity-based exception "
     "was considered and deliberately reverted — popularity is not "
     "evidence of identity. If real data across a sample of songs shows "
     "this is a meaningful source of missed matches, that's a reason to "
     "revisit it with real numbers, not a guessed threshold."),
    ("Griddlë", "Yeat & Don Toliver", "Griddle", "Yeat", 2258, "title_artist", "pending",
     "Confident Tier 1 match — still lands in pending under auto_approve=False, "
     "never silently becomes canonical"),
]


# ── _classify_sound_match cases ──────────────────────────────────────────
# (sound_title, sound_author, song_title, song_artist, video_count,
#  discovered_via, expected, description)
CLASSIFY_CASES = [
    # Real, exact matches — should always approve
    ("ski slopes", "Lex Amarni", "Ski Slopes", "Lex Amarni", 5541, None, True,
     "Exact title + exact artist match — the original Ski Slopes bug case"),
    ("Thong Song", "PlaqueBoyMax", "Thong Song", "PlaqueBoyMax", 2000, None, True,
     "Exact title + exact artist match — the correct Thong Song sound"),
    ("Pink Dreads", "DDG & PlaqueBoyMax", "Pink Dreads", "PlaqueBoyMax", 138845, None, True,
     "Exact title + artist present in a multi-artist author field"),
    ("Back Home", "Yeat & Joji", "Back Home", "Yeat", 7024, None, True,
     "The real Back Home sound — artist embedded alongside a collaborator"),

    # Same artist, DIFFERENT song — must reject
    ("pink dreads", "DDG & PlaqueBoyMax", "Thong Song", "PlaqueBoyMax", 138845, None, False,
     "Pink Dreads must NOT match a Thong Song search just because "
     "PlaqueBoyMax is in the author field"),

    # Same title, DIFFERENT credited artist — must reject
    ("Thong Song", "Sisqo", "Thong Song", "PlaqueBoyMax", 4557, None, False,
     "Sisqo's original Thong Song must NOT match a PlaqueBoyMax search"),
    ("Thong Song (Re-Recorded)", "Sisqó", "Thong Song", "PlaqueBoyMax", 558, None, False,
     "Re-recorded/derivative variant, still wrong artist"),
    ("Thong Song (with Sisqo)", "Joezi & ADAME (US) & Sisqó", "Thong Song", "PlaqueBoyMax", 90, None, False,
     "Collab title mentioning the song name but crediting different artists"),

    # Tier 2 is GONE — generic uploads never pass on popularity alone,
    # regardless of discovery source
    ("original sound - maxfanpage", "maxfanpage", "Thong Song", "PlaqueBoyMax", 999999, "title_artist", False,
     "REGRESSION: Tier 2 is removed — no video_count, however high, "
     "substitutes for a real artist match, even from the 'trusted' "
     "title_artist source"),
    ("original sound - maxfanpage", "maxfanpage", "Thong Song", "PlaqueBoyMax", 50, "title_artist", False,
     "Low traction generic upload — also rejected"),
    ("original sound - veesun95", "veesun95", "Griddle", "Yeat", 1605136, "challenge", False,
     "The original Tier-2-breaking case: a 1.6M-video generic upload "
     "with zero relation, from the challenge crawl"),
    ("Original Sound", "TikTok Advertiser", "Back Home", "Yeat", 97335, "title_artist", False,
     "REGRESSION: the exact real-world case that broke Tier 2 a second "
     "time — a completely unrelated account, high video count, from the "
     "'trusted' title_artist source"),

    # Short/common artist name — the _artist_signal ratio fix
    ("original sound - booyahbooom", "bells_yeat", "Back Home", "Yeat", 259432, "title_artist", False,
     "REGRESSION: 'bells_yeat' is a fan handle that merely contains the "
     "substring 'yeat' — must NOT count as a real artist match, "
     "regardless of video_count"),

    # Derivative versions — never pass even with high popularity
    ("Thong Song Remix", "Yung Princey", "Thong Song", "PlaqueBoyMax", 500000, "title_artist", False,
     "A remix by an unrelated uploader must not pass"),
    ("Thong Song sped up", "randomclipz", "Thong Song", "PlaqueBoyMax", 500000, "title_artist", False,
     "Sped-up derivative should also be blocked"),

    # Artist name variants — substring + ratio, still working for real matches
    ("Thong Song", "PlaqueBoyMax Clips", "Thong Song", "PlaqueBoyMax", 1000, None, True,
     "Space-separated variant, ratio 0.706 — clears the 0.5 threshold"),
    ("Thong Song", "officialplaqueboymax", "Thong Song", "PlaqueBoyMax", 1000, None, True,
     "Concatenated variant, ratio 0.6 — clears the threshold"),
    ("Thong Song", "plaqueboymaxarchive", "Thong Song", "PlaqueBoyMax", 1000, None, True,
     "Another concatenated variant, ratio 0.632 — clears the threshold"),

    # No artist on file at all — fall back to title-only matching
    ("Ski Slopes", "some_random_uploader", "Ski Slopes", "", 1000, None, True,
     "No artist on file — exact title match alone should pass"),
    ("Completely Different Song", "some_random_uploader", "Ski Slopes", "", 1000, None, False,
     "No artist on file, title doesn't match either — reject"),

    # Edge cases
    ("Thong Song", "PlaqueBoyMax", "", "PlaqueBoyMax", 1000, None, False,
     "Missing song title entirely — never approve blind"),

    # Stylized/accented titles — the Yeat diacritic bug
    ("Griddlë", "Yeat & Don Toliver", "Griddle", "Yeat", 2223, None, True,
     "Stylized title with a diacritic must still exact-match the plain "
     "song title on file"),
    ("Monëy so big", "Yeat", "Money So Big", "Yeat", 66233, None, True,
     "Diacritic-stripped title must exact-match"),
]


# ── _artist_signal cases — the ratio guard specifically ──────────────────
# (raw_author, artist_norm, expected, description)
# NOTE: _artist_signal expects the RAW (pre-normalization) author string —
# normalization destroys the '&' that distinguishes a real collab credit
# from a fan-handle compound word. Do not pre-normalize these inputs.
ARTIST_SIGNAL_CASES = [
    ("bells_yeat", "yeat", False, "REGRESSION: fan handle, ratio 0.44 — must fail"),
    ("PlaqueBoyMax Clips", "plaqueboymax", True, "Real match, ratio 0.706 — must pass"),
    ("officialplaqueboymax", "plaqueboymax", True, "Real match, ratio 0.6 — must pass"),
    ("plaqueboymax", "plaqueboymax", True, "Exact match, ratio 1.0 — must pass"),
    ("randomuser123", "yeat", False, "No substring at all — must fail"),
    ("Yeat & Don Toliver", "yeat", True,
     "REGRESSION: real collab credit — 'Yeat' is only 29% of the full "
     "string but 100% of its own segment once split on '&'. Must pass."),
    ("DDG & PlaqueBoyMax", "plaqueboymax", True,
     "Same collab pattern with a longer artist name"),
]


# ── _is_plausible_candidate cases — discovery's looser filter ───────────
# (title, author, song_name, song_artist, discovered_via, expected, description)
DISCOVERY_FILTER_CASES = [
    ("Griddle", "Yeat", "Griddle", "Yeat", "title_only", True, "Plain match"),
    ("Griddle Remix", "randomuser", "Griddle", "Yeat", "title_only", True,
     "Derivative title from an unconfirmed uploader — still PLAUSIBLE to "
     "store, even though qualify will likely reject it as a derivative"),
    ("Way down We Go", "KELAO", "Griddle", "Yeat", "challenge", False,
     "Zero relation — must never be stored"),
    ("La Muchachita", "Anthony Santos", "Griddle", "Yeat", "challenge", False,
     "Zero relation — must never be stored"),
    ("original sound - randomuser", "randomuser", "Griddle", "Yeat", "title_only", False,
     "Generic upload, no title/artist signal — not plausible"),
]


def run():
    failures = []

    for i, (title, author, song, artist, video_count, source, expected, description) in enumerate(QUALIFY_BUCKET_CASES, 1):
        actual = _simulate_qualify_branch(title, author, song, artist, video_count, source)
        status = "PASS" if actual == expected else "FAIL"
        print(f"[{status}] qualify_bucket #{i}: {description}")
        if actual != expected:
            failures.append(f"qualify_bucket #{i}: {description} — expected {expected}, got {actual}")

    for i, (sound_title, sound_author, song_title, song_artist,
            video_count, discovered_via, expected, description) in enumerate(CLASSIFY_CASES, 1):
        actual = _classify_sound_match(
            sound_title, sound_author, song_title, song_artist,
            video_count, discovered_via=discovered_via
        )
        status = "PASS" if actual == expected else "FAIL"
        print(f"[{status}] classify #{i}: {description}")
        if actual != expected:
            failures.append(f"classify #{i}: {description} — expected {expected}, got {actual}")

    for i, (raw_author, artist, expected, description) in enumerate(ARTIST_SIGNAL_CASES, 1):
        actual = _artist_signal(raw_author, _normalize_str(artist))
        status = "PASS" if actual == expected else "FAIL"
        print(f"[{status}] artist_signal #{i}: {description}")
        if actual != expected:
            failures.append(f"artist_signal #{i}: {description} — expected {expected}, got {actual}")

    for i, (title, author, song_name, song_artist, discovered_via, expected, description) in enumerate(DISCOVERY_FILTER_CASES, 1):
        actual = _is_plausible_candidate(title, author, song_name, song_artist, discovered_via)
        status = "PASS" if actual == expected else "FAIL"
        print(f"[{status}] discovery_filter #{i}: {description}")
        if actual != expected:
            failures.append(f"discovery_filter #{i}: {description} — expected {expected}, got {actual}")

    total = len(QUALIFY_BUCKET_CASES) + len(CLASSIFY_CASES) + len(ARTIST_SIGNAL_CASES) + len(DISCOVERY_FILTER_CASES)
    print()
    if failures:
        print(f"{len(failures)} of {total} cases FAILED:\n")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    else:
        print(f"All {total} cases passed.")
        sys.exit(0)


if __name__ == "__main__":
    run()