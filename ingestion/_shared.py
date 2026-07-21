"""
ingestion/_shared.py — low-level utilities shared across boundaries.

Extracted here SPECIFICALLY to avoid a circular import: both discovery.py
and the qualification code remaining in service.py need _normalize_str,
_score_sound, _artist_signal, and _log — but service.py also needs to
import FROM discovery.py (to re-export discover_song_sounds etc. for
api.py's existing imports). If these helpers lived in either file, the
other would have to import back from it, which Python can't resolve.
This file has zero dependencies on discovery.py or service.py, so both
can depend on it without a cycle.

Content is otherwise UNCHANGED from where it used to live directly in
service.py — this is a pure relocation, not a rewrite.
"""

import re as _re
import unicodedata as _unicodedata

# Minimum proportion of an author string that the artist name must make
# up for _artist_signal to count it as a real match, not just a
# coincidental substring. Short/common artist names (e.g. "Yeat") can
# appear inside unrelated fan-account handles ("bells_yeat") that mention
# the artist without being any kind of official confirmation — a bare
# substring check can't tell "PlaqueBoyMax Clips" (a real match, artist
# name is 67% of the string) apart from "bells_yeat" (a fan handle, artist
# name is only 44% of the string). 0.5 cleanly separates every case seen
# so far — tune if new false positives/negatives turn up.
ARTIST_SIGNAL_MIN_RATIO = 0.5


def _log(msg):
    print(f"  [ingestion] {msg}", flush=True)


def _normalize_str(s):
    """Lowercase, transliterate accented/stylized characters to their plain
    equivalent, remove remaining punctuation, and collapse extra spaces.

    IMPORTANT: this used to just delete any character outside [a-z0-9 ],
    which silently mangled stylized titles instead of normalizing them.
    Many artists (Yeat especially — "Gët Busy", "Monëy so big", "Griddlë")
    use accented characters as stylization on otherwise plain song titles.
    Deleting 'ë' outright turned "Griddlë" into "griddl" (missing the final
    letter) or left a gap ("Monëy so big" -> "mon y so big"), so it could
    never exactly match the plain song title on file ("Griddle" ->
    "griddle") even though they're clearly the same song. This was
    silently breaking exact-title matching for a large share of this
    artist's own official sound titles specifically because of how he
    stylizes them — not a discovery or classifier bug, a normalization bug
    underneath both.

    Fix: NFKD-decompose first (splits 'ë' into 'e' + a combining diaeresis
    mark), then drop the combining marks, THEN strip remaining punctuation.
    'ë' becomes 'e' instead of vanishing.
    """
    s = s or ""
    s = _unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not _unicodedata.combining(c))
    return _re.sub(r'[^a-z0-9 ]', ' ', s.lower()).strip()


def _score_sound(sound, title, artist):
    """Score a sound candidate by relevance to the song title and artist.
    Higher score = better match. Used to RANK sounds — both during
    discovery (deciding which candidates are worth storing/logging first)
    and now also at qualify time (deciding which pending sounds are worth
    an actual provider call this batch, see QUALIFY_BATCH_SIZE). This is
    NOT used to gate approval — see _classify_sound_match() for the
    pass/fail decision.

    NOTE: video_count is NOT scored here because search APIs don't return it.
    It is fetched later via _update_sound_video_count() after sounds are selected.

    Scoring priority:
    1. Exact title match
    2. Title contained in sound title
    3. Multiple significant words match (2+ words, avoids single-word false positives)
    4. Verified artist match (normalized, punctuation-stripped)
    5. Official/original sound bonus
    6. Penalties for derivative versions
    """
    score = 0
    sound_title = _normalize_str(sound.get("title"))
    sound_author = _normalize_str(sound.get("author"))
    title_norm = _normalize_str(title)
    artist_norm = _normalize_str(artist) if artist else ""

    # Title matching
    if sound_title == title_norm:
        score += 150  # exact match
    elif title_norm in sound_title:
        score += 100  # title contained in sound title
    else:
        # Require 2+ significant words to match (avoids single common word false positives)
        sig_words = [w for w in title_norm.split() if len(w) > 3]
        matches = sum(1 for w in sig_words if w in sound_title)
        if len(sig_words) >= 2 and matches >= 2:
            score += 40

    # Artist matching — normalize punctuation before comparing
    if artist_norm:
        author_words = set(sound_author.split())
        artist_words = set(artist_norm.split())
        if sound_author == artist_norm:
            score += 100  # exact match
        elif artist_words.issubset(author_words):
            score += 75   # all artist words present in author
        elif any(w in author_words for w in artist_words):
            score += 30   # partial match only

    # Official/original sound bonus
    if sound.get("is_original"):
        score += 50

    # Penalties for derivative versions
    penalties = [
        "sped up", "slowed", "remix", "instrumental", "reverb", "cover",
        "nightcore", "bass boosted", "8d", "phonk", "edit audio", "mashup",
        "loop", "extended", "sped-up", "slow reverb", "lyrics"
    ]
    for word in penalties:
        if word in sound_title:
            score -= 30

    return score


import re as _collab_re_module
_COLLAB_SEPARATOR_RE = _collab_re_module.compile(r'&|,|\bx\b|\band\b|\bfeat\.?\b|\bft\.?\b', _collab_re_module.IGNORECASE)


def _artist_signal(raw_author, artist_norm):
    """Match an artist name against a sound's author field, handling two
    genuinely different situations that both shrink a naive length ratio:
      - A real multi-artist credit ("Yeat & Don Toliver") — the artist
        name is a small fraction of the FULL string, but 100% of its own
        segment once split on the collab separator.
      - A fan handle that merely mentions the artist ("bells_yeat") — no
        real separator, the artist name is genuinely just embedded in an
        unrelated compound word.

    A single whole-string ratio can't tell these apart — "Yeat" is a
    small fraction of "Yeat & Don Toliver" for the same arithmetic reason
    it's a small fraction of "bells_yeat" character-count-wise. The fix:
    split the RAW (pre-normalization — normalization destroys the '&')
    author string on real collab separators first, then check the ratio
    against each resulting segment individually. A short artist name
    still passes cleanly when it IS its own credited segment; it still
    fails when it's just embedded in one longer unrelated word.
    """
    if not artist_norm or not raw_author:
        return False
    artist_nospace = artist_norm.replace(" ", "")
    if not artist_nospace:
        return False

    segments = _COLLAB_SEPARATOR_RE.split(raw_author) or [raw_author]
    for seg in segments:
        seg_norm = _normalize_str(seg)
        seg_nospace = seg_norm.replace(" ", "")
        if not seg_nospace:
            continue
        contains = (artist_norm in seg_norm) or (artist_nospace in seg_nospace)
        if not contains:
            continue
        ratio = len(artist_nospace) / len(seg_nospace) if seg_nospace else 0
        if ratio >= ARTIST_SIGNAL_MIN_RATIO:
            return True
    return False