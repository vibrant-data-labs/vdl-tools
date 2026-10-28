"""
Grounding check: is each activity's evidence quote in the text the extractor saw?
==================================================================================

Every extracted activity carries ``evidence``, a verbatim phrase of at most 25 words
from the source. This module looks for that phrase in the source text the extractor
was given, with no LLM call. A quote that cannot be found points to an activity the
model invented or paraphrased rather than read.

Matching is done on normalized text (lowercase, curly quotes and dashes folded,
punctuation dropped, whitespace collapsed) at three levels:

``exact``      the whole normalized quote is a substring of the normalized source.
``fragments``  the quote is several pieces, split at ellipses or between separately
               quoted phrases (``"Soil Health" "Water"``), and every piece is found.
``partial``    at least ``PARTIAL_MIN`` of the quote's word 4-grams are found.
``not_found``  less than that.

``ngram_coverage`` (the share of 4-grams found) is reported for every quote, so the
cut-off can be revisited without re-running.

Main functions
--------------
``normalize``       the text normalization used on both sides.
``ground_quote``    status and coverage of one quote against one source text.
``ground_profiles`` one row per activity for a set of organizations.
"""

from __future__ import annotations

import re

import pandas as pd

# share of a quote's word 4-grams that must be found for "partial"
PARTIAL_MIN = 0.5
NGRAM = 4

_FOLD = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-", "—": "-", " ": " "})
# where a quote breaks into pieces: an ellipsis, or a closing quote followed by an opening one
_FRAGMENT_BREAK = re.compile(r"\.\.\.|…|[\"“”]\s*[,;]?\s*[\"“”]")
_NON_WORD = re.compile(r"[^a-z0-9]+")


def normalize(text: str) -> str:
    """Lowercase, fold typographic characters, drop punctuation, collapse whitespace."""
    text = str(text).translate(_FOLD).lower()
    return " ".join(_NON_WORD.sub(" ", text).split())


def _ngrams(words: list[str], n: int = NGRAM) -> set[tuple[str, ...]]:
    """Word n-grams of a token list (empty when the list is shorter than ``n``)."""
    return {tuple(words[i : i + n]) for i in range(len(words) - n + 1)}


def ground_quote(quote: str, source_norm: str, source_grams: set) -> tuple[str, float]:
    """Return ``(status, ngram_coverage)`` for one quote against a normalized source.

    ``source_grams`` is ``_ngrams(source_norm.split())``, computed once per source.
    """
    # quote characters the model wrapped around the phrase do not survive normalize()
    quote_norm = normalize(quote)
    if not quote_norm:
        return "not_found", 0.0
    found = f" {quote_norm} " in f" {source_norm} "
    grams = _ngrams(quote_norm.split())
    # a quote shorter than one 4-gram is covered only if found whole
    coverage = len(grams & source_grams) / len(grams) if grams else float(found)

    # whole quote, then its pieces, then 4-gram coverage
    if found:
        return "exact", coverage
    fragments = [normalize(f) for f in _FRAGMENT_BREAK.split(str(quote))]
    fragments = [f for f in fragments if f]
    if len(fragments) > 1 and all(f" {f} " in f" {source_norm} " for f in fragments):
        return "fragments", coverage
    return ("partial" if coverage >= PARTIAL_MIN else "not_found"), coverage


def ground_profiles(activities: pd.DataFrame, sources: dict[str, str]) -> pd.DataFrame:
    """Ground every activity's evidence quote against its organization's source text.

    ``activities`` needs ``uid``, ``activity_index``, ``activity`` and ``evidence``;
    ``sources`` maps uid to the source text the extractor saw. Organizations with no
    source text are skipped. Returns the activity identity columns plus
    ``grounding`` (status) and ``ngram_coverage``.
    """
    rows = []
    for uid, group in activities.groupby("uid", sort=False):
        if uid not in sources:
            continue
        # normalize and index each source once
        source_norm = normalize(sources[uid])
        source_grams = _ngrams(source_norm.split())
        for r in group.itertuples():
            status, coverage = ground_quote(r.evidence, source_norm, source_grams)
            rows.append(
                {
                    "uid": uid,
                    "activity_index": r.activity_index,
                    "activity": r.activity,
                    "evidence": r.evidence,
                    "grounding": status,
                    "ngram_coverage": round(coverage, 3),
                }
            )
    return pd.DataFrame(rows)
