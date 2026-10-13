"""Junk rules read from a website's SUMMARY: squatted gambling sites and
for-sale pages, whose page text reads as ordinary prose.

Add a phrase here when a junk summary gets through. Page-TEXT markers (parked,
bot wall, binary) are in ``scraper/text_quality.py``. Summary rules only apply
to readable pages: for an unreadable one the summarizer may invent a
description from the org's name, so its summary is no evidence.
"""

# Category words every gambling site carries. Deliberately NOT specific game or
# brand names: those would fit this dataset rather than the problem.
GAMBLING_PHRASES = [
    "online gambling", "gambling platform", "online gaming platform", "slot game",
    "slot machine", "live casino", "online casino", "casino game", "sports betting",
    "betting platform", "online betting", "online lottery", "online poker",
    "igaming", "wagering",
]
# If the org's own records mention gaming, a gaming website is correct (a real
# casino company). "lottery" is not a gambling phrase: in education it means
# admissions lotteries.
OWN_GAMING_WORDS = ["casino", "gaming", "gambling", "wager", "lottery", "betting"]

# Marketplace names, unambiguous enough to match against summary prose.
PARKED_SUMMARY_MARKERS = [
    "expireddomains", "hugedomains", "sedo.com", "squadhelp", "afternic",
    "domain marketplace", "domain for sale", "domains for sale",
]


def is_parked_summary(summary):
    """The summary names a domain marketplace the page copy did not."""
    text = (summary or "").lower()
    return any(m in text for m in PARKED_SUMMARY_MARKERS)


def is_gambling(summary, records):
    """A gambling site on a domain whose owner has nothing to do with gambling.

    ``records`` is the org's name and descriptions (name matters: many orgs have
    no description).
    """
    text = (summary or "").lower()
    if not any(phrase in text for phrase in GAMBLING_PHRASES):
        return False
    own = (records or "").lower()
    return not any(word in own for word in OWN_GAMING_WORDS)


def is_junk_summary(summary, records):
    """A squatted gambling site or a for-sale page, judged from the summary.

    Also used before the relevance model. ``records``: the org's name and descriptions.
    """
    return is_parked_summary(summary) or is_gambling(summary, records)
