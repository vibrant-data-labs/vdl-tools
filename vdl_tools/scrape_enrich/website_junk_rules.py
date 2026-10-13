"""Junk rules read from a website's SUMMARY: squatted gambling sites and
for-sale pages whose copy reads as ordinary prose.

Add a phrase here when a junk summary gets through. The page-TEXT markers
(parked, bot wall, binary) live in ``scraper/text_quality.py``; those run at
scrape time, before anything is summarized.

Which evidence a rule may read depends on whether the page was readable.

Readable page (a casino, a marketplace): the summary is a faithful rendering
of it, so gambling and parked may be detected from the summary.

Unreadable page (binary, a bot wall, an empty page): the summarizer sometimes
invents a description from the org's NAME instead of reporting it saw
nothing, so the summary is evidence of nothing. Those rules read the scraped
page text only (``text_quality``). A fabrication is always shaped like the
org — a squatted nonprofit yields an invented nonprofit description, never an
invented casino — which is why this asymmetry holds rather than being a
coincidence.
"""

# Category words every gambling site carries. Deliberately NOT specific game or
# brand names: those would fit this dataset rather than the problem.
GAMBLING_PHRASES = [
    "online gambling", "gambling platform", "online gaming platform", "slot game",
    "slot machine", "live casino", "online casino", "casino game", "sports betting",
    "betting platform", "online betting", "online lottery", "online poker",
    "igaming", "wagering",
]
# If the org's own records mention gaming, a gaming website is correct — that is
# what separates a real casino company from a squatted nonprofit domain.
# "lottery" is missing on purpose: in an education dataset it means admissions.
# Amistad Academy's summary says "blind lottery admissions", Lynn Public Schools
# "lottery systems" for enrollment. Adding single words took 66 flags to 218,
# almost all of them schools.
OWN_GAMING_WORDS = ["casino", "gaming", "gambling", "wager", "lottery", "betting"]

# Marketplace names, unambiguous enough to match against summary prose.
PARKED_SUMMARY_MARKERS = [
    "expireddomains", "hugedomains", "sedo.com", "squadhelp", "afternic",
    "domain marketplace", "domain for sale", "domains for sale",
]


def is_parked_summary(summary):
    """The summarizer naming a marketplace the page copy does not.

    handwritingcounts.com renders as "Fresh Listings Daily / Powerful Domain
    Insights", which matches no marker, but the summary says "primarily
    associated with ExpiredDomains.com and GoDaddy".
    """
    text = (summary or "").lower()
    return any(m in text for m in PARKED_SUMMARY_MARKERS)


def is_gambling(summary, records):
    """A gambling site on a domain whose owner has nothing to do with gambling.

    ``records`` should include the org's name as well as its descriptions: 852
    orgs (all Crunchbase) have no description at all, and for those the name is
    the only thing standing between a real gaming company and a blanked summary.
    """
    text = (summary or "").lower()
    if not any(phrase in text for phrase in GAMBLING_PHRASES):
        return False
    own = (records or "").lower()
    return not any(word in own for word in OWN_GAMING_WORDS)


def is_junk_summary(summary, records):
    """Junk that only the summary shows: a squatted gambling site, or a for-sale
    page whose copy names no marketplace.

    The page text of both reads as ordinary prose, so the scrape-time text
    check passes them and they get summarized. Used before the relevance model
    too, which would otherwise judge an org with a short description on the
    squatter's page. ``records`` is the org's own name and descriptions.
    """
    return is_parked_summary(summary) or is_gambling(summary, records)
