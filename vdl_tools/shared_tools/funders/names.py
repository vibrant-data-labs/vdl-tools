"""Clean and key funder names; no network and no database.

Funders come from two sources that both carry an id: a Crunchbase investor uuid
and a Giving Tuesday granter EIN. A name is only a display field, never a join
key: 8 different EINs are filed as "SMITH FAMILY FOUNDATION". So everything here
works on one funder at a time.

- ``funder_filed_name`` completes a Giving Tuesday granter name from its three
  name lines (line 1 alone is cut off for about 1 name in 6).
- ``clean_funder_text`` removes invisible characters and stray punctuation.
- ``pick_spelling`` picks one spelling for a funder, preferring mixed case.
- ``funder_key`` is a loose key to compare names, never to merge on its own.
- ``us_state`` is the two-letter state of a state name.

For ALL CAPS names that need real casing, run ``display_name.pick_display_names``
on the result; it is not called here because it needs a model.
"""
import re
import unicodedata
from collections import Counter

import pandas as pd

from vdl_tools.scrape_enrich.display_name import drop_legal_suffix, normalize_name
from vdl_tools.scrape_enrich.givingtuesday.name_lines import resolve_name_lines

INVISIBLE_RE = re.compile("[​-‏‪-‮⁠﻿]")
TRAILING_JUNK_RE = re.compile(r"[\s.,;:|/\\&-]+$")
CARE_OF_RE = re.compile(r"\s*(?:c/o|\b(?:attn|attention)\b:?).*$", re.IGNORECASE)
CO_SUFFIX_RE = re.compile(r"\s+co$")

SOCIAL_DOMAINS = frozenset({"facebook.com", "linkedin.com", "twitter.com", "x.com", "instagram.com"})
US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "district of columbia": "DC",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
def _clean(value):
    return "" if value is None or pd.isna(value) else str(value).strip()


def clean_funder_text(name):
    """Plain characters, no invisible marks, ligatures expanded, no trailing junk."""
    name = unicodedata.normalize("NFKC", _clean(name))
    name = normalize_name(INVISIBLE_RE.sub("", name))
    name = TRAILING_JUNK_RE.sub("", " ".join(name.split()))
    return name


def funder_name_and_dba(name, name_secondary=None, dba_name=None):
    """The complete filed name of a Giving Tuesday granter and its DBA, from the three name lines.

    Line 2 often finishes a name cut at the field width ("COMMUNITY FOUNDATION OF" +
    "ELKHART COUNTY INC"); a "C/O ..." tail is care-of text, not part of the name. The DBA is
    ``""`` when there is none.
    """
    organization, dba = resolve_name_lines(_clean(name), _clean(name_secondary), _clean(dba_name))
    return clean_funder_text(CARE_OF_RE.sub("", organization)), clean_funder_text(dba)


def funder_filed_name(name, name_secondary=None, dba_name=None):
    """The complete filed name of a Giving Tuesday granter (see ``funder_name_and_dba``)."""
    return funder_name_and_dba(name, name_secondary, dba_name)[0]


def funder_key(name):
    """A loose key to compare funder names: case, punctuation, "The", "&" and a legal suffix ignored.

    Not an identity: many funders share a key, and one funder can have several.
    """
    key = drop_legal_suffix(unicodedata.normalize("NFKC", _clean(name))).lower().replace("&", " and ")
    key = re.sub(r"['\u2019.]", "", key)
    key = " ".join(re.sub(r"[^a-z0-9 ]", " ", key).split())
    return CO_SUFFIX_RE.sub("", re.sub(r"^the ", "", key))


def pick_spelling(names):
    """One spelling for a funder: the most frequent that is not ALL CAPS, else the most frequent.

    Never title-cases, which would mangle an acronym-only name like ``AARP``.
    """
    counts = Counter(clean_funder_text(n) for n in names if clean_funder_text(n))
    if not counts:
        return ""
    mixed = {n: c for n, c in counts.items() if not n.isupper()}
    pool = mixed or counts
    return max(pool, key=lambda n: (pool[n], n[:1].isupper(), n))


def us_state(value):
    """Two-letter US state from a name or an abbreviation; ``None`` if it is neither."""
    value = _clean(value)
    if value.upper() in US_STATES.values():
        return value.upper()
    return US_STATES.get(value.lower())


def website_domain(url):
    """Lowercase host without ``www.``; ``None`` for a blank, a placeholder or a social network."""
    url = _clean(url).lower()
    if not url or " " in url or url in {"n/a", "na", "none"}:
        return None
    host = re.sub(r"^[a-z]+://", "", url).split("/")[0].removeprefix("www.")
    return None if "." not in host or host in SOCIAL_DOMAINS else host
