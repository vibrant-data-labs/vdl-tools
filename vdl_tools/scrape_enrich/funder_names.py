"""Clean, key, flag and pair funder names; no network and no database.

Funders come from two sources that both carry an id: a Crunchbase investor uuid
and a Giving Tuesday granter EIN. A name is only a display field, never a join
key: 8 different EINs are filed as "SMITH FAMILY FOUNDATION". So everything here
works on one funder at a time, or pairs a uuid with an EIN only when the pair is
very likely the same funder.

- ``funder_filed_name`` completes a Giving Tuesday granter name from its three
  name lines (line 1 alone is cut off for about 1 name in 6).
- ``clean_funder_text`` removes invisible characters and stray punctuation.
- ``pick_spelling`` picks one spelling for a funder, preferring mixed case.
- ``funder_key`` is a loose key to compare names, never to merge on its own.
- ``funder_kind`` flags pass-through funds and Crunchbase placeholders.
- ``pair_cb_gt_funders`` pairs a Crunchbase uuid with a Giving Tuesday EIN.

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

# Donor-advised funds and giving platforms: real EINs, but the money they route
# belongs to individual donors, so the funder shown is not the source of it.
PASS_THROUGH_EINS = frozenset({
    "462626883",  # AmazonSmile Foundation
    "110303001",  # Fidelity Investments Charitable Gift Fund
    "311640316",  # Donor Advised Charitable Giving Inc
    "810739440",  # American Online Giving Foundation
    "200721133",  # The Bank of America Charitable Gift Fund
    "232888152",  # Vanguard Charitable Endowment Program
    "237825575",  # National Philanthropic Trust
    "431634280",  # Charities Aid Foundation America
    "680480736",  # Network for Good
    "450931286",  # PayPal Charitable Giving Fund
    "311774905",  # GS Donor Advised Philanthropy Fund
    "113813663",  # The Goldman Sachs Charitable Gift Fund
    "581493949",  # National Christian Foundation
    "460942102",  # The Blackbaud Giving Fund
    "527082731",  # Morgan Stanley Global Impact Funding Trust
    "593652538",  # Raymond James Charitable Endowment Fund
    "141782466",  # The Ayco Charitable Foundation
    "311663020",  # The US Charitable Gift Trust
    "311709466",  # T Rowe Price Program for Charitable Giving
    "341747398",  # American Endowment Foundation
    "863177440",  # Daffy Charitable Fund
    "262048480",  # ImpactAssets
    "237174183",  # Jewish Communal Fund
})
PLACEHOLDER_RE = re.compile(
    r"^(?:"
    r"(?:an? |multiple |group of )?(?:undisclosed |anonymous |private )?"
    r"(?:individual )?(?:angel )?investors?(?:\(s\))?"
    r"|multiple undisclosed.*|anonymous(?: angel| donor)?|confidential|undisclosed"
    r")$",
    re.IGNORECASE,
)

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
SOCIAL_DOMAINS = frozenset({"facebook.com", "linkedin.com", "twitter.com", "x.com", "instagram.com"})


def _clean(value):
    return "" if value is None or pd.isna(value) else str(value).strip()


def clean_funder_text(name):
    """Plain characters, no invisible marks, ligatures expanded, no trailing junk."""
    name = unicodedata.normalize("NFKC", _clean(name))
    name = normalize_name(INVISIBLE_RE.sub("", name))
    name = TRAILING_JUNK_RE.sub("", " ".join(name.split()))
    return name


def funder_filed_name(name, name_secondary=None, dba_name=None):
    """The complete filed name of a Giving Tuesday granter from its three name lines.

    Line 2 often finishes a name cut at the field width ("COMMUNITY FOUNDATION OF" +
    "ELKHART COUNTY INC"); a "C/O ..." tail is care-of text, not part of the name.
    """
    organization, _ = resolve_name_lines(_clean(name), _clean(name_secondary), _clean(dba_name))
    return clean_funder_text(CARE_OF_RE.sub("", organization))


def funder_key(name):
    """A loose key to compare funder names: case, punctuation, "The", "&" and a legal suffix ignored.

    Not an identity: many funders share a key, and one funder can have several.
    """
    key = drop_legal_suffix(unicodedata.normalize("NFKC", _clean(name))).lower().replace("&", " and ")
    key = re.sub(r"['\u2019.]", "", key)
    key = " ".join(re.sub(r"[^a-z0-9 ]", " ", key).split())
    return CO_SUFFIX_RE.sub("", re.sub(r"^the ", "", key))


def funder_kind(name=None, ein=None):
    """``"pass_through"``, ``"placeholder"`` or ``None`` for a real funder."""
    digits = re.sub(r"\D", "", _clean(ein))
    if digits in PASS_THROUGH_EINS:
        return "pass_through"
    if PLACEHOLDER_RE.match(clean_funder_text(name)):
        return "placeholder"
    return None


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


def pair_cb_gt_funders(cb, gt):
    """Pair Crunchbase investors with Giving Tuesday granters when very likely the same funder.

    ``cb`` has ``uuid``, ``name`` and optionally ``legal_name``, ``website``, ``state``;
    ``gt`` has ``ein``, ``name`` (the complete filed name) and optionally ``website``,
    ``state``. Returns ``uuid, ein, reason``, one row per funder on each side.

    A candidate shares the name key. It is paired when the states do not differ and:

    - the websites match, or
    - the CB legal name equals the filed name and one EIN carries the key
      (``website_or_legal_name``), or
    - the states match and exactly one uuid and one EIN carry the key
      (``state_unique_name``).

    Two states that differ are never paired, and a name carried by several EINs or
    uuids needs a website. Shared grantees are not used: funders co-fund, and the
    two sources cover different grantees.
    """
    cb, gt = cb.copy(), gt.copy()
    for df in (cb, gt):
        df["key"] = df["name"].map(funder_key)
        df["state"] = df["state"].map(us_state) if "state" in df else None
        df["domain"] = df["website"].map(website_domain) if "website" in df else None
    cb["legal_key"] = cb["legal_name"].map(funder_key) if "legal_name" in cb else ""
    n_uuid, n_ein = cb["key"].value_counts(), gt["key"].value_counts()

    pairs = cb[["uuid", "key", "legal_key", "state", "domain"]].merge(
        gt[["ein", "key", "state", "domain"]], on="key", suffixes=("_cb", "_gt")
    )
    pairs = pairs[pairs["key"] != ""]
    differ = pairs["state_cb"].notna() & pairs["state_gt"].notna() & (pairs["state_cb"] != pairs["state_gt"])
    same_site = pairs["domain_cb"].notna() & (pairs["domain_cb"] == pairs["domain_gt"])
    one_ein = pairs["key"].map(n_ein) == 1
    unique = one_ein & (pairs["key"].map(n_uuid) == 1)
    legal = (pairs["legal_key"] == pairs["key"]) & one_ein
    same_state = pairs["state_cb"].notna() & (pairs["state_cb"] == pairs["state_gt"])

    pairs["reason"] = None
    pairs.loc[~differ & (legal | same_site), "reason"] = "website_or_legal_name"
    pairs.loc[~differ & same_state & unique & pairs["reason"].isna(), "reason"] = "state_unique_name"
    pairs = pairs.dropna(subset=["reason"])
    # one funder on each side: an uuid or EIN that matched twice is ambiguous
    pairs = pairs[~pairs["uuid"].duplicated(keep=False) & ~pairs["ein"].duplicated(keep=False)]
    return pairs[["uuid", "ein", "reason"]].reset_index(drop=True)
