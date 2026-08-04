"""Name and URL normalization — the join keys for all matching tiers.

Pure functions (no network) so they are cheap to test and safe to apply to
both customer rows and baseline rows. Redirect resolution is separate and
optional because it needs the network.
"""

import re
from urllib.parse import urlparse

LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "llp", "lp", "ltd", "limited", "corp",
    "corporation", "co", "company", "pbc", "plc", "gmbh", "sa", "bv", "ag",
    "foundation", "fund", "trust",
}

_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")


def normalize_name(name: str | None) -> str:
    """Lowercase, strip punctuation and trailing legal suffixes."""
    if not name or not isinstance(name, str):
        return ""
    cleaned = _PUNCT_RE.sub(" ", name.lower())
    tokens = _WS_RE.sub(" ", cleaned).strip().split(" ")
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def name_tokens(name: str | None) -> set[str]:
    return set(normalize_name(name).split()) - {""}


def normalize_domain(url: str | None) -> str:
    """Reduce a URL (or bare domain) to its registrable host, no www."""
    if not url or not isinstance(url, str):
        return ""
    url = url.strip().lower()
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    host = urlparse(url).netloc.partition(":")[0]
    return host.removeprefix("www.")


def resolve_redirect(url: str, timeout: float = 10.0) -> str:
    """Follow redirects to the final domain. Returns the input's domain on failure."""
    import httpx

    domain = normalize_domain(url)
    if not domain:
        return ""
    try:
        resp = httpx.head(
            f"https://{domain}", follow_redirects=True, timeout=timeout
        )
        return normalize_domain(str(resp.url)) or domain
    except Exception:
        return domain


def normalize_ein(ein) -> str:
    """Canonical EIN form: 9 digits as NN-NNNNNNN; '' when unparseable."""
    if ein is None:
        return ""
    digits = re.sub(r"\D", "", str(ein))
    if len(digits) == 8:  # leading zero lost to numeric parsing
        digits = "0" + digits
    if len(digits) != 9:
        return ""
    return f"{digits[:2]}-{digits[2:]}"
