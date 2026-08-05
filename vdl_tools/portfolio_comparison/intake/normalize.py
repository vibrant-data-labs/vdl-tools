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


_REDIRECT_CACHE: dict[str, str] = {}


def domains_converge(domain_a: str, domain_b: str, resolver=None) -> bool:
    """True when two domains resolve (via HTTP redirects) to the same final
    domain — e.g. abalobi.info and abalobi.org both landing on abalobi.org.
    Mechanical identity evidence: makes human review unnecessary for
    renamed/moved domains that still redirect."""
    if not domain_a or not domain_b:
        return False
    if domain_a == domain_b:
        return True
    resolver = resolver or resolve_redirect
    for d in (domain_a, domain_b):
        if d not in _REDIRECT_CACHE:
            _REDIRECT_CACHE[d] = resolver(d)
    final_a, final_b = _REDIRECT_CACHE[domain_a], _REDIRECT_CACHE[domain_b]
    return bool(final_a) and final_a == final_b


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
