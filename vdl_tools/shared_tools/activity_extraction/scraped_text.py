"""
Read cached website text out of the psql scrape cache.
======================================================

Extraction never scrapes. It reads whatever
``vdl_tools.scrape_enrich.scraper.scrape_websites`` already stored, so a run costs one
LLM call per organization and nothing else. The two cache tables are queried directly:

``web_pages_scraped``
    One row per page (``cleaned_key``, ``subpath``, ``parsed_html``). Preferred,
    because per-page structure lets ``make_group_text`` budget tokens and
    order pages deterministically.
``web_pages_parsed``
    One row per site (``cleaned_home_key``, ``combined_text``). Fallback when
    no per-page rows exist.

Keys are the scraper's own ``extract_website_name`` (``scrape_enrich.url_keys``): the
home key is the host, and a subpage key is ``<host>/<underscore-joined subpath>``. www is
not stripped, and a site's pages can be split across the ``www.`` and bare-host spellings,
so both are read and merged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from sqlalchemy import func, or_
from sqlalchemy.exc import OperationalError

from vdl_tools.scrape_enrich.url_keys import extract_website_name
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text

# Pages shorter than this carry no usable content once boilerplate is stripped.
MIN_PAGE_CHARS = 50

# A blocked or errored fetch can still return a rendered error page, which the
# scraper stores as ordinary page text. Five sample sites cached the identical
# 124-character WAF block page ("403 - Forbidden"), which would otherwise be
# summarized as if it described the organization. Reject short pages that match
# an error-page signature; the length bound keeps a real page that happens to
# discuss access restrictions from being dropped.
MAX_ERROR_PAGE_CHARS = 600
ERROR_PAGE_MARKERS = (
    "access to this page is forbidden",
    "403 - forbidden",
    "403 forbidden",
    "404 - not found",
    "404 not found",
    "page not found",
    "access denied",
    "attention required",
    "checking your browser",
    "enable javascript to continue",
    "service temporarily unavailable",
    "internal server error",
    "site is temporarily down",
)


def looks_like_error_page(text: str) -> bool:
    """True when a short page is an HTTP error or bot-block page, not content."""
    if len(text) > MAX_ERROR_PAGE_CHARS:
        return False
    lowered = text.lower()
    return any(marker in lowered for marker in ERROR_PAGE_MARKERS)


def _key_variants(key: str) -> list[str]:
    """Both www and bare-host spellings of a cache key."""
    key = key.lower()
    other = key[4:] if key.startswith("www.") else f"www.{key}"
    # The bare host of a path-bearing key, so a URL with a path can still fall
    # back to the site it belongs to.
    host = key.split("_")[0]
    variants = [key, other, host, f"www.{host}" if not host.startswith("www.") else host[4:]]
    seen, out = set(), []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


@dataclass
class SiteText:
    """The cached text for one organization's website."""

    url: str
    pages: list[dict]  # {'source', 'subpath', 'text'} records
    source_table: str  # 'web_pages_scraped' | 'web_pages_parsed' | ''

    @property
    def n_chars(self) -> int:
        return sum(len(p["text"]) for p in self.pages)


# Websites per SQL statement in load_site_texts. Each variant becomes a LIKE clause, so the
# whole pool in one query runs to thousands of them and has dropped the connection.
_QUERY_BATCH = 500
_QUERY_RETRIES = 4
_QUERY_BACKOFF = 5  # seconds, multiplied by the attempt number


def load_site_texts(urls: list[str]) -> dict[str, SiteText]:
    """Return ``{url: SiteText}`` for every URL, empty ``pages`` when uncached.

    Both cache tables are read in two bulk queries; per-page rows win, and
    the site-level ``combined_text`` fills in for sites that only have that.
    """
    from vdl_tools.shared_tools.database_cache.database_models import (
        WebPagesParsed,
        WebPagesScraped,
    )
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    urls = [u for u in dict.fromkeys(urls) if u and str(u).strip()]
    url_to_key = {u: extract_website_name(u) for u in urls}
    variants = sorted({v for k in url_to_key.values() if k for v in _key_variants(k)})
    if not variants:
        return {}

    # One statement per batch rather than one for the whole list: at pool scale the filter
    # becomes thousands of OR'd LIKE clauses in a single query. Batching keeps each statement a
    # normal size, and each batch retries on a fresh connection, because the connection to RDS
    # drops intermittently on a lookup this long; a single drop would otherwise lose a
    # twenty-minute pass over the whole pool.
    page_rows: list[tuple] = []
    site_rows: list[tuple] = []
    for start in range(0, len(variants), _QUERY_BATCH):
        batch = variants[start : start + _QUERY_BATCH]
        for attempt in range(_QUERY_RETRIES):
            try:
                with get_session() as active:
                    # Match the stored column directly rather than lower(column): wrapping it in
                    # a function makes the btree index on cleaned_key unusable, turning each
                    # batch into a full scan. Measured at 34s per batch with lower() against
                    # 1.7s without, over 437k rows. The keys we build are already lowercase, so
                    # rows stored with uppercase are picked up separately below.
                    page_rows += (
                        active.query(
                            func.lower(WebPagesScraped.cleaned_key),
                            WebPagesScraped.home_url,
                            WebPagesScraped.subpath,
                            WebPagesScraped.parsed_html,
                        )
                        .filter(or_(*[WebPagesScraped.cleaned_key.like(v + "%") for v in batch]))
                        .all()
                    )
                    site_rows += (
                        active.query(
                            func.lower(WebPagesParsed.cleaned_home_key),
                            WebPagesParsed.home_url,
                            WebPagesParsed.combined_text,
                        )
                        .filter(WebPagesParsed.cleaned_home_key.in_(batch))
                        .all()
                    )
                break
            except OperationalError:
                if attempt == _QUERY_RETRIES - 1:
                    raise
                logger.warning(
                    "Website-text lookup lost the connection on batch starting %s; "
                    "retrying (%s of %s)",
                    start,
                    attempt + 2,
                    _QUERY_RETRIES,
                )
                time.sleep(_QUERY_BACKOFF * (attempt + 1))

    # The indexed match above is case-sensitive, so rows stored with an uppercase key never
    # match our lowercase variants. There are few of them, so pull just their keys, work out
    # which belong to a site we want, and fetch only those rows.
    wanted = set(variants)
    with get_session() as active:
        mixed_case = [
            k
            for (k,) in active.query(WebPagesScraped.cleaned_key)
            .filter(WebPagesScraped.cleaned_key != func.lower(WebPagesScraped.cleaned_key))
            .all()
            if any(k.lower() == v or k.lower().startswith((v + "/", v + "_")) for v in wanted)
        ]
        if mixed_case:
            logger.info("Fetching %s page rows stored under a mixed-case key", len(mixed_case))
            page_rows += (
                active.query(
                    func.lower(WebPagesScraped.cleaned_key),
                    WebPagesScraped.home_url,
                    WebPagesScraped.subpath,
                    WebPagesScraped.parsed_html,
                )
                .filter(WebPagesScraped.cleaned_key.in_(mixed_case))
                .all()
            )

    # LIKE 'host%' over-matches (host.co matches host.com), so a row only
    # belongs to a variant on a delimiter boundary: the home key itself, or
    # '<host>/<subpath>'. ('_' is accepted too — the scraper joins nested path
    # segments with it, and older rows use it as the top-level separator.)
    def _belongs(cleaned_key: str, variant: str) -> bool:
        return cleaned_key == variant or cleaned_key.startswith((variant + "/", variant + "_"))

    pages_by_variant: dict[str, list[tuple]] = {v: [] for v in variants}
    for cleaned_key, home_url, subpath, parsed_html in page_rows:
        for v in variants:
            if _belongs(cleaned_key, v):
                pages_by_variant[v].append((cleaned_key, home_url, subpath, parsed_html))
                break
    sites_by_variant = {k: (home_url, text) for k, home_url, text in site_rows}

    results: dict[str, SiteText] = {}
    for url, key in url_to_key.items():
        site = SiteText(url=url, pages=[], source_table="")
        if not key:
            results[url] = site
            continue
        # Merge every spelling rather than stopping at the first that has
        # rows: a site is routinely stored partly under www and partly not,
        # and taking only the first spelling silently truncates it to one page.
        best: dict[str, dict] = {}
        for variant in _key_variants(key):
            for cleaned_key, home_url, subpath, parsed_html in pages_by_variant.get(variant) or []:
                text = clean_scraped_text(parsed_html or "")
                if len(text) < MIN_PAGE_CHARS or looks_like_error_page(text):
                    continue
                # The same page under both spellings is one page; keep the
                # copy with more text.
                page_id = cleaned_key[4:] if cleaned_key.startswith("www.") else cleaned_key
                if len(text) > len(best.get(page_id, {}).get("text", "")):
                    best[page_id] = {
                        "source": home_url or url,
                        "subpath": subpath or "/",
                        "text": text,
                    }
        if best:
            # Single-page apps serve the same rendered text at every route, so
            # distinct subpaths can carry identical content; sending the model
            # N copies of one page wastes context and inflates apparent depth.
            seen_text, pages = set(), []
            for page in best.values():
                fingerprint = page["text"][:2000]
                if fingerprint in seen_text:
                    continue
                seen_text.add(fingerprint)
                pages.append(page)
            site.pages = pages
            site.source_table = "web_pages_scraped"
        if not site.pages:
            for variant in _key_variants(key):
                home_url, combined = sites_by_variant.get(variant, (None, None))
                text = clean_scraped_text(combined or "")
                if len(text) >= MIN_PAGE_CHARS and not looks_like_error_page(text):
                    site.pages = [{"source": home_url or url, "subpath": "/", "text": text}]
                    site.source_table = "web_pages_parsed"
                    break

        results[url] = site

    n_cached = sum(1 for s in results.values() if s.pages)
    logger.info("Website text cache: %s of %s URLs have text", n_cached, len(results))
    return results
