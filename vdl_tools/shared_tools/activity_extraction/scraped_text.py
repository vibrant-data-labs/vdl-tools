"""
Read cached website text out of the psql scrape cache.
======================================================

The tagging pipeline never scrapes. It reads whatever
``vdl_tools.scrape_enrich.scraper.scrape_websites`` already stored, so a
summarization run costs one LLM call per organization and nothing else.
Importing the scraper itself pulls in playwright, which this pipeline does
not need, so the two cache tables are queried directly:

``web_pages_scraped``
    One row per page (``cleaned_key``, ``subpath``, ``parsed_html``). Preferred,
    because per-page structure lets ``make_group_text`` budget tokens and
    order pages deterministically.
``web_pages_parsed``
    One row per site (``cleaned_home_key``, ``combined_text``). Fallback when
    no per-page rows exist.

Keys follow the scraper's own ``extract_website_name``: the home key is the
host, and a subpage key is ``<host>/<underscore-joined subpath>``. www is not
stripped, and a site's pages can be split across the ``www.`` and bare-host
spellings, so both are read and merged.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import func, or_
from sqlalchemy.exc import OperationalError

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


def extract_website_name(value: str) -> str | None:
    """Mirror of the scraper's key function, without the playwright import.

    ``https://example.org/a/b`` -> ``example.org_a_b``. Note it deliberately
    does NOT strip ``www.`` — the cache is keyed on whichever spelling the
    site was scraped under.
    """
    if not value:
        return None
    text = str(value).strip()
    website = text.split("//")[1].strip() if "//" in text else text
    website = website.split("?")[0].strip()
    website = website[:-1] if website.endswith("/") else website
    return website.replace("/", "_").replace(":", "_")


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
    key: str
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


# Pages recovered by a browser-rendering re-scrape (nature_tagging.deep_scrape), kept out of
# the shared psql cache so a local recovery pass cannot leak into other projects' data. Each
# overlay file is read once per process.
_OVERLAY_CACHE: dict[Path, dict[str, list[dict]]] = {}


def _load_overlay(path: Path | None) -> dict[str, list[dict]]:
    """Return {site key: [page dicts]} from the recovery overlay, empty when there is none.

    The overlay stores the website URL as scraped; keys are derived with the same
    ``extract_website_name`` the psql cache lookup uses, so a recovered site matches the
    organization it belongs to.
    """
    if path is None:
        return {}
    path = Path(path)
    if path in _OVERLAY_CACHE:
        return _OVERLAY_CACHE[path]
    overlay: dict[str, list[dict]] = {}
    _OVERLAY_CACHE[path] = overlay
    if not path.exists():
        return overlay
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:  # a malformed overlay must not break extraction
        logger.warning("Could not read the deep-scrape overlay: %s", exc)
        return overlay

    frame = frame[frame["text"].notna()]
    for url, group in frame.groupby("url"):
        key = extract_website_name(str(url))
        if not key:
            continue
        pages = [
            {"source": str(url), "subpath": r.subpath, "text": r.text}
            for r in group.itertuples()
            if isinstance(r.text, str) and r.text.strip()
        ]
        if pages:
            overlay.setdefault(key.lower(), []).extend(pages)
    logger.info(
        "Deep-scrape overlay: %s sites, %s pages",
        len(overlay),
        sum(len(v) for v in overlay.values()),
    )
    return overlay


def load_site_texts(
    urls: list[str],
    session=None,
    overlay_path: Path | None = None,
) -> dict[str, SiteText]:
    """Return ``{url: SiteText}`` for every URL, empty ``pages`` when uncached.

    Both cache tables are read in two bulk queries; per-page rows win, and
    the site-level ``combined_text`` fills in for sites that only have that.
    Pages in the overlay at ``overlay_path`` (None for no overlay) are merged in.
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
    # normal size, and each batch retries on its own connection, because the connection to RDS
    # drops intermittently on a lookup this long — a single drop would otherwise lose a
    # twenty-minute pass over the whole pool.
    page_rows: list[tuple] = []
    site_rows: list[tuple] = []
    for start in range(0, len(variants), _QUERY_BATCH):
        batch = variants[start : start + _QUERY_BATCH]
        for attempt in range(_QUERY_RETRIES):
            try:
                with get_session(session=session) as active:
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
                # A caller-supplied session is now poisoned; drop to a fresh one.
                session = None
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
    with get_session(session=session) as active:
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

    # Pages recovered by deep_scrape, which renders a site in a browser to reach content the
    # shared scraper could not find. Kept in a local file rather than the shared psql cache, so
    # they are merged here rather than arriving through the queries above.
    overlay = _load_overlay(overlay_path)

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
        site = SiteText(url=url, key=key or "", pages=[], source_table="")
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

        # Merge any browser-recovered pages for this site, skipping ones whose text the cache
        # already holds. The recovery pass runs where the cache is thin, so its pages are
        # usually additional rather than duplicates, but a site can be re-scraped later.
        recovered = None
        for variant in _key_variants(key) if key else []:
            recovered = overlay.get(variant)
            if recovered:
                break
        if recovered:
            have = {p["text"][:2000] for p in site.pages}
            added = [p for p in recovered if p["text"][:2000] not in have]
            if added:
                site.pages = site.pages + added
                site.source_table = (
                    f"{site.source_table}+deep_scrape" if site.source_table else "deep_scrape"
                )
        results[url] = site

    n_cached = sum(1 for s in results.values() if s.pages)
    logger.info("Website text cache: %s of %s URLs have text", n_cached, len(results))
    return results
