"""Delete cached scrape/parse rows so a URL gets scraped fresh.

Some past scrapes stored binary/mojibake text with status=200 and
num_errors=0. Because the row reads as a success, scrape_websites_psql's
skip_existing=True never re-fetches it, and the num_errors retry path can't
help either (retryable means 1 <= num_errors < max_errors, and 0 is "done").
Those rows are frozen until something deletes them from the cache tables so
the next run treats the URL as never scraped. See ed_tracker's
docs/website_quality.md for the origin of this gap.
"""

from sqlalchemy import or_

from vdl_tools.scrape_enrich.scraper.scrape_websites import extract_website_name
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    WebPagesParsed,
    WebPagesScraped,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger


def _escape_like(value: str, escape_char: str = "\\") -> str:
    """Escape a LIKE pattern's literal special characters (and the escape char itself)."""
    return (
        value.replace(escape_char, escape_char * 2)
        .replace("%", escape_char + "%")
        .replace("_", escape_char + "_")
    )


def delete_cached_urls(urls: list, session=None, dry_run: bool = False) -> dict:
    """Delete every cached scrape/parse row for the given URLs' hosts.

    Deletes the WebPagesParsed row (keyed by cleaned_home_key) and every
    WebPagesScraped row for that host - the index page (cleaned_key == the
    home key) plus every subpage (cleaned_key == "<home key>/<subpath>") -
    so a subsequent scrape_websites_psql(..., skip_existing=True) run treats
    the URL as never scraped.

    Both tables are matched purely via extract_website_name(url), the same
    function scrape_websites_psql uses to derive cleaned_home_key/cleaned_key
    when it writes these rows. That function strips scheme, query string and
    one trailing slash, so "example.org", "example.org/", "https://example.
    org?x=1" and "HTTPS://example.org" all resolve to the same key and match
    reliably. (An earlier version of this function matched WebPagesScraped by
    home_url instead, which is NOT normalized the same way - a stored
    home_url may or may not carry a trailing slash depending on how the URL
    was originally scraped, so that approach silently missed rows.)

    What it still does NOT normalize, because extract_website_name doesn't
    either: "www." is not stripped ("example.org" and "www.example.org" are
    different cache entries - they can even hold different content, e.g. a
    garbled "www.foo.org" scrape coexisting with a clean "foo.org" one), and
    the host is not lowercased. Pass the same host string that was actually
    scraped (e.g. a precomputed cache key, or the org's raw scrape-input
    column value) rather than a hand-cleaned guess.

    Args:
        urls: entity/website URLs (or precomputed cache keys) whose cached
            host(s) should be cleared.
        session: optional existing SQLAlchemy session; a new one is opened
            (and committed/closed) when omitted, same as scrape_websites_psql.
        dry_run: when True, only counts and logs what would be deleted.

    Returns:
        {"parsed_deleted": int, "scraped_deleted": int, "dry_run": bool}
    """
    home_keys = {extract_website_name(u) for u in urls if u}
    home_keys.discard(None)

    with get_session(session=session) as session:
        parsed_q = session.query(WebPagesParsed).filter(
            WebPagesParsed.cleaned_home_key.in_(home_keys)
        )

        scraped_filter = or_(
            WebPagesScraped.cleaned_key.in_(home_keys),
            *[
                WebPagesScraped.cleaned_key.like(f"{_escape_like(key)}/%", escape="\\")
                for key in home_keys
            ],
        )
        scraped_q = session.query(WebPagesScraped).filter(scraped_filter)

        parsed_count = parsed_q.count()
        scraped_count = scraped_q.count()

        logger.info(
            "%s %d WebPagesParsed row(s) and %d WebPagesScraped row(s) for %d url(s)",
            "Would delete" if dry_run else "Deleting",
            parsed_count,
            scraped_count,
            len(urls),
        )

        if not dry_run:
            parsed_q.delete(synchronize_session=False)
            scraped_q.delete(synchronize_session=False)

        return {
            "parsed_deleted": 0 if dry_run else parsed_count,
            "scraped_deleted": 0 if dry_run else scraped_count,
            "dry_run": dry_run,
        }
