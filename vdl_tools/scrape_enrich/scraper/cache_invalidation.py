"""Delete cached scrape/parse rows so a URL gets scraped fresh.

Some past scrapes stored binary/mojibake text with status=200 and
num_errors=0. Because the row reads as a success, scrape_websites_psql's
skip_existing=True never re-fetches it, and the num_errors retry path can't
help either (retryable means 1 <= num_errors < max_errors, and 0 is "done").
Those rows are frozen until something deletes them from the cache tables so
the next run treats the URL as never scraped. See ed_tracker's
docs/website_quality.md for the origin of this gap.
"""

from vdl_tools.scrape_enrich.scraper.scrape_websites import (
    extract_website_name,
    ensure_url_scheme,
)
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    WebPagesParsed,
    WebPagesScraped,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger


def delete_cached_urls(urls: list, session=None, dry_run: bool = False) -> dict:
    """Delete every cached scrape/parse row for the given URLs' hosts.

    Deletes the WebPagesParsed row (keyed by cleaned_home_key) and every
    WebPagesScraped row for that host (index page + subpages, matched by
    home_url) so a subsequent scrape_websites_psql(..., skip_existing=True)
    run treats the URL as never scraped.

    Normalization is exactly what extract_website_name/ensure_url_scheme
    already do, no more:
      - cleaned_home_key (extract_website_name) strips scheme, query string
        and one trailing slash, so it matches "example.org", "example.org/",
        "https://example.org?x=1" alike.
      - home_url (ensure_url_scheme) only adds/lowercases a scheme - it does
        NOT strip a trailing slash or query string. So the WebPagesScraped
        match (and therefore subpage deletion) requires the same URL modulo
        scheme, not just the same domain.
    Neither function strips "www." or lowercases the host, so
    "example.org" and "www.example.org" are different cache keys, and so are
    "Example.org" and "example.org". Pass the same URL string (e.g. the
    org's own Website field value) that was originally fed to
    scrape_websites_psql, not a hand-normalized guess - that guarantees a
    match on both tables since it reproduces exactly what was stored.

    Args:
        urls: entity/website URLs whose cached host(s) should be cleared.
        session: optional existing SQLAlchemy session; a new one is opened
            (and committed/closed) when omitted, same as scrape_websites_psql.
        dry_run: when True, only counts and logs what would be deleted.

    Returns:
        {"parsed_deleted": int, "scraped_deleted": int, "dry_run": bool}
    """
    home_keys = {extract_website_name(u) for u in urls if u}
    home_keys.discard(None)
    home_urls = {ensure_url_scheme(u) for u in urls if u}
    home_urls.discard(None)

    with get_session(session=session) as session:
        parsed_q = session.query(WebPagesParsed).filter(
            WebPagesParsed.cleaned_home_key.in_(home_keys)
        )
        scraped_q = session.query(WebPagesScraped).filter(
            WebPagesScraped.home_url.in_(home_urls)
        )
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
