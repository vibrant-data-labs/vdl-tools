"""Delete cached scrape/parse rows so a URL gets scraped fresh.

Some scrapes stored binary/mojibake text as status=200, num_errors=0, so
skip_existing and the num_errors retry path can never revisit them - only
deleting the rows forces a fresh scrape. See ed_tracker's
docs/website_quality.md for the origin of this gap.
"""

from sqlalchemy import or_

from vdl_tools.scrape_enrich.scraper.scrape_websites import extract_website_name
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    WebPagesParsed,
    WebPagesParsedV2,
    WebPagesScraped,
    WebPagesScrapedV2,
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


# backend -> (parsed model, scraped model). "inhouse" is the httpx/Playwright
# scraper's pair; "firecrawl" is the v2 pair scrape_websites_firecrawl writes.
BACKEND_TABLES = {
    "inhouse": (WebPagesParsed, WebPagesScraped),
    "firecrawl": (WebPagesParsedV2, WebPagesScrapedV2),
}


def delete_cached_urls(urls: list, session=None, dry_run: bool = False, backend: str = "inhouse") -> dict:
    """Delete every cached parsed/scraped row for the given URLs' hosts
    (index page + subpages) in one backend's table pair, so the next scrape
    with skip_existing=True fetches them again.

    backend="inhouse" (default) is web_pages_parsed / web_pages_scraped;
    backend="firecrawl" is web_pages_parsed_v2 / web_pages_scraped_v2. The
    two caches are independent, so deleting from one leaves the other.
    web_pages_history_v2 and org_site_keys are never touched: the history row
    is the only other copy of a page's text, and the id-to-site link stays true.
    A history row exists only for a page whose text was already replaced once,
    so for every other page this delete removes the only stored copy: unlike a
    re-fetch, it does not keep the last known good text.

    Matches via extract_website_name(url) - the key scrape_websites_psql
    itself writes - not home_url, which has inconsistent trailing slashes
    and can silently miss rows. It does NOT strip "www." or lowercase the
    host, so pass the exact host string that was actually scraped.

    dry_run=True only counts/logs. Returns {"parsed_deleted", "scraped_deleted", "dry_run"}.
    """
    if backend not in BACKEND_TABLES:
        raise ValueError(f"backend must be one of {sorted(BACKEND_TABLES)}, got {backend!r}")
    parsed_model, scraped_model = BACKEND_TABLES[backend]
    home_keys = {extract_website_name(u) for u in urls if u}
    home_keys.discard(None)

    with get_session(session=session) as session:
        parsed_q = session.query(parsed_model).filter(
            parsed_model.cleaned_home_key.in_(home_keys)
        )

        scraped_filter = or_(
            scraped_model.cleaned_key.in_(home_keys),
            *[
                scraped_model.cleaned_key.like(f"{_escape_like(key)}/%", escape="\\")
                for key in home_keys
            ],
        )
        scraped_q = session.query(scraped_model).filter(scraped_filter)

        parsed_count = parsed_q.count()
        scraped_count = scraped_q.count()

        logger.info(
            "%s %d %s row(s) and %d %s row(s) for %d url(s)",
            "Would delete" if dry_run else "Deleting",
            parsed_count,
            parsed_model.__tablename__,
            scraped_count,
            scraped_model.__tablename__,
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
