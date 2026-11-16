"""Make a cached URL get scraped fresh.

Some scrapes stored binary/mojibake text as status=200, num_errors=0, so
skip_existing and the num_errors retry path can never revisit them - only
deleting the rows forces a fresh scrape. See ed_tracker's
docs/website_quality.md for the origin of this gap.

The Firecrawl tables keep the last known good text through a failed
re-fetch, and a delete would throw that away. So on that backend the default
is to mark the home row for a re-fetch and delete nothing; deleting is the
explicit purge=True, for text that is itself wrong.
"""

import datetime as dt

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


def delete_cached_urls(urls: list, session=None, dry_run: bool = False, backend: str = "inhouse",
                       purge: bool = False) -> dict:
    """Make the next scrape fetch the given URLs' hosts again, in one
    backend's table pair. The two caches are independent: acting on one
    leaves the other.

    backend="inhouse" (default), web_pages_parsed / web_pages_scraped: deletes
    every parsed/scraped row for the host (index page + subpages), so the next
    scrape with skip_existing=True fetches them. purge is ignored; deleting is
    the only way to refresh these tables.

    backend="firecrawl", web_pages_parsed_v2 / web_pages_scraped_v2: sets
    refetch_requested_at on the host's home row and deletes nothing. The next
    scrape_websites_firecrawl run fetches the site whatever its age; if that
    fetch is blocked or fails, the stored text stays. Returns
    {"parsed_deleted": 0, "scraped_deleted": 0, "refetch_marked", "dry_run"}.

    backend="firecrawl", purge=True: deletes the rows as on the in-house
    tables. Use it only when the stored text is itself wrong (garbled, the
    wrong site). A history row exists only for a page whose text was already
    replaced once, so for every other page the purge removes the only stored
    copy. web_pages_history_v2 and org_site_keys are never touched.

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
        if backend == "firecrawl" and not purge:
            home_q = session.query(scraped_model).filter(scraped_model.cleaned_key.in_(home_keys))
            marked = home_q.count()
            logger.info("%s %d %s home row(s) for a re-fetch, for %d url(s)",
                        "Would mark" if dry_run else "Marking", marked, scraped_model.__tablename__, len(urls))
            if not dry_run:
                # naive UTC, like every other timestamp on the v2 tables
                now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
                home_q.update({scraped_model.refetch_requested_at: now}, synchronize_session=False)
            return {"parsed_deleted": 0, "scraped_deleted": 0,
                    "refetch_marked": 0 if dry_run else marked, "dry_run": dry_run}

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
