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
    """Delete every cached WebPagesParsed/WebPagesScraped row for the given
    URLs' hosts (index page + subpages), so scrape_websites_psql(...,
    skip_existing=True) re-scrapes them.

    Matches via extract_website_name(url) - the key scrape_websites_psql
    itself writes - not home_url, which has inconsistent trailing slashes
    and can silently miss rows. It does NOT strip "www." or lowercase the
    host, so pass the exact host string that was actually scraped.

    dry_run=True only counts/logs. Returns {"parsed_deleted", "scraped_deleted", "dry_run"}.
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
