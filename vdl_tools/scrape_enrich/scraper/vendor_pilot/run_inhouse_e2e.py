"""The in-house scraper end to end over ``sample_orgs.parquet``, without the cache.

Production's path for a home URL: ``AsyncScraper.scrape_url`` (httpx, then
Playwright on a JS wall or JS shell), ``_process_scraped_with_browser_retry``
(``unstructured`` extraction, browser retry on empty or wall text),
``extract_website_links`` over the fetched HTML (``subpage_type='about'``,
``max_per_subpath=6``), the same for each chosen subpage, then
``make_group_text`` over the pages' ``parsed_html``. Nothing is written to
Postgres. Concurrency as in ``scrape_websites_psql`` (20 HTTP, 3 browser,
3 per host); ``--label`` names the host (results go to ``inhouse_<label>_e2e_*``).

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse_e2e --label laptop --limit 5
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse_e2e --label laptop
"""

import platform
import socket
import time
from collections import Counter

import httpx

from vdl_tools.scrape_enrich.scraper.async_scraper import AsyncScraper
from vdl_tools.scrape_enrich.scraper.scrape_websites import (
    PageType,
    _process_scraped_with_browser_retry,
)
from vdl_tools.scrape_enrich.url_keys import extract_website_name
from vdl_tools.scrape_enrich.scraper.vendor_pilot._e2e import (
    PageFetcher,
    PageResult,
    e2e_cli,
    picks_from_html,
)


class InhouseFetcher(PageFetcher):
    name = "inhouse"
    default_org_concurrency = 20

    def __init__(self, label: str = "laptop", http_concurrency: int = 20,
                 browser_concurrency: int = 3, max_per_host: int = 3):
        self.name = f"inhouse_{label}"
        self.label = label
        self.http_concurrency, self.browser_concurrency, self.max_per_host = http_concurrency, browser_concurrency, max_per_host
        self.scraper = AsyncScraper(max_concurrent_http=http_concurrency, max_concurrent_browser=browser_concurrency,
                                    verify_ssl=True, max_per_host=max_per_host)
        self.retry_counter = Counter()

    async def start(self):
        await self.scraper.__aenter__()

    async def stop(self):
        await self.scraper.__aexit__(None, None, None)

    async def run_context(self):
        egress_ip = None
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                egress_ip = (await client.get("https://api.ipify.org")).text.strip()
        except Exception:  # noqa: BLE001
            pass
        return {"label": self.label, "hostname": socket.gethostname(), "platform": platform.platform(),
                "egress_ip": egress_ip, "http_concurrency": self.http_concurrency,
                "browser_concurrency": self.browser_concurrency, "max_per_host": self.max_per_host,
                "link_source": "html (extract_website_links over the fetched page)"}

    async def fetch_page(self, url: str, subpath: str, page_type: str) -> PageResult:
        t0 = time.monotonic()
        scraped = await self.scraper.scrape_url(url)
        first_method = scraped.get("method")
        is_index = page_type == "index"
        scraped, rows = await _process_scraped_with_browser_retry(
            self.scraper, scraped,
            cache_id=extract_website_name(url),
            data_type=PageType.INDEX if is_index else PageType.PAGE,
            clean_path=None if is_index else subpath,
            root_path=url, return_raw_html=True, filter_no_body=True, add_section_links=False,
        )
        row = rows[0] if rows else {}
        content = scraped.get("content") or ""
        if isinstance(content, bytes):
            content = ""
        page = PageResult(
            url=url, subpath=subpath, page_type=page_type, success=bool(scraped.get("success")),
            http_status=scraped.get("status_code"), text=row.get("parsed_html") or "",
            raw_html_len=len(content), mode=scraped.get("method"),
            error=None if scraped.get("success") else (scraped.get("failure_reason") or "no content"),
            wall_seconds=time.monotonic() - t0,
        )
        if scraped.get("method") == "browser" and first_method != "browser":
            page.mode = "browser"
        page._raw_html = content  # transient, for pick_subpages
        return page.finalize()

    def pick_subpages(self, home: PageResult, home_url: str):
        picks_html = picks_from_html(home_url, getattr(home, "_raw_html", ""))
        return picks_html, "html", picks_html, []


def _factory(args):
    return InhouseFetcher(label=args.label, http_concurrency=args.http_concurrency,
                          browser_concurrency=args.browser_concurrency, max_per_host=args.max_per_host)


def _extra_args(parser):
    parser.add_argument("--label", default="laptop")
    parser.add_argument("--http-concurrency", type=int, default=20)
    parser.add_argument("--browser-concurrency", type=int, default=3)
    parser.add_argument("--max-per-host", type=int, default=3)


if __name__ == "__main__":
    e2e_cli(_factory, "inhouse", 20, extra_args=_extra_args)
