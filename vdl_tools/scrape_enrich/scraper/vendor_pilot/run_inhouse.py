"""Fetch the sampled home URLs through the in-house scraper, as a baseline.

Same path production takes for an index page, minus the cache:
``AsyncScraper.scrape_url`` (httpx, then Playwright Chromium with stealth
when the HTTP response is a JS wall or JS shell), then
``_process_scraped_with_browser_retry`` (``unstructured`` text extraction,
browser retry when extraction is empty or the text is a bot wall). Nothing
is written to Postgres.

``--label`` names where it ran (``laptop``, ``ec2``): results go to
``inhouse_<label>_*``. The run meta records hostname, platform and the
egress IP, since a datacenter IP sees more bot walls than a residential one.
Concurrency defaults match ``scrape_websites_psql`` (20 HTTP, 3 browser,
3 per host).

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse --label laptop --limit 3
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse --label laptop
"""

import platform
import socket

import httpx

from vdl_tools.scrape_enrich.scraper.async_scraper import AsyncScraper
from vdl_tools.scrape_enrich.scraper.scrape_websites import (
    PageType,
    _process_scraped_with_browser_retry,
)
from vdl_tools.scrape_enrich.url_keys import extract_website_name
from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    FetchResult,
    VendorRunner,
    run_cli,
)


class InhouseRunner(VendorRunner):
    """In-house scraper (httpx + Playwright fallback + unstructured), no cache."""

    name = "inhouse"
    default_concurrency = 20
    default_rpm = None
    timeout_seconds = 30.0

    def __init__(self, label: str = "laptop", http_concurrency: int = 20,
                 browser_concurrency: int = 3, max_per_host: int = 3):
        super().__init__(api_key="")
        self.name = f"inhouse_{label}"
        self.label = label
        self.http_concurrency = http_concurrency
        self.browser_concurrency = browser_concurrency
        self.max_per_host = max_per_host
        self.scraper = AsyncScraper(
            max_concurrent_http=http_concurrency,
            max_concurrent_browser=browser_concurrency,
            verify_ssl=True,
            max_per_host=max_per_host,
        )

    def headers(self) -> dict:
        return {}

    async def start(self):
        await self.scraper.__aenter__()

    async def stop(self):
        await self.scraper.__aexit__(None, None, None)

    async def run_context(self):
        egress_ip = None
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                egress_ip = (await client.get("https://api.ipify.org")).text.strip()
        except Exception:  # noqa: BLE001 - context only
            pass
        return {
            "label": self.label,
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "egress_ip": egress_ip,
            "http_concurrency": self.http_concurrency,
            "browser_concurrency": self.browser_concurrency,
            "max_per_host": self.max_per_host,
        }

    async def fetch(self, client: httpx.AsyncClient, url: str) -> FetchResult:
        scraped = await self.scraper.scrape_url(url)
        first_method = scraped.get("method")
        scraped, rows = await _process_scraped_with_browser_retry(
            self.scraper,
            scraped,
            cache_id=extract_website_name(url),
            data_type=PageType.INDEX,
            clean_path=None,
            root_path=url,
            return_raw_html=True,
            filter_no_body=True,
            add_section_links=False,
        )
        row = rows[0] if rows else {}
        content = scraped.get("content") or ""
        if isinstance(content, bytes):
            content = ""
        result = FetchResult(vendor=self.name, url=url)
        result.http_status = scraped.get("status_code")
        result.raw_html = content
        result.markdown = row.get("parsed_html") or ""
        result.links = []  # our own extraction runs on raw_html in compare.py
        result.success = bool(scraped.get("success"))
        if not result.success:
            result.error = scraped.get("failure_reason") or "no content"
        result.vendor_metadata = {
            "method": scraped.get("method"),
            "first_method": first_method,
            "retried_with_browser": scraped.get("method") == "browser" and first_method != "browser",
            "failure_reason": scraped.get("failure_reason"),
            "num_errors": row.get("num_errors"),
        }
        return result


def _build(args):
    return InhouseRunner(
        label=args.label,
        http_concurrency=args.http_concurrency,
        browser_concurrency=args.browser_concurrency,
        max_per_host=args.max_per_host,
    )


def _extra_args(parser):
    parser.add_argument("--label", default="laptop", help="where this runs; results go to inhouse_<label>_*")
    parser.add_argument("--http-concurrency", type=int, default=20)
    parser.add_argument("--browser-concurrency", type=int, default=3)
    parser.add_argument("--max-per-host", type=int, default=3)


if __name__ == "__main__":
    run_cli(InhouseRunner, extra_args=_extra_args, build=_build)
