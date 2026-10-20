"""Fetch the sampled home URLs through Firecrawl ``POST /v2/scrape``.

Request: ``formats=['markdown', 'rawHtml', 'links']``, ``maxAge=0`` (no
vendor cache), ``proxy='auto'`` (retries blocked fetches through their
stealth pool, billed at a higher credit rate when used),
``onlyMainContent=false``.

``onlyMainContent`` (Firecrawl default ``true``) drops header, nav and
footer from the markdown AND from the ``links`` array: probed on one Wix
site, the default returned 0 internal links and ``false`` returned 8
(about, team, projects, ...). Our subpage rules live on exactly those nav
links, so the main run uses ``false``; ``--only-main-content`` runs the
default under the name ``firecrawl_mainonly`` to measure how much nav text
pads the markdown.

Free plan limits (docs, 2026-10): 1,000 credits/month, 10 ``/scrape``
requests per minute, 2 concurrent browsers. The defaults below sit at
those limits, so the pages/second this runner achieves is the plan's
rate limit, not the vendor's throughput.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl --limit 3   # smoke
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl
"""

import httpx

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    FetchResult,
    VendorRunner,
    run_cli,
)

# Metadata keys worth keeping per page; the rest (title, og:*, ...) is noise here.
KEEP_METADATA = (
    "statusCode", "error", "proxyUsed", "creditsUsed", "cacheState", "cachedAt",
    "contentType", "numPages", "concurrencyLimited", "concurrencyQueueDurationMs",
    "scrapeId", "language",
)


class FirecrawlRunner(VendorRunner):
    """Firecrawl v2 scrape, vendor cache bypassed."""

    name = "firecrawl"
    base_url = "https://api.firecrawl.dev/v2"
    default_concurrency = 2
    default_rpm = 10
    timeout_seconds = 150.0

    def __init__(self, api_key: str, only_main_content: bool = False):
        super().__init__(api_key)
        self.only_main_content = only_main_content
        if only_main_content:
            self.name = "firecrawl_mainonly"

    async def fetch(self, client: httpx.AsyncClient, url: str) -> FetchResult:
        payload = {
            "url": url,
            "formats": ["markdown", "rawHtml", "links"],
            "maxAge": 0,
            "proxy": "auto",
            "onlyMainContent": self.only_main_content,
            "timeout": 60000,
        }
        response = await client.post(f"{self.base_url}/scrape", json=payload)
        self.raise_for_api_status(response)
        result = FetchResult(vendor=self.name, url=url, api_status=response.status_code)
        try:
            body = response.json()
        except ValueError:
            result.error = f"non-JSON response: {response.text[:200]}"
            return result
        data = body.get("data") or {}
        meta = data.get("metadata") or {}
        result.vendor_metadata = {k: meta[k] for k in KEEP_METADATA if k in meta}
        result.vendor_metadata["api_code"] = body.get("code")
        if body.get("warning"):
            result.vendor_metadata["warning"] = str(body["warning"])[:300]
        result.http_status = meta.get("statusCode")
        result.final_url = meta.get("url") or meta.get("sourceURL")
        result.markdown = data.get("markdown") or ""
        result.raw_html = data.get("rawHtml") or ""
        result.links = data.get("links") or []
        result.credits = meta.get("creditsUsed")
        if not body.get("success"):
            result.error = str(body.get("error") or meta.get("error") or f"api_status {response.status_code}")[:500]
        elif meta.get("error"):
            result.error = str(meta["error"])[:500]
        result.success = bool(body.get("success")) and bool(result.markdown.strip() or result.raw_html.strip())
        return result

    async def credits(self, client: httpx.AsyncClient):
        response = await client.get(f"{self.base_url}/team/credit-usage")
        response.raise_for_status()
        return response.json().get("data")


def _build(args):
    from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import get_api_key
    return FirecrawlRunner(get_api_key(FirecrawlRunner.name), only_main_content=args.only_main_content)


if __name__ == "__main__":
    run_cli(
        FirecrawlRunner,
        extra_args=lambda p: p.add_argument(
            "--only-main-content", action="store_true",
            help="Firecrawl's default (drops nav/header/footer from markdown and links); "
                 "results go to firecrawl_mainonly_*",
        ),
        build=_build,
    )
