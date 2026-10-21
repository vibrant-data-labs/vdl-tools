"""Fetch the sampled home URLs through Spider.cloud ``POST /scrape``.

Request: ``return_format=['markdown', 'raw']``, ``return_page_links=true``,
``request='smart'`` (HTTP first, Chrome only when the page needs it),
``cache=false``.

Spider is pay-as-you-go, metered on bandwidth ($1/GB) and CPU time, with
failed requests free (pricing page, 2026-10). Per-response ``costs`` are
recorded as the API reports them; the account balance is read from
``GET /data/credits`` before and after the run.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_spider --limit 3   # smoke
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_spider
"""

import httpx

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    FetchResult,
    StopRun,
    VendorRunner,
    run_cli,
)

LINK_KEYS = ("links", "page_links")


class SpiderRunner(VendorRunner):
    """Spider.cloud scrape, vendor cache disabled, smart request mode."""

    name = "spider"
    base_url = "https://api.spider.cloud"
    default_concurrency = 20
    default_rpm = None
    timeout_seconds = 120.0

    async def fetch(self, client: httpx.AsyncClient, url: str) -> FetchResult:
        payload = {
            "url": url,
            "return_format": ["markdown", "raw"],
            "return_page_links": True,
            "request": "smart",
            "cache": False,
            "metadata": True,
        }
        response = await client.post(f"{self.base_url}/scrape", json=payload)
        if response.status_code == 403:
            # Spider answers an exhausted balance with 403; nothing else in this
            # pilot can trigger a 403 from the API itself.
            raise StopRun(f"vendor API returned 403: {response.text[:300]}")
        self.raise_for_api_status(response)
        result = FetchResult(vendor=self.name, url=url, api_status=response.status_code)
        try:
            body = response.json()
        except ValueError:
            result.error = f"non-JSON response: {response.text[:200]}"
            return result
        # The scrape route answers with a list of page objects (one here).
        items = body if isinstance(body, list) else [body]
        if not items:
            result.error = "empty response list"
            return result
        item = items[0] if isinstance(items[0], dict) else {"content": items[0]}
        content = item.get("content")
        if isinstance(content, dict):
            result.markdown = content.get("markdown") or ""
            result.raw_html = content.get("raw") or content.get("html") or ""
        elif isinstance(content, str):
            # Only one format honored: tell HTML from markdown by its first char.
            if content.lstrip().startswith("<"):
                result.raw_html = content
            else:
                result.markdown = content
        links = next((item[k] for k in LINK_KEYS if isinstance(item.get(k), list)), [])
        result.links = links
        result.http_status = item.get("status")
        result.final_url = item.get("url")
        costs = item.get("costs") or {}
        result.cost_usd = _as_float(costs.get("total_cost"))
        meta = item.get("metadata") or {}
        result.vendor_metadata = {
            "keys": sorted(item.keys()),
            "costs": costs,
            "content_type": type(content).__name__,
            "content_keys": sorted(content.keys()) if isinstance(content, dict) else None,
            "provider": item.get("provider") or costs.get("vendor"),
            "metadata_keys": sorted(meta.keys()) if isinstance(meta, dict) else None,
        }
        error = item.get("error")
        if error:
            result.error = str(error)[:500]
        result.success = not error and bool(result.markdown.strip() or result.raw_html.strip())
        return result

    async def credits(self, client: httpx.AsyncClient):
        response = await client.get(f"{self.base_url}/data/credits")
        response.raise_for_status()
        return response.json()


def _as_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    run_cli(SpiderRunner)
