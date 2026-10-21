"""Fetch the sampled home URLs through Olostep ``POST /v1/scrapes``.

Request: ``formats=['html', 'markdown']``, ``links_on_page`` enabled
(``{}`` is enough; links come back as absolute URLs), ``max_age`` left at
its default of 0 (fresh scrape), and ``remove_css_selectors`` set to the
custom list ``["script", "style", "noscript"]`` (as a JSON array). The ask
was ``"none"`` so that nav/header/footer stay in (links comparable with
Firecrawl's ``onlyMainContent=false``), but ``"none"`` also leaves
``<style>`` and ``<script>`` bodies in the markdown (a 6.6k-character page
came back as 43k characters, mostly CSS, in the smoke test), which would
let junk pass the text gate. Olostep's ``"default"`` removes nav and
footer too, so the custom list is the setting that keeps the nav links and
drops the code.

Response fields used: ``result.markdown_content``, ``result.html_content``,
``result.links_on_page`` (absolute URLs), ``result.page_metadata.status_code``,
and the top-level ``credits_consumed`` / ``cost_usd`` (the API's own billing
figures; Olostep documents no balance endpoint, so the run meta has no
before/after balance).

Olostep answers a bad target with 400 (``dns_resolution_failed``,
``invalid_url``), 502 (``tls_error``) or 504 (``scrape_poll_timeout``,
about 55 s). Those describe the target and are recorded, not retried; 402
(exhausted key) stops the run. API-level attempts are capped at 2 and the
whole run at ``--request-budget`` calls (default 1,000) so a run can never
spend more credits than it was given.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_olostep --limit 3   # smoke
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_olostep
"""

import json

import httpx

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    FetchResult,
    StopRun,
    VendorRunner,
    get_api_key,
    run_cli,
)

# Target-side failures the API reports as its own HTTP status: terminal.
TERMINAL_API_STATUSES = {400, 502, 504}
# Selector list: drop code, keep nav/header/footer. Sent as a JSON array; the
# JSON-*stringified* form the docs describe was silently ignored (markdown
# came back byte-identical to "none"), and "default" drops nav links from
# links_on_page as well as timing out more often (probed 2026-10-21).
REMOVE_CSS_SELECTORS = ["script", "style", "noscript"]


class OlostepRunner(VendorRunner):
    """Olostep scrape, fresh (max_age 0), full page (no CSS removal)."""

    name = "olostep"
    base_url = "https://api.olostep.com/v1"
    default_concurrency = 10
    default_rpm = None
    timeout_seconds = 120.0
    max_attempts = 2

    def __init__(self, api_key: str, request_budget: int = 1000,
                 remove_css_selectors: str | list = REMOVE_CSS_SELECTORS):
        super().__init__(api_key)
        self.request_budget = request_budget
        self.requests_made = 0
        self.remove_css_selectors = remove_css_selectors

    async def fetch(self, client: httpx.AsyncClient, url: str) -> FetchResult:
        if self.requests_made >= self.request_budget:
            raise StopRun(f"request budget of {self.request_budget} API calls reached")
        self.requests_made += 1
        payload = {
            "url_to_scrape": url,
            "formats": ["html", "markdown"],
            "remove_css_selectors": self.remove_css_selectors,
            "links_on_page": {},
        }
        response = await client.post(f"{self.base_url}/scrapes", json=payload)
        result = FetchResult(vendor=self.name, url=url, api_status=response.status_code)
        if response.status_code in TERMINAL_API_STATUSES:
            result.error = _error_text(response)
            result.vendor_metadata = {"api_error": result.error[:120]}
            return result
        self.raise_for_api_status(response)
        try:
            body = response.json()
        except ValueError:
            result.error = f"non-JSON response: {response.text[:200]}"
            return result
        res = body.get("result") or {}
        page_meta = res.get("page_metadata") or {}
        result.markdown = res.get("markdown_content") or ""
        result.raw_html = res.get("html_content") or ""
        result.links = res.get("links_on_page") or []
        result.http_status = page_meta.get("status_code")
        result.credits = _as_float(body.get("credits_consumed"))
        result.cost_usd = _as_float(body.get("cost_usd"))
        result.vendor_metadata = {
            "scrape_id": body.get("id"),
            "page_title": (page_meta.get("title") or "")[:120],
            "page_metadata_keys": sorted(page_meta.keys()),
            "result_keys": sorted(res.keys()),
            "top_keys": sorted(body.keys()),
        }
        if body.get("error"):
            result.error = str(body["error"])[:500]
        result.success = not result.error and bool(result.markdown.strip() or result.raw_html.strip())
        if not result.success and not result.error:
            result.error = "empty content"
        return result

    async def run_context(self):
        return {"request_budget": self.request_budget, "max_attempts": self.max_attempts,
                "remove_css_selectors": self.remove_css_selectors}


def _error_text(response: httpx.Response) -> str:
    try:
        err = response.json().get("error") or {}
        if isinstance(err, dict):
            return f"{response.status_code} {err.get('code') or err.get('type') or ''}: {err.get('message') or err.get('detail') or ''}"[:500]
        return f"{response.status_code} {err}"[:500]
    except ValueError:
        return f"{response.status_code} {response.text[:200]}"


def _as_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _build(args):
    selectors = args.remove_css_selectors
    if isinstance(selectors, str) and selectors.strip().startswith("["):
        selectors = json.loads(selectors)
    return OlostepRunner(get_api_key(OlostepRunner.name), request_budget=args.request_budget,
                         remove_css_selectors=selectors)


def _extra_args(parser):
    parser.add_argument("--request-budget", type=int, default=1000,
                        help="hard cap on API calls (retries included) for this run")
    parser.add_argument("--remove-css-selectors", default=json.dumps(REMOVE_CSS_SELECTORS),
                        help='Olostep remove_css_selectors: "default", "none" or a JSON list of selectors '
                             '(sent as an array)')


if __name__ == "__main__":
    run_cli(OlostepRunner, extra_args=_extra_args, build=_build)
