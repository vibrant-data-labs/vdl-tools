"""Firecrawl end to end over ``sample_orgs.parquet``: home page, the subpages
our rules pick from the vendor's links array, combined text, verdict.

Per page: ``POST /v2/scrape`` with ``formats=['markdown','rawHtml','links']``,
``onlyMainContent=false``, ``maxAge=0``, ``proxy='auto'`` (the phase-1
settings). Subpages come from ``filter_links`` over the links array
(``picks_array``); the HTML-derived set production would compute from
``rawHtml`` is recorded alongside (``picks_html``) for comparison. The
page text that feeds ``make_group_text`` is the markdown.

Concurrency defaults to the Standard plan's limits: 25 requests in flight,
500 per minute (``--max-concurrent``, ``--rpm``). A 402 stops the run;
429s and 5xx are retried with backoff and counted in the run meta.

``--fake`` swaps in a deterministic fake vendor (no network, no credits) so
the whole pipeline, the comparison and the hybrid simulation can be
dry-run; results go to ``fake_e2e_*``.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e --fake --limit 40
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e --dry-run
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e --limit 20   # smoke, ~100 credits
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e              # ~9k credits
"""

import asyncio
import hashlib
import random
from collections import Counter

import httpx

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    RateLimiter,
    StopRun,
    _fetch_with_retry,
    get_api_key,
)
from vdl_tools.scrape_enrich.scraper.vendor_pilot._e2e import (
    PageFetcher,
    PageResult,
    e2e_cli,
    picks_from_array,
    picks_from_html,
)
from vdl_tools.scrape_enrich.scraper.vendor_pilot.compare import markdown_to_plain
from vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl import FirecrawlRunner


class FirecrawlFetcher(PageFetcher):
    name = "firecrawl"
    default_org_concurrency = 25

    def __init__(self, api_key: str, max_concurrent: int = 25, rpm: float | None = 500,
                 credit_cap: float | None = 15000):
        self.runner = FirecrawlRunner(api_key, only_main_content=False)
        self.max_concurrent, self.rpm = max_concurrent, rpm
        self.credit_cap = credit_cap
        self.credits_used = 0.0   # per-page creditsUsed summed over this process
        self.sem = asyncio.Semaphore(max_concurrent)
        self.limiter = RateLimiter(rpm)
        self.client: httpx.AsyncClient | None = None
        self.retry_counter = Counter()

    async def start(self):
        self.client = httpx.AsyncClient(headers=self.runner.headers(), timeout=self.runner.timeout_seconds)

    async def stop(self):
        if self.client:
            await self.client.aclose()

    async def balance(self):
        return await self.runner.credits(self.client)

    async def run_context(self):
        return {"formats": ["markdown", "rawHtml", "links"], "onlyMainContent": False, "maxAge": 0,
                "proxy": "auto", "max_concurrent": self.max_concurrent, "rpm": self.rpm,
                "credit_cap": self.credit_cap,
                "link_source": "array (filter_anchors + filter_links over data.links); picks_html recorded from rawHtml"}

    async def fetch_page(self, url: str, subpath: str, page_type: str) -> PageResult:
        if self.credit_cap is not None and self.credits_used >= self.credit_cap:
            raise StopRun(f"credit cap of {self.credit_cap:g} reached ({self.credits_used:g} used this run)")
        async with self.sem:
            await self.limiter.acquire()
            fr = await _fetch_with_retry(self.runner, self.client, url, self.retry_counter)
        self.credits_used += float(fr.credits or 0)
        page = PageResult(
            url=url, subpath=subpath, page_type=page_type, success=fr.success,
            api_status=fr.api_status, http_status=fr.http_status, text=fr.markdown,
            raw_html_len=fr.raw_html_len, n_links=fr.n_links, credits=fr.credits,
            wall_seconds=fr.wall_seconds, attempts=fr.attempts, error=fr.error,
            mode=fr.vendor_metadata.get("proxyUsed"),
        )
        page._links, page._raw_html = fr.links, fr.raw_html  # transient, for pick_subpages
        return page.finalize(plain_text=markdown_to_plain(fr.markdown))

    def pick_subpages(self, home: PageResult, home_url: str):
        picks_array = picks_from_array(home_url, getattr(home, "_links", []))
        picks_html = picks_from_html(home_url, getattr(home, "_raw_html", ""))
        return picks_array, "array", picks_html, picks_array


VOCAB = ("climate finance nonprofit community grants energy solar wind resilience adaptation mitigation youth "
         "education housing health equity justice program partners impact report annual board staff volunteers "
         "donate events news mission vision history policy research data tools workshop coalition river forest "
         "farm food water carbon transit buildings efficiency storage grid rural urban county state federal "
         "foundation investment fund capital loan grant award fellowship initiative campaign story member").split()


class FakeFetcher(PageFetcher):
    """Deterministic stand-in: no network, no credits. Outcomes keyed on a
    hash of the URL so reruns and resumes agree."""

    name = "fake"
    default_org_concurrency = 25
    PATHS = ["/about-us", "/our-team", "/news", "/news/story-one", "/news/story-two",
             "/programs", "/programs/youth", "/contact", "/privacy-policy", "/donate"]

    def __init__(self):
        self.retry_counter = Counter()

    def _rng(self, url: str) -> random.Random:
        return random.Random(int(hashlib.md5(url.encode()).hexdigest()[:8], 16))

    async def run_context(self):
        return {"fake": True}

    async def fetch_page(self, url: str, subpath: str, page_type: str) -> PageResult:
        rng = self._rng(url)
        await asyncio.sleep(rng.uniform(0.005, 0.03))
        roll = rng.random()
        page = PageResult(url=url, subpath=subpath, page_type=page_type, api_status=200, mode="basic")
        if roll < 0.08:
            page.success, page.http_status, page.error = False, None, "DNS resolution failed (fake)"
        elif roll < 0.14:
            page.success, page.http_status, page.credits = True, 404, 1
            page.text = "# 404\n\nPage not found. [Home](" + url + ")"
        elif roll < 0.18:
            page.success, page.http_status, page.credits = True, 200, 1
            page.text = "Checking your browser before accessing the site. Cloudflare"
        else:
            page.success, page.http_status, page.credits = True, 200, 1
            # Varied vocabulary: make_group_text drops text whose most common
            # 2-3 character sequence covers 20% of it (a "word123 word456"
            # filler trips that check).
            words = rng.randint(120, 600)
            page.text = " ".join(rng.choice(VOCAB) for _ in range(words))
            if page_type == "index":
                links = [url.rstrip("/") + p for p in rng.sample(self.PATHS, rng.randint(3, 9))]
                links += ["https://facebook.com/x", "mailto:info@example.org"]
                page._links = links
                page._raw_html = "<html><body>" + "".join(f'<a href="{l}">x</a>' for l in links) + "</body></html>"
                page.n_links = len(links)
        page.raw_html_len = len(page.text) * 3
        page.wall_seconds = rng.uniform(0.5, 6.0)
        return page.finalize(plain_text=markdown_to_plain(page.text))

    def pick_subpages(self, home: PageResult, home_url: str):
        picks_array = picks_from_array(home_url, getattr(home, "_links", []))
        picks_html = picks_from_html(home_url, getattr(home, "_raw_html", ""))
        return picks_array, "array", picks_html, picks_array


def _factory(args):
    if args.fake:
        return FakeFetcher()
    return FirecrawlFetcher(get_api_key("firecrawl"), max_concurrent=args.max_concurrent, rpm=args.rpm or None,
                            credit_cap=args.credit_cap or None)


def _extra_args(parser):
    parser.add_argument("--fake", action="store_true", help="deterministic fake vendor: no network, no credits")
    parser.add_argument("--max-concurrent", type=int, default=25, help="requests in flight (Standard: 25 browsers)")
    parser.add_argument("--rpm", type=float, default=500, help="requests per minute (Standard: 500); 0 = unlimited")
    parser.add_argument("--credit-cap", type=float, default=15000,
                        help="stop the run once this many credits were billed in this process (0 = no cap)")


if __name__ == "__main__":
    e2e_cli(_factory, "firecrawl", 25, extra_args=_extra_args)
