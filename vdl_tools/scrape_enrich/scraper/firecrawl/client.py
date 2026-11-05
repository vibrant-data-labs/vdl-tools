"""Firecrawl ``POST /v2/scrape`` client for the scraping adapter (D4, VIB-56).

One client per process. It holds the account-wide limits (25 browsers and
500 requests per minute on the Standard plan), the retry policy, the credit
ceiling and the run bookkeeping: balance before and after, credits used,
retries absorbed, why the run stopped. The per-org pipeline (D7) calls
``scrape`` once per page. Everything else here exists so that one call
answers the question the cache needs: whose fault was it?

Error semantics (plan section 6). A page's ``failure_reason`` is one of

    dead          Firecrawl could not reach the site: DNS, SSL, every engine
                  failed, unsupported file (a ``SCRAPE_*`` code other than
                  ``SCRAPE_TIMEOUT``, at API status 500 or 200). Terminal.
    http_error    the site answered, but not 2xx (``metadata.statusCode``)
    blocked       the site refused us: 401, 403 or 429 from the target
    invalid_url   rejected before any request (blank, whitespace, no host,
                  no top-level domain) or by Firecrawl with a 400
    vendor_error  Firecrawl's problem: a 408 scrape timeout, a 429 or 5xx
                  that outlived the retries, a transport error, an answer
                  with no status code, or a page cut off by a stop

Only the first four count as errors (``ScrapeResult.counts_as_error``). The
climate caller runs with ``max_errors=1``, so a ``vendor_error`` written as
an error would be a permanent skip that routes the org to the paid
Coresignal fallback; it is stored with zero errors and the next run retries
it. The pilot saw 13 timeouts per 1,000 home pages.

A 402 (out of credits) or 401 (bad key) from the API stops the run:
``scrape`` raises ``RunStopped`` for that page and for every page not yet
sent; pages already in flight finish and are kept. Reaching the credit
ceiling stops the run the same way, after the in-flight pages land.

Request profile (Q2, decided 2026-11-04): ``formats [markdown, links]`` and
``onlyMainContent: false`` on every page, home and subpages alike.
Firecrawl's default is ``true``, which also drops the navigation links the
subpage rules read, so it is sent explicitly and asserted in the tests.

Config, ``[scraping_vendors]`` in config.ini (``FirecrawlClient.from_config``)::

    firecrawl_api_key = ...        required; never logged, printed or repr'd
    firecrawl_concurrency = 25     optional: the plan's browser limit
    firecrawl_rpm = 500            optional: the plan's /scrape limit

Dry run from the command line, against today's scrape cache, no credits::

    python -m vdl_tools.scrape_enrich.scraper.firecrawl.client --dry-run --urls urls.txt --limit 20
"""

from __future__ import annotations

import argparse
import asyncio
import re
import time
from collections import Counter
from collections.abc import Callable, Collection, Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx

from vdl_tools.scrape_enrich.scraper.firecrawl.site_links import normalize_host
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger

BASE_URL = "https://api.firecrawl.dev/v2"
CONFIG_SECTION = "scraping_vendors"
DEFAULT_CONCURRENCY = 25        # Standard plan: concurrent browsers
DEFAULT_RPM = 500               # Standard plan: /scrape requests per minute
REQUEST_TIMEOUT_MS = 60_000     # Firecrawl's per-page cap; its queue time counts against it
HTTP_TIMEOUT_SECONDS = 150.0    # our socket timeout: the vendor's 60 s plus queueing
MAX_ATTEMPTS = 4                # API-level attempts per page (429, 5xx, transport)
MAX_RETRY_DELAY = 120.0
CREDITS_PER_PAGE = 1.0          # one credit per fetched page; failures bill nothing

# What every page is asked for. ``onlyMainContent`` must stay False (Q2).
REQUEST_PROFILE = {
    "formats": ["markdown", "links"],
    "onlyMainContent": False,
    "maxAge": 0,            # never the vendor's cache
    "proxy": "auto",        # retries a blocked fetch through the stealth pool
    "timeout": REQUEST_TIMEOUT_MS,
}

DEAD = "dead"
HTTP_ERROR = "http_error"
BLOCKED = "blocked"
INVALID_URL = "invalid_url"
VENDOR_ERROR = "vendor_error"
TARGET_FAILURES = frozenset({DEAD, HTTP_ERROR, BLOCKED, INVALID_URL})

BLOCKED_STATUSES = frozenset({401, 403, 429})       # the target's own status
RETRYABLE_API_STATUSES = frozenset({429, 500, 502, 503, 504})
STOP_API_STATUSES = {402: "payment_required", 401: "unauthorized"}
TIMEOUT_CODE = "SCRAPE_TIMEOUT"

# Metadata keys kept on the result; title, description, og:* and the rest are noise here.
KEEP_METADATA = (
    "statusCode", "url", "sourceURL", "error", "proxyUsed", "creditsUsed", "cacheState",
    "contentType", "numPages", "concurrencyLimited", "concurrencyQueueDurationMs",
    "scrapeId", "language",
)

# Firecrawl's 429 carries no Retry-After header; the body says "please retry after 17s".
_RETRY_AFTER_IN_BODY = re.compile(r"retry after (\d+(?:\.\d+)?)\s*s", re.IGNORECASE)
_HOST_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", re.IGNORECASE)
_OTHER_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*://")


class RunStopped(Exception):
    """The run must not send another request. ``kind`` is ``payment_required``
    (402), ``unauthorized`` (401) or ``credit_ceiling``."""

    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind
        self.reason = reason


class _Retry(Exception):
    def __init__(self, status: int | None, delay: float, message: str):
        super().__init__(message)
        self.status = status
        self.delay = delay


def _excerpt(text: str | None, limit: int = 300) -> str:
    return " ".join((text or "").split())[:limit]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_url(url: str | None) -> str | None:
    """The URL as it is sent, or None when it cannot be sent.

    ``https://`` is added when the scheme is missing and the scheme is
    lowercased, the same rule as the in-house ``ensure_url_scheme``.
    None for a blank value, whitespace inside the URL, no host, a host
    label that is not a hostname, or a top-level domain that is not two or
    more letters: Firecrawl answers those with a 400, so no request is made.
    """
    value = url.strip() if isinstance(url, str) else ""
    if not value or any(ch.isspace() for ch in value):
        return None
    lowered = value.lower()
    if lowered.startswith(("http://", "https://")):
        n = lowered.index("://") + 3
        value = value[:n].lower() + value[n:]
    elif _OTHER_SCHEME.match(lowered):
        return None                       # ftp://, file://, ...
    else:
        value = f"https://{value}"
    try:
        parts = urlsplit(value)
        host = parts.hostname or ""
        if parts.username is not None:    # credentials in a website field: mailto:info@x.org, user@host
            return None
    except ValueError:
        return None
    labels = host.split(".")
    if len(labels) < 2 or not all(_HOST_LABEL.match(label) for label in labels):
        return None
    if not (labels[-1].isalpha() and len(labels[-1]) >= 2):
        return None
    return value


@dataclass
class ScrapeResult:
    """One page as Firecrawl answered it, classified."""

    url: str                            # as sent (normalized)
    ok: bool = False                    # Firecrawl succeeded and the target answered 2xx
    failure_reason: str | None = None   # None or one of the five reasons above
    error: str | None = None
    status_code: int | None = None      # metadata.statusCode: the target's own answer
    api_status: int | None = None       # the vendor API's HTTP status
    api_code: str | None = None         # body.code: SCRAPE_TIMEOUT, SCRAPE_DNS_RESOLUTION_ERROR, ...
    source_url: str | None = None       # metadata.sourceURL: the URL Firecrawl was asked for
    final_url: str | None = None        # metadata.url: where the page was served from, after redirects
    final_host: str = ""                # normalize_host(final_url); what same_site_paths compares against
    markdown: str = ""                  # kept on non-2xx pages too
    links: list[str] = field(default_factory=list)
    credits_used: float = 0.0
    proxy_used: str | None = None
    content_type: str | None = None
    language: str | None = None
    scrape_id: str | None = None
    concurrency_limited: bool | None = None
    concurrency_queue_ms: int | None = None
    metadata: dict = field(default_factory=dict)   # the KEEP_METADATA subset, as returned
    attempts: int = 0                   # API calls made for this page
    wall_seconds: float = 0.0
    fetched_at: str = ""

    @property
    def counts_as_error(self) -> bool:
        """Whether this failure increments ``num_errors``: target failures do, vendor errors do not."""
        return self.failure_reason in TARGET_FAILURES

    @property
    def host_changed(self) -> bool:
        """The page was served from another host than the one asked for (11% of home pages in the pilot)."""
        asked = normalize_host(self.source_url or self.url)
        return bool(self.final_host) and bool(asked) and self.final_host != asked

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class RunStats:
    """Bookkeeping for one client's lifetime; the source of the ``scrape_runs`` row."""

    started_at: str | None = None
    ended_at: str | None = None
    balance_before: dict | None = None
    balance_after: dict | None = None
    credits_used: float = 0.0
    n_pages: int = 0                    # results returned to the caller
    n_ok: int = 0
    n_requests: int = 0                 # API calls sent, retries included
    retries: Counter = field(default_factory=Counter)    # by API status, "transport" for socket errors
    failures: Counter = field(default_factory=Counter)   # by failure_reason
    stop_reason: str | None = None

    def record(self, result: ScrapeResult) -> None:
        self.n_pages += 1
        self.n_ok += int(result.ok)
        self.credits_used += float(result.credits_used or 0)
        if result.failure_reason:
            self.failures[result.failure_reason] += 1

    def as_dict(self) -> dict:
        data = asdict(self)
        data["retries"], data["failures"] = dict(self.retries), dict(self.failures)
        return data


@dataclass
class DryRunReport:
    """What a run would fetch and cost, computed without fetching."""

    n_urls: int
    n_cached: int
    n_invalid: int
    misses: list[str]
    invalid: list[str]
    credits_per_miss: float
    estimated_credits: float
    credit_ceiling: float | None
    within_ceiling: bool | None
    balance: dict | None
    remaining_credits: float | None
    within_balance: bool | None

    @property
    def n_misses(self) -> int:
        return len(self.misses)

    def summary(self) -> str:
        lines = [
            f"{self.n_urls} URLs: {self.n_cached} cached, {self.n_misses} to fetch, {self.n_invalid} invalid",
            f"estimated credits: {self.estimated_credits:g} ({self.credits_per_miss:g} per miss)",
        ]
        if self.credit_ceiling is not None:
            lines.append(f"credit ceiling: {self.credit_ceiling:g} ({'within' if self.within_ceiling else 'EXCEEDED'})")
        if self.remaining_credits is not None:
            lines.append(f"balance: {self.remaining_credits:g} credits remaining "
                         f"({'enough' if self.within_balance else 'NOT ENOUGH'})")
        elif self.balance is not None:
            lines.append(f"balance: unavailable ({_excerpt(str(self.balance.get('error')), 120)})")
        return "\n".join(lines)


def _retry_delay(response: httpx.Response, body_error: str | None, attempt: int) -> float:
    header = (response.headers.get("Retry-After") or "").strip()
    if header.replace(".", "", 1).isdigit():
        return min(float(header), MAX_RETRY_DELAY)
    match = _RETRY_AFTER_IN_BODY.search(body_error or "")
    if match:
        return min(float(match.group(1)) + 1.0, MAX_RETRY_DELAY)   # one second past the reset
    return min(60.0, 2.0 ** attempt)


def _fail(result: ScrapeResult, reason: str, error: str | None) -> ScrapeResult:
    result.ok = False
    result.failure_reason = reason
    result.error = _excerpt(error, 500) or reason
    return result


def parse_response(url: str, response: httpx.Response, attempt: int = 1) -> ScrapeResult:
    """Classify one API answer. Raises ``_Retry`` for a 429 or a 5xx without a
    ``SCRAPE_*`` code, ``RunStopped`` for 401 and 402."""
    status = response.status_code
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = None
    code = (body or {}).get("code") or None
    data = (body or {}).get("data") or {}
    meta = data.get("metadata") or {} if isinstance(data, dict) else {}
    body_error = (body or {}).get("error") or meta.get("error")
    body_error = str(body_error) if body_error else None

    result = ScrapeResult(url=url, api_status=status, api_code=code)
    result.metadata = {k: meta[k] for k in KEEP_METADATA if k in meta}
    result.status_code = int(meta["statusCode"]) if meta.get("statusCode") is not None else None
    result.source_url = meta.get("sourceURL") or None
    result.final_url = meta.get("url") or None
    result.final_host = normalize_host(result.final_url) if result.final_url else ""
    result.credits_used = float(meta.get("creditsUsed") or 0)
    result.proxy_used = meta.get("proxyUsed")
    result.content_type = meta.get("contentType")
    result.language = meta.get("language")
    result.scrape_id = meta.get("scrapeId")
    result.concurrency_limited = meta.get("concurrencyLimited")
    result.concurrency_queue_ms = meta.get("concurrencyQueueDurationMs")
    if isinstance(data, dict):
        result.markdown = data.get("markdown") or ""
        result.links = [x for x in (data.get("links") or []) if isinstance(x, str)]

    if status in STOP_API_STATUSES:
        raise RunStopped(STOP_API_STATUSES[status],
                         f"Firecrawl answered {status}: {_excerpt(body_error or response.text)}")
    if code == TIMEOUT_CODE or status == 408:
        return _fail(result, VENDOR_ERROR, body_error or "scrape timed out (408)")
    if code and code.startswith("SCRAPE_"):
        return _fail(result, DEAD, body_error or code)
    if status == 400:
        return _fail(result, INVALID_URL, body_error or "Firecrawl rejected the URL (400)")
    if status in RETRYABLE_API_STATUSES:
        raise _Retry(status, _retry_delay(response, body_error, attempt),
                     f"Firecrawl answered {status}: {_excerpt(body_error or response.text)}")
    if status != 200 or body is None:
        return _fail(result, VENDOR_ERROR, f"unexpected answer {status}: {_excerpt(body_error or response.text)}")
    if not body.get("success"):
        return _fail(result, VENDOR_ERROR, body_error or f"success=false (code {code})")
    if result.status_code is None:
        return _fail(result, VENDOR_ERROR, "no statusCode in metadata")
    if 200 <= result.status_code < 300:
        result.ok = True
        return result
    if result.status_code in BLOCKED_STATUSES:
        return _fail(result, BLOCKED, body_error or f"target answered {result.status_code}")
    return _fail(result, HTTP_ERROR, body_error or f"target answered {result.status_code}")


class AccountLimits:
    """The account-wide semaphore and rpm limiter.

    Firecrawl counts browsers and requests per account, not per client, so
    every client in a process shares one instance (the default client builds
    its own; pass the same ``limits`` to share). The semaphore is bound to
    the running event loop on first use and rebuilt for a new loop.
    """

    def __init__(self, concurrency: int = DEFAULT_CONCURRENCY, rpm: float | None = DEFAULT_RPM):
        self.concurrency = max(1, int(concurrency))
        self.rpm = rpm or None
        self._interval = 60.0 / rpm if rpm else 0.0
        self._next_slot = 0.0
        self._loop = None
        self._semaphore: asyncio.Semaphore | None = None

    def semaphore(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        if self._semaphore is None or self._loop is not loop:
            self._loop, self._semaphore = loop, asyncio.Semaphore(self.concurrency)
        return self._semaphore

    async def wait_for_slot(self, sleep=asyncio.sleep) -> None:
        """Space requests ``60 / rpm`` seconds apart across every task.
        No await between reading and advancing the slot, so no lock is needed."""
        if not self._interval:
            return
        now = time.monotonic()
        wait = self._next_slot - now
        self._next_slot = max(now, self._next_slot) + self._interval
        if wait > 0:
            await sleep(wait)


def vendor_config() -> dict:
    """The ``[scraping_vendors]`` section of config.ini as a plain dict (empty when absent)."""
    config = get_configuration()
    if CONFIG_SECTION not in config:
        return {}
    return {k: v.strip() for k, v in config[CONFIG_SECTION].items()}


class FirecrawlClient:
    """``POST /v2/scrape`` under the account limits, with the error semantics above.

    Use as an async context manager: entering opens the HTTP client and
    reads the balance, leaving reads it again and closes. ``transport`` and
    ``sleep`` are for the tests (an ``httpx.MockTransport`` and a recorder
    in place of ``asyncio.sleep``).
    """

    def __init__(
        self,
        api_key: str,
        *,
        concurrency: int = DEFAULT_CONCURRENCY,
        rpm: float | None = DEFAULT_RPM,
        credit_ceiling: float | None = None,
        limits: AccountLimits | None = None,
        payload_overrides: dict | None = None,
        base_url: str = BASE_URL,
        max_attempts: int = MAX_ATTEMPTS,
        http_timeout: float = HTTP_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep=asyncio.sleep,
    ):
        if not api_key:
            raise ValueError(f"FirecrawlClient needs an API key ([{CONFIG_SECTION}] firecrawl_api_key)")
        self._api_key = api_key
        self.limits = limits or AccountLimits(concurrency, rpm)
        self.credit_ceiling = credit_ceiling
        self.payload_overrides = dict(payload_overrides or {})
        self.base_url = base_url.rstrip("/")
        self.max_attempts = max(1, int(max_attempts))
        self.http_timeout = http_timeout
        self._transport = transport
        self._sleep = sleep
        self._http: httpx.AsyncClient | None = None
        self._stop: RunStopped | None = None
        self.stats = RunStats()

    @classmethod
    def from_config(cls, **overrides) -> "FirecrawlClient":
        """Key, concurrency and rpm from ``[scraping_vendors]``; keyword overrides win."""
        config = vendor_config()
        api_key = config.get("firecrawl_api_key", "")
        if not api_key:
            raise RuntimeError(
                f"No Firecrawl API key: add `firecrawl_api_key = ...` under [{CONFIG_SECTION}] "
                "in config.ini (VDL_GLOBAL_CONFIG_PATH or ./config.ini)."
            )
        settings = {
            "concurrency": int(config.get("firecrawl_concurrency") or DEFAULT_CONCURRENCY),
            "rpm": float(config.get("firecrawl_rpm") or DEFAULT_RPM),
        }
        settings.update(overrides)
        return cls(api_key, **settings)

    def __repr__(self) -> str:
        return (f"FirecrawlClient(concurrency={self.limits.concurrency}, rpm={self.limits.rpm}, "
                f"credit_ceiling={self.credit_ceiling}, credits_used={self.stats.credits_used:g})")

    @property
    def request_profile(self) -> dict:
        """What every page is sent, minus ``url``: the ``scrape_runs.request_profile`` value."""
        return {**REQUEST_PROFILE, **self.payload_overrides}

    @property
    def stop_reason(self) -> str | None:
        return self._stop.reason if self._stop else None

    @property
    def credits_used(self) -> float:
        return self.stats.credits_used

    # -- lifecycle -----------------------------------------------------------

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.base_url,
                headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
                timeout=self.http_timeout,
                transport=self._transport,
            )
        return self._http

    async def __aenter__(self) -> "FirecrawlClient":
        self._client()
        self.stats.started_at = _now()
        self.stats.balance_before = await self.balance()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._http is None:
            return
        try:
            # Before closing: the pilot read it after and recorded "client has been closed".
            self.stats.balance_after = await self.balance()
            self.stats.ended_at = _now()
        finally:
            http, self._http = self._http, None
            await http.aclose()

    # -- balance -------------------------------------------------------------

    async def balance(self) -> dict | None:
        """``GET /team/credit-usage`` as Firecrawl reports it (``remainingCredits``,
        ``planCredits``, the billing period). Costs no credits. ``{"error": ...}``
        when the call fails: the balance is bookkeeping, never a reason to fail a run."""
        try:
            response = await self._client().get("/team/credit-usage")
            response.raise_for_status()
            return (response.json() or {}).get("data")
        except Exception as exc:  # noqa: BLE001
            logger.warning("firecrawl: could not read the credit balance: %s", _excerpt(str(exc), 200))
            return {"error": _excerpt(str(exc), 200)}

    @staticmethod
    def remaining_credits(balance: dict | None) -> float | None:
        try:
            return float(balance["remainingCredits"])
        except (TypeError, KeyError, ValueError):
            return None

    # -- scraping ------------------------------------------------------------

    def _check_stop(self) -> None:
        if self._stop is None and self.credit_ceiling is not None and self.stats.credits_used >= self.credit_ceiling:
            self._set_stop(RunStopped("credit_ceiling",
                                      f"credit ceiling of {self.credit_ceiling:g} reached "
                                      f"({self.stats.credits_used:g} credits used this run)"))
        if self._stop is not None:
            raise RunStopped(self._stop.kind, self._stop.reason)

    def _set_stop(self, stop: RunStopped) -> None:
        if self._stop is None:
            self._stop = stop
            self.stats.stop_reason = stop.reason
            logger.error("firecrawl: run stopped (%s): %s", stop.kind, stop.reason)

    async def scrape(self, url: str) -> ScrapeResult:
        """One page. Never raises for the page's own failure; raises ``RunStopped``
        when the run may not send another request (402, 401, the credit ceiling).

        A page waits for a browser slot (the semaphore, held through its
        retries and their backoff) and then for an rpm slot per attempt. In-flight
        pages may overshoot the ceiling by up to ``concurrency`` credits.
        """
        t0 = time.monotonic()
        sent = normalize_url(url)
        if sent is None:
            result = ScrapeResult(url=url if isinstance(url, str) else "", attempts=0)
            _fail(result, INVALID_URL, "blank or malformed URL; not sent")
            return self._finish(result, t0)
        self._check_stop()
        async with self.limits.semaphore():
            self._check_stop()   # stopped while waiting for a slot: send nothing
            result = await self._scrape_with_retry(sent)
        return self._finish(result, t0)

    def _finish(self, result: ScrapeResult, t0: float) -> ScrapeResult:
        result.wall_seconds = round(time.monotonic() - t0, 3)
        result.fetched_at = _now()
        self.stats.record(result)
        return result

    async def _scrape_with_retry(self, url: str) -> ScrapeResult:
        payload = {"url": url, **self.request_profile}
        last: _Retry | None = None
        for attempt in range(1, self.max_attempts + 1):
            await self.limits.wait_for_slot(self._sleep)
            self.stats.n_requests += 1
            try:
                response = await self._client().post("/scrape", json=payload)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = _Retry(None, min(30.0, 2.0 ** attempt), f"{type(exc).__name__}: {_excerpt(str(exc), 200)}")
            else:
                try:
                    result = parse_response(url, response, attempt)
                except _Retry as exc:
                    last = exc
                except RunStopped as exc:
                    self._set_stop(exc)
                    raise
                else:
                    result.attempts = attempt
                    return result
            self.stats.retries[str(last.status or "transport")] += 1
            if attempt == self.max_attempts:
                break
            logger.warning("firecrawl: %s: %s; retry %s/%s in %.0fs", url, last, attempt, self.max_attempts, last.delay)
            await self._sleep(last.delay)
        result = ScrapeResult(url=url, api_status=last.status if last else None, attempts=self.max_attempts)
        return _fail(result, VENDOR_ERROR, f"gave up after {self.max_attempts} attempts: {last}")

    async def scrape_many(self, urls: Iterable[str]) -> dict[str, ScrapeResult | RunStopped]:
        """Every URL concurrently under the limits; a page cut off by a stop maps to
        its ``RunStopped`` instead of a result, so the caller stores nothing for it."""
        urls = list(urls)

        async def one(url):
            try:
                return await self.scrape(url)
            except RunStopped as exc:
                return exc

        results = await asyncio.gather(*(one(u) for u in urls))
        return dict(zip(urls, results))

    # -- dry run -------------------------------------------------------------

    async def dry_run(
        self,
        urls: Iterable[str],
        is_cached: Callable[[str], bool] | Collection[str] | None = None,
        credits_per_miss: float = CREDITS_PER_PAGE,
    ) -> DryRunReport:
        """What a run over ``urls`` would fetch and cost, without fetching.

        Counts the cache misses, the credits they would bill at
        ``credits_per_miss`` each (1 for a home page alone; the pilot measured
        4.7 per org with its subpages), whether that fits the ceiling, and the
        balance, read once with ``GET /team/credit-usage`` (no credits).
        ``is_cached`` is a predicate on the normalized URL, or a collection of
        cached URLs; None means nothing is cached. Duplicates count once, with
        and without a trailing slash, as they do under the cache key.
        """
        if is_cached is None:
            cached = lambda _url: False  # noqa: E731
        elif callable(is_cached):
            cached = is_cached
        else:
            members = set(is_cached)
            cached = lambda url: url in members  # noqa: E731
        seen, misses, invalid, n_cached = set(), [], [], 0
        for raw in urls:
            sent = normalize_url(raw)
            if sent is None:
                invalid.append(raw if isinstance(raw, str) else str(raw))
                continue
            key = sent.rstrip("/")
            if key in seen:
                continue
            seen.add(key)
            if cached(sent):
                n_cached += 1
            else:
                misses.append(sent)
        estimated = len(misses) * float(credits_per_miss)
        balance = await self.balance()
        remaining = self.remaining_credits(balance)
        return DryRunReport(
            n_urls=len(seen) + len(invalid), n_cached=n_cached, n_invalid=len(invalid),
            misses=misses, invalid=invalid,
            credits_per_miss=float(credits_per_miss), estimated_credits=estimated,
            credit_ceiling=self.credit_ceiling,
            within_ceiling=None if self.credit_ceiling is None else estimated <= self.credit_ceiling,
            balance=balance, remaining_credits=remaining,
            within_balance=None if remaining is None else estimated <= remaining,
        )


# ---------------------------------------------------------------------------
# Command line: dry run against today's cache
# ---------------------------------------------------------------------------

def _cached_in_web_pages_scraped(urls: list[str]) -> set[str]:
    """The URLs whose home page is in today's ``web_pages_scraped`` (any index row).
    D7 replaces this with the v2 lookup, which also knows the row's age."""
    from vdl_tools.scrape_enrich.url_keys import extract_website_name
    from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesScraped
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    keys = {url: extract_website_name(url) for url in urls}
    with get_session() as session:
        rows = session.query(WebPagesScraped.cleaned_key).filter(
            WebPagesScraped.cleaned_key.in_([k for k in keys.values() if k]),
            WebPagesScraped.page_type == "PageType.INDEX",
        ).all()
    present = {row.cleaned_key for row in rows}
    return {url for url, key in keys.items() if key in present}


def _read_urls(path: str, column: str) -> list[str]:
    if path.endswith(".parquet"):
        import pandas as pd
        return pd.read_parquet(path, columns=[column])[column].dropna().astype(str).tolist()
    with open(path) as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Firecrawl client: dry run (no credits are spent).")
    parser.add_argument("--dry-run", action="store_true", required=True,
                        help="report cache misses, estimated credits and the balance; fetch nothing")
    parser.add_argument("--urls", required=True, help="a text file (one URL per line) or a parquet")
    parser.add_argument("--column", default="home_url", help="URL column when --urls is a parquet")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--credit-ceiling", type=float, default=None)
    parser.add_argument("--credits-per-miss", type=float, default=CREDITS_PER_PAGE,
                        help="1 for home pages alone; 4.7 is the pilot's per-org figure with subpages")
    parser.add_argument("--assume-uncached", action="store_true",
                        help="skip the cache lookup and count every URL as a miss")
    args = parser.parse_args(argv)

    urls = _read_urls(args.urls, args.column)
    if args.limit:
        urls = urls[: args.limit]
    sendable = [u for u in (normalize_url(x) for x in urls) if u]
    cached = set() if args.assume_uncached else _cached_in_web_pages_scraped(sendable)

    async def go():
        async with FirecrawlClient.from_config(credit_ceiling=args.credit_ceiling) as client:
            return await client.dry_run(urls, is_cached=cached, credits_per_miss=args.credits_per_miss)

    report = asyncio.run(go())
    print(report.summary())
    for url in report.misses[:20]:
        print("  miss:", url)
    for url in report.invalid[:20]:
        print("  invalid:", url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
