"""Tests for the Firecrawl client (D4, VIB-56) on an httpx mock transport.

No network, no credits. ``FakeFirecrawl`` scripts the vendor's answers per
URL (a list of responses or exceptions, the last one repeating) and logs
every request so the tests can assert what was sent, how many times, and
in what order. ``sleep`` is replaced by a recorder, so retry backoff and the
rpm spacing are asserted without waiting.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from vdl_tools.scrape_enrich.scraper.firecrawl import client as fc
from vdl_tools.scrape_enrich.scraper.firecrawl.client import (
    BLOCKED,
    DEAD,
    HTTP_ERROR,
    INVALID_URL,
    VENDOR_ERROR,
    AccountLimits,
    FirecrawlClient,
    RunStopped,
    normalize_url,
)

KEY = "fc-test-key-do-not-print"
HOME = "https://www.example.org"


def page(status=200, *, url=None, source=None, markdown="# Example\n\nWe restore rivers.",
         links=("https://www.example.org/about", "https://www.example.org/team"),
         credits=1, proxy="basic", error=None, extra_meta=None, success=True):
    """A successful API answer (HTTP 200) whose target answered ``status``."""
    meta = {"statusCode": status, "url": url or HOME + "/", "sourceURL": source or HOME,
            "creditsUsed": credits, "proxyUsed": proxy, "contentType": "text/html; charset=utf-8",
            "scrapeId": "01a15ae7-9161-700c-b37c-2b33b395cae2", "language": "en",
            "concurrencyLimited": False, "title": "Example", "ogImage": "x.png"}
    if error:
        meta["error"] = error
    meta.update(extra_meta or {})
    return httpx.Response(200, json={"success": success, "data": {
        "markdown": markdown, "links": list(links), "metadata": meta}})


def api_error(status, *, code=None, error="boom", headers=None, json_body=True):
    body = {"success": False, "error": error}
    if code:
        body["code"] = code
    if not json_body:
        return httpx.Response(status, text=error, headers=headers)
    return httpx.Response(status, json=body, headers=headers)


class FakeFirecrawl:
    def __init__(self, remaining=1000):
        self.remaining = remaining
        self.scripts: dict[str, list] = {}
        self.requests: list[dict] = []
        self.headers: list[dict] = []
        self.balance_calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self.delay = 0.0

    def script(self, url, *answers):
        self.scripts[url] = list(answers)
        return self

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/team/credit-usage"):
            self.balance_calls += 1
            if self.remaining is None:
                return httpx.Response(500, json={"success": False, "error": "no balance"})
            return httpx.Response(200, json={"success": True, "data": {
                "remainingCredits": self.remaining, "planCredits": 100000,
                "billingPeriodStart": "2026-10-21T22:07:32.000Z", "billingPeriodEnd": "2026-11-21T22:07:32.000Z"}})
        assert request.url.path.endswith("/scrape"), request.url
        payload = json.loads(request.content)
        self.requests.append(payload)
        self.headers.append(dict(request.headers))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            queue = self.scripts.get(payload["url"]) or [page()]
            answer = queue.pop(0) if len(queue) > 1 else queue[0]
            if isinstance(answer, Exception):
                raise answer
            return answer
        finally:
            self.in_flight -= 1


class SleepRecorder:
    def __init__(self):
        self.delays: list[float] = []

    async def __call__(self, seconds):
        self.delays.append(round(seconds, 3))
        await asyncio.sleep(0)


def make_client(vendor: FakeFirecrawl, **kw) -> tuple[FirecrawlClient, SleepRecorder]:
    sleep = SleepRecorder()
    kw.setdefault("concurrency", 5)
    kw.setdefault("rpm", None)
    client = FirecrawlClient(KEY, transport=httpx.MockTransport(vendor.handler), sleep=sleep, **kw)
    return client, sleep


def run(coro):
    return asyncio.run(coro)


async def scrape_one(vendor, url=HOME, **kw):
    client, sleep = make_client(vendor, **kw)
    async with client:
        result = await client.scrape(url)
    return result, client, sleep


# ---------------------------------------------------------------------------
# Request profile (Q2)
# ---------------------------------------------------------------------------

def test_request_profile_is_full_page_markdown_and_links_on_every_page():
    vendor = FakeFirecrawl()
    client, _ = make_client(vendor)

    async def go():
        async with client:
            await client.scrape(HOME)
            await client.scrape(HOME + "/about")   # a subpage gets the same profile (Q2)

    run(go())
    assert len(vendor.requests) == 2
    for sent in vendor.requests:
        assert sent["formats"] == ["markdown", "links"]
        assert "onlyMainContent" in sent and sent["onlyMainContent"] is False   # Firecrawl's default is True
        assert sent["maxAge"] == 0 and sent["proxy"] == "auto" and sent["timeout"] == 60000
        assert "rawHtml" not in sent["formats"]
    assert vendor.requests[1]["url"] == HOME + "/about"
    assert client.request_profile == {k: v for k, v in vendor.requests[0].items() if k != "url"}


def test_payload_overrides_merge_into_the_profile():
    vendor = FakeFirecrawl()
    result, client, _ = run(scrape_one(vendor, payload_overrides={"waitFor": 500, "formats": ["markdown"]}))
    sent = vendor.requests[0]
    assert sent["waitFor"] == 500 and sent["formats"] == ["markdown"]
    assert sent["onlyMainContent"] is False and sent["maxAge"] == 0
    assert client.request_profile["waitFor"] == 500


def test_bearer_header_is_sent_and_the_key_never_appears_in_repr_or_errors():
    vendor = FakeFirecrawl().script(HOME, api_error(401, error="Unauthorized: Invalid token"))
    client, _ = make_client(vendor)

    async def go():
        async with client:
            with pytest.raises(RunStopped) as info:
                await client.scrape(HOME)
            return info.value

    stop = run(go())
    assert vendor.headers[0]["authorization"] == f"Bearer {KEY}"
    assert KEY not in repr(client) and KEY not in str(stop) and KEY not in str(client.stats.as_dict())


# ---------------------------------------------------------------------------
# Metadata capture and redirects
# ---------------------------------------------------------------------------

def test_success_captures_target_status_urls_text_links_and_credits():
    vendor = FakeFirecrawl().script(HOME, page(extra_meta={"concurrencyLimited": True, "concurrencyQueueDurationMs": 1200}))
    result, client, _ = run(scrape_one(vendor))
    assert result.ok and result.failure_reason is None and not result.counts_as_error
    assert result.api_status == 200 and result.status_code == 200
    assert result.source_url == HOME and result.final_url == HOME + "/" and result.final_host == "example.org"
    assert not result.host_changed
    assert result.markdown.startswith("# Example") and result.links == [HOME + "/about", HOME + "/team"]
    assert result.credits_used == 1 and result.proxy_used == "basic" and result.language == "en"
    assert result.scrape_id and result.content_type.startswith("text/html")
    assert result.concurrency_limited is True and result.concurrency_queue_ms == 1200
    assert result.attempts == 1 and result.fetched_at and result.wall_seconds >= 0
    assert "title" not in result.metadata and "ogImage" not in result.metadata   # noise dropped
    assert result.metadata["url"] == HOME + "/" and result.metadata["sourceURL"] == HOME
    assert client.stats.n_pages == 1 and client.stats.n_ok == 1 and client.credits_used == 1


def test_redirect_to_another_host_is_captured_from_metadata():
    asked = "http://www.thepurplecarrot.com"
    vendor = FakeFirecrawl().script(asked, page(url="https://www.purplecarrot.com/", source=asked))
    result, _, _ = run(scrape_one(vendor, url=asked))
    assert result.ok
    assert result.url == asked and result.source_url == asked
    assert result.final_url == "https://www.purplecarrot.com/" and result.final_host == "purplecarrot.com"
    assert result.host_changed


def test_redirect_within_the_host_is_not_a_host_change():
    vendor = FakeFirecrawl().script(HOME, page(url="https://example.org/home/", source=HOME))
    result, _, _ = run(scrape_one(vendor))
    assert result.final_host == "example.org" and not result.host_changed


# ---------------------------------------------------------------------------
# The status gate
# ---------------------------------------------------------------------------

def test_status_gate_404_is_http_error_and_keeps_markdown_and_credit():
    vendor = FakeFirecrawl().script(HOME, page(404, markdown="# 404\n\nNot found", error="Not Found"))
    result, client, _ = run(scrape_one(vendor))
    assert not result.ok and result.failure_reason == HTTP_ERROR and result.counts_as_error
    assert result.status_code == 404 and result.api_status == 200
    assert result.markdown.startswith("# 404") and result.credits_used == 1 and result.error == "Not Found"
    assert client.stats.failures == {HTTP_ERROR: 1} and client.credits_used == 1


@pytest.mark.parametrize("status", [401, 403, 429])
def test_status_gate_access_refusals_are_blocked(status):
    vendor = FakeFirecrawl().script(HOME, page(status, markdown="Checking your browser"))
    result, _, _ = run(scrape_one(vendor))
    assert result.failure_reason == BLOCKED and result.counts_as_error and result.status_code == status


@pytest.mark.parametrize("status", [402, 410, 500, 503, 521])
def test_status_gate_other_non_2xx_targets_are_http_errors(status):
    # A 402 from the TARGET (a suspended Shopify store, 10 of 1,000 pilot pages) is not a credit stop.
    vendor = FakeFirecrawl().script(HOME, page(status, error="Payment Required" if status == 402 else None))
    result, _, _ = run(scrape_one(vendor))
    assert result.failure_reason == HTTP_ERROR and result.counts_as_error and result.status_code == status


def test_success_without_a_status_code_is_a_vendor_error():
    answer = page()
    body = answer.json()
    del body["data"]["metadata"]["statusCode"]
    vendor = FakeFirecrawl().script(HOME, httpx.Response(200, json=body))
    result, _, _ = run(scrape_one(vendor))
    assert result.failure_reason == VENDOR_ERROR and not result.counts_as_error
    assert "statusCode" in result.error


# ---------------------------------------------------------------------------
# Terminal codes, timeouts and invalid URLs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("code", ["SCRAPE_DNS_RESOLUTION_ERROR", "SCRAPE_ALL_ENGINES_FAILED",
                                  "SCRAPE_SSL_ERROR", "SCRAPE_SITE_ERROR", "SCRAPE_UNSUPPORTED_FILE_ERROR"])
def test_scrape_codes_at_500_are_terminal_dead_without_retry(code):
    vendor = FakeFirecrawl().script(HOME, api_error(500, code=code, error=f"{code} happened"), page())
    result, client, sleep = run(scrape_one(vendor))
    assert result.failure_reason == DEAD and result.counts_as_error
    assert result.api_status == 500 and result.api_code == code and result.error == f"{code} happened"
    assert result.attempts == 1 and len(vendor.requests) == 1 and sleep.delays == []
    assert client.stats.retries == {}


def test_scrape_code_on_a_200_with_success_false_is_dead_too():
    # 60 of 1,000 pilot DNS failures came back as HTTP 200, success=false, code=SCRAPE_DNS_RESOLUTION_ERROR.
    body = {"success": False, "code": "SCRAPE_DNS_RESOLUTION_ERROR",
            "error": 'DNS resolution failed for hostname "www.autumgold.org".'}
    vendor = FakeFirecrawl().script(HOME, httpx.Response(200, json=body))
    result, _, _ = run(scrape_one(vendor))
    assert result.failure_reason == DEAD and result.api_status == 200 and result.attempts == 1
    assert result.error.startswith("DNS resolution failed")


def test_408_scrape_timeout_is_a_vendor_error_without_retry():
    vendor = FakeFirecrawl().script(HOME, api_error(408, code="SCRAPE_TIMEOUT", error="The scrape operation timed out"), page())
    result, client, sleep = run(scrape_one(vendor))
    assert result.failure_reason == VENDOR_ERROR and not result.counts_as_error
    assert result.api_status == 408 and result.api_code == "SCRAPE_TIMEOUT"
    assert result.attempts == 1 and len(vendor.requests) == 1 and sleep.delays == []
    assert client.stats.failures == {VENDOR_ERROR: 1}


def test_api_400_is_invalid_url_without_retry():
    vendor = FakeFirecrawl().script("https://weebly.c", api_error(400, code="BAD_REQUEST",
                                                                  error="URL must have a valid top-level domain"))
    # the client's own check rejects this one first, so force a sendable URL through the API answer instead
    vendor.script(HOME, api_error(400, code="BAD_REQUEST", error="URL must have a valid top-level domain"))
    result, _, _ = run(scrape_one(vendor))
    assert result.failure_reason == INVALID_URL and result.counts_as_error
    assert result.api_status == 400 and result.attempts == 1 and len(vendor.requests) == 1


@pytest.mark.parametrize("bad", ["", "   ", None, "https://michigan bulls.com", "hhtp://www.uchsmotherbrown.prg",
                                 "https://grundypreschool.weebly.c", "https://", "mailto:info@example.org",
                                 "info@example.org", "ftp://files.example.org", "localhost", "https://192.168.0.1"])
def test_malformed_urls_are_invalid_without_a_request(bad):
    vendor = FakeFirecrawl()
    result, client, _ = run(scrape_one(vendor, url=bad))
    assert result.failure_reason == INVALID_URL and result.counts_as_error
    assert result.attempts == 0 and vendor.requests == [] and client.stats.n_requests == 0
    assert normalize_url(bad) is None


@pytest.mark.parametrize("raw, sent", [
    ("example.org", "https://example.org"),
    ("HTTPS://WWW.EXAMPLE.ORG/About", "https://WWW.EXAMPLE.ORG/About"),
    ("  http://example.org/  ", "http://example.org/"),
    ("https://sub.example.co.uk/path?q=1", "https://sub.example.co.uk/path?q=1"),
])
def test_normalize_url_adds_and_lowercases_the_scheme_only(raw, sent):
    assert normalize_url(raw) == sent


# ---------------------------------------------------------------------------
# Retry paths
# ---------------------------------------------------------------------------

def test_429_retries_with_the_retry_after_header_then_succeeds():
    vendor = FakeFirecrawl().script(HOME, api_error(429, error="Rate limit exceeded", headers={"Retry-After": "3"}),
                                    api_error(429, error="Rate limit exceeded", headers={"Retry-After": "5"}), page())
    result, client, sleep = run(scrape_one(vendor))
    assert result.ok and result.attempts == 3 and len(vendor.requests) == 3
    assert sleep.delays == [3.0, 5.0]
    assert client.stats.retries == {"429": 2} and client.stats.n_requests == 3


def test_429_delay_is_read_from_the_body_when_there_is_no_header():
    body = ("Rate limit exceeded. Consumed (req/min): 12, Remaining (req/min): 0. Upgrade your plan "
            "or please retry after 17s, resets at Tue Oct 20 2026 20:07:19 GMT+0000")
    vendor = FakeFirecrawl().script(HOME, api_error(429, error=body), page())
    result, _, sleep = run(scrape_one(vendor))
    assert result.ok and result.attempts == 2 and sleep.delays == [18.0]   # one second past the reset


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_5xx_without_a_scrape_code_retries_with_backoff_then_succeeds(status):
    vendor = FakeFirecrawl().script(HOME, api_error(status, code="UNKNOWN_ERROR" if status == 500 else None),
                                    api_error(status, json_body=False, error="Bad Gateway"), page())
    result, client, sleep = run(scrape_one(vendor))
    assert result.ok and result.attempts == 3
    assert sleep.delays == [2.0, 4.0]     # 2 ** attempt
    assert client.stats.retries == {str(status): 2}


def test_transport_errors_retry_then_succeed():
    vendor = FakeFirecrawl().script(HOME, httpx.ConnectError("connection reset"), httpx.ReadTimeout("read timed out"), page())
    result, client, sleep = run(scrape_one(vendor))
    assert result.ok and result.attempts == 3 and sleep.delays == [2.0, 4.0]
    assert client.stats.retries == {"transport": 2}


def test_exhausted_retries_are_a_vendor_error_not_an_error():
    vendor = FakeFirecrawl().script(HOME, api_error(429, error="Rate limit exceeded", headers={"Retry-After": "1"}))
    result, client, sleep = run(scrape_one(vendor, max_attempts=4))
    assert result.failure_reason == VENDOR_ERROR and not result.counts_as_error
    assert result.attempts == 4 and len(vendor.requests) == 4 and sleep.delays == [1.0, 1.0, 1.0]
    assert result.api_status == 429 and "gave up after 4 attempts" in result.error
    assert client.stats.retries == {"429": 4} and client.stats.failures == {VENDOR_ERROR: 1}


def test_exhausted_transport_errors_are_a_vendor_error():
    vendor = FakeFirecrawl().script(HOME, httpx.ConnectError("no route"))
    result, _, _ = run(scrape_one(vendor, max_attempts=2))
    assert result.failure_reason == VENDOR_ERROR and result.api_status is None and result.attempts == 2
    assert "ConnectError" in result.error


# ---------------------------------------------------------------------------
# Stops: 402, 401 and the credit ceiling
# ---------------------------------------------------------------------------

def test_402_stops_the_run_and_pending_pages_send_nothing():
    urls = [f"https://site{i}.org" for i in range(6)]
    vendor = FakeFirecrawl().script(urls[0], api_error(402, error="Payment Required: insufficient credits"))
    vendor.delay = 0.01
    client, _ = make_client(vendor, concurrency=1)   # one browser: pages queue behind the first

    async def go():
        async with client:
            return await client.scrape_many(urls)

    outcome = run(go())
    assert len(vendor.requests) == 1 and vendor.requests[0]["url"] == urls[0]
    assert all(isinstance(v, RunStopped) and v.kind == "payment_required" for v in outcome.values())
    assert client.stop_reason.startswith("Firecrawl answered 402")
    assert client.stats.stop_reason == client.stop_reason and client.stats.n_pages == 0

    async def after():
        async with client:
            with pytest.raises(RunStopped):
                await client.scrape("https://later.org")

    run(after())
    assert len(vendor.requests) == 1    # still nothing sent after the stop


def test_402_lets_pages_already_in_flight_finish():
    urls = ["https://a.org", "https://b.org", "https://c.org"]
    vendor = FakeFirecrawl().script("https://b.org", api_error(402, error="Payment Required"))
    vendor.delay = 0.02
    client, _ = make_client(vendor, concurrency=3)

    async def go():
        async with client:
            return await client.scrape_many(urls)

    outcome = run(go())
    assert len(vendor.requests) == 3                                  # all three were in flight together
    assert outcome["https://a.org"].ok and outcome["https://c.org"].ok   # paid for, kept
    assert isinstance(outcome["https://b.org"], RunStopped)
    assert client.stats.n_pages == 2 and client.credits_used == 2


def test_401_stops_as_unauthorized():
    vendor = FakeFirecrawl().script(HOME, api_error(401, error="Unauthorized"))
    client, _ = make_client(vendor)

    async def go():
        async with client:
            with pytest.raises(RunStopped) as info:
                await client.scrape(HOME)
            return info.value

    stop = run(go())
    assert stop.kind == "unauthorized" and client.stats.stop_reason == stop.reason


def test_credit_ceiling_stops_before_the_next_request():
    vendor = FakeFirecrawl()
    client, _ = make_client(vendor, credit_ceiling=2)

    async def go():
        async with client:
            a = await client.scrape("https://a.org")
            b = await client.scrape("https://b.org")
            with pytest.raises(RunStopped) as info:
                await client.scrape("https://c.org")
            return a, b, info.value

    a, b, stop = run(go())
    assert a.ok and b.ok and len(vendor.requests) == 2
    assert stop.kind == "credit_ceiling" and "ceiling of 2" in stop.reason
    assert client.credits_used == 2 and client.stats.stop_reason == stop.reason


def test_failures_bill_nothing_and_do_not_count_toward_the_ceiling():
    vendor = FakeFirecrawl().script("https://dead.org", api_error(500, code="SCRAPE_DNS_RESOLUTION_ERROR"))
    client, _ = make_client(vendor, credit_ceiling=1)

    async def go():
        async with client:
            dead = await client.scrape("https://dead.org")
            ok = await client.scrape("https://alive.org")
            return dead, ok

    dead, ok = run(go())
    assert dead.failure_reason == DEAD and dead.credits_used == 0 and ok.ok and client.credits_used == 1


# ---------------------------------------------------------------------------
# Concurrency and rpm
# ---------------------------------------------------------------------------

def test_semaphore_bounds_requests_in_flight():
    vendor = FakeFirecrawl()
    vendor.delay = 0.01
    client, _ = make_client(vendor, concurrency=3)
    urls = [f"https://site{i}.org" for i in range(12)]

    async def go():
        async with client:
            return await client.scrape_many(urls)

    outcome = run(go())
    assert all(r.ok for r in outcome.values()) and len(vendor.requests) == 12
    assert vendor.max_in_flight == 3


def test_shared_limits_bound_two_clients_together():
    vendor = FakeFirecrawl()
    vendor.delay = 0.01
    limits = AccountLimits(concurrency=2, rpm=None)
    sleep = SleepRecorder()
    clients = [FirecrawlClient(KEY, limits=limits, transport=httpx.MockTransport(vendor.handler), sleep=sleep)
               for _ in range(2)]

    async def go():
        async with clients[0], clients[1]:
            await asyncio.gather(clients[0].scrape_many([f"https://a{i}.org" for i in range(5)]),
                                 clients[1].scrape_many([f"https://b{i}.org" for i in range(5)]))

    run(go())
    assert len(vendor.requests) == 10 and vendor.max_in_flight == 2


def test_rpm_limiter_spaces_requests_and_counts_retries_as_requests():
    vendor = FakeFirecrawl().script("https://b.org", api_error(503), page())
    client, sleep = make_client(vendor, rpm=120)   # one request every 0.5 s

    async def go():
        async with client:
            await client.scrape("https://a.org")
            await client.scrape("https://b.org")   # two attempts, each takes an rpm slot

    run(go())
    assert client.stats.n_requests == 3
    # The recorder never advances the clock, so each slot is asked to wait for the whole
    # backlog: 0.5 s before the second request, the 503 backoff of 2.0 s, then 1.0 s
    # (two intervals) before the third request.
    assert len(sleep.delays) == 3
    assert 0.45 <= sleep.delays[0] <= 0.5 and sleep.delays[1] == 2.0 and 0.95 <= sleep.delays[2] <= 1.0


def test_limits_rebind_to_a_new_event_loop():
    vendor = FakeFirecrawl()
    limits = AccountLimits(concurrency=2, rpm=None)
    for _ in range(2):   # a second asyncio.run must not hit "bound to a different event loop"
        client = FirecrawlClient(KEY, limits=limits, transport=httpx.MockTransport(vendor.handler))

        async def go():
            async with client:
                return await client.scrape(HOME)

        assert run(go()).ok
    assert len(vendor.requests) == 2


# ---------------------------------------------------------------------------
# Dry run and balance
# ---------------------------------------------------------------------------

def test_dry_run_reports_misses_credits_and_balance_without_fetching():
    vendor = FakeFirecrawl(remaining=91047)
    client, _ = make_client(vendor, credit_ceiling=100)
    urls = ["https://cached1.org", "cached2.org", "https://new1.org", "https://new1.org/", "new2.org",
            "https://michigan bulls.com", ""]

    async def go():
        async with client:
            return await client.dry_run(urls, is_cached={"https://cached1.org", "https://cached2.org"},
                                        credits_per_miss=4.7)

    report = run(go())
    assert vendor.requests == [] and client.stats.n_requests == 0        # nothing fetched
    assert report.n_urls == 6 and report.n_cached == 2 and report.n_invalid == 2
    assert report.misses == ["https://new1.org", "https://new2.org"]   # new1.org/ is the same page
    assert report.invalid == ["https://michigan bulls.com", ""]
    assert report.estimated_credits == pytest.approx(2 * 4.7)
    assert report.credit_ceiling == 100 and report.within_ceiling is True
    assert report.remaining_credits == 91047 and report.within_balance is True
    assert report.balance["planCredits"] == 100000
    text = report.summary()
    assert "2 to fetch" in text and "9.4" in text and "91047" in text and "within" in text


def test_dry_run_accepts_a_predicate_and_flags_ceiling_and_balance_shortfalls():
    vendor = FakeFirecrawl(remaining=1)
    client, _ = make_client(vendor, credit_ceiling=2)

    async def go():
        async with client:
            return await client.dry_run(["a.org", "b.org", "c.org"], is_cached=lambda u: u.startswith("https://a"))

    report = run(go())
    assert report.n_cached == 1 and report.n_misses == 2 and report.estimated_credits == 2
    assert report.within_ceiling is True and report.within_balance is False
    assert "NOT ENOUGH" in report.summary()


def test_dry_run_without_a_cache_counts_every_url_and_survives_a_failed_balance_call():
    vendor = FakeFirecrawl(remaining=None)   # the balance endpoint answers 500
    client, _ = make_client(vendor)

    async def go():
        async with client:
            return await client.dry_run(["a.org", "b.org"])

    report = run(go())
    assert report.n_misses == 2 and report.estimated_credits == 2
    assert report.remaining_credits is None and report.within_balance is None and report.within_ceiling is None
    assert "error" in report.balance and "unavailable" in report.summary()


def test_balance_is_read_before_and_after_a_run_before_the_client_closes():
    vendor = FakeFirecrawl(remaining=500)
    client, _ = make_client(vendor)

    async def go():
        async with client:
            assert client.stats.balance_before["remainingCredits"] == 500
            vendor.remaining = 499
            await client.scrape(HOME)

    run(go())
    assert vendor.balance_calls == 2
    assert client.stats.balance_after["remainingCredits"] == 499 and "error" not in client.stats.balance_after
    assert client.stats.started_at and client.stats.ended_at and client.stats.ended_at >= client.stats.started_at
    assert FirecrawlClient.remaining_credits(client.stats.balance_after) == 499
    assert FirecrawlClient.remaining_credits({"error": "x"}) is None


def test_stats_summarize_the_run():
    vendor = (FakeFirecrawl()
              .script("https://dead.org", api_error(500, code="SCRAPE_SITE_ERROR"))
              .script("https://gone.org", page(404))
              .script("https://slow.org", api_error(408, code="SCRAPE_TIMEOUT"))
              .script("https://flaky.org", api_error(502), page()))
    client, _ = make_client(vendor)

    async def go():
        async with client:
            return await client.scrape_many(["https://dead.org", "https://gone.org", "https://slow.org",
                                             "https://flaky.org", "https://fine.org", "not a url"])

    outcome = run(go())
    stats = client.stats.as_dict()
    assert stats["n_pages"] == 6 and stats["n_ok"] == 2 and stats["n_requests"] == 6
    assert stats["credits_used"] == 3           # 404 bills one credit; dead, timeout and invalid bill nothing
    assert stats["retries"] == {"502": 1}
    assert stats["failures"] == {DEAD: 1, HTTP_ERROR: 1, VENDOR_ERROR: 1, INVALID_URL: 1}
    assert stats["stop_reason"] is None
    assert sum(r.counts_as_error for r in outcome.values()) == 3


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def test_from_config_reads_the_section_and_keeps_the_key_private(tmp_path: Path, monkeypatch):
    ini = tmp_path / "config.ini"
    ini.write_text("[scraping_vendors]\nfirecrawl_api_key = cfg-secret\nfirecrawl_concurrency = 7\nfirecrawl_rpm = 120\n")
    monkeypatch.setenv("VDL_GLOBAL_CONFIG_PATH", str(ini))
    client = FirecrawlClient.from_config(credit_ceiling=50)
    assert client.limits.concurrency == 7 and client.limits.rpm == 120 and client.credit_ceiling == 50
    assert "cfg-secret" not in repr(client)
    assert FirecrawlClient.from_config(concurrency=3).limits.concurrency == 3   # overrides win


def test_from_config_without_a_key_names_the_setting_not_a_value(tmp_path: Path, monkeypatch):
    ini = tmp_path / "config.ini"
    ini.write_text("[scraping_vendors]\nspider_api_key = other\n")
    monkeypatch.setenv("VDL_GLOBAL_CONFIG_PATH", str(ini))
    with pytest.raises(RuntimeError, match=r"firecrawl_api_key"):
        FirecrawlClient.from_config()
    with pytest.raises(ValueError):
        FirecrawlClient("")


def test_module_constants_pin_the_decisions():
    assert fc.REQUEST_PROFILE["onlyMainContent"] is False and fc.REQUEST_PROFILE["formats"] == ["markdown", "links"]
    assert fc.DEFAULT_CONCURRENCY == 25 and fc.DEFAULT_RPM == 500
    assert fc.TARGET_FAILURES == {DEAD, HTTP_ERROR, BLOCKED, INVALID_URL} and VENDOR_ERROR not in fc.TARGET_FAILURES
