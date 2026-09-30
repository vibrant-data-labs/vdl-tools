"""Regression tests for the JS-shell browser fallback.

Bug this pins down: sites rendered entirely client-side (React/Vue shells)
return HTTP 200 with a body that is just a mount-point div plus <script> tags.
The old flow only fell back to Playwright on HTTP-level failures (JS walls,
403s), so these "successful" responses were cached with empty extracted text
and never retried. Pilot examples: lucernemed.com, proseware.app, contoso.space.

Two layers guard against this now:
1. AsyncScraper.scrape_url retries via browser when the HTTP body *looks like*
   a JS shell (cheap heuristic, catches the class before extraction).
2. scrape_websites retries via browser when a 200 HTTP fetch yields empty
   extracted text (authoritative signal, catches what the heuristic misses).
"""

import asyncio

from vdl_tools.scrape_enrich.scraper.async_scraper import (
    AsyncScraper,
    FailureReason,
    JS_WALL_PATTERNS,
    looks_like_js_shell,
)
from vdl_tools.scrape_enrich.scraper.scrape_websites import (
    PageType,
    _process_scraped_with_browser_retry,
    _should_retry_blocked_with_browser,
    _should_retry_with_browser,
)
from vdl_tools.scrape_enrich.scraper.text_quality import looks_like_bot_wall


JS_SHELL_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Embo Med</title>
<link rel="stylesheet" href="/static/css/main.4f2a.css"/>
<script defer src="/static/js/vendors.2b1d9e.js"></script>
<script defer src="/static/js/main.8f7a2c.js"></script>
</head>
<body>
<div id="root"></div>
<script>window.__APP_CONFIG__ = {"api": "/api", "flags": ["a", "b"]};</script>
</body>
</html>"""

# Long enough that unstructured extraction clears the min-length threshold
RENDERED_HTML = (
    "<html><head><title>Embo Med</title></head><body>"
    "<h1>Embolization, simplified</h1>"
    + "".join(
        f"<p>Paragraph {i} about the company's medical devices and clinical "
        "work, written out at length so that text extraction comfortably "
        "clears the minimum length threshold used by the parser.</p>"
        for i in range(20)
    )
    + "</body></html>"
)

STATIC_HTML = (
    "<html><head><title>Plain site</title>"
    "<script src='/analytics.js'></script></head><body>"
    + "".join(
        f"<p>Server-rendered paragraph {i} with plenty of visible text that "
        "any fetcher can see without executing a line of JavaScript.</p>"
        for i in range(20)
    )
    + "</body></html>"
)


def _shell_scrape_result(**overrides):
    result = {
        "url": "https://lucernemed.com/",
        "content": JS_SHELL_HTML,
        "status_code": 200,
        "method": "http",
        "success": True,
        "failure_reason": None,
    }
    result.update(overrides)
    return result


class StubBrowserScraper:
    """Stands in for AsyncScraper where only fetch_browser is needed."""

    def __init__(self, browser_content):
        self.browser_content = browser_content
        self.browser_calls = []

    async def fetch_browser(self, url):
        self.browser_calls.append(url)
        return self.browser_content


# --------------------------------------------------------------------------
# looks_like_js_shell heuristic
# --------------------------------------------------------------------------

def test_js_shell_html_is_detected():
    assert looks_like_js_shell(JS_SHELL_HTML) is True


def test_server_rendered_page_is_not_a_shell():
    assert looks_like_js_shell(STATIC_HTML) is False
    assert looks_like_js_shell(RENDERED_HTML) is False


def test_degenerate_inputs_are_not_shells():
    assert looks_like_js_shell(None) is False
    assert looks_like_js_shell("") is False
    assert looks_like_js_shell(b"%PDF-1.4") is False
    # no scripts at all -> empty static page, browser won't help
    assert looks_like_js_shell("<html><body></body></html>") is False


# --------------------------------------------------------------------------
# AsyncScraper.scrape_url with stubbed fetchers
# --------------------------------------------------------------------------

def _make_scraper(http_result, browser_content):
    """AsyncScraper with both fetchers stubbed; never opens network/Playwright."""
    scraper = AsyncScraper()
    browser_calls = []

    async def fake_http(url):
        return http_result

    async def fake_browser(url):
        browser_calls.append(url)
        return browser_content

    scraper.fetch_http = fake_http
    scraper.fetch_browser = fake_browser
    return scraper, browser_calls


def test_scrape_url_retries_js_shell_via_browser():
    scraper, browser_calls = _make_scraper(
        (JS_SHELL_HTML, FailureReason.NONE, 200), RENDERED_HTML
    )
    result = asyncio.run(scraper.scrape_url("https://lucernemed.com/"))
    assert browser_calls == ["https://lucernemed.com/"]
    assert result["content"] == RENDERED_HTML
    assert result["method"] == "browser"
    assert result["success"] is True
    assert result["status_code"] == 200


def test_scrape_url_keeps_shell_when_browser_fails():
    scraper, browser_calls = _make_scraper(
        (JS_SHELL_HTML, FailureReason.NONE, 200), None
    )
    result = asyncio.run(scraper.scrape_url("https://lucernemed.com/"))
    assert browser_calls == ["https://lucernemed.com/"]
    # raw shell HTML is preserved for diagnosis rather than dropped
    assert result["content"] == JS_SHELL_HTML
    assert result["method"] == "http"
    assert result["success"] is True


def test_scrape_url_skips_browser_for_server_rendered_pages():
    scraper, browser_calls = _make_scraper(
        (STATIC_HTML, FailureReason.NONE, 200), RENDERED_HTML
    )
    result = asyncio.run(scraper.scrape_url("https://example.org/"))
    assert browser_calls == []
    assert result["content"] == STATIC_HTML
    assert result["method"] == "http"


def test_scrape_url_js_wall_fallback_still_works():
    scraper, browser_calls = _make_scraper(
        (None, FailureReason.JS_WALL, 403), RENDERED_HTML
    )
    result = asyncio.run(scraper.scrape_url("https://walled.example/"))
    assert browser_calls == ["https://walled.example/"]
    assert result["content"] == RENDERED_HTML
    assert result["method"] == "browser"


# --------------------------------------------------------------------------
# scrape_websites retry trigger (empty extracted text)
# --------------------------------------------------------------------------

def test_should_retry_when_http_extraction_is_empty():
    rows = [{"parsed_html": ""}]
    assert _should_retry_with_browser(_shell_scrape_result(), rows) is True


def test_should_not_retry_browser_fetched_pages():
    rows = [{"parsed_html": ""}]
    result = _shell_scrape_result(method="browser")
    assert _should_retry_with_browser(result, rows) is False


def test_should_not_retry_when_text_was_extracted():
    rows = [{"parsed_html": "Real extracted text"}]
    assert _should_retry_with_browser(_shell_scrape_result(), rows) is False


def test_should_not_retry_without_content_or_for_pdfs():
    rows = [{"parsed_html": ""}]
    assert _should_retry_with_browser(_shell_scrape_result(content=None), rows) is False
    assert _should_retry_with_browser(_shell_scrape_result(content=b"%PDF"), rows) is False
    pdf = _shell_scrape_result(url="https://example.org/report.pdf")
    assert _should_retry_with_browser(pdf, rows) is False


def test_process_retry_reprocesses_with_rendered_html():
    stub = StubBrowserScraper(RENDERED_HTML)
    result, rows = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _shell_scrape_result(),
            cache_id="lucernemed.com",
            data_type=PageType.INDEX,
            root_path="https://lucernemed.com/",
            return_raw_html=True,
        )
    )
    assert stub.browser_calls == ["https://lucernemed.com/"]
    assert result["method"] == "browser"
    assert result["content"] == RENDERED_HTML
    assert len(rows) == 1
    assert rows[0]["parsed_html"].strip()
    assert rows[0]["num_errors"] == 0
    # the stored raw HTML is the rendered DOM, not the shell
    assert rows[0]["raw_html"] == RENDERED_HTML


def test_process_retry_keeps_original_rows_when_browser_fails():
    stub = StubBrowserScraper(None)
    result, rows = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _shell_scrape_result(),
            cache_id="lucernemed.com",
            data_type=PageType.INDEX,
            root_path="https://lucernemed.com/",
            return_raw_html=True,
        )
    )
    assert stub.browser_calls == ["https://lucernemed.com/"]
    assert result["method"] == "http"
    assert len(rows) == 1
    assert not rows[0]["parsed_html"].strip()


def test_process_no_retry_for_pages_with_text():
    stub = StubBrowserScraper(RENDERED_HTML)
    result, rows = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _shell_scrape_result(content=STATIC_HTML),
            cache_id="example.org",
            data_type=PageType.INDEX,
            root_path="https://example.org/",
        )
    )
    assert stub.browser_calls == []
    assert result["method"] == "http"
    assert rows[0]["parsed_html"].strip()


# --- bot walls ----------------------------------------------------------------
# Second class, same shape as the JS shell and a layer further in: the wall
# answers HTTP 200 *and* extracts cleanly, so neither "scrape failed" nor
# "extraction empty" catches it and the challenge screen is stored as the page.
# Left uncaught it is permanent — the row reads as a success, so skip_existing
# never revisits it and max_errors cannot reach it.

CLOUDFLARE_WALL_HTML = (
    "<html><head><title>Just a moment...</title></head><body>"
    "<div class='main-wrapper'><h1>Sorry, you have been blocked</h1>"
    "<p>This website uses a security service to protect against malicious bots. "
    "This page is displayed while the website verifies you are not a bot.</p>"
    "</div></body></html>"
)
FORBIDDEN_WALL_HTML = (
    "<html><body><h1>403 - Forbidden</h1>"
    "<p>Access to this page is forbidden.</p></body></html>"
)
# A real page that merely *mentions* the vendor. Long, so it must not trip the rule.
MENTIONS_CLOUDFLARE_HTML = (
    "<html><head><title>Our infrastructure</title></head><body>"
    + "".join(
        f"<p>Section {i}: we serve the site through Cloudflare and use a CAPTCHA "
        "on our donation form, described here at length so that extraction "
        "produces a page far longer than any challenge screen.</p>"
        for i in range(20)
    )
    + "</body></html>"
)


def _wall_scrape_result(**overrides):
    overrides.setdefault("url", "https://scouting.org/")
    overrides.setdefault("content", CLOUDFLARE_WALL_HTML)
    return _shell_scrape_result(**overrides)


def test_bot_wall_text_is_detected():
    assert looks_like_bot_wall("This website uses a security service to protect against malicious bots.")
    assert looks_like_bot_wall("403 - Forbidden Access to this page is forbidden.")
    assert not looks_like_bot_wall("Welcome to our after-school mentoring program.")
    assert not looks_like_bot_wall("")
    assert not looks_like_bot_wall(None)
    # a real page discussing the vendor runs long; a wall does not
    assert not looks_like_bot_wall("We serve this site through Cloudflare. " * 60)


def test_wall_and_empty_triggers_are_disjoint():
    """Each trigger owns its class; neither should claim the other's."""
    wall_rows = [{"parsed_html": "Sorry, you have been blocked. Cloudflare."}]
    empty_rows = [{"parsed_html": "   "}]
    http = {"method": "http", "url": "https://scouting.org/", "content": CLOUDFLARE_WALL_HTML}

    assert _should_retry_blocked_with_browser(http, wall_rows)
    assert not _should_retry_with_browser(http, wall_rows)
    assert _should_retry_with_browser(http, empty_rows)
    assert not _should_retry_blocked_with_browser(http, empty_rows)


def test_wall_not_retried_when_already_fetched_by_browser():
    rows = [{"parsed_html": "Sorry, you have been blocked. Cloudflare."}]
    already = {"method": "browser", "url": "https://scouting.org/", "content": CLOUDFLARE_WALL_HTML}
    assert not _should_retry_blocked_with_browser(already, rows)


def test_process_retries_wall_through_browser_and_keeps_rendered_page():
    stub = StubBrowserScraper(RENDERED_HTML)
    result, rows = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _wall_scrape_result(),
            cache_id="scouting.org",
            data_type=PageType.INDEX,
            root_path="https://scouting.org/",
        )
    )
    assert stub.browser_calls == ["https://scouting.org/"]
    assert result["method"] == "browser"
    assert rows[0]["parsed_html"].strip()
    assert not looks_like_bot_wall(rows[0]["parsed_html"])


def test_process_retries_403_wall_through_browser():
    stub = StubBrowserScraper(RENDERED_HTML)
    result, _ = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _wall_scrape_result(
                url="https://natya.com/", content=FORBIDDEN_WALL_HTML, status_code=403
            ),
            cache_id="natya.com",
            data_type=PageType.INDEX,
            root_path="https://natya.com/",
        )
    )
    assert stub.browser_calls == ["https://natya.com/"]
    assert result["method"] == "browser"


def test_process_does_not_retry_real_page_mentioning_the_vendor():
    stub = StubBrowserScraper(RENDERED_HTML)
    result, rows = asyncio.run(
        _process_scraped_with_browser_retry(
            stub,
            _wall_scrape_result(
                url="https://example.org/", content=MENTIONS_CLOUDFLARE_HTML
            ),
            cache_id="example.org",
            data_type=PageType.INDEX,
            root_path="https://example.org/",
        )
    )
    assert stub.browser_calls == []
    assert result["method"] == "http"
    assert rows[0]["parsed_html"].strip()


# --- JS_WALL_PATTERNS: vendor coverage --------------------------------------
# fetch_http checks every response body (not just failures) against this list,
# so a pattern that is too generic silently discards good content and burns a
# browser-retry slot on pages that never needed one. bbbsba.org is the pilot
# case for the SiteGround marker: a 202 response whose entire body is a
# meta-refresh to SiteGround's own bot-check, invisible to every vendor marker
# already in the list.

SITEGROUND_CHALLENGE_BODY = (
    '<html><head><link rel="icon" href="data:;">'
    '<meta http-equiv="refresh" '
    'content="0;/.well-known/sgcaptcha/?r=%2F&y=ipr:23.93.133.24:1790275577.098">'
    "</meta></head></html>"
)
# The dominant false-positive risk measured against the corpus: 277 of ~90k
# scraped domains carry this exact Google reCAPTCHA footer on an ordinary,
# fully-readable contact/donation form — not a wall. A too-broad pattern
# (a bare "captcha") would misclassify all of them; this is why only the
# SiteGround-specific string was added, not the generic word.
RECAPTCHA_FOOTER_TEXT = (
    "or updates, promotions, and more. This site is protected by reCAPTCHA "
    "and the Google Privacy Policy and Terms of Service apply."
)


def test_siteground_challenge_is_detected():
    lowered = SITEGROUND_CHALLENGE_BODY.lower()
    assert any(pattern in lowered for pattern in JS_WALL_PATTERNS)


def test_recaptcha_footer_boilerplate_is_not_flagged():
    """Regression guard: this ordinary boilerplate must never match."""
    lowered = RECAPTCHA_FOOTER_TEXT.lower()
    assert not any(pattern in lowered for pattern in JS_WALL_PATTERNS)
