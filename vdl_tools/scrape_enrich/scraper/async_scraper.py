import asyncio
import contextlib
import logging
import re
import httpx
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

# Default retry configuration
DEFAULT_HTTP_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0  # seconds

# Default timeout configuration
DEFAULT_CONNECT_TIMEOUT = 5.0  # Fast fail on dead links
DEFAULT_READ_TIMEOUT = 15.0    # Allow time for slow servers

# JS wall / bot protection patterns that indicate browser rendering is needed
# These are checked against lowercased response text
JS_WALL_PATTERNS = [
    'forbidden',
    'access denied',
    # Generic JS required messages
    'enable javascript',
    'javascript is required',
    'javascript is disabled',
    'please turn javascript on',
    'please enable javascript',
    'requires javascript',
    'you need to enable javascript',

    # Cloudflare
    'challenges.cloudflare.com',
    'cf-browser-verification',
    'checking your browser',
    'just a moment...',  # Cloudflare waiting page title

    # Akamai
    'akamaihd.net',
    'akamai bot manager',

    # PerimeterX
    'perimeterx',
    'px-captcha',
    'human challenge',

    # Incapsula / Imperva
    'incapsula incident',
    'visid_incap',

    # DataDome
    'datadome',
    'geo.captcha-delivery.com',

    # Generic bot detection / CAPTCHA
    'verify you are human',
    'are you a robot',
    'human verification',
    'bot detection',
    'please verify you are a human',
    'access denied',  # Often paired with bot detection
    'sorry, you have been blocked',
]


# A 200 response whose visible text (tags and script/style bodies stripped) is
# shorter than this is treated as a JS app shell when it also contains scripts.
JS_SHELL_MAX_VISIBLE_TEXT = 200

_SCRIPT_TAG_RE = re.compile(r'<script\b', re.IGNORECASE)
_INVISIBLE_BLOCK_RE = re.compile(r'(?is)<(script|style|noscript|template|svg)\b.*?</\1\s*>')
_HTML_COMMENT_RE = re.compile(r'(?s)<!--.*?-->')
_TAG_RE = re.compile(r'(?s)<[^>]*>')


def looks_like_js_shell(html) -> bool:
    """Whether a successful (200) response body is a client-side app shell.

    Sites rendered entirely in the browser (React/Vue/etc.) return a "success"
    whose body is just a mount-point div plus <script> tags — all real content
    arrives only when JS executes, so a plain HTTP fetch yields nothing to
    extract. These need the browser path even though nothing failed at the
    HTTP level.
    """
    if not html or not isinstance(html, str):
        return False
    if not _SCRIPT_TAG_RE.search(html):
        return False
    visible = _INVISIBLE_BLOCK_RE.sub(' ', html)
    visible = _HTML_COMMENT_RE.sub(' ', visible)
    visible = _TAG_RE.sub(' ', visible)
    visible = re.sub(r'\s+', ' ', visible).strip()
    return len(visible) < JS_SHELL_MAX_VISIBLE_TEXT


class FailureReason(Enum):
    """Categorize HTTP failures to determine if browser fallback is worthwhile."""
    NONE = "none"              # Success
    DEAD_LINK = "dead_link"    # Connection/DNS failure - browser won't help
    JS_WALL = "js_wall"        # JS required - browser will help
    HTTP_ERROR = "http_error"  # 4xx/5xx error - browser unlikely to help
    OTHER = "other"            # Other failure


class AsyncScraper:
    def __init__(
        self,
        max_concurrent_http: int = 20,
        max_concurrent_browser: int = 3,
        verify_ssl: bool = False,
        http_retries: int = DEFAULT_HTTP_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        max_per_host: Optional[int] = None,
    ):
        """
        Initialize the async scraper with separate concurrency limits for lightweight (HTTP)
        and heavyweight (Browser) tasks.

        Args:
            max_concurrent_http: Maximum concurrent HTTP requests
            max_concurrent_browser: Maximum concurrent browser instances
            verify_ssl: Whether to verify SSL certificates
            http_retries: Number of retries for failed HTTP requests (only for transient errors)
            retry_delay: Base delay between retries (uses exponential backoff)
            connect_timeout: Timeout for establishing connection (fast fail on dead links)
            read_timeout: Timeout for reading response (allow time for slow servers)
            max_per_host: Max concurrent HTTP requests to a single host. None disables
                per-host throttling (default). Set to a small value (e.g. 3) to avoid
                tripping rate limits (429s) when scraping many pages of one site.
        """
        self.http_sem = asyncio.Semaphore(max_concurrent_http)
        self.browser_sem = asyncio.Semaphore(max_concurrent_browser)
        self.client: Optional[httpx.AsyncClient] = None
        self.verify_ssl = verify_ssl
        self.http_retries = http_retries
        self.retry_delay = retry_delay
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_per_host = max_per_host
        self._host_sems: Dict[str, asyncio.Semaphore] = {}
        self.playwright = None
        self.browser = None

    def _host_limiter(self, url: str):
        """Return a context manager limiting concurrent requests to url's host.

        When max_per_host is None this is a no-op. Per-host semaphores are created
        lazily and cached; dict access is safe because asyncio is single-threaded.
        """
        if self.max_per_host is None:
            return contextlib.nullcontext()
        host = urlparse(url).netloc
        sem = self._host_sems.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self.max_per_host)
            self._host_sems[host] = sem
        return sem

    async def __aenter__(self):
        # Use separate timeouts: fast connect timeout to fail quickly on dead links,
        # longer read timeout to handle slow but live servers
        timeout = httpx.Timeout(
            connect=self.connect_timeout,
            read=self.read_timeout,
            write=10.0,
            pool=5.0,
        )
        self.client = httpx.AsyncClient(
            verify=self.verify_ssl,
            timeout=timeout,
            follow_redirects=True,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            }
        )

        # Initialize Playwright and Browser once
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--no-zygote"
            ]
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def fetch_http(self, url: str) -> Tuple[Optional[str], FailureReason, int]:
        """
        Attempt to fetch the page using lightweight HTTP request with retry logic.
        Uses exponential backoff for transient failures only (read timeouts, 5xx errors).

        Returns:
            Tuple of (content, failure_reason, status_code).
            status_code is the actual HTTP status code, or 0 if the request never completed.
        """
        if not url.startswith('http'):
            url = f'https://{url}'

        # Per-host limiter is acquired OUTSIDE the global semaphore so that a single
        # rate-limited host can't occupy the global HTTP pool while waiting on its own
        # (smaller) slot, which would starve other hosts.
        async with self._host_limiter(url), self.http_sem:
            for attempt in range(self.http_retries):
                try:
                    response = await self.client.get(url)

                    if response.status_code >= 400:
                        logger.warning(f"HTTP {response.status_code} for {url}")
                        # 403 is commonly used by bot protection -- check body before giving up
                        if response.status_code == 403:
                            lower_text = response.text.lower()
                            if any(pattern in lower_text for pattern in JS_WALL_PATTERNS):
                                logger.info(f"Detected bot protection behind 403 for {url}, will try browser")
                                return None, FailureReason.JS_WALL, response.status_code
                        # Don't retry on 4xx client errors (except 429 rate limit)
                        if 400 <= response.status_code < 500 and response.status_code != 429:
                            return None, FailureReason.HTTP_ERROR, response.status_code
                        # Retry on 5xx server errors or 429 rate limit
                        if attempt < self.http_retries - 1:
                            delay = self.retry_delay * (2 ** attempt)
                            logger.debug(f"Retrying {url} in {delay}s (attempt {attempt + 1}/{self.http_retries})")
                            await asyncio.sleep(delay)
                            continue
                        return None, FailureReason.HTTP_ERROR, response.status_code

                    text = response.text
                    if not text:
                        return None, FailureReason.OTHER, response.status_code

                    # Detect JS-wall / bot protection patterns that require a browser
                    lower_text = text.lower()
                    if any(pattern in lower_text for pattern in JS_WALL_PATTERNS):
                        logger.info(f"Detected JS wall for {url}, will try browser")
                        return None, FailureReason.JS_WALL, response.status_code

                    return text, FailureReason.NONE, response.status_code

                except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                    logger.debug(f"Dead link (connection failed) for {url}: {type(e).__name__}")
                    return None, FailureReason.DEAD_LINK, 0

                except (httpx.ReadTimeout, httpx.ReadError) as e:
                    if attempt < self.http_retries - 1:
                        delay = self.retry_delay * (2 ** attempt)
                        logger.debug(f"Retrying {url} in {delay}s after {type(e).__name__} (attempt {attempt + 1}/{self.http_retries})")
                        await asyncio.sleep(delay)
                    else:
                        logger.debug(f"HTTP fetch failed for {url} after {self.http_retries} attempts: {str(e)}")
                        return None, FailureReason.OTHER, 0

                except Exception as e:
                    logger.debug(f"HTTP fetch failed for {url}: {str(e)}")
                    return None, FailureReason.OTHER, 0

            return None, FailureReason.OTHER, 0

    async def fetch_browser(self, url: str) -> Optional[str]:
        """
        Fetch page using full browser (Playwright) when HTTP fails or is insufficient.
        Uses semaphore for concurrency control with hard timeout to prevent hangs.
        """
        # Ensure browser is initialized
        if not self.browser:
            logger.error("Browser not initialized. Use 'async with AsyncScraper()'.")
            return None

        if not url.startswith('http'):
            url = f'https://{url}'

        # Acquire semaphore OUTSIDE the timeout - we don't want to timeout while waiting for a slot
        # The timeout only applies to the actual browser work
        async with self.browser_sem:
            context = None
            try:
                # Hard timeout for the browser operation itself
                return await asyncio.wait_for(
                    self._fetch_browser_impl(url),
                    timeout=45.0  # 45 seconds hard limit
                )
            except asyncio.TimeoutError:
                logger.warning(f"Browser operation timed out (hard limit) for {url}")
                return None
            except asyncio.CancelledError:
                logger.warning(f"Browser operation cancelled for {url}")
                raise  # Re-raise to propagate cancellation
            finally:
                # Context cleanup is handled inside _fetch_browser_impl
                pass

    async def _fetch_browser_impl(self, url: str) -> Optional[str]:
        """Internal browser fetch implementation. Must be called with semaphore held."""
        context = None
        try:
            # Create context with resource blocking to save bandwidth/memory
            context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            # Block heavy resources but keep CSS (blocking it is a bot fingerprint)
            async def _block_resource(route):
                await route.abort()
            await context.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,mp4,mp3}", _block_resource)

            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            try:
                # Navigate with timeout
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Wait a bit for potential client-side rendering
                await page.wait_for_timeout(2000)

                content = await page.content()
                return content

            except PlaywrightTimeoutError:
                logger.warning(f"Browser timeout for {url}, attempting to return partial content")
                try:
                    return await page.content()
                except Exception:
                    return None

        except asyncio.CancelledError:
            # Task was cancelled (e.g., by timeout) - ensure cleanup happens
            logger.debug(f"Browser task cancelled for {url}")
            raise
        except Exception as e:
            error_str = str(e)
            # Detect DNS/connection errors and log at debug level (not warning)
            if any(err in error_str for err in [
                'ERR_NAME_NOT_RESOLVED',      # DNS failure
                'ERR_CONNECTION_REFUSED',     # Server not listening
                'ERR_CONNECTION_RESET',       # Connection dropped
                'ERR_CONNECTION_TIMED_OUT',   # Connection timeout
                'ERR_ADDRESS_UNREACHABLE',    # Can't reach host
                'ERR_NETWORK_CHANGED',        # Network changed during request
                'Target closed',              # Browser/context closed
                'Browser closed',             # Browser closed
            ]):
                logger.debug(f"Browser: dead link for {url}: {error_str.split(chr(10))[0]}")
            else:
                logger.warning(f"Browser fetch failed for {url}: {error_str}")
            return None
        finally:
            # Always try to close context, even on cancellation
            if context:
                try:
                    await asyncio.wait_for(context.close(), timeout=5.0)
                except Exception:
                    pass  # Ignore errors during cleanup - don't block

    async def scrape_url(self, url: str) -> Dict[str, Any]:
        """
        Main entry point for scraping a single URL.
        Tries HTTP first, falls back to Browser only when it might help.
        """
        logger.debug(f"Scraping {url}")

        # 1. Try lightweight HTTP first
        content, failure_reason, status_code = await self.fetch_http(url)
        method = "http"

        # 1b. HTTP can "succeed" with a JS app shell: a 200 whose body is a
        # mount-point div plus scripts, with all content rendered client-side.
        # Retry through the browser (which executes JS); keep the HTTP shell if
        # the browser fails so callers still see the raw HTML.
        if content and looks_like_js_shell(content):
            logger.info(f"HTTP response for {url} looks like a JS shell, retrying with browser")
            browser_content = await self.fetch_browser(url)
            if browser_content:
                content = browser_content
                method = "browser"

        # 2. Only fall back to browser if it might help (JS wall detected)
        # Skip browser for dead links, HTTP errors, etc. - it won't help
        if not content and failure_reason == FailureReason.JS_WALL:
            logger.info(f"Falling back to browser for {url} (JS wall detected)")
            content = await self.fetch_browser(url)
            method = "browser"
            if content:
                status_code = 200
        elif not content:
            logger.debug(f"Skipping browser fallback for {url} (failure reason: {failure_reason.value})")

        return {
            "url": url,
            "content": content,
            "status_code": status_code,
            "method": method if content else "failed",
            "success": bool(content),
            "failure_reason": failure_reason.value if not content else None,
        }

async def scrape_urls_async(
    urls: List[str],
    max_concurrent_http: int = 20,
    max_concurrent_browser: int = 3,
    verify_ssl: bool = False,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float = DEFAULT_READ_TIMEOUT,
    max_per_host: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Helper function to run the scraper for a list of URLs.

    Args:
        urls: List of URLs to scrape
        max_concurrent_http: Maximum concurrent HTTP requests
        max_concurrent_browser: Maximum concurrent browser instances
        verify_ssl: Whether to verify SSL certificates
        connect_timeout: Timeout for establishing connection (fast fail on dead links)
        read_timeout: Timeout for reading response (allow time for slow servers)
        max_per_host: Max concurrent HTTP requests to a single host (None disables)

    Returns a list of results, one per URL. Failed URLs will have success=False.
    """
    async with AsyncScraper(
        max_concurrent_http=max_concurrent_http,
        max_concurrent_browser=max_concurrent_browser,
        verify_ssl=verify_ssl,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        max_per_host=max_per_host,
    ) as scraper:
        tasks = [scraper.scrape_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to failed results
        processed_results = []
        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                logger.warning(f"Scraping failed for {url}: {result}")
                processed_results.append({
                    "url": url,
                    "content": None,
                    "status_code": 0,
                    "method": "failed",
                    "success": False,
                    "failure_reason": FailureReason.OTHER.value,
                    "error": str(result),
                })
            else:
                processed_results.append(result)
        return processed_results
