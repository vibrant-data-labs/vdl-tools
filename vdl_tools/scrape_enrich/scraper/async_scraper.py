import asyncio
import logging
import httpx
from typing import Optional, Dict, Any, List
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)

class AsyncScraper:
    def __init__(self, max_concurrent_http: int = 20, max_concurrent_browser: int = 3, verify_ssl: bool = False):
        """
        Initialize the async scraper with separate concurrency limits for lightweight (HTTP)
        and heavyweight (Browser) tasks.
        """
        self.http_sem = asyncio.Semaphore(max_concurrent_http)
        self.browser_sem = asyncio.Semaphore(max_concurrent_browser)
        self.client: Optional[httpx.AsyncClient] = None
        self.verify_ssl = verify_ssl
        self.playwright = None
        self.browser = None

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            verify=self.verify_ssl,
            timeout=30.0,
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

    async def fetch_http(self, url: str) -> Optional[str]:
        """
        Attempt to fetch the page using lightweight HTTP request.
        """
        async with self.http_sem:
            try:
                if not url.startswith('http'):
                    url = f'https://{url}'
                    
                response = await self.client.get(url)
                
                if response.status_code >= 400:
                    logger.warning(f"HTTP {response.status_code} for {url}")
                    return None
                    
                text = response.text
                if not text:
                    return None
                    
                # Detect common JS-wall patterns (Cloudflare, etc.) that require a browser
                lower_text = text.lower()
                if any(x in lower_text for x in ['enable javascript', 'please turn javascript on', 'challenges.cloudflare.com']):
                    logger.info(f"Detected JS requirement for {url}, fallback to browser")
                    return None
                    
                return text
            except Exception as e:
                logger.debug(f"HTTP fetch failed for {url}: {str(e)}")
                return None

    async def fetch_browser(self, url: str) -> Optional[str]:
        """
        Fetch page using full browser (Playwright) when HTTP fails or is insufficient.
        """
        # Ensure browser is initialized
        if not self.browser:
            logger.error("Browser not initialized. Use 'async with AsyncScraper()'.")
            return None

        async with self.browser_sem:
            try:
                if not url.startswith('http'):
                    url = f'https://{url}'

                try:
                    # Create context with resource blocking to save bandwidth/memory
                    context = await self.browser.new_context(
                        user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
                    )
                    
                    # Block images, fonts, media to speed up loading
                    await context.route("**/*.{png,jpg,jpeg,gif,webp,svg,css,woff,woff2,mp4,mp3}", lambda route: route.abort())
                    
                    page = await context.new_page()
                    
                    try:
                        # Navigate with timeout
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        
                        # Wait a bit for potential client-side rendering
                        await page.wait_for_timeout(2000)
                        
                        content = await page.content()
                        return content
                        
                    except PlaywrightTimeoutError:
                        logger.warning(f"Browser timeout for {url}, attempting to return partial content")
                        return await page.content()
                    finally:
                        await context.close()
                        
                except Exception as e:
                    logger.warning(f"Browser context error for {url}: {str(e)}")
                    return None
                        
            except Exception as e:
                logger.warning(f"Browser fetch failed for {url}: {str(e)}")
                return None

    async def scrape_url(self, url: str) -> Dict[str, Any]:
        """
        Main entry point for scraping a single URL.
        Tries HTTP first, falls back to Browser.
        """
        logger.debug(f"Scraping {url}")
        
        # 1. Try lightweight HTTP first
        content = await self.fetch_http(url)
        method = "http"
        
        # 2. If HTTP failed or returned unusable content, try Browser
        if not content:
            # Only log if we're actually falling back, to keep logs clean
            logger.info(f"Falling back to browser for {url}")
            content = await self.fetch_browser(url)
            method = "browser"
        else:
            logger.info(f"HTTP Success: {url}")
            
        return {
            "url": url,
            "content": content,
            "status_code": 200 if content else 0, # Simplified status
            "method": method if content else "failed",
            "success": bool(content)
        }

async def scrape_urls_async(urls: List[str], max_concurrent_http: int = 20, max_concurrent_browser: int = 3, verify_ssl: bool = False) -> List[Dict[str, Any]]:
    """
    Helper function to run the scraper for a list of URLs.
    """
    async with AsyncScraper(max_concurrent_http, max_concurrent_browser, verify_ssl) as scraper:
        tasks = [scraper.scrape_url(url) for url in urls]
        return await asyncio.gather(*tasks)
