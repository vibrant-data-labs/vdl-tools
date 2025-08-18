from bs4 import BeautifulSoup
import re
import logging


from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links
from vdl_tools.shared_tools.tools.logger import logger as logger
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger('datasets').setLevel(logging.WARNING)


def get_page_text(url, html):
    if isinstance(html, bytes):
        return ""

    if url.endswith('pdf'):
        return ""

    page_text = clean_scraped_text(process_page_source(url, html))
    if page_text is None or len(page_text) < 500:
        logger.error("Failed to get page text from %s", url)
    return page_text


def process_page_source(url: str, source: str):
    if 'https://challenges.cloudflare.com' in source:
        logger.warn(f'Looks like Cloudflare protection is enabled for {url}. Data may be invalid')

    try:
        html = source
        soup = BeautifulSoup(html, "lxml")

        logger.debug(f"scraping text from ({url})")
    except Exception as e:
        logger.warn(f"URL {url} not valid")
        logger.warn(e)
        return None
    else:
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

            # Try to find main content areas
            content_selectors = [
                'main',
                '[role="main"]',
                '.content',
                '.main-content',
                '#content',
                '#main',
                'article',
                '.post-content',
                '.entry-content'
            ]

            content_element = None
            for selector in content_selectors:
                content_element = soup.select_one(selector)
                if content_element:
                    break

            if not content_element:
                # Fallback to body
                content_element = soup.find('body')

            if content_element:
                # Get text content
                text = content_element.get_text(separator=' ', strip=True)
                # Clean up whitespace
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
            return ""


def filter_anchors(url, links):
    urls = [
        url.replace('www.', ''),
        url[:-1] if url.endswith('/') else url,
        '/'
    ]

    res_links = []
    for x in links:
        matches = [s for s in urls if x.startswith(s) and len(x) > len(s)]
        if len(matches) == 0:
            continue

        # exclude links without protocol like '//www.example.com/contact' -> 'contact'
        if x.startswith('//'):
            split_parts = x[2:].split('/', maxsplit=1)
            split_parts = [s for s in split_parts if s]
            # skip the link if it is relative path to index like '/' or '//'
            if len(split_parts) <= 1:
                continue
            url_parts = split_parts[1]
        else:
            url_parts = x
        # exclude protocol if it is fully qualified url like 'https://example.com/contact' -> 'contact'
        url_parts = url_parts.split(
            '//')[1] if '//' in url_parts else url_parts
        # exclude the domain name and save only a relative path like 'example.com/contact' -> '/contact'
        url_parts = '/' + \
            '/'.join(url_parts.split('/')
                        [1:]) if x.startswith('http') else url_parts
        # skip the link if it is relative path to index like '/'
        if len(url_parts) == 1:
            continue

        # skip the link if it is a link to blob like '/promo.mp4' or '/splash.jpg'
        if '.' in url_parts:
            ext_parts = url_parts.split('.')
            if len(ext_parts) == 2 and ext_parts[1].lower() != 'html':
                continue

        res_links.append(url_parts)

    extracted_links = list(set(res_links))
    return extracted_links, res_links

def extract_website_links(
    url: str,
    website_content: str,
    subpage_type: str,
    max_per_subpath=6,
):
    try:
        html = website_content
        soup = BeautifulSoup(html, "lxml")

        logger.debug(f"getting links from ({url})")
    except Exception as e:
        logger.warning(f"URL {url} not valid")
        logger.warning(e)
        return None
    else:
        anchors = soup.find_all('a')
        links = [x.get('href') for x in anchors if x.get('href')]
        extracted_links, res_links = filter_anchors(url, links)

        if subpage_type == 'all':
            return extracted_links

        if subpage_type == 'about':
            return filter_links(
                links=list(set(res_links)),
                keep_paths=PATHS_TO_KEEP,
                max_per_subpath=max_per_subpath,
            )

        raise ValueError(f"Unknown filter strategy {subpage_type}")
