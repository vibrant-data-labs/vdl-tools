import asyncio
from concurrent.futures import ProcessPoolExecutor as ProcessPool


from enum import Enum
from more_itertools import chunked
import pandas as pd
from urllib.parse import urljoin, urlparse

import vdl_tools.scrape_enrich.scraper.website_processor as wp
from vdl_tools.scrape_enrich.scraper.async_scraper import AsyncScraper
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesScraped, WebPagesParsed
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text


class PageType(Enum):
    INDEX = "index",
    PAGE = "page"


MAX_ERRORS = 5


# if the page from one of these websites is to be scraped
# then the scraper should only take the first page
SINGLE_PAGE_WEBSITES = [
    'medium.com',
    'facebook.com',
    'linkedin.com',
    'linktr.ee',
    'about.me',
    'wikipedia.org',
    'github.com',
    'scholar.google.com/',
    'meetup.com',
]


BAD_URL_PATH_CHARS = '/?=&#@'


def check_is_single_page_websites(url: str, single_page_websites: list = []):
    return len([x for x in [*SINGLE_PAGE_WEBSITES, *single_page_websites] if x in url.lower()]) > 0


def __clean_website_path(value: str) -> str:
    '''
    Clean the website subpath

    /pages/contact -> pages_contact
    /pages/contact/ -> pages_contact
    /about-us -> about-us
    /page?page_title=about-us -> page_page_title_about-us
    '''
    if 'www' in value:
        logger.warning(f"Invalid value because path contains 'www': {value}")
        return None
    try:
        path = value.split(
            '/', maxsplit=1)[1] if value.startswith('/') else value
        path = path[:-1] if path.endswith('/') else path

        for c in BAD_URL_PATH_CHARS:
            path = path.replace(c, '_')

        return path
    except Exception as ex:
        logger.warning(f'Error cleaning website path: {value}')
        return None


def extract_website_name(value: str) -> str:
    '''
    Clean the website name

    https://google.com -> google.com
    https://google.com/ -> google.com
    https://somewebsite.com:port -> somewebsite.com_port
    https://coolwebsite.com/inner/page/1 -> coolwebsite.com_inner_page_1
    https://anotherwebsite.com?query=my-query -> anotherwebsite.com
    '''
    try:
        if "//" in value:
            website = value.split('//')[1].strip()
        else:
            website = value
        website = website.split('?')[0].strip()
        # remove trailing slash
        website = website[:-1] if website.endswith('/') else website
        return website.replace('/', '_').replace(':', '_')
    except Exception as ex:
        logger.warning(f'Error getting website name: {value}')
        return None


def ensure_url_scheme(url: str) -> str:
    """Ensure URL has a scheme (https:// by default).

    This is necessary for urljoin to work correctly when constructing
    internal page URLs from relative paths.
    """
    if not url:
        return url
    if not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url


def _parsed_row_is_retryable(num_errors, max_errors):
    """Whether a WebPagesParsed row is a prior combine failure that should be retried.

    Retryable means it has between 1 and max_errors-1 errors (inclusive of 1). Successful
    rows (0 errors) and rows that have hit the limit (>= max_errors) are NOT retryable:
    the former are already done, the latter have been given up on. Mirrors the scrape-layer
    retry condition used for urls_to_scrape so max_errors is honored on the parse layer too.
    """
    return 1 <= (num_errors or 0) < max_errors


def process_website_text(url, data, add_section_links=False):
    return wp.get_page_text(url, data, add_section_links=add_section_links)


def get_netloc(url):
    if not url:
        return
    netloc = urlparse(url).netloc
    if 'www.' in netloc:
        netloc = netloc.replace('www.', '')
    return netloc

def process_scraped_content(
    scraped_result: dict,
    cache_id: str,
    data_type: PageType = PageType.INDEX,
    clean_path: str = None,
    root_path: str = None,
    single_page_websites: list = [],
    clean_text=True,
    return_raw_html: bool = False,
    filter_no_body: bool=True,
    add_section_links=False,
):
    """
    Process raw scraped content into structured data format.
    Replaces get_page_data/load_website synchronous logic.
    """
    res = []
    url = scraped_result['url']
    web_content = scraped_result['content']
    status_code = scraped_result.get('status_code', 0)

    root_url = url if not root_path else root_path
    subpath = '/' if data_type == PageType.INDEX else clean_path

    logger.info("Processing page data for %s", url)

    if web_content:
        # Check for empty body if required
        if filter_no_body and len(web_content.strip()) < 50:  # Simple check, refine if needed
             logger.warning('Received empty page content for %s', url)
             # Mark as error if body is effectively empty
             res.append({
                "cleaned_key": cache_id,
                "full_path": url,
                "home_url": root_url,
                "subpath": subpath,
                "raw_html": "",
                "parsed_html": "",
                "response_status_code": status_code,
                "num_errors": 1,
                "page_type": str(data_type)
            })
             return res

        website_text = process_website_text(
            url,
            web_content,
            add_section_links=add_section_links,
        )
        res.append({
            "cleaned_key": cache_id,
            "full_path": url,
            "home_url": root_url,
            "subpath": subpath,
            "parsed_html": website_text,
            "response_status_code": status_code,
            "num_errors": 0,
            "page_type": str(data_type),
            "raw_html": web_content if return_raw_html else "",
        })

        is_single_page = check_is_single_page_websites(url, single_page_websites)

        if is_single_page:
            logger.debug(f'{url} is marked as single page website, skipping internal pages')

    else:
        logger.warning(f"Failed to receive data for {url}, marking it as error")
        res.append({
            "cleaned_key": cache_id,
            "full_path": url,
            "home_url": root_url,
            "subpath": subpath,
            "raw_html": "",
            "parsed_html": "",
            "response_status_code": status_code,
            "num_errors": 1,
            "page_type": str(data_type)
        })

    if clean_text:
        for row in res:
            row['parsed_html'] = clean_scraped_text(row['parsed_html'])
            row['parsed_html'] = row['parsed_html'].replace("\x00", "\uFFFD")
            # Have to replace NULL text for SQL to work
            if isinstance(row['raw_html'], bytes):
                row['raw_html'] = "PDF file"
            row['raw_html'] = row['raw_html'].replace("\x00", "\uFFFD")

    return res


def _should_retry_with_browser(scraped_result: dict, processed_rows: list) -> bool:
    """Whether a scraped page should be retried through the Playwright browser.

    Trigger: the plain HTTP fetch "succeeded" (content present) but text
    extraction came up empty — the JS-shell class, where a 200 response is just
    a React/Vue mount-point div plus scripts and all content renders client
    side. The browser executes JS, so it can produce what HTTP cannot. Pages
    already fetched via the browser are not retried again.
    """
    if scraped_result.get('method') != 'http':
        return False
    content = scraped_result.get('content')
    if not content or not isinstance(content, str):
        return False
    # PDFs never extract via this path; a browser won't change that
    if scraped_result.get('url', '').endswith('.pdf'):
        return False
    return all(not (row.get('parsed_html') or '').strip() for row in processed_rows)


async def _process_scraped_with_browser_retry(
    scraper,
    scraped_result: dict,
    **process_kwargs,
):
    """Process a scraped page, retrying via the browser when extraction is empty.

    Returns (scraped_result, processed_rows). When the retry produces content,
    scraped_result is the browser-rendered version so downstream consumers
    (link extraction) also see the rendered DOM; otherwise the original result
    and rows are returned unchanged.
    """
    processed_rows = process_scraped_content(scraped_result, **process_kwargs)
    if not _should_retry_with_browser(scraped_result, processed_rows):
        return scraped_result, processed_rows

    url = scraped_result['url']
    logger.info(
        "Empty extracted text for %s despite HTTP %s, retrying with browser",
        url,
        scraped_result.get('status_code'),
    )
    browser_content = await scraper.fetch_browser(url)
    if not browser_content:
        logger.warning("Browser retry produced no content for %s", url)
        return scraped_result, processed_rows

    retried_result = {
        **scraped_result,
        'content': browser_content,
        'method': 'browser',
        'success': True,
    }
    return retried_result, process_scraped_content(retried_result, **process_kwargs)


def __combine_texts_parallel(args):
    try:
        index_key, source, data, prompt_str_for_counting = args
        combined_text = make_group_text(prompt_str_for_counting, data)
        return index_key, source, combined_text
    except Exception as e:
        raise e
    finally:
        # Clean up any large objects created in this process
        if 'data' in locals():
            del data
        if 'combined_text' in locals():
            del combined_text


def scrape_websites_psql(
    urls: list,
    session=None,
    skip_existing: bool = True,
    subpage_type='about',
    single_page_websites: list = [],
    n_per_commit: int = 10,
    max_errors: int = MAX_ERRORS,
    max_workers: int = 5,  # Default workers - can be adjusted based on system resources
    return_raw_html: bool = False,
    filter_no_body: bool = True,
    add_section_links: bool = False,
    summary_prompt: str = None,
    return_combined_res: bool = True,
    verify_ssl: bool = True,
    max_per_subpath: int = 6,
    max_per_host: int = 3,  # Cap concurrent requests per host to avoid 429s. None disables.
) -> pd.DataFrame:

    urls_ids = [(ensure_url_scheme(url), extract_website_name(url)) for url in urls]

    with get_session(session=session) as session:

        if return_combined_res:
            # Step 1: Check which URLs already exist in WebPagesParsed
            logger.info("Checking which URLs already exist in WebPagesParsed...")
            existing_parsed_keys = {}
            existing_parsed_data = []
            if skip_existing:
                existing_parsed = session.query(WebPagesParsed).filter(
                    WebPagesParsed.cleaned_home_key.in_([x[1] for x in urls_ids])
                ).all()
                parsed_key_to_num_errors = {x.cleaned_home_key: (x.num_errors or 0) for x in existing_parsed}
                existing_parsed_keys = parsed_key_to_num_errors
                # Return previously-parsed rows as-is ONLY when we are not going to retry
                # them. Retryable rows (1 <= num_errors < max_errors) are excluded here and
                # re-combined below, so they are neither returned stale nor duplicated.
                existing_parsed_data = [
                    x.to_dict() for x in existing_parsed
                    if not _parsed_row_is_retryable(x.num_errors, max_errors)
                ]
            else:
                existing_parsed_keys = {}
                parsed_key_to_num_errors = {}

            # URLs that need processing (not in WebPagesParsed)
            urls_to_process = [
                (url, website_id) for url, website_id in urls_ids if
                website_id not in existing_parsed_keys or
                parsed_key_to_num_errors[website_id] < max_errors
            ]
        else:
            urls_to_process = urls_ids

        # Step 2: Check which URLs already exist in WebPagesScraped
        logger.info("Checking which URLs already exist in WebPagesScraped...")
        existing_scraped_keys = dict()
        existing_scraped_data = []
        if skip_existing:
            existing_scraped = session.query(
                WebPagesScraped.cleaned_key,
                WebPagesScraped.home_url,
                WebPagesScraped.num_errors
            ).filter(
                WebPagesScraped.cleaned_key.in_([x[1] for x in urls_to_process]),
                WebPagesScraped.page_type == str(PageType.INDEX)
            ).all()
            existing_scraped_keys = {x.cleaned_key: x.num_errors for x in existing_scraped}
            existing_home_urls = {x.home_url for x in existing_scraped if x.num_errors < max_errors}

            # Fetch all scraped data for URLs that need processing
            if urls_to_process:
                columns = [
                    WebPagesScraped.num_errors,
                    WebPagesScraped.page_type,
                    WebPagesScraped.home_url,
                    WebPagesScraped.subpath,
                    WebPagesScraped.parsed_html,
                    WebPagesScraped.response_status_code,
                    WebPagesScraped.cleaned_key,
                ]
                if return_raw_html:
                    columns.append(WebPagesScraped.raw_html)

                scraped_data = session.query(*columns).filter(
                    WebPagesScraped.home_url.in_(existing_home_urls)
                ).all()

                for x in scraped_data:
                    existing_scraped_data.append(
                        {
                            "num_errors": x.num_errors,
                            "page_type": x.page_type,
                            "home_url": x.home_url,
                            "subpath": x.subpath,
                            "parsed_html": x.parsed_html,
                            "response_status_code": x.response_status_code,
                            "raw_html": x.raw_html if return_raw_html else "",
                            "cleaned_key": x.cleaned_key,
                        }
                    )

        # URLs that need scraping (not in WebPagesScraped or have errors)
        urls_to_scrape = [
            x for x in urls_to_process
            if x[1] not in existing_scraped_keys or
            1 <= existing_scraped_keys[x[1]] < max_errors
        ]

        # Step 3: Scrape URLs that don't exist in WebPagesScraped
        newly_scraped_data = []

        async def process_scraping_job(chunks, existing_scraped_keys):
            """Process scraping jobs asynchronously with proper error handling."""
            # Configure Async Scraper Limits
            max_http = min(20, max_workers * 4)
            max_browser = min(3, max_workers)

            async with AsyncScraper(max_http, max_browser, verify_ssl=verify_ssl, max_per_host=max_per_host) as scraper:
                for i, chunk in enumerate(chunks):
                    logger.info(f"Scraping chunk {i+1} / {len(chunks)}")
                    chunk_urls = [x[0] for x in chunk]

                    # Scrape main pages with error handling
                    tasks = [scraper.scrape_url(url) for url in chunk_urls]
                    scrape_results = await asyncio.gather(*tasks)

                    # Process main pages
                    internal_links_to_scrape = []
                    for res, (original_url, website_id) in zip(scrape_results, chunk):

                        # Process the main index page (with a browser retry when
                        # a 200 HTTP fetch yields no extractable text)
                        res, processed_pages = await _process_scraped_with_browser_retry(
                            scraper,
                            res,
                            cache_id=website_id,
                            data_type=PageType.INDEX,
                            clean_path=None,
                            root_path=original_url,
                            single_page_websites=single_page_websites,
                            return_raw_html=return_raw_html,
                            filter_no_body=filter_no_body,
                            add_section_links=add_section_links
                        )
                        # Store main page
                        for row in processed_pages:
                            if row['num_errors']:
                                row['num_errors'] += existing_scraped_keys.get(row['cleaned_key'], 0)
                            webpage_obj = WebPagesScraped(**row)
                            session.merge(webpage_obj)
                            newly_scraped_data.append(webpage_obj.to_dict())

                        # Check for internal pages to scrape (if not single page app)
                        is_single_page = check_is_single_page_websites(original_url, single_page_websites)
                        if res.get('success') and not is_single_page and res.get('content'):
                            # Extract links synchronously here (fast operation on local HTML)
                            links = wp.extract_website_links(original_url, res['content'], subpage_type, max_per_subpath=max_per_subpath)
                            if links:
                                for link in links:
                                    clean_path = __clean_website_path(link)
                                    if not clean_path:
                                        continue
                                    full_path = f'{website_id}/{clean_path}'
                                    full_url = urljoin(original_url, link)
                                    internal_links_to_scrape.append((full_url, full_path, original_url, link))

                    # Scrape internal pages
                    if internal_links_to_scrape:
                        logger.info(f"Scraping {len(internal_links_to_scrape)} internal pages for chunk {i+1}")
                        internal_urls = [x[0] for x in internal_links_to_scrape]

                        internal_tasks = [scraper.scrape_url(url) for url in internal_urls]
                        internal_results = await asyncio.gather(*internal_tasks)

                        for int_res, (full_url, full_path, root_url, clean_path) in zip(internal_results, internal_links_to_scrape):

                            int_res, processed_internal = await _process_scraped_with_browser_retry(
                                scraper,
                                int_res,
                                cache_id=full_path,
                                data_type=PageType.PAGE,
                                clean_path=clean_path,
                                root_path=root_url,
                                return_raw_html=return_raw_html,
                                filter_no_body=filter_no_body,
                                add_section_links=add_section_links
                            )
                            for row in processed_internal:
                                if row['num_errors']:
                                    row['num_errors'] += existing_scraped_keys.get(row['cleaned_key'], 0)
                                webpage_obj = WebPagesScraped(**row)
                                session.merge(webpage_obj)
                                newly_scraped_data.append(webpage_obj.to_dict())

                    session.commit()

        if urls_to_scrape:
            logger.info(f"Scraping {len(urls_to_scrape)} websites...")

            # Batch URLs for async scraping
            # We do it in chunks to avoid overwhelming everything at once and allow partial commits
            scrapping_chunks = list(chunked(urls_to_scrape, n_per_commit))

            interrupted = False
            try:
                asyncio.run(process_scraping_job(scrapping_chunks, existing_scraped_keys))
            except KeyboardInterrupt:
                logger.warning("Received KeyboardInterrupt, returning the currently scraped data...")
                interrupted = True
                # Commit any pending data before returning
                try:
                    session.commit()
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Error during scraping: {e}")
                # Try to commit any successful scrapes before re-raising
                try:
                    session.commit()
                except Exception:
                    pass
                raise

            if interrupted:
                logger.info(f"Returning {len(newly_scraped_data)} scraped pages after interruption")

        # Combine existing and newly scraped data
        all_scraped_data = existing_scraped_data + newly_scraped_data
        all_scraped_data_df = _format_scraped_sites(list(all_scraped_data))

        if not return_combined_res:
            return all_scraped_data_df

        # Step 4: Combine the scraped data for URLs that need processing
        combined_data = []
        if all_scraped_data_df.shape[0] > 0:
            # Add cleaned_home_key to the dataframe so we can look up the data by home_url
            scraped_df_for_combining = all_scraped_data_df.copy()
            scraped_df_for_combining['cleaned_home_key'] = scraped_df_for_combining['source'].apply(extract_website_name)

            # Combine data for websites that have scraped data and either have no parsed
            # row yet OR have a prior parse failure still under the retry limit. The
            # retryable clause mirrors the scrape-layer logic so max_errors is honored on
            # the parse layer too (previously a single failure was permanent).
            available_keys = set(scraped_df_for_combining['cleaned_home_key'].unique())
            unfound_index_rows = [
                (url, website_id) for url, website_id in urls_to_process if
                website_id in available_keys and
                (website_id not in existing_parsed_keys or
                 _parsed_row_is_retryable(existing_parsed_keys[website_id], max_errors))
            ]
            unfound_chunks = list(chunked(unfound_index_rows, n_per_commit))
        else:
            unfound_index_rows = []
            unfound_chunks = []

            if unfound_index_rows:
                logger.info(
                    "Starting to combine texts for %s chunks of %s total home website urls",
                    len(unfound_chunks),
                    len(unfound_index_rows),
                )

        prompt_str_for_counting = summary_prompt or "test prompt " * 100

        # Use ProcessPool for CPU-intensive text combination
        with ProcessPool(max_workers=max_workers) as executor:
            for i, chunk in enumerate(unfound_chunks):
                logger.info(f"Processing chunk {i+1}/{len(unfound_chunks)}")
                try:
                    # Prepare arguments for parallel processing
                    chunk_args = [
                        (
                            index_key,
                            source,
                            scraped_df_for_combining[scraped_df_for_combining['cleaned_home_key'] == index_key],
                            prompt_str_for_counting
                        )
                        for source, index_key in chunk
                        if scraped_df_for_combining[scraped_df_for_combining['cleaned_home_key'] == index_key].shape[0] > 0
                    ]
                    # Process chunk and immediately clear results
                    results = list(executor.map(__combine_texts_parallel, chunk_args))

                    # Process results and clear them one by one
                    for result in results:
                        index_key, source, combined_text = result
                        if not combined_text:
                            combined_obj = WebPagesParsed(
                                cleaned_home_key=index_key,
                                home_url=source,
                                combined_text=combined_text,
                                num_errors=parsed_key_to_num_errors.get(index_key, 0) + 1,
                            )
                        else:
                            combined_obj = WebPagesParsed(
                                cleaned_home_key=index_key,
                                home_url=source,
                                combined_text=combined_text,
                            )
                        session.merge(combined_obj)
                        combined_data.append(combined_obj.to_dict())
                        # Clear individual result
                        del result

                    # Clear the entire results list
                    results = None

                    # Commit and clear session
                    session.commit()
                    session.expire_all()  # Clear session cache

                except Exception as e:
                    logger.error(f"Error processing chunk {i+1}: {str(e)}")
                    continue
                finally:
                    # Force garbage collection after each chunk
                    import gc
                    gc.collect()

        # Combine existing and newly combined data
        all_combined_data = existing_parsed_data + combined_data

        # Step 5: Format the results
        combined_res = pd.DataFrame(all_combined_data)

        return combined_res

def _format_scraped_sites(results):
    final_res = []
    for row in results:
        final_res.append({
            "type": row['page_type'],
            "source": row['home_url'],
            "subpath": row['subpath'],
            "text": row['parsed_html'],
            "response_status_code": row['response_status_code'],
            "raw_html": row.get('raw_html', ''),
            "cleaned_key": row['cleaned_key'],
        })
    return pd.DataFrame(final_res)


if __name__ == '__main__':
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    urls = [
        'https://www.vibrantdatalabs.org',
        'https://www.spicqyxdl.com/',
        'https://elementalimpact.com/',
        'https://elementalimpact.com/funding-opportunities/commercial-projects/'
    ]
    combined_res = scrape_websites_psql(
        urls,
        add_section_links=True,
        skip_existing=True,
        subpage_type='about',
        return_combined_res=True,
        single_page_websites=['https://elementalimpact.com/funding-opportunities/commercial-projects/']
    )
