from concurrent.futures import ThreadPoolExecutor as ThreadPool
from concurrent.futures import ProcessPoolExecutor as ProcessPool
import threading

from enum import Enum
from more_itertools import chunked
import pandas as pd
from urllib.parse import urljoin, urlparse

import vdl_tools.scrape_enrich.scraper.direct_loader as dl
import vdl_tools.scrape_enrich.scraper.page_scraper as ps
import vdl_tools.scrape_enrich.scraper.website_processor as wp
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text, MIN_TEXT_LENGTH
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import WebPagesScraped, WebPagesParsed
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text


thread_local = threading.local()


def __get_driver():
    Driver = getattr(thread_local, 'driver', None)
    if Driver is None:
        Driver = ps.page_scraper()
        Driver.set_page_load_timeout(25)
        setattr(thread_local, 'driver', Driver)

    return Driver

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

__selenium_instance = None


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
        logger.warn("Invalid value because path contains 'www': {value}")
        return None
    try:
        path = value.split(
            '/', maxsplit=1)[1] if value.startswith('/') else value
        path = path[:-1] if path.endswith('/') else path

        for c in BAD_URL_PATH_CHARS:
            path = path.replace(c, '_')

        return path
    except Exception as ex:
        logger.warn(f'Error cleaning website path: {value}')
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


def process_website_text(url, data, add_section_links=False):
    return wp.get_page_text(url, data, add_section_links=add_section_links)


def get_netloc(url):
    if not url:
        return
    netloc = urlparse(url).netloc
    if 'www.' in netloc:
        netloc = netloc.replace('www.', '')
    return netloc


def load_website(url, scraper, filter_no_body=True, verify_ssl=True):
    data, status_code = dl.load_website_psql(
        url,
        filter_no_body=filter_no_body,
        verify_ssl=verify_ssl,
    )

    if data:
        return scraper, data, status_code

    data = ps.scrape_website(url, scraper)
    if data:
        status_code = 200

    return scraper, data, status_code


def get_page_data(
    url: str,
    cache_id: str,
    data_type: PageType = PageType.INDEX,
    clean_path: str = None,
    root_path: str = None,
    subpage_type: str = 'all',
    single_page_websites: list = [],
    clean_text=True,
    max_per_subpath: int = 6,
    scraper=None,
    return_raw_html: bool = False,
    filter_no_body: bool=True,
    add_section_links=False,
    verify_ssl: bool = True,
):
    res = []
    root_url = url if not root_path else root_path
    subpath = '/' if data_type == PageType.INDEX else clean_path

    scraper, web_content, status_code = load_website(
        url,
        scraper=scraper,
        filter_no_body=filter_no_body,
        verify_ssl=verify_ssl,
    )

    logger.info("Getting page data for %s", url)
    if web_content:
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

        if data_type == PageType.INDEX and not is_single_page:
            res.extend(process_internal_pages(
                url,
                web_content,
                subpage_type,
                max_per_subpath,
                scraper=scraper,
                return_raw_html=return_raw_html,
                filter_no_body=filter_no_body,
                add_section_links=add_section_links,
            ))
        elif is_single_page:
            logger.debug(f'{url} is marked as single page website, proceeding without scraping the internal pages')
    else:
        logger.warn(f"Failed to receive data for {url}, marking it as error")
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


def process_internal_pages(
    url: str,
    website_content: str,
    subpage_type:str,
    max_per_subpath:int = 6,
    scraper=None,
    return_raw_html: bool = False,
    filter_no_body: bool=True,
    add_section_links=False,
):
    links = wp.extract_website_links(url, website_content, subpage_type, max_per_subpath)
    res = []
    for link in links:
        website_id = extract_website_name(url)
        logger.debug(f'({website_id}) Processing {link}')
        clean_path = __clean_website_path(link)
        if not clean_path:
            continue
        full_path = f'{website_id}/{clean_path}'
        page_data = get_page_data(
            url=urljoin(url, link),
            cache_id=full_path,
            data_type=PageType.PAGE,
            clean_path=link,
            root_path=url,
            scraper=scraper,
            return_raw_html=return_raw_html,
            filter_no_body=filter_no_body,
            add_section_links=add_section_links,
        )
        res.extend(page_data)

    return res


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
    max_workers: int = 5,
    return_raw_html: bool = False,
    filter_no_body: bool = True,
    add_section_links: bool = False,
    summary_prompt: str = None,
    return_combined_res: bool = True,
    verify_ssl: bool = True,
) -> pd.DataFrame:
    def __get_page_data_parallel(
        url_website_id,
    ):
        url, website_id = url_website_id
        scraper = __get_driver()
        try:
            response = get_page_data(
                url,
                website_id,
                data_type=PageType.INDEX,
                subpage_type=subpage_type,
                single_page_websites=single_page_websites,
                scraper=scraper,
                return_raw_html=return_raw_html,
                filter_no_body=filter_no_body,
                add_section_links=add_section_links,
                verify_ssl=verify_ssl,
            )
            return response
        except Exception as ex:
            logger.warning(f'Error processing website: {url} {ex}')
            return []

    urls_ids = [(url, extract_website_name(url)) for url in urls]

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
                existing_parsed_data = [x.to_dict() for x in existing_parsed]
                parsed_key_to_num_errors = {x.cleaned_home_key: (x.num_errors or 0) for x in existing_parsed}
                existing_parsed_keys = parsed_key_to_num_errors
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
        if urls_to_scrape:
            logger.info(f"Scraping {len(urls_to_scrape)} websites...")
            # Scraping logic here (similar to current implementation)
            # Store results in newly_scraped_data

            logger.info("Starting to scrape %s websites...", len(urls_to_scrape))
            scrapping_chunks = list(chunked(urls_to_scrape, n_per_commit))
            with ThreadPool(max_workers=max_workers) as executor:
                for i, chunk in enumerate(scrapping_chunks):
                    try:
                        logger.info(f"Scraping chunk {i+1} / {len(scrapping_chunks)}")
                        results = list(executor.map(__get_page_data_parallel, chunk))
                        flat_results = [page for pagelist in results if pagelist  for page in pagelist]
                        for row in flat_results:
                            if row['num_errors']:
                                row['num_errors'] += existing_scraped_keys.get(row['cleaned_key'], 0)
                            webpage_obj = WebPagesScraped(**row)
                            session.merge(webpage_obj)
                            newly_scraped_data.append(webpage_obj.to_dict())
                        session.commit()
                    except KeyboardInterrupt:
                        logger.warn("Received KeyboardInterrupt, returning the currently scraped data...")
                        break

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

            # scraped_df_for_combining = scraped_df_for_combining[
            #     ~(
            #         (scraped_df_for_combining['text'].isnull()) |
            #         (scraped_df_for_combining['text'].apply(lambda x: len(x) <= MIN_TEXT_LENGTH))
            #     )
            # ]

            # Only combine data for websites that exist in WebPagesScraped but not in WebPagesParsed
            unfound_index_rows = [
                (url, website_id) for url, website_id in urls_to_process if
                website_id not in existing_parsed_keys and
                website_id in scraped_df_for_combining['cleaned_home_key'].unique()
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
    # res = scrape_websites_psql(
    #     urls,
    #     add_section_links=True,
    #     skip_existing=True,
    #     subpage_type='about',
    #     return_combined_res=False,
    # )