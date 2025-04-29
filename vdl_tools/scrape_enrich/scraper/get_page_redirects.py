import concurrent.futures

import requests

from vdl_tools.scrape_enrich.scraper.direct_loader import HEADERS
from vdl_tools.shared_tools.tools.logger import logger


def get_final_redirect(url):
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=30,
        )
        return response.url
    except Exception as e:
        logger.error("Error getting final redirect for %s: %s", url, e)
        return None


def get_final_redirects_parallel(urls):
    results = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_url = {executor.submit(get_final_redirect, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results[url] = result
            except Exception as e:
                logger.error("Error processing URL %s: %s", url, e)
                results[url] = None
    return results
