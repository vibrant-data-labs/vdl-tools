from vdl_tools.linkedin.cache import LinkedInCache, LinkedInRawDataCache
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id
import vdl_tools.linkedin.handlers.base_handler as bh
import vdl_tools.linkedin.handlers.coresignal_query as csq
import vdl_tools.linkedin.processors.coresignal_processor as csp
import vdl_tools.linkedin.processors.raw_profile_processor as rpp

import pandas as pd
import json
import vdl_tools.shared_tools.tools.log_utils as log

from vdl_tools.linkedin.utils.shared import is_error_item, save_dataset, total_list_handler


def get_profile_cache_item(li_id: str, cache: LinkedInCache, raw_cache: LinkedInRawDataCache):
    cache_item = cache.get_cache_item(li_id)
    if cache_item:
        json_data = json.loads(cache_item)
        return 'json', json_data
    cache_item = raw_cache.get_cache_item(li_id)
    if cache_item:
        return 'html', cache_item

    return None, None


def process_profile_data(li_id: str, item_type, cache_item):
    if item_type == 'json':
        return csp.process_profile(cache_item)
    
    if item_type == 'html':
        return rpp.process_profile(li_id, cache_item)

    return None


def scrape_profiles(urls: pd.Series, skip_existing: bool = True, configuration: dict = None) -> pd.DataFrame:
    json_cache = LinkedInCache('profile', configuration)
    raw_cache = LinkedInRawDataCache('profile', configuration)
    config = json_cache.config

    res = []
    for _, value in urls.items():
        try:
            linkedin_id = extract_linkedin_id(value)
            item_type, cache_item = get_profile_cache_item(linkedin_id, json_cache, raw_cache)
            if skip_existing and cache_item:
                profile_data = process_profile_data(linkedin_id, item_type, cache_item)
                profile_data['original_id'] = value
                res.append(profile_data)
                total_list_handler(res, json_cache)
                continue

            if is_error_item(linkedin_id, json_cache, raw_cache):
                continue

            item_type, data = bh.load_profile(linkedin_id, config, json_cache, raw_cache)
            if data:
                profile_data = process_profile_data(linkedin_id, item_type, data)
                profile_data['original_id'] = value
                res.append(profile_data)

            total_list_handler(res, json_cache)
        except KeyboardInterrupt:
            log.warn("Received Keyboard Interrupt, exiting gracefully...")
            break

    save_dataset(res, json_cache)
    return pd.DataFrame(res)

if __name__ == '__main__':
    df = scrape_profiles(pd.Series(['https://www.linkedin.com/in/mikhail-tyulyubaev-4a12a21b4/']))
