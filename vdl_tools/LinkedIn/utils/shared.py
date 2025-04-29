import json
from typing import List
import vdl_tools.shared_tools.tools.log_utils as log
from vdl_tools.linkedin.cache import LinkedInCache, LinkedInRawDataCache

LI_CACHE = LinkedInCache | LinkedInRawDataCache

def save_dataset(res: list, cache: LI_CACHE):
    data = cache.get_cache_item(cache.TOTAL_KEY)
    if data:
        data = json.loads(data)
    initial_len = len(data) if data else 0

    if not data:
        cache.store_item(cache.TOTAL_KEY, json.dumps(res), force=True)
        return
    if isinstance(data, list):
        data_keys = [x['url'] for x in data]

        for item in res:
            if item['url'] in data_keys:
                continue
            data.append(item)

    if len(data) > initial_len:
        cache.store_item(cache.TOTAL_KEY, json.dumps(data), force=True)

def total_list_handler(data: list, cache: LI_CACHE, force_save=False):
    if (len(data) % 1000 == 0) or force_save:
        log.warn(f'Processed {len(data)} items. Saving the full dataset...')
        save_dataset(data, cache)

def is_error_item(li_id: str, *args: List[LI_CACHE]):
    return any([x.is_error(li_id) for x in args])
