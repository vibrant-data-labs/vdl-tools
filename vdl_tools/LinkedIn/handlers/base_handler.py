import vdl_tools.shared_tools.tools.log_utils as log
from vdl_tools.linkedin.cache import LinkedInCache, LinkedInRawDataCache
import vdl_tools.linkedin.handlers.coresignal_query as cs_query
import vdl_tools.linkedin.handlers.direct_query as dq
import json

def load_profile(id: str, config: dict, cache: LinkedInCache, raw_cache: LinkedInRawDataCache):
    try:
        log.warn(f"({id}) Loading from CoreSignal...")
        if config.get('coresignal_api_key', None):
            res = cs_query.get_profile(id, config['coresignal_api_key'])
            if res:
                cache.store_item(id, json.dumps(res))
                return 'json', res
    except Exception as ex:
        cache.save_as_error(id)
        log.warn('Failed to load from CoreSignal')


    return None, None


def load_organization(id: str, config: dict, cache: LinkedInCache, raw_cache: LinkedInRawDataCache):
    try:
        log.warn(f"({id}) Loading using direct query")
        res = dq.get_organization(id)
        if res:
            raw_cache.store_item(id, res)
            return 'html', res
    except FileNotFoundError as fnfe:
        raw_cache.save_as_error(id)
        log.warn('Item does not exist')
        return None, None
    except Exception as ex:
        log.warn('Failed to load using direct query')

    return None, None


def load_organization_no_cache(id: str):
    try:
        log.warn(f"({id}) Loading using direct query")
        res = dq.get_organization(id)
        if res:
            return res
    except Exception as ex:
        log.warn('Failed to load using direct query')


    return None
