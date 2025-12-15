import vdl_tools.linkedin.handlers.coresignal_query as cs_query
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id
from vdl_tools.linkedin.processors import base_profile_processor as bpp
from vdl_tools.linkedin.processors import clean_profile_processor as cpp

import pandas as pd    

def process_profile_joined(linkedin_id:str, api_key: str):
    base_data = cs_query.get_base_person(linkedin_id, api_key)
    cleen_data = cs_query.get_clean_person(linkedin_id, api_key)

    # Process both profiles
    base_processed = bpp.process_profile(base_data)
    clean_processed = cpp.process_profile(cleen_data)
    
    # Join data
    joined_data = {**base_processed, **clean_processed}
    return joined_data


if __name__ == '__main__':
    from vdl_tools.shared_tools.tools.config_utils import get_configuration
    GLOBAL_CONFIG = get_configuration()
    process_profile_joined("alex-haber", GLOBAL_CONFIG["linkedin"]["coresignal_api_key"])