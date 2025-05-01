import json

from more_itertools import chunked
import pandas as pd
from vdl_tools.shared_tools.database_cache.database_models.linkedin_people import LinkedInPerson
from vdl_tools.shared_tools.tools.logger import logger

import vdl_tools.linkedin.handlers.coresignal_query as cs_query
from vdl_tools.linkedin.processors.coresignal_processor import process_profile
from vdl_tools.linkedin.org_loader import CORESIGNAL_DATASOURCE
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id


MAX_ERRORS = 5


def get_people_profile(
    urls: pd.Series,
    session,
    api_key,
    skip_existing: bool = True,
    n_per_commit: int = 10,
    max_errors=MAX_ERRORS,
) -> pd.DataFrame:

    logger.info("Received %s LinkedIn urls for querying", len(urls))
    urls_ids = [(url, extract_linkedin_id(url)) for url in urls if extract_linkedin_id(url)]
    
    query_columns = [
        k for k in LinkedInPerson.__table__.columns.keys()
        if k != 'raw_json'
    ]

    if skip_existing:
        found_rows = (
            session
            .query(
                *[getattr(LinkedInPerson, k) for k in  query_columns]
            )
            .filter(
                LinkedInPerson.linkedin_id.in_([x[1] for x in urls_ids]),
            )
            .all()
        )
        found_keys_to_errors = {x.linkedin_id: x.num_errors for x in found_rows}

        unfound_rows = [
            x for x in urls_ids
            if x[1] not in found_keys_to_errors or 1 <= found_keys_to_errors[x[1]] < max_errors
        ]
    else:
        found_rows = []
        unfound_rows = urls_ids

    def _tuple_to_dict(item):
        return {field: getattr(item, field) for field in  item._fields}

    res = [
        process_profile(_tuple_to_dict(x))
        for x in found_rows if x.num_errors == 0
    ]

    if not unfound_rows:
        return pd.DataFrame(res)

    newly_found = []
    logger.info("Found %s previously queried results in our cache", len(found_rows))
    for chunk in chunked(unfound_rows, n_per_commit):
        chunk_processed_results = []
        for url, linkedin_id in chunk:
            result = cs_query.get_profile(linkedin_id, api_key)

            if not result:
                logger.warning("No result for %s", url)
                result_clean = {
                    'linkedin_id': linkedin_id,
                    'original_id': url,
                    'num_errors': 1,
                }

            else:
                result_clean = {
                    k: result.get(k)
                    for k in LinkedInPerson.__table__.columns.keys()
                    if k in result
                }
                result_clean['linkedin_id'] = linkedin_id
                result_clean['original_id'] = url
                result_clean['datasource'] = CORESIGNAL_DATASOURCE
                result_clean['num_errors'] = 0
                result_clean["raw_json"] = result
                newly_found.append(url)

                processed_profile = process_profile(result)
                processed_profile['original_id'] = url
                res.append(processed_profile)

            sql_obj = LinkedInPerson(**result_clean)
            session.merge(sql_obj)

        logger.info("Committing %s pepole", len(chunk_processed_results))
        newly_found.extend(chunk_processed_results)
        session.commit()
        logger.info("Got %s of %s results", len(newly_found), len(unfound_rows))

    return pd.DataFrame(res)


if __name__ == '__main__':
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.tools.config_utils import get_configuration

    config = get_configuration()
    with get_session() as session:
        df = get_people_profile(
            ['https://www.linkedin.com/in/zeintawil/'],
            session,
            config['linkedin']['coresignal_api_key'],
        )
