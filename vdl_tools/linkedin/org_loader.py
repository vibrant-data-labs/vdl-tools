import json
import urllib

from more_itertools import chunked
import pandas as pd
from vdl_tools.scrape_enrich.scraper.scrape_websites import extract_website_name
from vdl_tools.shared_tools.database_cache.database_models.linkedin_orgs import LinkedInOrganization
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger as log
from vdl_tools.shared_tools.tools.text_similarity import similar, jaccard_similarity

from vdl_tools.linkedin.cache import LinkedInCache, LinkedInRawDataCache
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id
import vdl_tools.linkedin.handlers.base_handler as bh
from vdl_tools.linkedin.handlers.coresignal_bulk import get_bulk_organization_results
import vdl_tools.linkedin.processors.coresignal_processor as csp
from vdl_tools.linkedin.handlers.coresignal_query import (
    get_company,
    get_clean_company,
    get_multi_source_company,
    search_es_dsl,
    EXCLUDED_LINKEDINS
)
import vdl_tools.linkedin.processors.raw_org_processor as rop
from vdl_tools.linkedin.utils.shared import is_error_item, save_dataset, total_list_handler


MAX_ERRORS = 3
import logging
log.setLevel(logging.INFO)

SCRAPING_DATASOURCE = "linkedin_scraping"
CORESIGNAL_DATASOURCE = "coresignal"



def get_org_cache_item(li_id: str, cache: LinkedInCache, raw_cache: LinkedInRawDataCache):
    cache_item = cache.get_cache_item(li_id)
    if cache_item:
        json_data = json.loads(cache_item)
        return 'json', json_data
    cache_item = raw_cache.get_cache_item(li_id)
    if cache_item:
        return 'html', cache_item

    return None, None


def process_org_data(li_id: str, item_type, cache_item):
    if item_type == 'json':
        return csp.process_organization(cache_item)
    
    if item_type == 'html':
        return rop.process_org(li_id, cache_item)

    return None


def scrape_organizations(
    urls: pd.Series,
    skip_existing: bool=True,
    load_cache_only: bool=False,
    configuration: dict = None,
) -> pd.DataFrame:

    log.warn('Cache initialization...')
    json_cache = LinkedInCache('organization', configuration)
    raw_cache = LinkedInRawDataCache('organization', configuration)
    config = json_cache.config

    log.warn('Cache initialized. Loading the total file...')
    total_cache_item = raw_cache.get_cache_item(raw_cache.TOTAL_KEY)

    log.warn('Total file loaded')
    if load_cache_only:
        if total_cache_item:
            return pd.DataFrame(json.loads(total_cache_item))
        else:
            log.error('No cache found. Please run the scraper first, set `load_cache_only` to False.')
            return pd.DataFrame([])

    res = json.loads(total_cache_item) if total_cache_item else []
    req_keys = urls.apply(extract_linkedin_id).to_list()
    res = [x for x in res if extract_linkedin_id(x['url']) in req_keys]
    res_keys = { extract_linkedin_id(x['url']): i for i, x in enumerate(res)}

    for _, value in urls.items():
        try:
            linkedin_id = extract_linkedin_id(value)
            if skip_existing and linkedin_id in res_keys:
                log.warn(f'({linkedin_id}) is in cache and total file, skipping...')
                idx = res_keys[linkedin_id]
                res[idx]['original_id'] = value
                continue

            if is_error_item(linkedin_id, json_cache, raw_cache):
                continue

            item_type, cache_item = get_org_cache_item(linkedin_id, json_cache, raw_cache)
            if cache_item:
                org_data = process_org_data(linkedin_id, item_type, cache_item)
                org_data['original_id'] = value
                res.append(org_data)
                total_list_handler(res, raw_cache)
                continue

            item_type, data = bh.load_organization(linkedin_id, config, json_cache, raw_cache)
            if data:
                org_data = process_org_data(linkedin_id, item_type, data)
                org_data['original_id'] = value
                res.append(org_data)

            total_list_handler(res, raw_cache)
        except KeyboardInterrupt:
            log.warn("Received Keyboard Interrupt, exiting gracefully...")
            total_list_handler(res, raw_cache, force_save=True)
            break

    save_dataset(res, raw_cache)
    return pd.DataFrame(res)


def _tuple_to_dict(item):
    return {field: getattr(item, field) for field in  item._fields}


def process_result(result):
    primary_location = [
        x for x in result["company_locations_collection"]
        if x['deleted'] == 0 and x['is_primary'] == 1
    ]
    if primary_location:
        hq_location = primary_location[0]["location_address"]
    else:
        hq_location = ""
        if result.get('headquarters_new_address'):
            hq_location += f"{result.get('headquarters_new_address')}, "
        if result.get('headquarters_country_parsed'):
            hq_location += result.get('headquarters_country_parsed')


    processed_result_dict = {
        "linkedin_id": extract_linkedin_id(result['canonical_url']),
        "original_id": result['canonical_url'],
        "coresignal_id": result['id'],
        "name": result['name'],
        "image": result['logo_url'],
        "url": result['canonical_url'],
        "summary": result['description'],
        "about": result['description'],
        "website": result['website'],
        "industry": result['industry'],
        "specialties": [x['specialty'] for x in result.get('company_specialties_collection')],
        "company_size": result['size'],
        "hq_location": hq_location,
        "company_type": result['type'],
        "founded": result['founded'],
        "locations": [json.dumps(x) for x in result["company_locations_collection"]],
        "raw_html": json.dumps(result),
        "datasource": CORESIGNAL_DATASOURCE,
        "num_errors": 0
    }
    return processed_result_dict


def process_multi_source_result(result):
    if not result:
        return None

    processed_result_dict = {
        "linkedin_id": extract_linkedin_id(result['linkedin_url']),
        "original_id": result['linkedin_url'],
        "coresignal_id": result['id'],
        "name": result['company_name'],
        "image": result['company_logo'],
        "url": result['website'],
        "summary": result['description'],
        "about": result['description'],
        "website": result['website'],
        "industry": result['industry'],
        "specialties": [],
        "company_size": result['size_range'],
        "hq_location": result['hq_full_address'],
        "company_type": result['type'],
        "founded": result['founded_year'],
        "locations": json.dumps(result['company_locations_full']),
        "raw_html": json.dumps(result),
        "datasource": CORESIGNAL_DATASOURCE,
        "num_errors": 0
    }
    return processed_result_dict


def scrape_organizations_psql(
    urls: pd.Series,
    session,
    api_key,
    skip_existing: bool = True,
    n_per_commit: int = 10,
    max_errors=MAX_ERRORS,
    include_raw_html: bool = False,
) -> pd.DataFrame:

    log.info("Received %s LinkedIn urls for scraping", len(urls))
    urls_ids = [(url, extract_linkedin_id(url)) for url in urls]

    urls_ids = [x for x in urls_ids if x[1] not in EXCLUDED_LINKEDINS]

    if include_raw_html:
        KEY_COLUMNS.append(LinkedInOrganization.raw_html)

    found_rows = (
        session
        .query(*KEY_COLUMNS)
        .filter(
            LinkedInOrganization.linkedin_id.in_([x[1] for x in urls_ids]),
        )
        .all()
    )
    found_keys_to_errors = {x.linkedin_id: x.num_errors for x in found_rows}

    if skip_existing:
        unfound_rows = [
            x for x in urls_ids
            if x[1] not in found_keys_to_errors or 1 <= found_keys_to_errors[x[1]] < max_errors
        ]
        res = [_tuple_to_dict(x) for x in found_rows if x.num_errors == 0]
        # Don't want ones that errored before because that
        # means likely were unfound from coresignal originally
        linkedin_ids = [x[1] for x in unfound_rows if x[1] if not found_keys_to_errors.get(x[1])]

    else:
        unfound_rows = urls_ids
        res = []
        linkedin_ids = [x[1] for x in unfound_rows]

    if not unfound_rows:
        return pd.DataFrame(res)

    log.info("Found %s previously scraped results in our cache", len(found_rows))

    log.info("Trying bulk retrieval for %s results not found in cache", len(unfound_rows))
    # Try getting bulk results here

    if linkedin_ids:
        found_in_bulk_results = []

        # Need to map the returned results to the original urls we are sent.
        # The returned results don't have a concept of that, so need to do something....
        linkedin_ids_to_urls = {linkedin_id: original_url for original_url, linkedin_id in urls_ids}

        for chunk in chunked(linkedin_ids, 200):
            chunk_bulk_results = get_bulk_organization_results(
                linkedin_ids=chunk,
                api_key=api_key,
            )

            chunk_processed_results = []
            for result in chunk_bulk_results:
                original_sent_url = None
                # now we only have the coresignal processed result--any of these could be what we sent
                # Try each to see if we sent it and if we did, select that url
                potential_ids = [
                    result['canonical_shorthand_name'],
                    result["company_shorthand_name"],
                    result['source_id']
                ]
                for potential_id in potential_ids:
                    original_sent_url = linkedin_ids_to_urls.get(str(potential_id))
                    if not original_sent_url:
                        # Sometimes the linkedin_id is url encoded
                        original_sent_url = linkedin_ids_to_urls.get(urllib.parse.unquote(str(potential_id)))
                    if original_sent_url:
                        break
                if not original_sent_url:
                    log.warning("No originally sent url found for %s", result["company_shorthand_name"])
                    continue

                processed_result_dict = process_result(result)
                processed_result_dict['original_id'] = original_sent_url
                processed_result_dict['linkedin_id'] = extract_linkedin_id(original_sent_url)

                chunk_processed_results.append(original_sent_url)

                sql_obj = LinkedInOrganization(**processed_result_dict)
                session.merge(sql_obj)
                res.append(sql_obj.to_dict())

            log.info("Committing %s companies from bulk retrieval", len(chunk_processed_results))
            found_in_bulk_results.extend(chunk_processed_results)
            session.commit()
            log.info("Got %s of %s bulk results", len(found_in_bulk_results), len(linkedin_ids))

        unfound_rows = [x for x in unfound_rows if x[0] not in found_in_bulk_results]

    commit_group = []
    idx = 0
    len_unfound = len(unfound_rows)
    log.info("%s unfound through Coresignal, using HTML processing", len_unfound)
    for url, linkedin_id in unfound_rows:
        idx = idx + 1
        log.warn(f'({idx} / {len_unfound}) Processing {url}')
        try:
            data = bh.load_organization_no_cache(linkedin_id)
            if data:
                result = process_org_data(linkedin_id, 'html', data)
                result['original_id'] = url
                result['hq_location'] = result.pop('hq location')

                result['linkedin_id'] = linkedin_id
                result['raw_html'] = data
                result['num_errors'] = 0
                result["datasource"] = SCRAPING_DATASOURCE
            else:
                result = {
                    'linkedin_id': linkedin_id,
                    'original_id': url,
                    'num_errors': 1,
                }

            commit_group.append(result)
        except KeyboardInterrupt:
            log.warn("Received KeyboardInterrupt, returning the currently scraped data...")
            break
        except Exception as ex:
            log.warn(f'Error processing website {url}: {ex}')

        if len(commit_group) % n_per_commit == 0:
            for row in commit_group:
                if row['num_errors']:
                    row['num_errors'] += found_keys_to_errors.get(row['linkedin_id'], 0)
                sql_obj = LinkedInOrganization(**row)
                session.merge(sql_obj)
                res.append(sql_obj.to_dict())
            session.commit()
            commit_group = []

    # Final one
    for row in commit_group:
        if row['num_errors']:
            row['num_errors'] += found_keys_to_errors.get(row['linkedin_id'], 0)
        webpage_obj = sql_obj = LinkedInOrganization(**row)
        session.merge(webpage_obj)
        res.append(webpage_obj.to_dict())
    session.commit()

    return pd.DataFrame(res)


def get_organizations_by_coresignal_id(
    coresignal_ids: list[int],
    session,
    api_key,
    skip_existing: bool = True,
    n_per_commit: int = 10,
    max_errors=MAX_ERRORS,
    include_raw_html: bool = False,
    endpoint: str = 'company',
) -> pd.DataFrame:

    log.info("Received %s Coresignal Ids for retrieval", len(coresignal_ids))

    if include_raw_html:
        KEY_COLUMNS.append(LinkedInOrganization.raw_html)

    found_rows = (
        session
        .query(*KEY_COLUMNS)
        .filter(
            LinkedInOrganization.coresignal_id.in_(coresignal_ids),
        )
        .all()
    )
    found_keys_to_errors = {x.coresignal_id: x.num_errors for x in found_rows}

    if skip_existing:
        unfound_ids = [
            x for x in coresignal_ids
            if x not in found_keys_to_errors or 1 <= found_keys_to_errors[x] < max_errors
        ]
        res = [_tuple_to_dict(x) for x in found_rows if x.num_errors == 0]
        if not unfound_ids:
            return pd.DataFrame(res)

    else:
        unfound_ids = coresignal_ids
        res = []


    log.info("Found %s previously scraped results in our cache", len(found_rows))

    if unfound_ids:
        found_results = []
        log.info("Trying API retrieval for %s results not found in cache", len(unfound_ids))

        coresignal_function = get_company
        process_function = process_result

        if endpoint == 'clean_company':
            coresignal_function = get_clean_company
        elif endpoint == 'multi_source':
            coresignal_function = get_multi_source_company
            process_function = process_multi_source_result

        for chunk in chunked(unfound_ids, n_per_commit):
            for coresignal_id in chunk:
                process_function = process_result

                if endpoint == 'clean_company':
                    coresignal_function = get_clean_company
                elif endpoint == 'multi_source':
                    coresignal_function = get_multi_source_company
                    process_function = process_multi_source_result

                result = coresignal_function(
                    coresignal_id,
                    api_key=api_key,
                )
                if not result and endpoint == 'multi_source':
                    log.warning("No result found for %s in multi_source, trying classic", coresignal_id)
                    result = get_company(
                        coresignal_id,
                        api_key=api_key,
                    )
                    process_function = process_result
                    if not result:
                        log.warning("No result found for %s in classic either", coresignal_id)
                        continue

                processed_result_dict = process_function(result)

                sql_obj = LinkedInOrganization(**processed_result_dict)
                session.merge(sql_obj)
                res.append(sql_obj.to_dict())
                found_results.append(result)
            session.commit()

    return pd.DataFrame(res)


KEY_COLUMNS = [
    LinkedInOrganization.linkedin_id,
    LinkedInOrganization.coresignal_id,
    LinkedInOrganization.original_id,
    LinkedInOrganization.name,
    LinkedInOrganization.image,
    LinkedInOrganization.url,
    LinkedInOrganization.summary,
    LinkedInOrganization.about,
    LinkedInOrganization.website,
    LinkedInOrganization.industry,
    LinkedInOrganization.specialties,
    LinkedInOrganization.company_size,
    LinkedInOrganization.hq_location,
    LinkedInOrganization.company_type,
    LinkedInOrganization.founded,
    LinkedInOrganization.locations,
    LinkedInOrganization.datasource,
    LinkedInOrganization.num_errors,
]


def _clean_website(website_url):
    extracted_name = extract_website_name(website_url.lower())
    # extract_website_name replaces "/" with "_". We want to remove that
    # And anything that would come after
    extracted_name = extracted_name.split('_')[0]
    return extracted_name.replace('www.', '')


def add_original_name_match(
    df,
    names_list,
    remove_duplicates=True,
    similarity_threshold=0.7,
    jaccard_threshold=0.6,
):
    df['original_name'] = ""
    for name in names_list:
        temp_df = df.copy()
        temp_df['name_similarity'] = temp_df['name'].apply(lambda x: similar(x.lower(), name.lower()))
        temp_df['name_jaccard'] = temp_df['name'].apply(lambda x: jaccard_similarity(x.lower(), name.lower()))

        perfect_match_temp_df = temp_df[
            (temp_df['name_similarity'] == 1) |
            (temp_df['name_jaccard'] == 1)
        ]
        if perfect_match_temp_df.shape[0] == 1:
            temp_df = perfect_match_temp_df
        else:
            temp_df = temp_df[
                (temp_df['name_similarity'] >= similarity_threshold) |
                (temp_df['name_jaccard'] >= jaccard_threshold)
            ]
        df.loc[temp_df.index, 'original_name'] = name

    if not remove_duplicates:
        return df
    return dedupe_df(df, 'original_name')


def add_original_website_match(
    df,
    website_urls,
    remove_duplicates=True,
):
    cleaned_original_urls = pd.DataFrame(website_urls, columns=['original_url'])
    cleaned_original_urls['cleaned_original'] = (
        cleaned_original_urls['original_url'].apply(_clean_website)
    )
    df['cleaned_website'] = df['website'].apply(lambda x: _clean_website(x) if x else None)
    df = df.merge(
        cleaned_original_urls,
        left_on='cleaned_website',
        right_on='cleaned_original',
        how='left'
    )
    # Drop rows where the original_url is null
    df = df[df['original_url'].notnull()].copy()

    # Drop the columns we don't need anymore
    df.drop(columns=['cleaned_original', 'cleaned_website'], inplace=True)

    if not remove_duplicates:
        return df
    return dedupe_df(df, 'original_url')


def dedupe_group(group):
    group['is_max_followers'] = group['num_followers'] == group['num_followers'].max()
    group['is_max_employees'] = group['num_employees'] == group['num_employees'].max()
    group['dupe_vote'] = group['is_max_followers'].astype(int) + group['is_max_employees'].astype(int)
    group["is_max_dupe_vote"] = group['dupe_vote'] == group['dupe_vote'].max()
    if group['is_max_dupe_vote'].sum() == 1:
        chosen_id = group.sort_values('dupe_vote', ascending=False).iloc[0]['coresignal_id']
    else:
        chosen_id = group.sort_values('num_followers', ascending=False).iloc[0]['coresignal_id']
    return chosen_id


def dedupe_df(df, on_key):
    duplicate_mask = df.duplicated(subset=on_key, keep=False)
    non_dupes = df[~duplicate_mask]
    dupes = df[duplicate_mask].copy()

    dupes['raw_html'] = dupes['raw_html'].apply(json.loads)

    dupes['num_followers'] = dupes['raw_html'].apply(lambda x: x.get('followers', 0))
    dupes['num_employees'] = dupes['raw_html'].apply(lambda x: x.get('employees_count', 0))
    dupes['last_updated'] = dupes['raw_html'].apply(lambda x: x.get('last_updated', None))
    dupes['last_response_code'] = dupes['raw_html'].apply(lambda x: x.get('last_response_code', None))

    dupes = dupes[dupes['last_response_code'] == 200]

    grouped_dupes = dupes.groupby(on_key)

    chosen_rows = []

    for _, group in grouped_dupes:
        chosen_id = dedupe_group(group)
        chosen_row = df[df['coresignal_id'] == chosen_id]
        chosen_rows.append(chosen_row)
    non_dupes = pd.concat([non_dupes, *chosen_rows])
    return non_dupes


def get_organizations_by_search(
    api_key: str = None,
    linkedin_ids: list[str] = None,
    industry_filters: list[str] = None,
    website_urls: list[str] = None,
    name_filters: list[str] = None,
    employee_size_filters: list[str] = None,
    country_filters: list[str] = None,
    include_raw_html: bool = False,
    skip_existing: bool = True,
    attempt_original_matches: bool = True,
    remove_duplicates: bool = True,
    max_found_multiplier: int = 2,
    coresignal_endpoint: str = 'company',
):

    linkedin_ids = linkedin_ids or []
    industry_filters = industry_filters or []
    website_urls = website_urls or []
    name_filters = name_filters or []

    # Need the raw html to do the original matches
    if attempt_original_matches:
        include_raw_html = True

    if not any([linkedin_ids, industry_filters, website_urls, name_filters]):
        log.warning("No search parameters provided")
        return pd.DataFrame([])

    coresignal_ids = search_es_dsl(
        api_key=api_key,
        linkedin_ids=linkedin_ids,
        industry_filters=industry_filters,
        website_urls=website_urls,
        name_filters=name_filters,
        employee_size_filters=employee_size_filters,
        country_filters=country_filters,
    )

    if not coresignal_ids:
        log.warning("No results found for search query")
        return pd.DataFrame([])

    max_filter_set = max(
        [
            len(linkedin_ids),
            len(industry_filters),
            len(website_urls),
            len(name_filters),
        ]
    )

    if len(coresignal_ids) > max_found_multiplier * max_filter_set:
        log.warning("Too many results found %s please add more filters", len(coresignal_ids))
        return pd.DataFrame([])

    with get_session() as session:
        results = get_organizations_by_coresignal_id(
            coresignal_ids=coresignal_ids,
            session=session,
            api_key=api_key,
            include_raw_html=include_raw_html,
            skip_existing=skip_existing,
            endpoint=coresignal_endpoint,
        )

    if attempt_original_matches:
        if website_urls:
            results = add_original_website_match(
                results,
                website_urls,
                remove_duplicates=remove_duplicates,
            )
        if name_filters:
            results = add_original_name_match(
                results,
                name_filters,
                remove_duplicates=remove_duplicates
            )

    return results

if __name__ == '__main__':
    df = scrape_organizations(pd.Series(['https://www.linkedin.com/company/vibrant-data-labs/',
                                    'https://www.linkedin.com/company/hcl-enterprise/']))
