import json
import requests
import urllib

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.linkedin.utils.linkedin_url import extract_linkedin_id

_base_url = "https://api.coresignal.com/cdapi/v2"


ENDPOINTS = {
    'member': 'member',
    'company': 'company_base',
    'historical_headcount': 'historical_headcount',
    'clean_company': 'company_clean',
    'multi_source': 'company_multi_source',
}


EXCLUDED_LINKEDIN_URLS = {
    "https://www.linkedin.com/company/home",
    "https://www.linkedin.com/company/about",
    "https://www.linkedin.com/company/posts",
    "https://www.linkedin.com/company/mycompany"
}


EXCLUDED_LINKEDINS = [extract_linkedin_id(x) for x in EXCLUDED_LINKEDIN_URLS]


def _get_coresignal_base(_id: str, api_key: str, endpoint: str):
    headers = {
        'Content-Type': 'application/json',
        'apikey': api_key
    }
    if endpoint not in ENDPOINTS:
        raise ValueError(f'Invalid endpoint: {endpoint}')
    res = requests.get(
        f'{_base_url}/{ENDPOINTS[endpoint]}/collect/{_id}',
        headers=headers,
        timeout=60,
        )
    if not res.ok:
        logger.warning('CoreSignal response is not OK')
        logger.warning(res.text)
        return None

    logger.info(
        'CoreSignal response is ok. Collect Credits Remaining: %s',
        res.headers["x-credits-remaining"]
    )

    return res.json()


def get_company(id: str, api_key: str):
    return _get_coresignal_base(id, api_key, 'company')


def get_clean_company(id: str, api_key: str):
    return _get_coresignal_base(id, api_key, 'clean_company')


def get_multi_source_company(id: str, api_key: str):
    return _get_coresignal_base(id, api_key, 'multi_source')


def get_profile(id: str, api_key: str):
    return _get_coresignal_base(id, api_key, 'member')


def get_historical_headcount(id: str, api_key: str):
    return _get_coresignal_base(id, api_key, 'historical_headcount')


def create_esl_dsl_query(
    linkedin_ids: list[str] = None,
    industry_filters: list[str] = None, 
    website_urls: list[str] = None,
    name_filters: list[str] = None,
    employee_size_filters: list[str] = None,
    country_filters: list[str] = None,
    coresignal_ids: list[int] = None,
):
    filters = [
        {"bool": {"must": [{"terms": {"deleted": [0]} }]}}
    ]

    def _create_should_filter(field, values, filter_type='match'):
        return {
            "bool": {
                "should": [
                    {
                        filter_type: {
                            field: value
                        }
                    }
                    for value in values
                ]
            }
        }

    if linkedin_ids:
        linkedin_ids = list(set(linkedin_ids))
        # Seperate out the ids that are ints (likely the source id (linkedin's universal id))
        # and those that are strings (the shorthand name)
        int_linkedin_ids = []
        str_linkedin_ids = []
        for linkedin_id in linkedin_ids:
            if linkedin_id.isnumeric():
                int_linkedin_ids.append(int(linkedin_id))
            else:
                # Uncode in case we received an already encoded one
                uncoded_linkedin_id = urllib.parse.unquote(linkedin_id)
                str_linkedin_ids.append(urllib.parse.quote(uncoded_linkedin_id))

        filters.append(
            {
                "bool": {
                    "should": [
                        {
                            "terms": {
                                "shorthand_name": str_linkedin_ids
                            }
                        },
                        {
                            "terms": {
                                "canonical_shorthand_name": str_linkedin_ids
                            }
                        },
                        {
                            "terms": {
                                "source_id": int_linkedin_ids
                            }
                        }
                    ]
                }
            }
        )
    if name_filters:
        name_filters = [name.replace("/", "\/") for name in name_filters]
        match_phrase_bool = _create_should_filter("name", name_filters, "match_phrase")
        match_phrase_bool["bool"]["should"].extend([
              {
                "query_string": {
                  "query": name,
                  "fields": ["name"],
                  "default_operator": "AND"
                }
              }
              for name in name_filters
        ])
        filters.append(match_phrase_bool)

    if industry_filters:
        filters.append(
            _create_should_filter("industry", industry_filters, "match_phrase")
        )

    if website_urls:
        filters.append(
            _create_should_filter("website.filter", website_urls, "match_phrase")
        )

    if employee_size_filters:
        filters.append(
            _create_should_filter("employee_size", employee_size_filters, "term")
        )
    if country_filters:
        filters.append(
            _create_should_filter("headquarters_country_parsed", country_filters, "term")
        )
    if coresignal_ids:
        filters.append(
            _create_should_filter("id", coresignal_ids, "term")
        )

    must_not = [
        {"terms": {"canonical_shorthand_name": EXCLUDED_LINKEDINS}},
        {"terms": {"shorthand_name": EXCLUDED_LINKEDINS}},
    ]

    payload = {
        "query": {
            "bool": {
                "must": filters,
                "must_not": must_not,
            }
            },
            "sort": ["_score"]
        }
    logger.debug(json.dumps(payload, indent=2))
    return payload



def search_es_dsl(
    api_key: str = None,
    linkedin_ids: list[str] = None,
    industry_filters: list[str] = None, 
    website_urls: list[str] = None,
    name_filters: list[str] = None,
    employee_size_filters: list[str] = None,
    country_filters: list[str] = None,
    coresignal_ids: list[int] = None,
    coresignal_endpoint: str = 'company',
):
    query = create_esl_dsl_query(
        linkedin_ids=linkedin_ids,
        industry_filters=industry_filters,
        website_urls=website_urls,
        name_filters=name_filters,
        employee_size_filters=employee_size_filters,
        country_filters=country_filters,
        coresignal_ids=coresignal_ids,
    )

    headers = {
        'Content-Type': 'application/json',
        'apikey': api_key
    }

    endpoint = ENDPOINTS[coresignal_endpoint]
    res = requests.post(
        f'{_base_url}/{endpoint}/search/es_dsl',
        headers=headers,
        json=query,
        timeout=60,
    )
    if not res.ok:
        logger.warning('CoreSignal response is not OK')
        logger.warning(res.text)
        return None

    logger.info(
        'CoreSignal response is ok. Search Credits Remaining: %s',
        res.headers["x-credits-remaining"]
    )

    return res.json()


if __name__ == '__main__':
    import json
    print(
        json.dumps(
            create_esl_dsl_query(
                website_urls=[
                    "www.vibrantdatalabs.org",
                ],
            )['es_dsl_query'],
            indent=2,
        )
    )
