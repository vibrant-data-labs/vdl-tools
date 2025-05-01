import concurrent.futures
from urllib.parse import urljoin

import requests
import pandas as pd

from vdl_tools.scrape_enrich.scraper.direct_loader import HEADERS
from vdl_tools.scrape_enrich.scraper.scrape_websites import SINGLE_PAGE_WEBSITES
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.linkedin.org_loader import get_organizations_by_search, scrape_organizations_psql


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


def clean_url(
    url,
    home_url=None,
    remove_socials=False,
    keep_only_home_url=False,
):
    if not url:
        return url
    if remove_socials:
        if any(social in url for social in SINGLE_PAGE_WEBSITES):
            return None
    url = url.strip()
    if url.startswith("/") and home_url:
        url = url.lstrip("/")
        url = urljoin(home_url, url)

    if not url.startswith('http'):
        url = f'https://{url}'
    url = url.replace("http://", "https://").rstrip("/")
    if not url.endswith(".pdf"):
        url = url.lower()

    if keep_only_home_url and not home_url:
        url = url.replace("https://", "").split("/")[0]
        url = f"https://{url}"
    return url
    

INDUSTRY_FILTERS = [
    "Banking",
    "Capital Markets",
    "Civic and Social Organizations",
    "Economic Programs",
    "Environmental Services",
    "Financial Services",
    "Government Administration",
    "Housing and Community Development",
    "Non-profit Organization Management",
    "Non-profit Organizations",
    "Real Estate",
    "Renewable Energy",
    "Renewable Energy Semiconductor Manufacturing",
    "Renewables & Environment",
    "Services for Renewable Energy",
    "Semiconductor Manufacturing",
    "Solar Electric Power Generation",
    "Venture Capital and Private Equity Principals",
]


def get_linkedin_by_urls(
    api_key,
    website_urls,
    use_cached_result=True,
    remove_duplicates=True,
    max_found_multiplier=2,
):
    if len(website_urls) <=3:
        max_found_multiplier = 4
    if len(website_urls) == 0:
        return {}
    results = get_organizations_by_search(
        api_key=api_key,
        website_urls=website_urls,
        include_raw_html=True,
        skip_existing=use_cached_result,
        attempt_original_matches=True,
        remove_duplicates=remove_duplicates,
        max_found_multiplier=max_found_multiplier,
    )
    return {x['original_url']: x for x in results.to_dict(orient='records')}


def get_linkedin_by_names(
    api_key,
    names,
    use_cached_result=True,
    remove_duplicates=True,
    add_country_filter=True,
    max_found_multiplier=2,
    industry_filters=INDUSTRY_FILTERS,
):
    if len(names) <=3:
        max_found_multiplier = 4
    if add_country_filter:
        country_filters = ['United States']
    else:
        country_filters = []
    if len(names) == 0:
        return {}

    results = get_organizations_by_search(
        api_key=api_key,
        name_filters=names,
        industry_filters=industry_filters,
        country_filters=country_filters,
        include_raw_html=True,
        skip_existing=use_cached_result,
        attempt_original_matches=True,
        remove_duplicates=remove_duplicates,
        max_found_multiplier=max_found_multiplier,
    )
    return {x['original_name']: x for x in results.to_dict(orient='records')}


def get_linkedin_urls(
    df,
    name_col,
    website_url_col,
    linkedin_url_col,
    api_key,
    add_country_filter=True,
    industry_filters=INDUSTRY_FILTERS,
    use_cached_result=True,
    max_found_multiplier=2,
):
    pd.options.mode.chained_assignment = None  # default='warn'
    df.loc[:, name_col] = df.loc[:, name_col].apply(lambda x: x.strip() if pd.notnull(x) else x)
    df.loc[:, website_url_col] = df.loc[:, website_url_col].apply(lambda x: x.strip()  if pd.notnull(x) else x)
    df.loc[:, linkedin_url_col] = df.loc[:, linkedin_url_col].apply(lambda x: x.strip() if pd.notnull(x) else x)

    has_linkedin_url_mask = df[linkedin_url_col].notnull()
    has_name_mask = df[name_col].notnull()
    has_website_mask = df[website_url_col].notnull()

    if df[~has_linkedin_url_mask].shape == 0:
        return df

    url_to_linkedin_data = get_linkedin_by_urls(
        api_key=api_key,
        website_urls=df[~has_linkedin_url_mask & has_website_mask][website_url_col].values.tolist(),
        use_cached_result=use_cached_result,
        max_found_multiplier=max_found_multiplier,
    )

    df[linkedin_url_col] = df.apply(
        lambda x: url_to_linkedin_data.get(x[website_url_col], {})
        .get('original_id', x[linkedin_url_col]),
        axis=1,
    )

    has_linkedin_url_mask = df[linkedin_url_col].notnull()
    if df[~has_linkedin_url_mask].shape == 0:
        return df

    # Sometimes the websites we get are old and have been redirected
    # Try to get the redirected site
    urls_to_redirect = (
        df[~has_linkedin_url_mask & has_website_mask]
        [website_url_col]
        .values
        .tolist()
    )

    urls_to_redirect = [
        clean_url(x, remove_socials=True, keep_only_home_url=True)
        for x in urls_to_redirect
        if x and not pd.isnull(x)
    ]

    original_to_redirected_urls = get_final_redirects_parallel(urls_to_redirect)
    original_to_redirected_urls = {
        k: clean_url(v, remove_socials=True, keep_only_home_url=True) for
        k,v in original_to_redirected_urls.items() if v
    }
    redirected_urls_no_null = original_to_redirected_urls.values()

    # Update the website urls to the redirected urls
    df[website_url_col] = df.apply(
        lambda x: original_to_redirected_urls.get(x[website_url_col], x[website_url_col]),
        axis=1,
    )
    url_to_linkedin_data = get_linkedin_by_urls(
        api_key=api_key,
        website_urls=redirected_urls_no_null,
        use_cached_result=use_cached_result,
        max_found_multiplier=max_found_multiplier,
    )

    df[linkedin_url_col] = df.apply(
        lambda x: url_to_linkedin_data.get(x[website_url_col], {})
        .get('original_id', x[linkedin_url_col]),
        axis=1,
    )

    has_linkedin_url_mask = df[linkedin_url_col].notnull()
    if df[~has_linkedin_url_mask].shape == 0:
        return df

    name_length_mask = df[name_col].apply(lambda x: len(x) >= 5)
    name_to_linkedin_data = get_linkedin_by_names(
        api_key=api_key,
        names=df[~has_linkedin_url_mask & has_name_mask & name_length_mask][name_col].values.tolist(),
        add_country_filter=add_country_filter,
        industry_filters=industry_filters,
        use_cached_result=use_cached_result,
        max_found_multiplier=max_found_multiplier,
    )
    df[linkedin_url_col] = df.apply(
        lambda x: name_to_linkedin_data.get(x[name_col], {})
        .get('original_id', x[linkedin_url_col]),
        axis=1,
    )

    pd.options.mode.chained_assignment = "warn"
    return df


def merge_linkedin_chunk(
    df,
    name_col,
    website_url_col,
    linkedin_url_col,
    api_key,
    add_country_filter=True,
    industry_filters=INDUSTRY_FILTERS,
    use_cached_result=True,
    max_found_multiplier=2,
):
    df = get_linkedin_urls(
        df,
        name_col=name_col,
        website_url_col=website_url_col,
        linkedin_url_col=linkedin_url_col,
        api_key=api_key,
        add_country_filter=add_country_filter,
        industry_filters=industry_filters,
        use_cached_result=use_cached_result,
        max_found_multiplier=max_found_multiplier,
    )

    missing_linkedin_mask = df[linkedin_url_col].isnull()
    linkedin_urls = df[~missing_linkedin_mask][linkedin_url_col].values.tolist()
    with get_session() as session:
        linkedin_data = scrape_organizations_psql(
            linkedin_urls,
            api_key=api_key,
            session=session,
            skip_existing=use_cached_result,
        )

    rename_columns = {
        "about": "About LinkedIn",
        "name": "LinkedIn Name",
        "website": "LinkedIn Website URL",
        "industry": "LinkedIn Industry",
        "url": "LinkedIn URL",
        "company_size": "LinkedIn Num Employees",
        "hq_location": "LinkedIn HQ Location",
        "company_type": "LinkedIn Company Type",
        "image": "LinkedIn Image URL",

    }

    if linkedin_data.shape[0] == 0:
        for col in rename_columns.values():
            df[col] = None
        return df

    linkedin_data['website'] = linkedin_data['website'].apply(clean_url)

    linkedin_data.rename(
        columns=rename_columns,
        inplace=True
    )

    df = df.merge(
        linkedin_data[['original_id', *rename_columns.values()]],
        left_on=linkedin_url_col,
        right_on='original_id',
        how="left",
    )
    return df


def merge_linkedin(
    df,
    name_col,
    website_url_col,
    linkedin_url_col,
    api_key,
    chunk_size=100,
    use_cached_result=True,
    add_country_filter=True,
    industry_filters=INDUSTRY_FILTERS,
    max_found_multiplier=2,
):
    if linkedin_url_col not in df.columns:
        df[linkedin_url_col] = None

    df[name_col].fillna("", inplace=True)

    df[linkedin_url_col].fillna("", inplace=True)
    df[linkedin_url_col] = df[linkedin_url_col].apply(lambda x: x.df() if x else x)
    df[linkedin_url_col] = df[linkedin_url_col].apply(lambda x: x.rstrip('/') if x else x)
    df[linkedin_url_col].replace("", None, inplace=True)

    

    sub_results = []
    ranges = list(range(0, df.shape[0] + chunk_size, chunk_size))
    for i, max_val in enumerate(ranges):
        if i == 0:
            continue
        else:
            min_val = ranges[i - 1]

        logger.info("Getting linkedin data for %s to %s", min_val, max_val)
        sub_result = merge_linkedin_chunk(
            df=df[min_val:max_val],
            api_key=api_key,
            name_col=name_col,
            website_url_col=website_url_col,
            linkedin_url_col=linkedin_url_col,
            add_country_filter=add_country_filter,
            industry_filters=industry_filters,
            use_cached_result=use_cached_result,
            max_found_multiplier=max_found_multiplier,
        )
        sub_results.append(sub_result)
    results = pd.concat(sub_results)
    return results
