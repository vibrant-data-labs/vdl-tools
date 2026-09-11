from datetime import datetime
from pathlib import Path
import pandas as pd


from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.crunchbase import api
import vdl_tools.scrape_enrich.crunchbase.organizations_api_db as orgs_api
import vdl_tools.shared_tools.cb_funding_calculations as fcalc
import vdl_tools.shared_tools.cb_funding_types as ft
import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used functions
import vdl_tools.shared_tools.project_config as pc

from vdl_tools.scrape_enrich.crunchbase.organizations_api_db import (
    CB_TABLE_NAMES,
    load_search_results_from_parquet,
)
from vdl_tools.shared_tools.parquet_cache import read_dataframe, read_dataframes, write_dataframe


def __validate_crunchbase_args(
    search_terms_list=None,
    search_terms_path=None,
    company_ids=None
):
    if not search_terms_list and not search_terms_path and not company_ids:
        raise ValueError('Must provide one of search_terms_list, search_terms_path, or company_ids')

    if search_terms_list and search_terms_path:
        raise ValueError('Cannot provide both search_terms_list and search_terms_path')


def __extract_field(lst_record, field):
    if lst_record is None:
        return None

    return [item[field] for item in lst_record]


def __precompute_founders_dict(founders_df: pd.DataFrame):
    founders_dict = {}

    for _, row in founders_df.iterrows():
        org_permalink = row['org_permalink']
        if org_permalink not in founders_dict:
            founders_dict[org_permalink] = []

        founders_dict[org_permalink].append({
            "name": row['name'],
            "uuid": row['uuid']
        })

    # Remove duplicates based on 'uuid'
    for key in founders_dict:
        unique_founders = {}
        for founder in founders_dict[key]:
            if founder['uuid'] not in unique_founders:
                unique_founders[founder['uuid']] = founder
        founders_dict[key] = list(unique_founders.values())

    return founders_dict


def __extract_founders(org_permalink: str, founders_dict: dict):
    return founders_dict.get(org_permalink, None)


def __precompute_funding_rounds(funding_rounds_df: pd.DataFrame):
    funding_rounds_dict = {}
    funding_dates_dict = {}

    for _, row in funding_rounds_df.iterrows():
        org_permalink = row['org_permalink']
        announced_on = row['announced_on']
        investment_type = row['investment_type']

        if org_permalink not in funding_rounds_dict:
            funding_rounds_dict[org_permalink] = []
            funding_dates_dict[org_permalink] = []

        funding_rounds_dict[org_permalink].append((announced_on, investment_type))
        funding_dates_dict[org_permalink].append(announced_on)

    # Sort the lists for each org_permalink
    for key in funding_rounds_dict:
        funding_rounds_dict[key].sort(key=lambda x: datetime.strptime(x[0], "%Y-%m-%d"))
        funding_dates_dict[key].sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))

    return funding_rounds_dict, funding_dates_dict


def __extract_funding_rounds(org_permalink: str, funding_rounds_dict: dict):
    return [item[1] for item in funding_rounds_dict.get(org_permalink, [])]


# Extract funding rounds dates
def __extract_funding_rounds_dates(org_permalink: str, funding_rounds_dates_dict: dict):
    return funding_rounds_dates_dict.get(org_permalink, None)


def convert_employees_enum(emp_str):
    if not emp_str or emp_str != emp_str:
        return None

    emp_enum_map = {
        'c_00001_00010': '1-10',
        'c_00011_00050': '11-50',
        'c_00051_00100': '51-100',
        'c_00101_00250': '101-250',
        'c_00251_00500': '251-500',
        'c_00501_01000': '501-1000',
        'c_01001_05000': '1001-5000',
        'c_05001_10000': '5001-10000',
        'c_10001_max': '10001+'
    }

    return emp_enum_map[emp_str] or None


def add_investors(
    df: pd.DataFrame,
    orgs_investors_df: pd.DataFrame,
    people_investors_df: pd.DataFrame
):

    investor_cols = [
        'name',
        'uuid',
        'entity_def_id',
        'investor_type',
        'investor_categories',
        'org_permalink'
    ]
    investor_df = pd.concat(
        [
            orgs_investors_df[investor_cols],
            people_investors_df[investor_cols]
        ]
    )

    # group investors by org_permalink, this creates a dictionary with org_permalink as key and list of investors as value
    org_to_investors = {k: v.to_dict(orient='records') for k, v in  investor_df.groupby('org_permalink')}
    df['Investors Data'] = df['permalink'].apply(lambda p: org_to_investors.get(p, None))
    return df


def get_gov_investor_uuids(investors_df: pd.DataFrame):
    """
    Returns a set of UUIDs for investors that are either:
    1. investor_type = 'government_office' (Agencies, State Banks)
    2. facet_ids contains 'government_entity' (The Government Itself)

    It will be applied at funding round level and at the company level.
    """
    return {
        uuid
        for uuid, types, facets in zip(investors_df['uuid'],
                                       investors_df['investor_type'],
                                       investors_df['facet_ids'])
        if (isinstance(types, list) and 'government_office' in types)
           or (isinstance(facets, list) and 'government_entity' in facets)
    }


def check_investor_overlap(investor_list, gov_uuids):
    """
    Checks if any investor in the list has a UUID present in the gov_uuids set.
    """
    if not investor_list or not isinstance(investor_list, list):
        return False

    # Extract UUIDs from this row's list
    current_uuids = {inv.get('uuid') for inv in investor_list if inv.get('uuid')}

    # Check for intersection
    return bool(current_uuids & gov_uuids)

def flag_gov_funder_to_rounds(
    funding_rounds_df: pd.DataFrame,
    orgs_investors_df: pd.DataFrame
):
    """ Add a boolean column to funding rounds dataframe indicating if any investor is a government investor """

    # get government investor UUIDs
    gov_investor_uuids = get_gov_investor_uuids(orgs_investors_df)

    # Apply the global helper using lambda
    # pass the row value (x) and our pre-calculated set
    return funding_rounds_df['investor_identifiers'].apply(
        lambda x: check_investor_overlap(x, gov_investor_uuids)
    )


def __get_values(data, field_name="value", default_value=None):
    if not coerced_bool(data):
        return default_value
    if type(data) == list:
        return [item[field_name] for item in data]
    return data.get(field_name, default_value)

def __add_funding_by_year(funding_rounds_df: pd.DataFrame, orgs_df: pd.DataFrame, filter_yr: int):
    # groupby org and year and summarize total raised
    df_by_org_yr = funding_rounds_df.groupby(['org_permalink', 'year_announced'])['raised_usd'].agg("sum").reset_index()
    df_by_org_yr = df_by_org_yr[df_by_org_yr['year_announced'] >= filter_yr]
    # transpose year
    df_pivot = pd.pivot_table(df_by_org_yr, values='raised_usd', index=['org_permalink'],
                              columns='year_announced').reset_index()
    df_pivot.fillna(0, inplace=True)  # fill totals by
    # clean column names
    df_pivot.columns = ['org_permalink'] + [f"Funding_{year}" for year in df_pivot.columns.tolist()[1:]]
    # add funding by year cols to org data
    orgs_df = orgs_df.merge(df_pivot, left_on='permalink', right_on='org_permalink', how='left')
    return orgs_df  # orgs with funding by year


def __get_org_country_from_fr(entries):
    # find and extract the country name from the list of dictionaries
    for entry in entries:
        if entry.get('location_type') == 'country':
            return entry.get('value')
    return None  # Return None if no country entry is found


def query_crunchbase_raw_data(
    company_ids=None,
    search_terms_list=None,
    search_terms_path=None,
    use_cache=True,
    force_query=False,
    force_refresh_records=False,
    save_to_uri=None,
    filter_yr=2016,
    usa_only=True,
    active_only=True,
):
    """Query raw Crunchbase company data for a set of search terms or IDs.

    Parameters
    ----------
    company_ids : list[str] | None, optional
        Crunchbase company identifiers to fetch directly. When provided, this
        function calls ``query_companies_extended(..., search_condition="id")``
        so each item is treated as a direct company lookup rather than a text
        search.
    search_terms_list : list[str] | None, optional
        Free-text search terms for the Crunchbase companies search. In
        ``vdl-tools`` this maps to
        ``query_companies_extended(..., search_condition="search_terms")``,
        which queries the organizations endpoint using the ``description``
        field, along with this function's extra location and funding filters.
        Use this for topical or keyword-based company discovery.
    search_terms_path : str | Path | None, optional
        Path to an Excel file whose first column contains search terms. This is
        read into a list and used the same way as ``search_terms_list``.
        Mutually exclusive with ``search_terms_list``.
    filter_yr : int, optional
        Filter the data to only include companies whose ``last_funding_at`` is
        on or after ``01/01/{filter_yr}``.
    usa_only : bool, optional
        Restrict results to companies with ``location_identifiers`` including
        ``united-states``.
    active_only : bool, optional
        Restrict results to companies whose ``operating_status`` is ``active``.
    use_cache : bool, default True
        Passed through to ``query_companies_extended``. When ``False``, skip
        all DB reads and fetch everything from the API.
    force_query : bool, default False
        Passed through to ``query_companies_extended``. When ``True``, bypass
        the query-level cache so the org search and funding rounds are re-run
        against the API — required for refresh runs to pick up new companies,
        since the query cache has no expiry.
    force_refresh_records : bool, default False
        Passed through to ``query_companies_extended``. When ``True``,
        re-fetch investor and founder records from the API even when they are
        already cached in the DB, so previously cached records pick up
        updated data.
    save_to_uri : str | None, optional
        Optional destination URI passed through to
        ``query_companies_extended``. When provided, the raw Crunchbase result
        tables are written as Parquet files under this location.

    Notes
    -----
    Provide exactly one lookup mode: ``company_ids`` for direct identifier
    lookups, or ``search_terms_list`` / ``search_terms_path`` for
    description-based search.
    """
    logger.info('querying for crunchbase for raw data')

    __validate_crunchbase_args(
        search_terms_path=search_terms_path,
        search_terms_list=search_terms_list,
        company_ids=company_ids
    )
    if search_terms_list:
        search_terms = search_terms_list
    elif search_terms_path:
        search_terms_df = pd.read_excel(search_terms_path)
        search_terms = search_terms_df.iloc[:, 0].to_list()
    else:
        search_terms = None

    if search_terms:
        logger.info("Found %s search terms", len(search_terms))

    search_kwargs = {}

    if search_terms:
        search_kwargs = {
            "items_list": search_terms,
            "search_condition": "search_terms",
        }
    elif company_ids:
        search_kwargs = {
            "items_list": company_ids,
            "search_condition": "id",
        }

    extra_filters = [api.gte("last_funding_at", f"01/01/{filter_yr}")]
    if usa_only:
        extra_filters.append(api.includes("location_identifiers", ["united-states"]))
    if active_only:
        extra_filters.append(api.eq("operating_status", "active"))
    crunchbase_results = orgs_api.query_companies_extended(
        **search_kwargs,
        extra_filters=extra_filters,
        use_cache=use_cache,
        force_query=force_query,
        force_refresh_records=force_refresh_records,
        save_to_uri=save_to_uri,
    )

    return crunchbase_results


def _process_crunchbase_data(
    df_orgs: pd.DataFrame,
    df_funding_rounds: pd.DataFrame,
    df_founders: pd.DataFrame,
    df_investor_orgs: pd.DataFrame,
    df_investor_person: pd.DataFrame,
    filter_yr: int,
    filter_to_companies: bool,
):
    """
    Process Crunchbase raw data
    """
    # print('filtering crunchbase data to keep only active organizations')
    # keep all organizations despite of funding amount
    # df_orgs = df_orgs[df_orgs['operating_status'] == 'active']

    keep_columns = [
        'uuid',
        'permalink',
        'name',
        # 'aliases',
        'identifier',
        'company_type',
        'categories',
        'category_groups',
        'funding_stage',
        'last_equity_funding_type',
        'last_funding_type',
        'description',
        'location_identifiers',
        'num_employees_enum',
        'funding_total',
        'equity_funding_total',
        'last_funding_at',
        'num_funding_rounds',
        'founded_on',
        'website_url',
        'linkedin',
        # 'acquirer_identifier',
        'facet_ids',  # investor vs company
        'ipo_status',  # private vs public
        'num_articles',  # news mentions
        'diversity_spotlights',  # women or bipoc founders
        'permalink_aliases',  # list of company permalinks that are the same company
        'operating_status'  # active vs closed
    ]

    df_cb = df_orgs[keep_columns].copy()

    if filter_to_companies:
        logger.info('Filtering out facet_ids that do not have "company"')
        df_cb = df_cb[df_cb['facet_ids'].apply(lambda x: isinstance(x, (list, str)) and 'company' in x)]

    logger.info('Precomputing founders dictionary')
    founders_dict = __precompute_founders_dict(df_founders)

    logger.info('Applying functions to extract founders')
    df_cb['Founder Data'] = df_cb['permalink'].apply(lambda p: __extract_founders(p, founders_dict))
    df_cb['Founders'] = df_cb['Founder Data'].apply(lambda x: __extract_field(x, 'name'))
    logger.info('Extracted founders')

    logger.info('Precomputing funding rounds')
    df_funding_rounds['org_permalink'] = (
        df_funding_rounds['funded_organization_identifier']
        .apply(lambda x: x['permalink'])
    )
    funding_rounds_dict, funding_rounds_dates_dict = __precompute_funding_rounds(df_funding_rounds)

    logger.info('Applying functions to extract funding rounds')
    # Round types per org, chronological, duplicates kept, aligned 1:1 with the
    # dates list. The raw slugs stay in 'Funding Types Raw'; the display names in
    # 'Funding Types' are assigned HERE and nowhere else (see cb_funding_types.py).
    new_slugs = ft.unknown_slugs(df_funding_rounds['investment_type'])
    if new_slugs:
        logger.warning('Crunchbase round types not in cb_funding_types.ROUND_TYPES '
                       '(they will show unmapped): %s', sorted(new_slugs))
    df_cb['Funding Types Raw'] = (
        df_cb['permalink']
        .apply(lambda p: __extract_funding_rounds(p, funding_rounds_dict))
    )
    df_cb['Funding Types Dates'] = df_cb['permalink'].apply(
        lambda p: __extract_funding_rounds_dates(p, funding_rounds_dates_dict))
    df_cb['Funding Types'] = df_cb['Funding Types Raw'].apply(ft.to_display)
    df_cb['Last_Funding_Type'] = df_cb['Funding Types'].apply(lambda types: types[-1] if types else None)
    logger.info('Extracted funding types from funding rounds')

    logger.info('Extracting year last funded')
    df_cb['last_funding_at'] = pd.to_datetime(df_cb['last_funding_at'])
    df_cb['Year_Last_Funded'] = df_cb['last_funding_at'].apply(lambda lf: lf.year if len(str(lf)) >= 10 else None)

    logger.info('Getting total funding by year for each organization')
    df_funding_rounds['year_announced'] = df_funding_rounds['announced_on'].apply(lambda x: int(x.split("-")[0]))
    df_funding_rounds['raised_usd'] = df_funding_rounds.money_raised.apply(lambda x: __get_values(x, 'value_usd', 0))
    df_cb = __add_funding_by_year(df_funding_rounds, df_cb, filter_yr)
    logger.info('Extracted funding by year from funding rounds')

    logger.info('Adding year of first funding round from funding rounds data')
    df_first_yr = df_funding_rounds.groupby('org_permalink')['year_announced'].agg("min").reset_index()
    df_cb = df_cb.merge(df_first_yr, on="org_permalink", how="left")
    df_cb.rename(columns={"year_announced": "First_Year_Funded"}, inplace=True)

    logger.info('Adding investor categories')
    df_investor_orgs['investor_categories'] = df_investor_orgs.categories.apply(__get_values)
    df_investor_person['investor_categories'] = None

    logger.info('Adding investors to organizations')
    df_cb = add_investors(df_cb, df_investor_orgs, df_investor_person)
    logger.info('Added investors to organizations')

    # 1. Combine ALL investor types (Orgs + People) for the "DNA" check
    all_investors = pd.concat([df_investor_orgs, df_investor_person], ignore_index=True)

    # 2. Use the Master Function (Single Source of Truth)
    gov_investor_uuids = get_gov_investor_uuids(all_investors)

    logger.info('Extracted investors')
    df_cb['Investors'] = df_cb['Investors Data'].apply(lambda i: __extract_field(i, 'name'))
    # Use the shared helper
    df_cb['Gov_Funder'] = df_cb['Investors Data'].apply(
        lambda x: check_investor_overlap(x, gov_investor_uuids)
    )

    df_cb['n_Funders'] = df_cb['Investors Data'].apply(lambda i: len(i) if i is not None else 0)
    df_cb['n_Employees'] = df_cb['num_employees_enum'].apply(convert_employees_enum)

    logger.info('Extracting founded on and Year Founded')
    df_cb['founded_on'] = df_cb['founded_on'].apply(__get_values)
    df_cb['Year_Founded'] = df_cb['founded_on'].apply(
        lambda lf: datetime.strptime(lf, '%Y-%m-%d').year if len(str(lf)) >= 10 else None)

    logger.info('Extracting categories and category groups')
    df_cb['category_groups'] = df_cb['category_groups'].apply(__get_values)
    df_cb['categories'] = df_cb['categories'].apply(__get_values)

    logger.info('Extracting total funding')
    df_cb['funding_total'] = df_cb['funding_total'].apply(lambda x: __get_values(x, 'value_usd', 0))

    logger.info('Extracting diversity spotlights')
    df_cb['diversity'] = df_cb['diversity_spotlights'].apply(__get_values)

    # df_cb['Acquired by'] = df_cb['acquirer_identifier'].apply(__get_values)
    df_cb['linkedin'] = df_cb['linkedin'].apply(lambda l: __get_values(l))

    logger.info('Extracting location identifiers')
    df_cb['location_identifiers'] = df_cb['location_identifiers'].apply(lambda li: __get_values(li))
    df_cb['hq_address'] = df_cb['location_identifiers'].apply(lambda li: ', '.join(li) if li else None)

    df_cb['Data Source'] = "Crunchbase"

    logger.info('Adding link back to Crunchbase')
    df_cb['Crunchbase_URL'] = 'https://www.crunchbase.com/organization/' + df_cb['permalink']

    # The three classifications below read the raw round slugs ('Funding Types Raw')
    logger.info('Deducing organization type')
    df_cb['company_type'] = df_cb.apply(lambda x: fcalc.deduce_org_type(company_row=x), axis=1)

    logger.info('Deducing funding stage')
    # raw stage label in 'Funding Stage Raw'; display name in 'Funding Stage'
    df_cb['Funding Stage Raw'] = df_cb.apply(
        lambda x: fcalc.complete_stage_from_type(company_row=x), axis=1
    )
    df_cb['Funding Stage'] = df_cb['Funding Stage Raw'].apply(ft.to_display)

    logger.info('Granting loan flags')
    df_cb = fcalc.grant_loan_flags(df_cb)

    logger.info('Renaming columns')
    df_cb.rename(columns={
        "company_type": "Org Type",
        "name": "Organization",
        "facet_ids": "Investor_Company",
        "categories": "sectors_cb_cd",
        "category_groups": "industries_cb_cd",
        "website_url": "Website_cb_cd",
        'funding_stage': 'Funding Status',
        'last_equity_funding_type': "last_equity_type",
        'funding_total': 'Total_Funding_$',
        'description': 'Description',
        'linkedin': 'LinkedIn'
    }, inplace=True)

    df_cb['id'] = df_cb['uuid']

    if filter_yr:
        logger.info(f'Filtering out companies last funded before {filter_yr} (keeping unfunded)')
        df_cb = df_cb[(df_cb['Year_Last_Funded'] >= filter_yr) | (df_cb['Year_Last_Funded'].isna())]

    df_cb['Description_990'] = ''

    df_cb['logo'] = ""
    df_cb['Donors'] = ""
    df_cb["Executives"] = ""
    df_cb['Board'] = ""
    df_cb['About_web'] = ""

    logger.info('Processing list columns')
    list_cols = ['Investors', 'Founders', 'industries_cb_cd', 'sectors_cb_cd']
    cf.string2list(df_cb, list_cols)
    for col in list_cols:
        # remove any empty elements of the list
        df_cb[col] = df_cb[col].apply(lambda l: [x for x in l if str(x) != 'nan'])

    return df_cb


def process_crunchbase_raw_data_from_parquet(
    dir_uri: str | None = None,
    filter_yr: int = 2016,
    filter_to_companies: bool = True,
    save_cleaned: bool = True,
    cleaned_filename: str | None = None,
    *,
    tables: dict[str, pd.DataFrame] | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Clean the 5 raw CB tables and (by default) persist the cleaned DataFrame.

    Supply **either** the raw tables in-memory (``tables=``) **or** a directory
    URI to read them from (``dir_uri=``). Passing ``tables=`` skips the
    round-trip through storage entirely — the natural pairing with
    ``query_companies_extended``, which already returns the 5 DataFrames::

        # zero-round-trip pipeline
        raw = query_companies_extended(items_list=..., save_to_uri="s3://X")
        cleaned = process_crunchbase_raw_data_from_parquet(
            tables=raw,
            cleaned_filename="s3://X/cb_companies_cleaned.parquet",
        )

        # pure reader (no prior query in this process)
        cleaned = process_crunchbase_raw_data_from_parquet(
            dir_uri="s3://X",
            cleaned_filename="s3://X/cb_companies_cleaned.parquet",
        )

    Parameters
    ----------
    dir_uri
        URI of the directory-like container to read the raw tables from.
        Required when ``tables`` is not provided. Accepts a local directory
        path, ``file://`` URI, or full ``s3://`` URI including bucket
        (e.g. ``s3://shared-data-clone/cb_raw/fisheries``).
    tables
        Pre-loaded raw tables keyed by table name
        (``organizations``, ``funding_rounds``, ``founders``, ``org_investors``,
        ``people_investors``). Typically the return value from
        :func:`query_companies_extended`. When provided, skips all S3 reads.
    filter_yr
        Earliest year of founding to keep.
    filter_to_companies
        If True, restrict to companies (drop investors etc.).
    save_cleaned
        If True (default), write the cleaned DataFrame to ``cleaned_filename``.
        Pass False to skip the write and only return in memory.
    cleaned_filename
        Full URI to write the cleaned output file to (passed straight to
        ``write_dataframe``). Accepts a local path, ``file://`` URI, or full
        ``s3://`` URI. The default writes ``cb_companies_cleaned.parquet`` to
        the current working directory — pass an explicit URI to write
        elsewhere (e.g. ``s3://shared-data-clone/cb_raw/fisheries/cb_companies_cleaned.parquet``).
    use_cache
        Read-cache control, only relevant when ``tables`` is not provided.
        See :func:`load_search_results_from_parquet`.

    Notes
    -----
    ``cb_companies_cleaned.parquet`` is a **mutable shared artifact** — the
    last writer wins. Parquet handles column/type drift between writers
    gracefully (schema is per-file), so different cleaning conventions will
    not error; they will simply overwrite each other. If your team's cleaning
    diverges materially, use ``cleaned_filename`` to namespace your output
    (e.g. ``s3://.../cb_companies_cleaned__zein.parquet``).
    """
    if tables is None and not dir_uri:
        raise ValueError("Must provide either tables= (in-memory) or dir_uri= (storage).")
    if save_cleaned and not cleaned_filename:
        raise ValueError("save_cleaned=True requires cleaned_filename= as the write destination.")

    if tables is None:
        tables = load_search_results_from_parquet(
            dir_uri=dir_uri,
            names=list(CB_TABLE_NAMES),
            use_cache=use_cache,
        )
        data_source = str(dir_uri)
    else:
        data_source = "in-memory (caller-supplied)"

    cleaned_df = _process_crunchbase_data(
        df_orgs=tables["organizations"],
        df_funding_rounds=tables["funding_rounds"],
        df_founders=tables["founders"],
        df_investor_orgs=tables["org_investors"],
        df_investor_person=tables["people_investors"],
        filter_yr=filter_yr,
        filter_to_companies=filter_to_companies,
    )

    if save_cleaned:
        write_dataframe(
            cleaned_df,
            cleaned_filename,
            lineage={
                "source": "prepare_crunchbase.process_crunchbase_raw_data_from_parquet",
                "filter_yr": filter_yr,
                "filter_to_companies": filter_to_companies,
                "input_source": data_source,
                "n_rows": len(cleaned_df),
            },
        )

    return cleaned_df



def load_process_funding_rounds_from_parquet(
    fr_uri,
    investor_orgs_uri,
    filter_yr=2010,
):
    logger.info('loading funding rounds')
    df_fr = read_dataframe(uri=fr_uri)
    logger.info('loading investor orgs')
    orgs_investors_df = read_dataframe(uri=investor_orgs_uri)
    logger.info('processing funding rounds')
    return process_funding_rounds(df_fr, orgs_investors_df, filter_yr)


def load_process_funding_rounds_from_json(
    fr_path,
    investor_orgs_path,
    filter_yr=2010,
):
    logger.info('loading funding rounds')
    df_fr = pd.read_json(fr_path)
    logger.info('loading investor orgs')
    orgs_investors_df = pd.read_json(investor_orgs_path)
    logger.info('processing funding rounds')
    return process_funding_rounds(df_fr, orgs_investors_df, filter_yr)


def process_funding_rounds(
    df_fr: pd.DataFrame,
    orgs_investors_df: pd.DataFrame,
    filter_yr=2010,
):
    logger.info('processing funding rounds')
    # get year announced
    df_fr['year_announced'] = df_fr['announced_on'].apply(lambda x: int(x.split("-")[0]))
    # get total raised
    df_fr['raised_usd'] = df_fr.money_raised.apply(lambda x: __get_values(x, 'value_usd', 0))
    df_fr = df_fr[df_fr['year_announced'] >= filter_yr].copy()
    # get org permalink and id
    df_fr['org_permalink'] = df_fr['funded_organization_identifier'].apply(
        lambda x: x.get('permalink') if isinstance(x, dict) else None)
    df_fr['org_uuid'] = df_fr['funded_organization_identifier'].apply(
        lambda x: x.get('uuid') if isinstance(x, dict) else None)
    # get org country
    df_fr['country'] = df_fr['funded_organization_location'].apply(lambda x: __get_org_country_from_fr(x) if isinstance(x, list) else None)
    # get diversity spotlights
    logger.info('adding diversity tags for each funding round')
    df_fr['diversity'] = df_fr['funded_organization_diversity_spotlights'].apply(lambda x: __get_values(x, 'value', None))
    # add gov funder flag to funding rounds
    logger.info('adding government funder flag for each funding round')
    df_fr['gov_funder'] = flag_gov_funder_to_rounds(
        df_fr,
        orgs_investors_df
    )

    keep_columns = [
        'org_uuid',
        'org_permalink',
        'announced_on',
        'year_announced',
        'raised_usd',
        'investment_stage',
        'investment_type',
        'investor_identifiers',
        'gov_funder',
        'country',
        'diversity',
        "funded_organization_identifier",
        'funded_organization_description',
        'permalink',
        'uuid'
    ]
    df_fr = df_fr[keep_columns].copy()
    df_fr.rename(columns={'permalink': 'fr_permalink',
                          'uuid': 'fr_uuid'
                          }, inplace=True)
    return df_fr



def prepare_raw_crunchbase(
    query_crunchbase,
    process_crunchbase,
    save_to_uri_path,
    cleaned_organizations_filename,
    cleaned_funding_rounds_filename,
    use_cache=True,
    force_query=False,
    force_refresh_records=False,
    company_ids=None,
    search_terms_list=None,
    search_terms_path=None,  # paths.get('expanded_search_terms_crunchbase'),
    filter_yr=2016,
    usa_only=True,
    active_only=True,
):

    __validate_crunchbase_args(
        search_terms_list=search_terms_list,
        search_terms_path=search_terms_path,
        company_ids=company_ids
    )

    if query_crunchbase:
        logger.info("Querying Crunchbase for raw data")
        crunchbase_results = query_crunchbase_raw_data(
            search_terms_list=search_terms_list,
            search_terms_path=search_terms_path,
            company_ids=company_ids,
            use_cache=use_cache,
            force_query=force_query,
            force_refresh_records=force_refresh_records,
            save_to_uri=save_to_uri_path,
            filter_yr=filter_yr,
            usa_only=usa_only,
            active_only=active_only,
        )
    else:
        crunchbase_results = read_dataframes(
            dir_uri=save_to_uri_path,
            names=list(CB_TABLE_NAMES),
            use_cache=use_cache,
        )

    if process_crunchbase:
        logger.info("Processing Crunchbase raw data")
        cleaned_organizations_df = process_crunchbase_raw_data_from_parquet(
            tables=crunchbase_results,
            filter_yr=filter_yr,
            filter_to_companies=True,
            save_cleaned=True,
            cleaned_filename=cleaned_organizations_filename,
        )

        cleaned_funding_rounds_df = process_funding_rounds(
            df_fr=crunchbase_results["funding_rounds"],
            orgs_investors_df=crunchbase_results["org_investors"],
            filter_yr=filter_yr,
        )
        write_dataframe(
            cleaned_funding_rounds_df,
            cleaned_funding_rounds_filename,
            lineage={
                "source": "prepare_crunchbase.process_funding_rounds",
                "filter_yr": filter_yr,
            },
        )

    else:
        cleaned_organizations_df = read_dataframe(uri=cleaned_organizations_filename)

    return cleaned_organizations_df


if __name__ == '__main__':
    from vdl_tools.shared_tools.project_config import get_s3_paths

    run_query_crunchbase = True
    run_crunchbase = True
    filter_year = 2026

    s3_paths = get_s3_paths()

    s3_bucket = s3_paths.get('bucket_name')
    save_to_uri_path = f"{s3_bucket}/crunchbase/test_run"
    cleaned_organizations_filename = f"{save_to_uri_path}/cb_companies_cleaned.parquet"
    cleaned_funding_rounds_filename = f"{save_to_uri_path}/cb_funding_rounds_cleaned.parquet"

    cleaned_organizations_df = prepare_raw_crunchbase(
        search_terms_list=['farmers', 'agriculture', 'food', 'sustainability', 'environment', 'climate'],
        query_crunchbase=run_query_crunchbase,
        process_crunchbase=run_crunchbase,
        filter_yr=filter_year,
        save_to_uri_path=save_to_uri_path,
        cleaned_organizations_filename=cleaned_organizations_filename,
        cleaned_funding_rounds_filename=cleaned_funding_rounds_filename,
    )
