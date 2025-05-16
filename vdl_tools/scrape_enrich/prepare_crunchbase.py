from datetime import datetime
from pathlib import Path
import pandas as pd
from vdl_tools.shared_tools.tools.logger import logger
import vdl_tools.scrape_enrich.crunchbase.organizations_api_extended as orgs_api
import vdl_tools.shared_tools.cb_funding_calculations as fcalc
import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used functions
import vdl_tools.shared_tools.project_config as pc

paths = pc.get_paths()


def __validate_crunchbase_args(
    search_terms_list=None,
    organizations_file_path=None,
    funding_rounds_file_path=None,
    founders_file_path=None,
    investor_orgs_file_path=None,
    investor_person_file_path=None,
    search_terms_path=None,
    company_ids=None
):
    if not search_terms_list and not search_terms_path and not company_ids:
        raise ValueError('Must provide either search_terms_list or search_terms_path')

    if search_terms_list and search_terms_path:
        raise ValueError('Cannot provide both search_terms_list and search_terms_path')

    if not all([
        organizations_file_path,
        funding_rounds_file_path,
        founders_file_path,
        investor_orgs_file_path,
        investor_person_file_path
    ]):
        raise ValueError('Must provide all file paths')


def query_crunchbase_raw_data(
    company_ids=None,
    search_terms_list=None,
    organizations_file_path=paths.get('expanded_orgs_data'),
    funding_rounds_file_path=paths.get('expanded_orgs_funding_rounds'),
    founders_file_path=paths.get('expanded_orgs_founders'),
    investor_orgs_file_path=paths.get('expanded_orgs_investor_orgs'),
    investor_person_file_path=paths.get('expanded_orgs_investor_person'),
    search_terms_path=paths.get('expanded_search_terms_crunchbase'),
):
    logger.info('querying for crunchbase for raw data')

    __validate_crunchbase_args(
        search_terms_list=search_terms_list,
        organizations_file_path=organizations_file_path,
        funding_rounds_file_path=funding_rounds_file_path,
        founders_file_path=founders_file_path,
        investor_orgs_file_path=investor_orgs_file_path,
        investor_person_file_path=investor_person_file_path,
        search_terms_path=search_terms_path,
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

    output_files = {
        "organizations": organizations_file_path,
        "funding_rounds": funding_rounds_file_path,
        "founders": founders_file_path,
        "investor_orgs": investor_orgs_file_path,
        "investor_person": investor_person_file_path,
    }

    if search_terms:
        orgs_api.query_companies_extended(
            items_list=search_terms, 
            output_files=output_files,
            force_query=True,
            search_condition='search_terms'
        )
    elif company_ids:
        orgs_api.query_companies_extended(
            items_list=company_ids,
            output_files=output_files,
            force_query=True,
            search_condition='id'
        )


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

def has_government_investor(investors_data):
    if not investors_data:
        return False
    for investor in investors_data:
        investor_types = investor['investor_type']
        if not investor_types:
            continue
        if 'government_office' in investor_types:
            return True
    return False

def __get_values(data, field_name="value", default_value=None):
    if not data:
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


def process_crunchbase_raw_data(
    filter_yr=2016,
    organizations_file_path=paths.get('expanded_orgs_data'),
    funding_rounds_file_path=paths.get('expanded_orgs_funding_rounds'),
    founders_file_path=paths.get('expanded_orgs_founders'),
    investor_orgs_file_path=paths.get('expanded_orgs_investor_orgs'),
    investor_person_file_path=paths.get('expanded_orgs_investor_person'),
    clean_file_path=paths.get('cb_companies_cleaned'),
    filter_to_companies=True,
):
    logger.info('loading raw crunchbase data')
    df_orgs = pd.read_json(organizations_file_path)
    logger.info('loading orgs')
    df_funding_rounds = pd.read_json(funding_rounds_file_path)
    logger.info('loading fr')
    df_founders = pd.read_json(founders_file_path)
    logger.info('loading founders')
    df_investor_orgs = pd.read_json(investor_orgs_file_path)
    logger.info('loading investor orgs')
    df_investor_person = pd.read_json(investor_person_file_path)
    logger.info('loading investor persons')


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
        'acquirer_identifier',
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
        df_cb = df_cb[df_cb['facet_ids'].apply(lambda x: 'company' in x)]

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
    df_cb['Funding Types'] = (
        df_cb['permalink']
        .apply(lambda p: __extract_funding_rounds(p, funding_rounds_dict))
    )
    df_cb['Funding Types Dates'] = df_cb['permalink'].apply(
        lambda p: __extract_funding_rounds_dates(p, funding_rounds_dates_dict))
    df_cb['Last_Funding_Type'] = df_cb['Funding Types'].apply(lambda ft: ft[-1] if ft else None)
    logger.info('Extracted funding types from funding rounds')

    logger.info('Extracting year last funded')
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
    df_cb['Investors'] = df_cb['Investors Data'].apply(lambda i: __extract_field(i, 'name'))
    df_cb['Gov_Funder'] = df_cb['Investors Data'].apply(has_government_investor)
    df_cb['n_Funders'] = df_cb['Investors Data'].apply(lambda i: len(i) if i is not None else 0)
    df_cb['n_Employees'] = df_cb['num_employees_enum'].apply(convert_employees_enum)
    logger.info('Extracted investors')

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

    df_cb['Acquired by'] = df_cb['acquirer_identifier'].apply(__get_values)
    df_cb['linkedin'] = df_cb['linkedin'].apply(lambda l: __get_values(l))

    logger.info('Extracting location identifiers')
    df_cb['location_identifiers'] = df_cb['location_identifiers'].apply(lambda li: __get_values(li))
    df_cb['hq_address'] = df_cb['location_identifiers'].apply(lambda li: ', '.join(li) if li else None)

    df_cb['Data Source'] = "Crunchbase"

    logger.info('Adding link back to Crunchbase')
    df_cb['Crunchbase_URL'] = 'https://www.crunchbase.com/organization/' + df_cb['permalink']

    logger.info('Deducing organization type')
    df_cb['company_type'] = df_cb.apply(lambda x: fcalc.deduce_org_type(company_row=x), axis=1)

    logger.info('Deducing funding stage')
    df_cb['Funding Stage'] = df_cb.apply(
        lambda x: fcalc.complete_stage_from_type(company_row=x), axis=1
    )

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
        logger.info(f'Filtering out companies last funded before {filter_yr}')
        df_cb = df_cb[df_cb['Year_Last_Funded'] >= filter_yr]

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

    logger.info(f'Writing cleaned Crunchbase data to {clean_file_path}')
    # add directory if it does not exist
    if not isinstance(clean_file_path, Path):
        clean_file_path = Path(clean_file_path)
    clean_file_path.parent.mkdir(parents=True, exist_ok=True)
    df_cb.to_json(clean_file_path, orient='records')

    return df_cb


def __get_org_country_from_fr(entries):
    # find and extract the country name from the list of dictionaries
    for entry in entries:
        if entry.get('location_type') == 'country':
            return entry.get('value')
    return None  # Return None if no country entry is found


def process_funding_rounds(fr_path=paths['expanded_orgs_funding_rounds'],
                           filter_yr=2010):
    logger.info('processing funding rounds')
    df_fr = pd.read_json(fr_path)
    logger.info('loaded funding rounds')
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
    df_fr['diversity'] = df_fr['funded_organization_diversity_spotlights'].apply(lambda x: __get_values(x, 'value', None))
    keep_columns = ['org_uuid',
                    'org_permalink',
                    'announced_on',
                    'year_announced',
                    'raised_usd',
                    'investment_stage',
                    'investment_type',
                    'country',
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
    company_ids=None,
    search_terms_list=None,
    search_terms_path=None,  # paths.get('expanded_search_terms_crunchbase'),
    organizations_file_path=paths.get('expanded_orgs_data'),
    funding_rounds_file_path=paths.get('expanded_orgs_funding_rounds'),
    founders_file_path=paths.get('expanded_orgs_founders'),
    investor_orgs_file_path=paths.get('expanded_orgs_investor_orgs'),
    investor_person_file_path=paths.get('expanded_orgs_investor_person'),
    clean_file_path=paths.get('cb_companies_cleaned'),
    filter_yr=2016,
    filter_to_companies=True,
):

    if query_crunchbase and not process_crunchbase:
        raise ValueError("Cannot query Crunchbase without processing the data")

    __validate_crunchbase_args(
        search_terms_list=search_terms_list,
        organizations_file_path=organizations_file_path,
        funding_rounds_file_path=funding_rounds_file_path,
        founders_file_path=founders_file_path,
        investor_orgs_file_path=investor_orgs_file_path,
        investor_person_file_path=investor_person_file_path,
        search_terms_path=search_terms_path,
        company_ids=company_ids
    )

    if query_crunchbase:
        logger.info("Querying Crunchbase for raw data")
        query_crunchbase_raw_data(
            search_terms_list=search_terms_list,
            search_terms_path=search_terms_path,
            company_ids=company_ids,
            organizations_file_path=organizations_file_path,
            funding_rounds_file_path=funding_rounds_file_path,
            founders_file_path=founders_file_path,
            investor_orgs_file_path=investor_orgs_file_path,
            investor_person_file_path=investor_person_file_path,
        )

    if process_crunchbase:
        logger.info("Processing Crunchbase raw data")
        return process_crunchbase_raw_data(
            filter_yr=filter_yr,
            organizations_file_path=organizations_file_path,
            funding_rounds_file_path=funding_rounds_file_path,
            founders_file_path=founders_file_path,
            investor_orgs_file_path=investor_orgs_file_path,
            investor_person_file_path=investor_person_file_path,
            clean_file_path=clean_file_path,
            filter_to_companies=filter_to_companies
        )
    if paths['cb_companies_cleaned'].exists():
        print("loading pre-processed crunchbase data")
        return pd.read_json(clean_file_path)

    return pd.read_json(clean_file_path)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Process Crunchbase data')
    parser.add_argument('--query_crunchbase', action='store_true', default=False, help='Query Crunchbase for raw data',)
    parser.add_argument('--process_crunchbase', action='store_true', default=False, help='Process Crunchbase raw data')
    parser.add_argument('--filter_yr', type=int, default=2010, help='Filter year for funding rounds')

    args = parser.parse_args()

    query_crunchbase=args.query_crunchbase
    process_crunchbase=args.process_crunchbase
    filter_yr=args.filter_yr

    prepare_raw_crunchbase(
        query_crunchbase=query_crunchbase,
        process_crunchbase=process_crunchbase,
        filter_yr=filter_yr,
    )
