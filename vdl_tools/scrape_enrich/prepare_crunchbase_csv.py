import ast
from datetime import datetime

import pandas as pd
from vdl_tools.shared_tools.tools.logger import logger
import vdl_tools.scrape_enrich.crunchbase.organizations_api_extended as orgs_api
import vdl_tools.shared_tools.cb_funding_calculations as fcalc
import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used functions
import vdl_tools.shared_tools.project_config as pc

paths = pc.get_paths()
config = pc.get_project_config()


# %%
def __query_crunchbase_raw_data():
    logger.info('querying for crunchbase for raw data')
    search_terms_path = paths['expanded_search_terms_crunchbase']
    search_terms_df = pd.read_excel(search_terms_path)
    search_terms = search_terms_df.iloc[:, 0].to_list()
    # search_terms = ['electric vehicles']  # test search

    logger.info(f"Found {len(search_terms)} search terms")

    orgs_api.query_companies_extended(search_terms, {
        "organizations": paths['expanded_orgs_data'],
        "funding_rounds": paths["expanded_orgs_funding_rounds"],
        "founders": paths["expanded_orgs_founders"],
        "investor_orgs": paths["expanded_orgs_investor_orgs"],
        "investor_person": paths["expanded_orgs_investor_person"]
    }, force_query=True)


def __remove_duplicates(input_list, track_field=None):
    if track_field is None:
        return list(set(input_list))

    seen = set()
    result = list()
    for item in input_list:
        if item[track_field] not in seen:
            result.append(item)
            seen.add(item[track_field])

    return result


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


def __get_investors(org_permalink: str, orgs_investors_df: pd.DataFrame, people_investors_df: pd.DataFrame):
    org_investors = orgs_investors_df[orgs_investors_df['org_permalink'] == org_permalink]
    people_investors = people_investors_df[people_investors_df['org_permalink'] == org_permalink]

    if org_investors.shape[0] == 0 and people_investors.shape[0] == 0:
        return None

    orgs_lst = org_investors.to_dict('records')
    people_lst = people_investors.to_dict('records')

    return __remove_duplicates([
        {
            "name": item['name'],
            "uuid": item['uuid'],
            "type": item['entity_def_id'],
            "category": item['investor_categories']
        } for item in [*orgs_lst, *people_lst]
    ], 'uuid')


def __get_values(input, field_name="value", default_value=None):
    if type(input) != str:
        return default_value

    data = ast.literal_eval(input)
    if type(data) == list:
        return [item[field_name] for item in data]

    return data[field_name] or default_value


def __add_funding_by_year(funding_rounds_df: pd.DataFrame, orgs_df: pd.DataFrame, filter_yr: int):
    # groupby org and year and summarize total raised
    df_by_org_yr = funding_rounds_df.groupby(['org_permalink', 'year_announced'])['raised_usd'].agg(sum).reset_index()
    df_by_org_yr = df_by_org_yr[df_by_org_yr['year_announced'] >= filter_yr]
    # transpose year 
    df_pivot = pd.pivot_table(df_by_org_yr, values='raised_usd', index=['org_permalink'],
                              columns='year_announced').reset_index()
    df_pivot.fillna(0, inplace=True)  # fill totals by
    # clean column names
    df_pivot.columns = ['org_permalink'] + [f"Funding_{year}" for year in df_pivot.columns.tolist()[1:]]
    # add funding by year cols to org data
    orgs_df = orgs_df.merge(df_pivot, left_on='permalink', right_on='org_permalink')
    return orgs_df  # orgs with funding by year


def __flag_goverment_investor(df_cb):
    investor_cats = df_cb['Investors Data'].apply(lambda i: __extract_field(i, 'category'))
    investor_cats = investor_cats.fillna("").apply(list)  # fill with empty list
    investor_cats = investor_cats.apply(lambda l: [c for c in l if c is not None])
    investor_cats = investor_cats.apply(lambda x: sum(x, []))
    return investor_cats.apply(lambda x: 'Government' in x)


# %%
def __process_crunchbase_raw_data(filter_yr=2016):
    logger.info('processing raw crunchbase data')
    df_orgs = cf.read_excel(paths['expanded_orgs_data'], ['facet_ids',
                                                          'aliases',
                                                          'permalink_aliases'])
    logger.info('processing orgs')
    df_funding_rounds = pd.read_excel(paths['expanded_orgs_funding_rounds'])
    logger.info('processing fr')
    df_founders = pd.read_excel(paths['expanded_orgs_founders'])
    logger.info('processing founders')
    df_investor_orgs = pd.read_excel(paths['expanded_orgs_investor_orgs'])
    logger.info('processing investor orgs')
    df_investor_person = pd.read_excel(paths['expanded_orgs_investor_person'])
    logger.info('processing investor persons')

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
    # filter out facet_ids that does not have 'company'
    df_cb = df_cb[df_cb['facet_ids'].apply(lambda x: 'company' in x)]
    founders_dict = __precompute_founders_dict(df_founders)

    # Apply functions
    df_cb['Founder Data'] = df_cb['permalink'].apply(lambda p: __extract_founders(p, founders_dict))
    df_cb['Founders'] = df_cb['Founder Data'].apply(lambda x: __extract_field(x, 'name'))
    logger.info('extracted founders')

    df_funding_rounds['org_permalink'] = df_funding_rounds['funded_organization_identifier'].apply(
        lambda x: ast.literal_eval(x)['permalink'])
    funding_rounds_dict, funding_rounds_dates_dict = __precompute_funding_rounds(df_funding_rounds)

    df_cb['Funding Types'] = df_cb['permalink'].apply(lambda p: __extract_funding_rounds(p, funding_rounds_dict))
    df_cb['Funding Types Dates'] = df_cb['permalink'].apply(
        lambda p: __extract_funding_rounds_dates(p, funding_rounds_dates_dict))
    df_cb['Last_Funding_Type'] = df_cb['Funding Types'].apply(lambda ft: ft[-1] if ft else None)
    logger.info('extracted funding types from funding rounds')

    df_cb['Year_Last_Funded'] = df_cb['last_funding_at'].apply(
        lambda lf: datetime.strptime(lf, '%Y-%m-%d').year if len(str(lf)) >= 10 else None)

    # get total funding by year for each org
    df_funding_rounds['year_announced'] = df_funding_rounds['announced_on'].apply(lambda x: int(x.split("-")[0]))
    df_funding_rounds['raised_usd'] = df_funding_rounds.money_raised.apply(lambda x: __get_values(x, 'value_usd', 0))
    df_cb = __add_funding_by_year(df_funding_rounds, df_cb, filter_yr)
    logger.info('extracted funding by year from funding rounds')

    # add year of first funding round from funding rounds data
    df_first_yr = df_funding_rounds.groupby('org_permalink')['year_announced'].agg(min).reset_index()
    df_cb = df_cb.merge(df_first_yr, on="org_permalink", how="left")
    df_cb.rename(columns={"year_announced": "First_Year_Funded"}, inplace=True)

    df_investor_orgs['investor_categories'] = df_investor_orgs.categories.apply(__get_values)
    df_investor_person['investor_categories'] = None

    df_cb['Investors Data'] = df_cb['permalink'].apply(
        lambda p: __get_investors(p, df_investor_orgs, df_investor_person))
    df_cb['Investors'] = df_cb['Investors Data'].apply(lambda i: __extract_field(i, 'name'))
    df_cb['Gov_Funder'] = __flag_goverment_investor(df_cb)
    df_cb['n_Funders'] = df_cb['Investors Data'].apply(lambda i: len(i) if i is not None else 0)
    df_cb['n_Employees'] = df_cb['num_employees_enum'].apply(convert_employees_enum)
    logger.info('extracted investors')

    # process json columns
    df_cb['founded_on'] = df_cb['founded_on'].apply(__get_values)
    df_cb['Year_Founded'] = df_cb['founded_on'].apply(
        lambda lf: datetime.strptime(lf, '%Y-%m-%d').year if len(str(lf)) >= 10 else None)

    df_cb['category_groups'] = df_cb['category_groups'].apply(__get_values)
    df_cb['categories'] = df_cb['categories'].apply(__get_values)
    df_cb['funding_total'] = df_cb['funding_total'].apply(lambda x: __get_values(x, 'value_usd', 0))
    df_cb['diversity'] = df_cb['diversity_spotlights'].apply(__get_values)

    df_cb['Acquired by'] = df_cb['acquirer_identifier'].apply(__get_values)
    df_cb['linkedin'] = df_cb['linkedin'].apply(lambda l: __get_values(l))

    df_cb['location_identifiers'] = df_cb['location_identifiers'].apply(lambda li: __get_values(li))
    df_cb['hq_address'] = df_cb['location_identifiers'].apply(lambda li: ', '.join(li) if li else None)

    df_cb['Data Source'] = "Crunchbase"

    # add link back to crunchbase with identifier column as https://www.crunchbase.com/entity_def_id/permalink
    df_cb['Crunchbase_URL'] = 'https://www.crunchbase.com/organization/' + df_cb['permalink']
    df_cb['company_type'] = df_cb.apply(lambda x: fcalc.deduce_org_type(company_row=x), axis=1)
    df_cb['Funding Stage'] = df_cb.apply(
        lambda x: fcalc.complete_stage_from_type(company_row=x), axis=1
    )

    df_cb = fcalc.grant_loan_flags(df_cb)

    df_cb.rename(columns={
        "company_type": "Org Type",
        "name": "Organization",
        "facit_ids": "Investor_Company",
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

    # extra filtering
    logger.info(f'filtering out companies last funded before {filter_yr}')
    df_cb = df_cb[df_cb['Year_Last_Funded'] >= filter_yr]

    df_cb['Description_990'] = ''

    df_cb['logo'] = ""
    df_cb['Donors'] = ""
    df_cb["Executives"] = ""
    df_cb['Board'] = ""
    df_cb['About_web'] = ""

    list_cols = ['Investors', 'Founders', 'industries_cb_cd', 'sectors_cb_cd']
    cf.string2list(df_cb, list_cols)
    for col in list_cols:
        # remove any empty elements of the list
        df_cb[col] = df_cb[col].apply(lambda l: [x for x in l if str(x) != 'nan'])
        df_cb[col] = df_cb[col].apply(lambda x: "|".join(x) if x else "")

    logger.info(f'writing cleaned cb data to {paths["cb_companies_cleaned"]}')
    cf.write_excel_no_hyper(df_cb, paths['cb_companies_cleaned'])

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
    df_fr = cf.read_excel(fr_path, ['funded_organization_diversity_spotlights',
                                    'funded_organization_categories',
                                    'funded_organization_location',
                                    'lead_investor_identifiers',
                                    'investor_identifiers'
                                    ])

    logger.info('loaded funding rounds')
    # get year announced
    df_fr['year_announced'] = df_fr['announced_on'].apply(lambda x: int(x.split("-")[0]))
    # get total raised
    df_fr['raised_usd'] = df_fr.money_raised.apply(lambda x: __get_values(x, 'value_usd', 0))
    df_fr = df_fr[df_fr['year_announced'] >= filter_yr].copy()
    # get org permalink
    df_fr['org_permalink'] = df_fr['funded_organization_identifier'].apply(lambda x: __get_values(x, 'permalink', None))
    df_fr['org_uuid'] = df_fr['funded_organization_identifier'].apply(lambda x: __get_values(x, 'uuid', None))
    # get org country
    df_fr['country'] = df_fr['funded_organization_location'].apply(lambda x: __get_org_country_from_fr(x) if isinstance(x, list) else None)
    # get diversity spotlights
    df_fr['diversity'] = df_fr['funded_organization_diversity_spotlights'].apply(lambda x: __get_values(x, 'value', None))
    keep_columns = ['org_permalink',
                    'org_uuid',
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


def prepare_raw_crunchbase(filter_yr=2016):
    run_query_crunchbase = config.getboolean('run_query_crunchbase')
    run_crunchbase = config.getboolean('run_crunchbase')

    if run_query_crunchbase:
        __query_crunchbase_raw_data()

    if run_crunchbase:
        return __process_crunchbase_raw_data(filter_yr=filter_yr)

    if paths['cb_companies_cleaned'].exists():
        #  TODO: this should only run if "run_crunchbase" is False?
        print("loading pre-processed crunchbase data")
        return cf.read_excel_wLists(paths['cb_companies_cleaned'])

    return pd.read_excel(paths['cb_companies_cleaned'])


if __name__ == '__main__':
    # res = __process_crunchbase_raw_data()
    # cf.write_excel_no_hyper(res, 'test_cb_orgs.xlsx')
    import json
    import pathlib as pl

    wd = pl.Path.cwd()

    # old_search_terms = pd.read_excel(wd / 'climate_landscape' / 'common' / 'data' / 'search_terms' / 'climate_search_terms_2022Q1.xlsx')
    # new_search_terms = pd.read_excel(wd / 'climate_landscape' / 'common' / 'data' / 'search_terms' / 'climate_search_terms_2023Q1_expanded.xlsx')
    # new_search_terms['search_term'] = new_search_terms['search_term'].apply(lambda x: ast.literal_eval(x))
    # new_search_terms = new_search_terms.explode('search_term')

    # res_lst = {}
    # for lbl, item in old_search_terms.iterrows():
    #     res_lst[item['search_term']] = ['old']

    # for lbl, item in new_search_terms.iterrows():
    #     st = item['search_term']
    #     if st in res_lst:
    #         res_lst[st].append('new')
    #     else:
    #         res_lst[st] = ['new']

    # keys = list(res_lst.keys())
    # for key in keys:
    #     if len(res_lst[key]) > 1:
    #         del res_lst[key]

    # res_df = pd.DataFrame.from_dict(res_lst, orient="index")
    # res_df.to_csv('us-cft-search-terms-diff.csv')

    with open(wd / 'us-cft-nodes.json', 'r') as f:
        old_us_cft_data = json.loads(f.read())


    def __get_id(data):
        if not data:
            return None
        return ''.split('/')[-1]


    orgs = [{
        'Name': item['attr']['Name'],
        'Crunchbase': item['attr']['Crunchbase'],
        'Total Funding': item['attr']['Total Funding'],
        'Description': item['attr']['Description'],
        'Description_990': item['attr']['Description_990'],
        'search_terms': item['attr']['search_terms']
    } for item in old_us_cft_data['datapoints'] if len(item["attr"]['Crunchbase']) > 0]

    # df_orgs = pd.read_csv(wd / 'climate_landscape' / 'common' / 'data' / 'candid' / '2023_04_27' / 'candid_main.txt', sep="|", encoding="UTF-16 LE", error_bad_lines=False)
    df_orgs = pd.read_excel(
        wd / 'climate_landscape' / 'common' / 'data' / 'crunchbase' / 'organizations_search_terms2023Q1_us_only.xlsx')

    # check that all orgs from previous dataset are in the current one
    old_new_diff = []

    for old_item in orgs:
        # if len(df_orgs[df_orgs['profile_link'] == old_item['Candid']]) == 0:
        if len(df_orgs[df_orgs['name'] == old_item['Name']]) == 0:
            old_new_diff.append(old_item)

    print(f'Grand total diff: {len(old_new_diff)}')

    with open(wd / 'us-cft-cb-diff.json', 'w+') as f:
        f.write(json.dumps(old_new_diff))

    total_wo_funding = len([item for item in old_new_diff if item['Total Funding'] == 0])
    print(f'Total orgs without funding: {total_wo_funding}')
    print(f'Total orgs not included in the new dataset: {len(old_new_diff) - total_wo_funding}')
