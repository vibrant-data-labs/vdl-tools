from typing import TypedDict
import pandas as pd
import pathlib as pl
import ast
import vdl_tools.scrape_enrich.crunchbase.companies_api as companies
import vdl_tools.scrape_enrich.crunchbase.funding_rounds_api as funding_rounds
import vdl_tools.scrape_enrich.crunchbase.people_api as people
import vdl_tools.scrape_enrich.crunchbase.api as api
import vdl_tools.shared_tools.common_functions as cf

__query_step = 200

class OutputFilePaths(TypedDict):
    organizations: str
    funding_rounds: str
    investor_person: str
    investor_orgs: str
    founders: str

wd = pl.Path.cwd()

default_file_paths: OutputFilePaths = {
    "organizations": wd / 'organizations.xlsx',
    "funding_rounds": wd / 'funding_rounds.xlsx',
    "investor_person": wd / 'investors_person.xlsx',
    "investor_orgs": wd / 'investor_organizations.xlsx',
    "founders": wd / 'org_founders.xlsx'
}


def __iterative_query(query_method, fields, search_field, query_list, extra_filters = [], operator = 'contains'):
    if operator == 'includes':
        method = api.includes
    elif operator == 'eq' or operator == 'equals':
        method = api.eq
    elif operator == 'domain_eq':
        method = api.domain_eq
    else:
        method = api.contains

    result_dict = dict()
    for i in range(0, len(query_list), __query_step):
        print(f'Filtering by items from {i + 1} to {i + __query_step} out of {len(query_list)}')
        result_dict[i] = query_method(
            fields = fields,
            filters = [
                method(search_field, query_list[i:i+__query_step]),
                *extra_filters
            ]
        )

    return pd.concat(result_dict, ignore_index=True)


def companies_query(search_terms_list, extra_filters):
    print('Querying companies by description...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "description",
        search_terms_list,
        extra_filters=extra_filters
    )


def companies_query_names(search_terms_list, extra_filters):
    print('Querying companies by names...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "identifier",
        search_terms_list,
        extra_filters=extra_filters
    )


def companies_id_query(ids):
    print('Querying companies by id...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "identifier",
        ids,
        operator="includes"
    )


def companies_query_urls(urls, extra_filters=[]):
    print('Querying companies by url...')
    return __iterative_query(
        companies.query,
        companies.all_fields,
        "website_url",
        [urls],
        operator="domain_includes",
        extra_filters=extra_filters,
    )


def funding_rounds_query(companies_uuids):
    print('Querying funding rounds by organization identifier...')
    return __iterative_query(
        funding_rounds.query,
        funding_rounds.all_fields,
        "funded_organization_identifier",
        companies_uuids,
        operator="includes"
    )


def funding_rounds_query_by_investor_id(investor_uuids):
    print('Querying funding rounds by organization identifier...')
    return __iterative_query(
        funding_rounds.query,
        funding_rounds.all_fields,
        "investor_identifiers",
        investor_uuids,
        operator="includes"
    )


def people_query(people_ids):
    print('Querying people by identifier...')
    return __iterative_query(
        people.query,
        people.all_fields,
        "identifier",
        people_ids,
        operator="includes"
    )

def __value_to_obj(x):
    if type(x) is str:
        return ast.literal_eval(x)
    
    return x

def __get_aggregated_data(organizations: pd.DataFrame, list_column: str):
    temp_data = organizations[[list_column, "permalink"]]
    temp_data = temp_data.rename(columns = {"permalink": "org_permalink"})
    temp_data = temp_data[temp_data[list_column].notnull()]
    temp_data[list_column] = temp_data[list_column].apply(lambda x: ast.literal_eval(x) if type(x) is str else x)
    temp_data = temp_data.explode(list_column)
    temp_data = temp_data.reset_index()
    temp_data = temp_data[temp_data[list_column].notnull()]
    temp_data["type"] = temp_data.apply(lambda x: x[list_column]['entity_def_id'] if 'permalink' in x[list_column] else None, axis=1)
    temp_data = temp_data[temp_data['type'].notnull()]
    temp_data['permalink'] = temp_data.apply(lambda x: x[list_column]['permalink'], axis=1)
    return temp_data


def __investors_query(investor_temp_data: pd.DataFrame, entity_type):
    df = investor_temp_data[investor_temp_data["type"] == entity_type]
    if entity_type == "person":
        ids = df['permalink'].to_list()
        if len(ids) == 0:
            return None

        return people_query(ids)
    
    companies_ids = list(set(df['permalink'].to_list()))
    if len(companies_ids) == 0:
        return None

    return companies_id_query(companies_ids)


def __combine_lists(x):
    res_list = x[~x.isnull()].apply(__value_to_obj).to_list()
    res_list = [item for sublist in res_list for item in sublist]
    res_list = list(
        {
            item['permalink']: item
            for item in res_list
        }.values()
    )

    return res_list


def __aggregate_investors(funding_rounds_df: pd.DataFrame):
    temp_data = funding_rounds_df.groupby('_funded_org_id').aggregate({
        'investor_identifiers': __combine_lists
    })
    temp_data = temp_data.reset_index()
    temp_data['org_permalink'] = temp_data['_funded_org_id']
    temp_data = temp_data.explode('investor_identifiers')
    temp_data = temp_data.reset_index()
    temp_data = temp_data[temp_data['investor_identifiers'].notnull()]
    temp_data['type'] = temp_data['investor_identifiers'].apply(lambda x: x['entity_def_id'])
    temp_data['permalink'] = temp_data['investor_identifiers'].apply(lambda x: x['permalink'])
    return temp_data


def query_companies_extended(search_terms_list, output_files: OutputFilePaths = default_file_paths, search_condition = '', extra_filters = [], force_query = False):
    organizations: pd.DataFrame = None

    if pl.Path.exists(output_files['organizations']) and not force_query:
        organizations = pd.read_excel(output_files['organizations'])
    else:
        if search_condition == 'name':
            organizations = companies_query_names(search_terms_list, extra_filters)
        elif search_condition == 'url':
            organizations = companies_query_urls(search_terms_list, extra_filters)

        else:
            organizations = companies_query(search_terms_list, extra_filters)
        organizations = organizations.loc[organizations.astype(str).drop_duplicates().index]
        cf.write_excel_no_hyper(organizations, output_files["organizations"])
    companies_funding_rounds: pd.DataFrame = None
    if pl.Path.exists(output_files['funding_rounds']) and not force_query:
        companies_funding_rounds = pd.read_excel(output_files['funding_rounds'])
    else:
        orgs_with_funding = organizations[organizations['num_funding_rounds'] > 0]
        org_ids = orgs_with_funding[orgs_with_funding['permalink'].notnull()]['permalink'].to_list()
        print(f"Found {len(org_ids)} organizations with funding")
        companies_funding_rounds = funding_rounds_query(org_ids)
        companies_funding_rounds = companies_funding_rounds.loc[companies_funding_rounds.astype(str).drop_duplicates().index]
        cf.write_excel_no_hyper(companies_funding_rounds, output_files["funding_rounds"])

    companies_funding_rounds['funded_organization_identifier'] = companies_funding_rounds['funded_organization_identifier'].apply(__value_to_obj)
    companies_funding_rounds['_funded_org_id'] = companies_funding_rounds['funded_organization_identifier'].apply(lambda x: x['permalink'])
    investor_temp_data = __aggregate_investors(companies_funding_rounds)
    
    print(f"Found {investor_temp_data.shape[0]} organizations with known investors")
    people_investors = __investors_query(investor_temp_data, "person")
    if people_investors is None:
        print('Investors of type `person` are not found, skipping...')
    else:
        temp_investors = investor_temp_data[investor_temp_data['type'] == 'person'][['permalink', 'org_permalink']]
        people_investors = pd.merge(temp_investors, people_investors, how="left", on="permalink")
        people_investors = people_investors.loc[people_investors.astype(str).drop_duplicates().index]
        cf.write_excel_no_hyper(people_investors, output_files['investor_person'])

    org_investors = __investors_query(investor_temp_data, "organization")
    if org_investors is None:
        print('Investors of type `organizations` are not found, skipping...')
    else:
        temp_investors = investor_temp_data[investor_temp_data['type'] == 'organization'][['permalink', 'org_permalink']]
        org_investors = pd.merge(temp_investors, org_investors, how="left", on="permalink")
        org_investors = org_investors.loc[org_investors.astype(str).drop_duplicates().index]
        cf.write_excel_no_hyper(org_investors, output_files['investor_orgs'])

    founders_temp_data = __get_aggregated_data(organizations, "founder_identifiers")
    print(f"Found {founders_temp_data.shape[0]} organizations with known founders")
    founder_ids = founders_temp_data['permalink'].to_list()
    if len(founder_ids) == 0:
        print('No founders found, skipping...')
    else:
        founders = people_query(founder_ids)
        founders = pd.merge(founders_temp_data[['permalink', 'org_permalink']], founders, how="left", on="permalink")
        founders = founders.loc[founders.astype(str).drop_duplicates().index]
        cf.write_excel_no_hyper(founders, output_files['founders'])

if __name__ == '__main__':
    '''
    res = query_companies_extended([
        "amply-power"
    ],
    search_condition='name',
    extra_filters=[
        api.eq("status", "was_acquired")
    ])

    print (res)
    '''
    query_companies_extended([
         "local farms",
         "fisheries"
     ],
     extra_filters=[
         api.eq("status", "operating")
     ])
