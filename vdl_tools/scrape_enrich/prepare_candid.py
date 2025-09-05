import pandas as pd

import vdl_tools.scrape_enrich.prepare_candid_old as cd  # from scrape-enrich
import vdl_tools.shared_tools.project_config as pc
import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used functions
from vdl_tools.shared_tools.tools.logger import logger
paths = pc.get_paths()


def __add_funding_by_year(df_cd: pd.DataFrame, start_year=2017, year_col_prefix='total_funding_'):
    logger.info("\nAdding total funding by year")
    # Read the columns from the Excel file
    df_cd_by_yr = pd.read_excel(paths['candid_source_data'] / "candid_main_programs_w_yrs.xlsx", sheet_name='main', engine='openpyxl')
    # Find columns matching the pattern and >= start_year
    funding_cols = [col for col in df_cd_by_yr.columns
                    if col.startswith(year_col_prefix) and int(col[-4:]) >= start_year]
    # Extract years from columns with the correct prefix and 4-digit year at the end
    years = [
        int(col[-4:])
        for col in df_cd_by_yr.columns
        if col.startswith(year_col_prefix) and col[-4:].isdigit()
    ]
    if years:
        logger.info(f"Funding year columns found for years: {min(years)} to {max(years)}")
    else:
        logger.info("No funding year columns found.")
    if not funding_cols:
        logger.info("Not all funding columns found in the data. Skipping merge.")
        return df_cd
    keep_cols = ['ein'] + funding_cols
    #df_cd_by_yr = pd.read_excel(paths['candid_source_data'] / "candid_main_programs_w_yrs.xlsx", sheet_name='main')[keep_cols]
    df_cd_by_yr = df_cd_by_yr[keep_cols].copy()
    df_cd_by_yr.columns = ['id'] + [f"Funding_{col[-4:]}" for col in funding_cols]
    df_cd_by_yr.fillna(0, inplace=True)
    df_cd = df_cd.merge(df_cd_by_yr, on='id', how='left')
    return df_cd


def prepare_raw_candid(
    process_candid: bool,
    total_funding=20000,
    filter_yr=2016
):
    if not process_candid:
        if paths['cd_orgs_cleaned'].exists():
            print("loading pre-processed candid data")
            return pd.read_excel(paths['cd_orgs_cleaned'])
        
        return None
     # process raw candid data
    df_cd = cd.process_candid(
        paths['candid_source_data']/"candid_main.txt",   # cp.cd_main,
        paths['candid_source_data']/"candid_funders.txt",  # cp.cd_funders,
        paths['candid_source_data']/"candid_personnel.txt",  # cp.cd_personnel,
        paths['candid_source_data']/"candid_programs.txt",  # cp.cd_programs,        
        cd_filings=paths['candid_source_data']/"candid_filings.txt",  # cp.cd_filings, descriptions from 990s
        outfile=None  # ref.candid_orgs_cleaned
        )
    df_cd['Data Source'] = "Candid"
    
    # ADD GOVERNMENT FUNDER
    cd_funders_meta = paths['candid_source_data']/"candid_funders_metadata.txt" # funder metadata
    print('reading funder metadata')
    if cd_funders_meta.exists():
        df_funder_meta = pd.read_csv(cd_funders_meta, sep="|", encoding="UTF-16", on_bad_lines='warn', dtype=str)
        print('reading funder metadata')
        # look for the column that maybe is missing in candid's new pull
        if 'gm_type' in df_funder_meta.columns: 
            gov_funders = df_funder_meta[df_funder_meta.gm_type == 'GO']['gm_name'].tolist()
            df_cd['Funders'].fillna('', inplace=True)
            df_cd['Gov_Funder'] = df_cd['Funders'].apply(lambda x: any(f in x.split('|') for f in gov_funders))
        else:
            if 'funder_type' in df_funder_meta.columns:
                gov_dictionary = {
                    'Governments and agencies',
                    'Local governments and agencies',
                    'National governments and agencies',
                    'Quasi-governmental agencies',
                    'State or provincial governments and agencies',
                    'Tribal governments and agencies'
                }
                pattern = '|'.join(gov_dictionary) # temp fix b/c their metadata should have one type only and now can have more than one type
                gov_funders = df_funder_meta[df_funder_meta.funder_type.str.contains(pattern, na=False)][
                    'gm_name'].tolist()
                df_cd['Funders'].fillna('', inplace=True)
                df_cd['Gov_Funder'] = df_cd['Funders'].apply(lambda x: any(f in x.split('|') for f in gov_funders))
            else: # try to use old metadata if funder type not available
                print('funder metadata missing gm_type and funder_type, trying old metadata')
                cd_funders_meta = paths['candid_source_data_previous']/"candid_funders_metadata.txt"
                if cd_funders_meta.exists():
                    print('reading funder metadata old')

                    df_funder_meta = pd.read_csv(cd_funders_meta, sep="|", encoding="UTF-16", on_bad_lines='warn',dtype=str)
                    gov_funders = df_funder_meta[df_funder_meta.gm_type == 'GO']['gm_name'].tolist()
                    df_cd['Funders'].fillna('', inplace=True)
                    df_cd['Gov_Funder'] = df_cd['Funders'].apply(lambda x: any(f in x.split('|') for f in gov_funders))
                    print('done with funder metadata')
                else:
                    df_cd['Gov_Funder'] = None
    
    # filter candid:
    numeric_columns = ['Total_Funding_$', 'Year_Last_Funded']
    for col in numeric_columns:
        df_cd[col] = pd.to_numeric(df_cd[col], errors='coerce')
    print("\nFiltering Candid data for total funding <", total_funding)
    df_cd = df_cd[df_cd['Total_Funding_$'] > total_funding ]
    df_cd = df_cd.reset_index(drop=True)

    cd_metacols = ['Organization Name', 'Funders', 'Org Type',  # core info
                    'sector_cd', 'industry_cd',  # sectors and industry tags
                    'Funding Stage', 'Funding Types', 'Last_Funding_Type',  # type of funding
                    'Founders', 'Board', 'Executives',  # people tags
                    'Data Source',
                    'Description', 'Description_990',  # text descriptions
                    'hq_address',  # address
                    'n_Employees', 'Employees_cd',  # org size
                    'Total_Funding_$', 'Year_Last_Funded',
                    'n_Funders', 'n_Grants',
                    'Website', 'LinkedIn',  # urls
                    'Candid_URL', 'logo',  # unique to candid
                    'Gov_Funder',
                    'id']
    df_cd = df_cd[cd_metacols]
    cd_rename = {'Organization Name': 'Organization',
                    'sector_cd': 'sectors_cb_cd',
                    'industry_cd': 'industries_cb_cd',
                    'Funders': 'Donors',
                    'Website': 'Website_cb_cd',
                    }
                    
    df_cd.rename(columns=cd_rename, inplace=True)
    
    # clean ALL CAPS 990 text
    print("\nCleaning 990 from ALL CAPS")
    df_cd['Description_990'].fillna('', inplace=True)
    df_cd['Description_990'] = df_cd['Description_990'].apply(lambda x: cd.clean_990(x) if x != '' else '')
    
    # add funding by year
    df_cd = __add_funding_by_year(df_cd, start_year=filter_yr)

    # write cleaned file
    cf.write_excel_no_hyper(df_cd, paths['cd_orgs_cleaned'])

    return df_cd
