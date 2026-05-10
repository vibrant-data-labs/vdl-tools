"""Build the Giving Tuesday CB-shaped DataFrame for ed-tracker / similar pipelines.

Cut over to gt_datamart via ``givingtuesday_datamart.client``: keyword search
runs as Postgres FTS over ``public.nonprofit_text.text_tsv`` (replacing the
S3 parquet + FlashText path), and the per-EIN basic-fields / grants reads go
through the typed client surface.
"""

from dataclasses import asdict

import pandas as pd
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.parquet_cache import write_dataframe

from givingtuesday_datamart.client import GtDatamartClient


def _make_default_client() -> GtDatamartClient:
    """Build a GtDatamartClient from vdl-tools' postgres config.

    The client itself does not depend on vdl-tools (so it is shippable as a
    standalone package); this adapter lives on the vdl-tools side and maps
    ``get_configuration()`` to the client's component args. Database is
    pinned to ``gt_datamart`` regardless of what the local config has set.
    """
    pg = get_configuration()["postgres"]
    return GtDatamartClient(
        host=pg["host"],
        port=pg["port"],
        user=pg["user"],
        password=pg["password"],
        database="gt_datamart",
    )


def search_for_eins(
    client,
    search_terms_list=None,
    search_terms_path=None,
    search_mode='exact',
    return_full_text=False,
):
    """Return EINs whose ``nonprofit_text`` matches any of the keywords.
    """
    logger.info("Searching for EINs with search terms")
    if not any([search_terms_list, search_terms_path]):
        raise ValueError("Must provide either search_terms_list or search_terms_path")

    if search_terms_list:
        keywords = search_terms_list
    else:
        keywords = pd.read_csv(search_terms_path)['term'].tolist()

    hits = client.search_nonprofits(
        keywords,
        return_text=return_full_text,
        search_mode=search_mode,
    )
    if not hits:
        return pd.DataFrame(columns=['filerein', 'full_text'])

    results = pd.DataFrame.from_records([
        {'filerein': h.ein, 'full_text': h.unique_text or ''} for h in hits
    ])
    logger.info(f"Found {len(results)} EINs with search terms")
    return results


def query_basic_fields_db_for_eins(
    client,
    eins_list,
    filter_yr=2017,
):
    rows = client.get_basic_fields(eins_list, min_taxyear=filter_yr)
    if not rows:
        return pd.DataFrame(columns=[
            'filerein', 'filername1', 'filername2', 'taxyear',
            'filerus1', 'filerus2', 'fileruscity', 'filerusstate', 'fileruszip',
            'websitsiteit',
            'total_revenue_current_year', 'total_cash_contributions',
            'total_cash_contributions_no_gov',
        ])
    df = pd.DataFrame.from_records([asdict(r) for r in rows])
    # Restore the old column names downstream pivot/reformat helpers expect.
    df.rename(columns={
        'ein': 'filerein',
        'name': 'filername1',
        'name_secondary': 'filername2',
        'addr_line_1': 'filerus1',
        'addr_line_2': 'filerus2',
        'city': 'fileruscity',
        'state': 'filerusstate',
        'zip': 'fileruszip',
        'website': 'websitsiteit',
    }, inplace=True)
    return df


def query_grant_summaries_for_eins(
    client,
    eins_list,
    filter_yr=2017,
):
    """Server-side aggregated grants for ``eins_list``.

    One row per ``(grantee_ein, taxyear)`` with the same totals + funder
    rollups the downstream pivot needs, so we never pay the wire cost of
    moving raw grant rows the caller immediately groups away.
    """
    logger.info(f"Querying grant summaries for {len(eins_list)} EINs")
    summaries = client.get_grant_summaries(eins_list, role='grantee', min_taxyear=filter_yr)
    if not summaries:
        return pd.DataFrame(columns=[
            'grantee_ein', 'taxyear', 'total_grant_amount', 'grant_count',
            'granter_eins', 'granter_names',
        ])
    df = pd.DataFrame.from_records([asdict(s) for s in summaries])
    df.rename(columns={'ein': 'grantee_ein'}, inplace=True)
    logger.info(f"Found {len(df)} (EIN, year) grant summaries for {len(eins_list)} EINs")
    return df


def filter_format_grants_data(
    grant_summaries,
    avg_yearly_funding_min=0, # Require they received a grant in any year but don't require a minimum amount
):
    """Pivot the per-(EIN, year) summaries into the wide ``grant_YYYY`` form.

    ``grant_summaries`` already has one row per ``(grantee_ein, taxyear)``
    with ``total_grant_amount`` summed server-side. The funder columns are
    deduped per year on the server, so the final ``Funders`` / ``Funder_Names``
    sets are just unions across years.
    """
    logger.info(f"Filtering and formatting grants data")
    grant_summaries = grant_summaries.copy()

    avg_yearly_funding = (
        grant_summaries
        .groupby('grantee_ein')['total_grant_amount']
        .mean()
        .reset_index()
    )
    filtered_eins = avg_yearly_funding[
        avg_yearly_funding['total_grant_amount'] >= avg_yearly_funding_min
    ]['grantee_ein'].tolist()
    filtered = grant_summaries[grant_summaries['grantee_ein'].isin(filtered_eins)]

    max_funding_year = filtered.groupby('grantee_ein')['taxyear'].max().to_dict()
    funding_df = filtered.pivot_table(
        index='grantee_ein',
        columns='taxyear',
        values='total_grant_amount',
        aggfunc='first',
    )
    funding_df = funding_df.fillna(0)
    funding_df = funding_df.reset_index()
    funding_df.rename(columns={year: f"grant_{year}" for year in funding_df.columns[1:]}, inplace=True)

    funding_df.set_index('grantee_ein', inplace=True)
    funding_df['total_grants_amount'] = funding_df.sum(axis=1)
    funding_df.reset_index(inplace=True)
    funding_df['last_grant_year'] = funding_df['grantee_ein'].map(max_funding_year)

    # Union across years — server already deduped within each year.
    funder_data = (
        filtered
        .groupby('grantee_ein')
        .agg({
            'granter_eins': lambda series: sorted({e for arr in series for e in (arr or [])}),
            'granter_names': lambda series: sorted({n for arr in series for n in (arr or [])}),
        })
        .reset_index()
        .rename(columns={'granter_eins': 'Funders', 'granter_names': 'Funder_Names'})
    )

    funding_df = funding_df.merge(funder_data, left_on='grantee_ein', right_on='grantee_ein', how='left')
    logger.info(f"Merged funder data with funding data")
    return funding_df


def filter_format_funding_data(
    basic_fields_data_long,
    column='total_cash_contributions',
    avg_yearly_contributions_min=100000
):
    logger.info(f"Filtering and formatting contributions data")
    basic_fields_data_long = basic_fields_data_long.copy()
    avg_yearly_contributions = basic_fields_data_long.groupby(['filerein', 'taxyear'])[column].sum().groupby('filerein').mean().reset_index()

    filtered_eins = avg_yearly_contributions[avg_yearly_contributions[column] >= avg_yearly_contributions_min]['filerein'].tolist()
    filtered_data = basic_fields_data_long[basic_fields_data_long['filerein'].isin(filtered_eins)]

    annual_funding = filtered_data.groupby(['filerein', 'taxyear'])[column].sum().reset_index()
    funding_df = annual_funding.pivot_table(
        index='filerein',
        columns='taxyear',
        values=column,
        aggfunc='first',
    )
    funding_df = funding_df.fillna(0)
    funding_df = funding_df.reset_index()
    funding_df.rename(columns={year: f"{column}_{year}" for year in funding_df.columns[1:]}, inplace=True)

    funding_df.set_index('filerein', inplace=True)
    funding_df[f'total_{column}'] = funding_df.sum(axis=1)
    funding_df.reset_index(inplace=True)

    return funding_df


def reformat_basic_fields_data(
    basic_fields_data,
    ein_to_full_text,
    funding_dfs=[],
    column_for_funding='total_cash_contributions',
):
    logger.info(f"Reformatting basic fields data")
    basic_fields_data = basic_fields_data.copy()
    basic_fields_data.drop_duplicates(subset=['filerein'], inplace=True)

    basic_fields_data['Description'] = basic_fields_data['filerein'].map(ein_to_full_text)
    for join_key, funding_df in funding_dfs:
        basic_fields_data = basic_fields_data.merge(
            funding_df,
            left_on='filerein',
            right_on=join_key,
            how='left',
        )
        if join_key != 'filerein':
            basic_fields_data.drop(columns=[join_key], inplace=True)

    basic_fields_data['hq_address'] = basic_fields_data.apply(
        lambda x: f"{x['filerus1']} {x['filerus2']} {x['fileruscity']}, {x['filerusstate']} {x['fileruszip']}",
        axis=1
    )
    basic_fields_data["Last_Funding_Type"] = "Grant"
    basic_fields_data['Funding Stage'] = 'Philanthropy'
    basic_fields_data['Philanthropy_vs_Venture'] = 'Philanthropy'
    basic_fields_data['Data Source'] = 'Giving Tuesday'
    basic_fields_data['Org Type'] = 'Non Profit'
    basic_fields_data.rename(columns={
        "filername1": "Organization",
        "websitsiteit": "Website_cb_cd",

    }, inplace=True)

    basic_fields_data['ein'] = basic_fields_data['filerein'].apply(reformat_ein)
    basic_fields_data['id'] = basic_fields_data['ein'].apply(lambda x: f"givingtuesday_{x}")

    basic_fields_data.drop(
        columns=[
            'filerein',
            'filername2',
            'filerus1',
            'filerus2',
            'fileruscity',
            'filerusstate',
            'fileruszip',
            'taxyear',
        ],
        inplace=True,
    )
    basic_fields_data['Website_cb_cd'] = basic_fields_data['Website_cb_cd'].apply(lambda x: x.lower() if isinstance(x, str) else None)

    # Now duplicate the funding data for the column_for_funding but with Funding_ prefix like the CB data
    funding_cols = [col for col in basic_fields_data.columns if col.startswith(column_for_funding)]
    for funding_col in funding_cols:
        # For the year columns, we need to get the year from the column name
        running_total = 0
        last_year = None
        if funding_col[-4:].isnumeric():
            year = int(funding_col[-4:])
            new_col_name = f"Funding_{year}"
            running_total += basic_fields_data[funding_col]
            basic_fields_data[new_col_name] = basic_fields_data[funding_col]
            if not last_year:
                last_year = year
            else:
                last_year = max(last_year, year)
    basic_fields_data['Total_Funding_$'] = running_total
    basic_fields_data['Last_Funding_Year'] = last_year

    return basic_fields_data


def reformat_ein(filerein):
    if len(filerein) == 10:
        if '-' not in filerein:
            raise Exception("Expecting 10 length EIN to have a `-`")
        return filerein
    filerein = str(filerein)
    if len(filerein) == 8:
        reformatted_ein = '0' + filerein
    else:
        reformatted_ein = filerein
    return reformatted_ein[:2] + "-" + reformatted_ein[2:]


def query_process_givingtuesday_data(
    search_terms_list=None,
    search_terms_path=None,
    search_mode='exact',
    proceessed_output_path=None,
    require_one_grant=True,
    filter_yr=2017,
    avg_yearly_contributions_min=300000,
    avg_yearly_funding_grants_min=0,
    column_for_funding='total_cash_contributions',
    client=None,
    return_full_text=True,
):
    client = client or _make_default_client()

    # 1) FTS over nonprofit_text → candidate EIN universe.
    search_results = search_for_eins(
        client=client,
        search_terms_list=search_terms_list,
        search_terms_path=search_terms_path,
        return_full_text=return_full_text,
        search_mode=search_mode,
    )
    candidate_eins = search_results['filerein'].unique().tolist()
    ein_to_full_text = dict(search_results[['filerein', 'full_text']].values)

    # 2) Push the avg-contributions threshold into Postgres so we don't pull
    #    multi-year basic_fields rows for EINs we'll discard. With the default
    #    avg_yearly_contributions_min=300000 this typically prunes the EIN
    #    universe ~5-10x. NOTE: this changes behavior vs. the prior LEFT-join
    #    flow, where below-threshold EINs survived to output with NaN contribs
    #    columns. They're now dropped — which appears to be the intent of the
    #    threshold parameter.
    if avg_yearly_contributions_min and avg_yearly_contributions_min > 0:
        eligible_eins = client.find_eins_with_min_avg_contributions(
            candidate_eins,
            min_taxyear=filter_yr,
            min_avg=avg_yearly_contributions_min,
        )
    else:
        eligible_eins = candidate_eins

    # 3) basic_fields for the (now smaller) eligible set.
    all_data_long = query_basic_fields_db_for_eins(client, eligible_eins, filter_yr=filter_yr)

    # 4) Server-side aggregated grants for the same eligible set.
    grant_summaries_df = query_grant_summaries_for_eins(
        client,
        eligible_eins,
        filter_yr=filter_yr,
    )
    grants_funding_df = filter_format_grants_data(
        grant_summaries_df,
        avg_yearly_funding_min=avg_yearly_funding_grants_min,
    )

    # 5) require_one_grant narrows the basic_fields rows to EINs that had a
    #    grant survive the avg_yearly_funding_grants_min filter above.
    if require_one_grant:
        kept = set(grants_funding_df['grantee_ein'].unique().tolist())
        all_data_long = all_data_long[all_data_long['filerein'].isin(kept)]

    total_cash_contributions_df = filter_format_funding_data(
        all_data_long,
        column='total_cash_contributions',
        avg_yearly_contributions_min=avg_yearly_contributions_min,
    )

    all_data_reformatted = reformat_basic_fields_data(
        all_data_long,
        ein_to_full_text,
        funding_dfs=[
            ('grantee_ein', grants_funding_df),
            ('filerein', total_cash_contributions_df)
        ],
        column_for_funding=column_for_funding,
    )
    if proceessed_output_path:
        write_dataframe(all_data_reformatted, proceessed_output_path)
    return all_data_reformatted


if __name__ == "__main__":
    filter_yr = 2023
    query_process_givingtuesday_data(
        search_terms_list=["teachers", "education"],
        # search_terms_path=paths['nonprofit_search_terms'],
        proceessed_output_path="s3://ed-tracker-data/givingtuesday/giving_tuesday_data_cleaned.json",
        filter_yr=filter_yr,
        column_for_funding='total_cash_contributions',
    )
