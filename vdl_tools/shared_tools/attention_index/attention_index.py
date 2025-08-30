import numpy as np
import pandas as pd

from vdl_tools.shared_tools.tools.numeric_normalizations import (
    geometric_mean_values,
    max_min_normalization,
)

MISSING_SUFFIX = ""  # suffix to add to missing levels, e.g. ": Other"

# %% helper function
def clean_no_level(
    df,
    levels,  # ['level0', 'level1', 'level2', 'level3'],
    suffix=MISSING_SUFFIX,  # suffix to add to missing levels
):
    """
    clean "No_Level_" prefix from the distributed funding results
    and replace it with add a new custom suffix after the parent name (e.g. "Parent: Other")
    """
    for level in levels:
        # convert "No level" to Parent + Suffix ("Other") for each one earth levels
        df[level] = df[level].fillna("")
        df[level] = df[level].apply(
            lambda x: f"{x.split('_')[-1]}{suffix}"
            if (isinstance(x, str) and "No_Level_" in x)
            else x
            if x != ""
            else ""
        )
    return df



def clean_industries(df, tax_cols, list_cols=None):
    # strip "Industries & Services: " from the beginning of each industry string
    for col in tax_cols:
        df[col] = df[col].apply(
            lambda x: x.replace("Industries & Services: ", "")
            if isinstance(x, str)
            else x
        )
    if list_cols:
        # strip "Industries & Services: " from each item in the list columns
        for col in list_cols:
            df[col] = df[col].apply(
                lambda x: [item.replace("Industries & Services: ", "") for item in x]
                if isinstance(x, list)
                else x
            )
    return df


def funding_attention_index(
    df,
    cols,  # list of columns to compute attention index for
    remove_nan=True,  # whether to remove NaN values before computing geometric mean
    remove_zero=False,  # whether to remove zero values before computing geometric mean
):
    """
    Compute a funding attention index based on total funding, funding per company, and percentage of late stage or IPO companies.
    """
    # Normalize total funding, funding per company, and percentage of late stage or IPO companies
    for col in cols:
        df[f"{col}_min_max"] = df[col].apply(
            lambda x: max_min_normalization(x, df[col].min(), df[col].max())
        )

    # Compute the geometric mean of the normalized values
    combine_cols = [f"{col}_min_max" for col in cols]
    funding_attention = df.apply(
        lambda x: geometric_mean_values(
            x[combine_cols],
            remove_nan=remove_nan,
            remove_zero=remove_zero,
        ),
        axis=1,
    )

   # rescale the funding attention index to [0, 1]
    funding_attention = funding_attention.apply(
        lambda x: max_min_normalization(x, funding_attention.min(), funding_attention.max())
    )

    return funding_attention  # series


def add_attention_index(
    df,
    level,  # level to add attention index for
    attention_cols,  # list of columns to compute attention index for
):
    """Add a combined funding attention index and attention summary stats for the specified level."""
    # add combined attention index for each level
    metric_cols = [f"{level}_{col}" for col in attention_cols]
    df[f"{level}_attention"] = funding_attention_index(
        df,
        metric_cols,  # list of columns to include in attention index
        remove_nan=True,  # remove NaN values before computing geometric mean
        remove_zero=False,  # keep zero values for geometric mean
    )
    return df


def log_transform_cols(df, cols):
    for col in cols:
        # log10 transformation
        df[f"{col}_log"] = np.log10(df[col].replace(0, np.nan)).round(2)
    return df


def add_tax_funding_share_rounds(
    df_orgs,  # cft orgs with metadata
    df_rounds,  # cft funding rounds dataframe
    df_fund_frac,  # taxonomy distribution results
    levels,  # list of tax levels to include
    id_col="uid",  # unique identifier for each company
    years=None,  # list of years to include
    round_types=None,  # list of funding stages to include
):
    """
    Add taxonomy funding share rounds to the dataframe.

    This function processes funding rounds and taxonomy funding fractions, filters and merges them,
    and computes funding share statistics for each taxonomy level and company.

    Parameters
    ----------
    df_orgs : pandas.DataFrame
        DataFrame containing organizations with metadata.
    df_rounds : pandas.DataFrame
        DataFrame containing funding rounds.
    df_fund_frac : pandas.DataFrame
        DataFrame containing taxonomy distribution results with funding fractions.
        This is the output of the add_taxonomy_distribution tool.
    levels : list of str
        List of taxonomy levels to include (e.g., ['level0', 'level1']).
    id_col : str, optional
        Unique identifier column for each company (default is "uid").
    years : list of int, optional
        List of years to include. If None, all years are included.
    round_types : list of str, optional
        List of funding stages to include. If None, all stages are included.

    Returns
    -------
    df_fr_avg : pandas.DataFrame
        DataFrame containing funding share statistics for each taxonomy level and company.

    Notes
    -----
    - Filters funding rounds and taxonomy fractions based on input parameters.
    - Merges funding rounds with taxonomy funding fractions.
    - Computes funding share for each round and aggregates statistics by taxonomy level and company.
    """
    df_fr = df_rounds.copy()

    import ipdb; ipdb.set_trace()
    # filter to only rows with FundingFrac > 0
    df_fund_frac = (
        df_fund_frac[df_fund_frac['FundingFrac'] > 0]
        [levels + ["FundingFrac", 'uid']]
        .copy()
    )

    #########################################################################################
    # Filter and clean funding fractions dataframe
    #########################################################################################
    # Clean taxonomy mapping results
    # df_fund_frac = clean_industries(df_fund_frac, tax_cols=levels)
    # df_fund_frac = clean_no_level(df_fund_frac, levels, suffix=MISSING_SUFFIX)
    # remove no taxonomy match
    df_fund_frac = df_fund_frac[~df_fund_frac["level0"].isnull()].copy()


    #########################################################################################
    # Filter and clean funding rounds dataframe
    #########################################################################################
    # keep only organizations from the df_orgs
    df_fr = df_fr[df_fr["uid"].isin(df_orgs[id_col])].copy()

    # filter funding rounds to only include specified years and stages
    if years:
        # only keep funding rounds for specified years
        df_fr = df_fr[df_fr["funding_year"].isin(years)].copy()
    if round_types:
        # filter to only funding rounds with stages in stages
        df_fr = df_fr[df_fr["funding_investment_type"].isin(round_types)].copy()

    # collapse to get unique funding rounds for each company
    df_fr = (
        df_fr.groupby(
            [id_col, "funding_round_uuid", "funding_year", "funding_investment_type"]
        )["funding_funding"]
        .first()
        .reset_index()
    )

    # add one earth funding distribution to cft funding rounds
    df_fr = df_fr.merge(df_fund_frac, on=id_col, how="left")

    # add FundingShare for each round
    df_fr['FundingFrac'] = df_fr['FundingFrac'].fillna(0)
    df_fr["FundingShare"] = df_fr["funding_funding"] * df_fr["FundingFrac"]
    df_fr['FundingShare'] = df_fr['FundingShare'].fillna(0)
    import ipdb; ipdb.set_trace()
    # aggregate to get total FundingShare across years for each level for each company

    df_level_total = (
        df_fr.groupby([id_col] + levels)[["FundingFrac", "FundingShare"]]
        .sum()
        .reset_index()
    )
    df_level_total.rename(
        columns={"FundingShare": "FundingShare_total"},
        inplace=True,
    )

    # aggregate to get average FundingShare across years for each level for each company
    df_fr_avg = (
        df_level_total.groupby([id_col] + levels)[["FundingFrac", "FundingShare_total"]]
        .mean()
        .reset_index()
    )
    df_fr_avg.rename(
        columns={"FundingShare_total": "FundingShare_avg_annual"},
        inplace=True,
    )
    return df_fr_avg


def get_child_level_sums(
    df_child,
    # child, # child level to summarize by
    all_levels,  # all levels to group by
    funding_col="FundingShare_avg_annual",
    frac_count_col="FundingFrac",  # None if no fractional count
    id_col="uid",  # unique id to remove dupes for % stage
):

    """
    Aggregate and summarize funding and company counts at the child level of a hierarchical taxonomy.


    Parameters
    ----------
    df_child : pandas.DataFrame
        DataFrame containing company-level funding and taxonomy information.
    all_levels : list of str
        List of column names representing the hierarchical taxonomy levels to group by.
    funding_col : str, default "FundingShare_avg_annual"
        Column name for the funding value to aggregate.
    frac_count_col : str or None, default "FundingFrac"
        Column name for the fractional count for a company, or None if not used.
    id_col : str, default "uid"
        Column name for unique company identifier.

    Returns
    -------
    df_child_sum : pandas.DataFrame
        DataFrame with aggregated funding, company counts, and top company names for each child node.

    """

    if not frac_count_col:
        frac_count_col = "FundingFrac"
        df_child[frac_count_col] = 1

    # set column names
    child = all_levels[-1]
    child_funding_col = f"{child}_funding"
    df_child[child_funding_col] = df_child[funding_col]

    # fractional count of companies at child level
    child_frac_count_col = f"{child}_frac_count"
    # count of unique companies at child level
    child_count_col = f"{child}_count"


    # aggregate funding at child level
    df_child_sum = df_child.groupby(all_levels)[child_funding_col].sum().reset_index()

    # add fractional count of companies at child level
    df_child_sum[child_frac_count_col] = (
        df_child.groupby(all_levels)[frac_count_col].sum().values
    )

    # add count of unique companies at child level
    df_child_sum[child_count_col] = df_child.groupby(all_levels)[id_col].nunique().values

    df_child_sum[f"{child}_funding_per_co"] = (
        (df_child_sum[child_funding_col] / df_child_sum[child_count_col]).round(2).fillna(0)
    )

    # add a list of top 10 funded company names at child level
    # sort by levels and child funding
    df_child = df_child.sort_values(
        by=all_levels + [child_funding_col], ascending=False
    ).copy()
    df_child_sum[f"{child}_companies"] = (
        df_child.groupby(all_levels)["Name"].agg(list).reset_index(drop=True)
    )
    df_child_sum[f"{child}_companies"] = df_child_sum[f"{child}_companies"].apply(
        lambda x: [name for name in x if pd.notna(name)]
    )
    # remove duplicate company names while preserving order (required if we aggregate to higher level than level3
    df_child_sum[f"{child}_companies"] = df_child_sum[f"{child}_companies"].apply(
        lambda x: list(dict.fromkeys(x))[:10]
    )

    # transform child funding and count columns
    df_child_sum = log_transform_cols(
        df_child_sum,
        [child_funding_col, child_frac_count_col, child_count_col, f"{child}_funding_per_co"],
    )
    return df_child_sum


def summarize_child_and_all_parents(
    df,
    df_tax,
    levels,  # list of all levels to summarize ['level0', 'level1', 'level2', 'level3']
    funding_col="FundingShare_avg_annual",
    frac_count_col="FundingFrac",
    stage_col="Funding Stage",  # column with funding stage
    id_col="uid",  # unique company id to remove dupes for % stage
    attention_cols=['funding', 'count', 'funding_per_co'],  # columns to compute attention index for
):
    """
    Aggregate funding and counts by child level and add summaries for each parent level.
    Add transformations at each level (e.g., log, sqrt).
    Add financial neglectedness index for each level.
    """
    # truncate full taxonomy to the specified child level
    import ipdb; ipdb.set_trace()
    df_tax = df_tax[levels].groupby(levels).first().reset_index()

    # remove rows with no Sub-Pillar match (where the level1 == level0 except for where level0 is "Cross-Cutting")
    df = df[
        ~((df["level1"] == df["level0"]) & (df["level0"] != "Cross-Cutting"))
    ].copy()
    # merge with full taxonomy and fill empty with zero
    df = df.merge(df_tax, on=levels, how="outer")
    # fill funding and count with 0's
    df[funding_col] = df[funding_col].fillna(0)
    df[frac_count_col] = df[frac_count_col].fillna(0)
    # for each level, summarize funding and counts at child level
    sum_dfs = []
    for i in range(0, len(levels) - 1):
        group = levels[: len(levels) - i]
        print(f"Getting sums for child of {group}")
        # add funding and counts for the child level
        df_sum = get_child_level_sums(
            df,
            all_levels=group,
            funding_col=funding_col,
            frac_count_col=frac_count_col,
            stage_col=stage_col,
            id_col=id_col,
        )
        # add to the list of child sums
        sum_dfs.append(df_sum)

    # iteratively merge the child sum dataframe to sums for each parent level
    df_child_sum = sum_dfs[0]
    for i in range(1, len(levels) - 1):
        merge_levels = levels[: len(levels) - i]
        # merge each higher levels sum with child sum dataframe
        print(f"Merging child sums with {merge_levels}")
        df_child_sum = df_child_sum.merge(sum_dfs[i], on=merge_levels, how="left")

    # add other metrics for each level
    for i in range(1, len(levels)):
        level = levels[i]
        # fill missing pct values with 0
        df_child_sum[f"{level}_early_pct"] = df_child_sum[f"{level}_early_pct"].fillna(
            0
        )
        df_child_sum[f"{level}_late_pct"] = df_child_sum[f"{level}_late_pct"].fillna(0)
        # add combined attention index for each level
        add_attention_index(
            df_child_sum,
            level,  # level to add attention index for
            attention_cols,  # list of columns to compute attention index for
        )

    return df_child_sum


def add_financial_attention_index(
    df_orgs,
    df_rounds,
    df_funding_frac,
    taxonomy_df,
    years,
    round_types,
    levels=["level0", "level1", "level2", "level3"],
    funding_col="FundingShare_avg_annual",
    frac_count_col="FundingFrac",
    stage_col="Funding Stage",
    id_col="uid",
    attention_cols=['funding', 'count', 'funding_per_co'],  # columns to compute attention index for
):

    df_aggregated = add_tax_funding_share_rounds(
        df_orgs,
        df_rounds=df_rounds,
        df_fund_frac=df_funding_frac,
        levels=levels,
        id_col=id_col,
        years=years,
        round_types=round_types,
    )

    # EARLY - LEVEL3: early stage round funding by level3 child ("approach) and add summary metrics for all levels
    df_att = summarize_child_and_all_parents(
        df_aggregated,
        taxonomy_df,
        levels=levels,
        funding_col=funding_col,
        frac_count_col=frac_count_col,
        stage_col=stage_col,
        id_col=id_col,
        attention_cols=attention_cols,
    )
    return df_att


if __name__ == "__main__":

    import json
    from vdl_tools.shared_tools.climate_landscape.add_taxonomy_mapping import load_one_earth_taxonomy

    df_orgs = pd.read_json('../climate-landscape/data/results/cb_cd_li_meta.json')

    cft_nodes = json.load(open('../climate-landscape/data/cft-published/US/internal/cft_network_cleaned.json'))['nodes']

    df_orgs = df_orgs[df_orgs['uid'].isin([node['uid'] for node in cft_nodes])].copy()

    df_rounds = pd.read_json('../climate-landscape/data/dashboard/timeseries_df_dashboard_data.json', lines=True)
    df_rounds['uid'] = df_rounds['funding_uid']

    df_funding_frac = pd.read_json('../grantham-neglectedness/data/results/cft_one_earth_taxonomy_mapping_distributed_funding_results.json')
    taxonomy_df = load_one_earth_taxonomy('../shared-data/data/taxonomies/oneearth/OE Solutions Terms 20250502_expanded_w_false.xlsx')
    years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
    round_types = ["angel", "pre_seed", "seed", "series_a"]
    levels = ["level0", "level1", "level2", "level3"]
    attention_cols = ['funding', 'count', 'funding_per_co']


    df_att = add_financial_attention_index(
        df_orgs,
        df_rounds,
        df_funding_frac,
        taxonomy_df,
        years,
        round_types,
        levels,
        attention_cols,
    )
