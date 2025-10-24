import os
import json
import datetime as dt
from sqlalchemy import text
import pandas as pd
from git import Repo

from vdl_tools.shared_tools.tools.falsey_checks import coerced_bool
# from vdl_tools.scrape_enrich.prepare_crunchbase import has_government_investor
from vdl_tools.shared_tools.cb_funding_calculations import ROUND_TO_STAGE
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.project_config import get_paths

CB_COL_MAP = {
    "uuid": "round_uuid",
    "organization_uuid": "uid",
    "year_announced": "year",
    "vdl_investment_stage": "vdl_stage",
    "money_raised_usd": "funding",
    "investment_type": "investment_type",
}

CD_COL_MAP = {
    "id": "uid",
    "Year": "year",
    "vdl_stage": "vdl_stage",
    "Funding": "funding",
}


def get_funding_cols(meta_df, prefix="Funding_"):
    return [col for col in meta_df.columns if col.startswith(prefix)]


def rename_df_cols(
    df,
    prefix,
    exclude_cols=[],
):
    rename_dict = {
        col: f"{prefix}_{col}".lower().replace(" ", "_") for col in df.columns if col not in exclude_cols
    }
    return df.rename(columns=rename_dict)


def combine_funding_data(
    round_df,
    candid_long_df,
    cb_col_map=CB_COL_MAP,
    cd_col_map=CD_COL_MAP,
    min_year=2010,
    max_year=2025,
    drop_missing=True,
):
    cb_rounds = round_df.copy()
    cb_rounds = cb_rounds.rename(columns=cb_col_map)
    cb_rounds = cb_rounds[cb_col_map.values()]
    cb_rounds["data_source"] = "crunchbase"

    cd_funding = candid_long_df.copy()
    cd_funding = cd_funding.rename(columns=cd_col_map)
    cd_funding = cd_funding[cd_col_map.values()]
    cd_funding["data_source"] = "candid"
    cd_funding["round_uuid"] = cd_funding.apply(
        lambda x: f"{x['uid']}_{x['year']}", axis=1
    )

    funding_df = pd.concat([cb_rounds, cd_funding], axis=0)
    funding_df = funding_df.sort_values("year", ascending=True)
    if min_year:
        funding_df = funding_df[funding_df["year"] >= min_year]
    if max_year:
        funding_df = funding_df[funding_df["year"] <= max_year]
    if drop_missing:
        funding_df = funding_df[
            (funding_df["funding"] > 0) & (funding_df["funding"].notnull())
        ]
    return funding_df


def load_cb_round_data(funding_rounds_path):
    full_round_df = pd.read_json(funding_rounds_path)
    full_round_df["organization_uuid"] = full_round_df[
        "funded_organization_identifier"
    ].apply(lambda x: x.get("uuid"))
    full_round_df["money_raised_usd"] = full_round_df["money_raised"].apply(
        lambda x: x.get("value_usd") if coerced_bool(x) else None
    )
    round_df = full_round_df[
        [
            "uuid",
            "announced_on",
            "investment_stage",
            "investment_type",
            "organization_uuid",
            "money_raised_usd",
        ]
    ]

    round_df = round_df.assign(
        vdl_investment_stage=lambda d: d["investment_type"].map(ROUND_TO_STAGE)
    )
    round_df = round_df.assign(
        year_announced=round_df["announced_on"].apply(
            lambda x: pd.to_datetime(x).year
        )
    )

    return round_df


def reshape_candid_funding(
    candid_orgs_df,
    id_col="uid",
    drop_no_funding=True
):
    """
    Reshape Candid funding data from wide to long format.

    This function takes a dataframe where each row represents an entity and each funding year is a separate column
    (e.g., 'Funding_2019', 'Funding_2020', ...). It filters for rows where the data source is 'Candid', and reshapes
    the data so that each (ID, Year) combination has its own row, with the corresponding funding amount.

    Parameters
    ----------
    candid_orgs_df : pandas.DataFrame
        Input dataframe containing funding data with one column per year
    id_col : str, optional
        The name of the column containing unique entity identifiers. Default is "uid".
    drop_no_funding : bool, optional
        If True, rows with zero or missing funding are dropped. Default is True.

    Returns
    -------
    pandas.DataFrame
        A long-form dataframe with columns: [id_col, 'Funding', 'Year', 'vdl_stage'], where each row is a unique
        (ID, Year) combination for Candid data.

    Notes
    -----
    - The 'vdl_stage' column is set to "Philanthropy" for all rows.
    - The 'Year' column is extracted from the funding column names.
    """
    funding_cols = get_funding_cols(candid_orgs_df)

    candid_funding_wide = candid_orgs_df[[id_col] + funding_cols]

    candid_funding_long = candid_funding_wide.melt(
        id_vars=[id_col], value_name="Funding", value_vars=funding_cols
    )

    candid_funding_long["Year"] = candid_funding_long["variable"].apply(
        lambda x: dt.date(int(x.split("_")[-1]), 1, 1).year
    )

    candid_funding_long.pop("variable")
    candid_funding_long["vdl_stage"] = "Philanthropy"
    candid_funding_long["funding_investment_type"] = "Philanthropy"

    if drop_no_funding:
        candid_funding_long["Funding"].fillna(0, inplace=True)
        return candid_funding_long[candid_funding_long["Funding"] > 0]
    return candid_funding_long
