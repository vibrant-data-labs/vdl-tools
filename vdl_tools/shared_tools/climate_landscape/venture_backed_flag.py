"""
Per-organization ``venture_backed`` flag from the raw Crunchbase funding rounds.

An org is venture-backed when, anywhere in its FULL round history, it has

  * at least one round whose ``investment_type`` is in ``VENTURE_BACKED_ROUNDS``
    (seed, series_a..series_j, series_unknown, corporate_round, secondary_market,
    angel, pre_seed, convertible_note, product_crowdfunding), OR
  * at least one ``grant`` round AND the org is classified For Profit
    (e.g. an early-stage startup whose only funding is an NSF SBIR grant).

Everything else is False: grant-only non-profits, orgs with only debt /
non-equity / private-equity / post-IPO rounds, orgs with no rounds, and
Candid-sourced orgs (which have no Crunchbase rounds at all).

This reads the UNFILTERED raw rounds parquet (all years), not the 2010+
cleaned rounds table, so a company whose only venture round was before 2010
still counts. It is deliberately narrower than
``cb_funding_calculations.raised_from_venture_rounds`` (which also counts
private equity and post-IPO rounds and is used only to infer org type).

Usage (inside the enrichment pipeline, after the org-type prediction step):

    df = add_venture_backed_flag(df, rounds_uri=s3_paths['cb_funding_rounds_raw'])
"""
import pandas as pd

from vdl_tools.shared_tools.cb_funding_calculations import VENTURE_BACKED_ROUNDS
from vdl_tools.shared_tools.parquet_cache import read_dataframe
from vdl_tools.shared_tools.tools.logger import logger


# Output column names
FLAG_COL = 'venture_backed'
REASON_COL = 'venture_backed_reason'

# Values of the reason column
REASON_VENTURE = 'venture round'
REASON_GRANT_FOR_PROFIT = 'grant + for-profit'


def venture_backed_org_uuids(rounds_df):
    """Return (venture_uuids, grant_uuids) from a raw funding-rounds frame.

    ``rounds_df`` needs the raw API columns ``investment_type`` and
    ``funded_organization_identifier`` (a dict holding the org ``uuid``).
    ``venture_uuids`` are orgs with >= 1 round in VENTURE_BACKED_ROUNDS;
    ``grant_uuids`` are orgs with >= 1 grant round. The two sets overlap.
    """
    # org uuid lives inside the identifier dict (same idiom as prepare_crunchbase)
    org_uuid = rounds_df['funded_organization_identifier'].apply(
        lambda x: x.get('uuid') if isinstance(x, dict) else None
    )
    is_venture = rounds_df['investment_type'].isin(VENTURE_BACKED_ROUNDS)
    is_grant = rounds_df['investment_type'] == 'grant'

    venture_uuids = set(org_uuid[is_venture].dropna())
    grant_uuids = set(org_uuid[is_grant].dropna())
    logger.info(
        "Raw rounds: %s rounds over %s orgs; %s orgs with a venture-type round, "
        "%s orgs with a grant round",
        len(rounds_df), org_uuid.nunique(), len(venture_uuids), len(grant_uuids),
    )
    return venture_uuids, grant_uuids


def add_venture_backed_flag(
    df,
    rounds_uri,
    id_col='id',
    org_type_col='OrgType Prediction',
    for_profit_label='For Profit',
):
    """Attach ``venture_backed`` (bool) and ``venture_backed_reason`` (str) to an org frame.

    ``df[id_col]`` must hold the Crunchbase org uuid for Crunchbase rows; rows
    from other sources (Candid) never match a round and get False. ``org_type_col``
    decides the grant-only rule, so this must run AFTER the org-type prediction
    step when using the default column. The reason column is 'venture round',
    'grant + for-profit', or '' and exists mainly for QA counts.
    """
    assert org_type_col in df.columns, (
        f"{org_type_col!r} not in the frame -- run add_venture_backed_flag after "
        "the org-type prediction step (or pass org_type_col='Org Type')"
    )

    # Only two columns of the raw rounds are needed
    rounds = read_dataframe(rounds_uri, columns=['investment_type', 'funded_organization_identifier'])
    venture_uuids, grant_uuids = venture_backed_org_uuids(rounds)

    has_venture = df[id_col].isin(venture_uuids)
    has_grant = df[id_col].isin(grant_uuids)
    is_for_profit = df[org_type_col].eq(for_profit_label)

    df[FLAG_COL] = (has_venture | (has_grant & is_for_profit)).astype(bool)
    # Reason: a venture round wins over the grant rule when an org has both
    df[REASON_COL] = ''
    df.loc[has_grant & is_for_profit, REASON_COL] = REASON_GRANT_FOR_PROFIT
    df.loc[has_venture, REASON_COL] = REASON_VENTURE

    if 'Data Source' in df.columns:
        logger.info("venture_backed by Data Source and reason:\n%s",
                    pd.crosstab(df['Data Source'], df[REASON_COL]))
    logger.info("venture_backed: %s of %s orgs True", int(df[FLAG_COL].sum()), len(df))
    return df
