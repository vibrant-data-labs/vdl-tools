"""
Per-organization ``venture_backed`` flag from the raw Crunchbase funding rounds.

An org is venture-backed when, anywhere in its FULL round history, it has

  * at least one round whose ``investment_type`` is in ``VENTURE_BACKED_ROUNDS``
    (seed, series_a..series_j, series_unknown, corporate_round, secondary_market,
    angel, pre_seed, convertible_note, product_crowdfunding), OR
  * ONLY ``grant`` rounds (every round in its history is a grant) AND the org
    is classified For Profit (e.g. an early-stage startup whose only funding
    is an NSF SBIR grant).

Everything else is False: grant-only non-profits; for-profits whose grants
sit alongside debt, non-equity, private-equity or post-IPO rounds (a utility
with a DOE grant and post-IPO debt is not venture-backed); orgs with only
debt / non-equity / private-equity / post-IPO rounds; orgs with no rounds;
and Candid-sourced orgs (which have no Crunchbase rounds at all).

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
REASON_GRANT_FOR_PROFIT = 'grant-only + for-profit'


def venture_backed_org_uuids(rounds_df):
    """Return (venture_uuids, grant_only_uuids) from a raw funding-rounds frame.

    ``rounds_df`` needs the raw API columns ``investment_type`` and
    ``funded_organization_identifier`` (a dict holding the org ``uuid``).
    ``venture_uuids`` are orgs with >= 1 round in VENTURE_BACKED_ROUNDS;
    ``grant_only_uuids`` are orgs whose EVERY round is a grant. The two sets
    are disjoint by construction.
    """
    # org uuid lives inside the identifier dict (same idiom as prepare_crunchbase)
    org_uuid = rounds_df['funded_organization_identifier'].apply(
        lambda x: x.get('uuid') if isinstance(x, dict) else None
    )
    is_venture = rounds_df['investment_type'].isin(VENTURE_BACKED_ROUNDS)
    is_grant = rounds_df['investment_type'] == 'grant'

    venture_uuids = set(org_uuid[is_venture].dropna())
    # Grant-only: an org with a grant AND no round of any other type. A grant
    # next to a debt, private-equity or post-IPO round does not qualify.
    has_grant = set(org_uuid[is_grant].dropna())
    has_other = set(org_uuid[~is_grant].dropna())
    grant_only_uuids = has_grant - has_other
    logger.info(
        "Raw rounds: %s rounds over %s orgs; %s orgs with a venture-type round, "
        "%s orgs with a grant round of which %s are grant-only",
        len(rounds_df), org_uuid.nunique(), len(venture_uuids), len(has_grant),
        len(grant_only_uuids),
    )
    return venture_uuids, grant_only_uuids


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
    'grant-only + for-profit', or '' and exists mainly for QA counts.
    """
    assert org_type_col in df.columns, (
        f"{org_type_col!r} not in the frame -- run add_venture_backed_flag after "
        "the org-type prediction step (or pass org_type_col='Org Type')"
    )

    # Only two columns of the raw rounds are needed
    rounds = read_dataframe(rounds_uri, columns=['investment_type', 'funded_organization_identifier'])
    venture_uuids, grant_only_uuids = venture_backed_org_uuids(rounds)

    has_venture = df[id_col].isin(venture_uuids)
    grant_only = df[id_col].isin(grant_only_uuids)
    is_for_profit = df[org_type_col].eq(for_profit_label)

    df[FLAG_COL] = (has_venture | (grant_only & is_for_profit)).astype(bool)
    # The two rules cannot both fire (grant-only orgs have no venture round)
    df[REASON_COL] = ''
    df.loc[grant_only & is_for_profit, REASON_COL] = REASON_GRANT_FOR_PROFIT
    df.loc[has_venture, REASON_COL] = REASON_VENTURE

    if 'Data Source' in df.columns:
        logger.info("venture_backed by Data Source and reason:\n%s",
                    pd.crosstab(df['Data Source'], df[REASON_COL]))
    logger.info("venture_backed: %s of %s orgs True", int(df[FLAG_COL].sum()), len(df))
    return df
