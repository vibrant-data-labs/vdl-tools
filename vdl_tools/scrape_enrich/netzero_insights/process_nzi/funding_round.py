import pandas as pd
from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.investor import process_nzi_investors



FUNDING_ROUND_COLUMNS = [
    # "id",
    # "source",
    # "status",
    "clientId",
    "roundDate",
    "roundType",
    # "exitStageID",
    # "roundAmount",
    # "fundingRange",
    # "roundNewsIDs",
    # "equityStageID",
    "financingType",
    # "roundAmountID",
    # "roundCurrency",
    "roundAmountUSD",
    "roundInvestors",
    "coFundingRoundID",
    "roundInvestorIDs",
    # "originalRoundAmount",
    "connectedToInfrastructureDeal",
    # "approvedBy",
    # "approvedDate",
    # "valuationOriginalCurrency",
    # "valuationAmount",
    # "valuationCurrency",
    # "valuationAmountInOriginalCurrency",
    # "infrastructureProjectID",
    "roundNews",
]

def filter_format_columns(
  funding_round_df,
  keep_suffix="_nzi",
):
    funding_round_df = funding_round_df.copy()
    keep_columns = [col for col in FUNDING_ROUND_COLUMNS]
    for col in funding_round_df.columns:
        if col.endswith(keep_suffix):
            keep_columns.append(col)

    rename_dict = {
        col: f"{camel_to_snake(col)}{keep_suffix}" for col in FUNDING_ROUND_COLUMNS
    }

    funding_round_df = funding_round_df[keep_columns]
    funding_round_df = funding_round_df.rename(columns=rename_dict)
    return funding_round_df


def add_investor_type_flag(
    funding_round_df: pd.DataFrame,
    investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    process_investors: bool = True,
    investor_type: str = 'Government',
) -> pd.DataFrame:

    investor_type_lower = investor_type.lower()
    if process_investors:
        processed_investor_df = process_nzi_investors(
            investor_df,
            keep_suffix=keep_suffix
        )
    else:
        processed_investor_df = investor_df
    round_id_to_investor_id = []
    for _, row in funding_round_df.iterrows():
        for investor_id in row['roundInvestorIDs']:
            round_id_to_investor_id.append((row['coFundingRoundID'], investor_id))

    round_id_to_investor_id = pd.DataFrame(round_id_to_investor_id, columns=['coFundingRoundID', 'investorId'])

    is_investor_type_flag_col_name = f'is_{investor_type_lower}_investor_calced{keep_suffix}'
    has_investor_type_flag_col_name = f'has_{investor_type_lower}_investor_calced{keep_suffix}'

    round_id_to_has_investor_type_flag = (
        round_id_to_investor_id.merge(
            processed_investor_df,
            left_on='investorId',
            right_on='investor_id_nzi',
            how='left'
        )
        .groupby('coFundingRoundID')
        .agg({is_investor_type_flag_col_name: 'sum'})
        .reset_index()
    )
    round_id_to_has_investor_type_flag[has_investor_type_flag_col_name] = round_id_to_has_investor_type_flag[is_investor_type_flag_col_name] > 0

    funding_round_df = funding_round_df.merge(
        round_id_to_has_investor_type_flag[['coFundingRoundID', has_investor_type_flag_col_name]],
        on='coFundingRoundID',
        how='left'
    )

    funding_round_df[has_investor_type_flag_col_name] = funding_round_df[has_investor_type_flag_col_name].fillna(False)

    return funding_round_df


def process_nzi_funding_rounds(
    funding_round_df: pd.DataFrame,
    investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    process_investors: bool = True,
) -> pd.DataFrame:

    funding_round_df = add_investor_type_flag(
        funding_round_df,
        investor_df,
        keep_suffix=keep_suffix,
        process_investors=process_investors,
        investor_type='Government'
    )
    funding_round_df = filter_format_columns(funding_round_df, keep_suffix=keep_suffix)
    return funding_round_df