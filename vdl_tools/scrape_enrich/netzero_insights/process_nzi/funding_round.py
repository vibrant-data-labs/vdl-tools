import pandas as pd
from vdl_tools.shared_tools.tools.text_cleaning import camel_to_snake
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.investor import INVESTOR_TYPES_TO_ADD, INVESTOR_BOOLEAN_FLAGS_TO_ADD



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


ACQUISITION_ROUND_TYPES = {
    'Merger',
    "Acquisition",
    "Buyout",
}


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
    processed_investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
    investor_type: str = 'Government',
) -> pd.DataFrame:

    investor_type_lower = investor_type.lower()

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


def project_finance_indicators(
    company_funding_rows,
    round_type_col: str = 'roundType',
    round_amount_usd_col: str = 'roundAmountUSD',
):

    number_of_rounds = company_funding_rows.shape[0]
    project_finance_mask = company_funding_rows[round_type_col] == 'Project Finance'
    project_finance_rows = company_funding_rows[project_finance_mask]
    num_project_finance_deals = project_finance_rows.shape[0]
    project_finance_raised = project_finance_rows[round_amount_usd_col].sum()
    had_project_finance = num_project_finance_deals > 0
    return {
        "num_project_finance_deals_calced_nzi": num_project_finance_deals,
        "project_finance_raised_calced_nzi": project_finance_raised,
        "has_project_finance_calced_nzi": had_project_finance,
        "ratio_rounds_project_finance_calced_nzi": num_project_finance_deals / number_of_rounds
    }


def add_project_finance_indicators(
    funding_round_df: pd.DataFrame,
    id_col: str = 'clientId',
    round_type_col: str = 'roundType',
    round_amount_usd_col: str = 'roundAmountUSD',
) -> pd.DataFrame:
    project_finance_indicators_values = (
        funding_round_df.groupby(id_col)
        .apply(
            project_finance_indicators,
            round_type_col=round_type_col,
            round_amount_usd_col=round_amount_usd_col
        )
        .reset_index()
        .values
    )
    project_finance_indicators_df = pd.DataFrame(
        [
            {id_col: x[0], **x[1]} for x in
            project_finance_indicators_values
        ]
    )
    return project_finance_indicators_df


def was_acquired_merged(
    company_funding_rows,
    round_type_col: str = 'roundType',
):
    company_round_types = company_funding_rows[round_type_col].values
    company_acquisition_events = ACQUISITION_ROUND_TYPES.intersection(company_round_types)
    return len(company_acquisition_events) > 0

def add_acquisition_indicators(
    funding_round_df: pd.DataFrame,
    id_col: str = 'clientId',
    round_type_col: str = 'roundType',
) -> pd.DataFrame:
    acquisition_indicators_values = (
        funding_round_df.groupby(id_col)
        .apply(
            was_acquired_merged,
            round_type_col=round_type_col,
        )
        .reset_index()
        .values
    )
    acquisition_indicators_df = pd.DataFrame(
        {id_col: x[0], 'was_acquired_merged_calced_nzi': x[1]} for x in
        acquisition_indicators_values
    )
    return acquisition_indicators_df


def process_nzi_funding_rounds(
    funding_round_df: pd.DataFrame,
    processed_investor_df: pd.DataFrame,
    keep_suffix: str = '_nzi',
) -> pd.DataFrame:

    for investor_type in INVESTOR_TYPES_TO_ADD + INVESTOR_BOOLEAN_FLAGS_TO_ADD:
        funding_round_df = add_investor_type_flag(
            funding_round_df,
            processed_investor_df,
            keep_suffix=keep_suffix,
            investor_type=investor_type
        )

    funding_round_df = filter_format_columns(funding_round_df, keep_suffix=keep_suffix)
    return funding_round_df