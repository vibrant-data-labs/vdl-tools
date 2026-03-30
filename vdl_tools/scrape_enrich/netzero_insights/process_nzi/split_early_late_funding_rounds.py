import datetime as dt


def did_raise_venture(company_funding_rows):
    return "Equity" in company_funding_rows['financing_type_nzi'].values

M_AND_A_SUCCESS_STAGE = "Series A"

# Leaving out Grant because it's not a venture round
EARLY_VC_STAGES = [
    "Early VC",
    "Pre-Seed",
    "Seed",
    "Series A",
]

# Anything Series B or later is considered a late venture round
LATE_VC_CUTOFF = "Series B"

DISCLOSED_STAGES_ORDERED = [
    "Pre-Seed",
    "Seed",
    "Series A",
    "Series B",
    "Series C",
    "Series D",
    "Series E",
    "Series F",
    "Series G",
    "Series H",
    "Series I",
    "Series J",
    "IPO",
    "SPAC",
    "Post IPO",
    "Post IPO - Equity",
]

M_AND_A_NAMES = [
    "Merger",
    "Acquisition",
    "Buyout",
]

SPLIT_ON_FIRST_LATE_ROUND = "first_late_round"
SPLIT_AFTER_LAST_EARLY_ROUND = "after_last_early_round"


TWO_YEARS_IN_DAYS = 365 * 2

def raised_equity_round(company_funding_rows):
    financing_types = company_funding_rows['financing_type_nzi'].values
    if"Equity" in financing_types:
        return True
    if "Grant" in financing_types:
        return True
    return False


def _get_late_stage_end_index(company_funding_rows, max_late_vc_stage):
    after_max_late_vc_types = DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(max_late_vc_stage) + 1:
    ]
    after_max_late_vc = company_funding_rows[
        company_funding_rows['round_type_nzi'].isin(after_max_late_vc_types)
    ]
    if after_max_late_vc.shape[0] > 0:
        return after_max_late_vc.index[0] - 1

    return len(company_funding_rows) - 1



def divide_funding_rows(
    company_funding_rows,
    early_vc_cutoff="Series A",
    max_late_vc_stage="Series B",
    split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
):

    if not raised_equity_round(company_funding_rows):
        return None, None, None

    company_funding_rows = company_funding_rows.copy()

    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)

    company_funding_rows = company_funding_rows.reset_index(drop=True)

    if split_strategy == SPLIT_ON_FIRST_LATE_ROUND:
        late_vc_types = DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(early_vc_cutoff) + 1:
        ]
        early_vc_types = DISCLOSED_STAGES_ORDERED[
            :DISCLOSED_STAGES_ORDERED.index(early_vc_cutoff) + 1
        ] + ["Early VC"]

        # Early stage continues until the first late venture round appears.
        late_stage_start_rows = company_funding_rows[
            company_funding_rows['round_type_nzi'].isin(late_vc_types + ["Late VC"])
        ]
        if late_stage_start_rows.shape[0] == 0:
            if company_funding_rows['round_type_nzi'].isin(early_vc_types).any():
                return company_funding_rows, None, None
            return None, None, company_funding_rows

        late_stage_start_index = late_stage_start_rows.index[0]
        early_stage_end_index = late_stage_start_index - 1

        if early_stage_end_index < 0:
            return None, None, company_funding_rows

        early_candidate_rows = company_funding_rows.loc[:early_stage_end_index]
        if not early_candidate_rows['round_type_nzi'].isin(early_vc_types).any():
            return None, None, company_funding_rows
    elif split_strategy == SPLIT_AFTER_LAST_EARLY_ROUND:
        raise NotImplementedError("Split after last early round is not implemented")
        # early_stage_anchor_types = [early_vc_cutoff]
        # if early_vc_cutoff == "Series A":
        #     early_stage_anchor_types.append("Early VC")

        # early_stage_anchor_rows = company_funding_rows[
        #     company_funding_rows['round_type_nzi'].isin(early_stage_anchor_types)
        # ]
        # # No early stage anchor rows found meaning no early stage funding
        # if early_stage_anchor_rows.shape[0] == 0:
        #     return None, None

        # early_stage_end_index = early_stage_anchor_rows.index[-1]
        # late_stage_start_index = early_stage_end_index + 1
        # # No late stage anchor rows found meaning no late stage funding
        # if late_stage_start_index >= len(company_funding_rows):
        #     early_stage_funding_rows = company_funding_rows.iloc[:early_stage_end_index]
        #     return early_stage_funding_rows, None
    else:
        raise ValueError(
            "split_strategy must be "
            f"'{SPLIT_ON_FIRST_LATE_ROUND}' or '{SPLIT_AFTER_LAST_EARLY_ROUND}'"
        )

    late_stage_end_index = _get_late_stage_end_index(
        company_funding_rows=company_funding_rows,
        max_late_vc_stage=max_late_vc_stage,
    )
    if late_stage_end_index < late_stage_start_index:
        early_stage_funding_rows = company_funding_rows.loc[:early_stage_end_index]
        after_late_stage_funding_rows = company_funding_rows.loc[late_stage_start_index:]
        return early_stage_funding_rows, None, after_late_stage_funding_rows

    early_stage_funding_rows = company_funding_rows.loc[:early_stage_end_index]
    after_max_late_vc_funding_rows = company_funding_rows.loc[
        late_stage_start_index:late_stage_end_index
    ]
    after_late_stage_end_index_funding_rows = company_funding_rows.loc[late_stage_end_index + 1:]

    return early_stage_funding_rows, after_max_late_vc_funding_rows, after_late_stage_end_index_funding_rows


def project_finance_indicators(company_funding_rows):
    number_of_rounds = company_funding_rows.shape[0]
    project_finance_mask = company_funding_rows['round_type_nzi'] == 'Project Finance'
    project_finance_rows = company_funding_rows[project_finance_mask]
    num_project_finance_deals = project_finance_rows.shape[0]
    project_finance_raised = project_finance_rows['round_amount_usd_nzi'].sum()
    had_project_finance = num_project_finance_deals > 0
    return {
        "num_project_finance_deals": num_project_finance_deals,
        "project_finance_raised": project_finance_raised,
        "had_project_finance": had_project_finance,
        "ratio_rounds_project_finance": num_project_finance_deals / number_of_rounds
    }
