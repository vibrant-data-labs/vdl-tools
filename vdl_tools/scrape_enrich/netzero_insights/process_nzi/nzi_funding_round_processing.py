import pandas as pd


def did_raise_venture(company_funding_rows):
    return "Equity" in company_funding_rows['financing_type_nzi'].values


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
    "Post IPO",
    "Post IPO - Equity",
]

SPLIT_ON_FIRST_LATE_ROUND = "first_late_round"
SPLIT_AFTER_LAST_EARLY_ROUND = "after_last_early_round"


def raised_equity_round(company_funding_rows):
    return "Equity" in company_funding_rows['financing_type_nzi'].values


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
        return None, None


    company_funding_rows = company_funding_rows.copy()

    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)

    company_funding_rows = company_funding_rows.reset_index(drop=True)

    if split_strategy == SPLIT_ON_FIRST_LATE_ROUND:
        late_vc_types = DISCLOSED_STAGES_ORDERED[
            DISCLOSED_STAGES_ORDERED.index(early_vc_cutoff) + 1:
        ]

        # Legacy behavior: early stage continues until the first late venture round appears.
        late_stage_start_rows = company_funding_rows[
            company_funding_rows['round_type_nzi'].isin(late_vc_types + ["Late VC"])
        ]
        if late_stage_start_rows.shape[0] == 0:
            return None, None

        late_stage_start_index = late_stage_start_rows.index[0]
        early_stage_end_index = late_stage_start_index - 1
    elif split_strategy == SPLIT_AFTER_LAST_EARLY_ROUND:
        early_stage_anchor_types = [early_vc_cutoff]
        if early_vc_cutoff == "Series A":
            early_stage_anchor_types.append("Early VC")

        early_stage_anchor_rows = company_funding_rows[
            company_funding_rows['round_type_nzi'].isin(early_stage_anchor_types)
        ]
        if early_stage_anchor_rows.shape[0] == 0:
            return None, None

        early_stage_end_index = early_stage_anchor_rows.index[-1]
        late_stage_start_index = early_stage_end_index + 1
        if late_stage_start_index >= len(company_funding_rows):
            return None, None
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
        return None, None

    early_stage_funding_rows = company_funding_rows.loc[:early_stage_end_index]
    after_max_late_vc_funding_rows = company_funding_rows.loc[
        late_stage_start_index:late_stage_end_index
    ]

    return early_stage_funding_rows, after_max_late_vc_funding_rows
