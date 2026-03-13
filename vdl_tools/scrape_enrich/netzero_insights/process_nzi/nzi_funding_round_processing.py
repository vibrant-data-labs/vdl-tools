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


def raised_equity_round(company_funding_rows):
    return "Equity" in company_funding_rows['financing_type_nzi'].values

def divide_funding_rows(
    company_funding_rows,
    early_vc_cutoff="Series A",
    max_late_vc_stage="Series B",
):

    if not raised_equity_round(company_funding_rows):
        return None, None


    company_funding_rows = company_funding_rows.copy()

    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)

    company_funding_rows = company_funding_rows.reset_index(drop=True)


    late_vc_types = DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(early_vc_cutoff) + 1:
        # DISCLOSED_STAGES_ORDERED.index(max_late_vc_stage) + 1
    ]

    # Find the first row index where the round type is Late VC or LATE_VC_CUTOFF
    late_vc_rows = (
        company_funding_rows[
            company_funding_rows['round_type_nzi'].isin(late_vc_types + ["Late VC"])
        ]
    )
    if late_vc_rows.shape[0] == 0:
        return None, None
    else:
        late_vc_index = late_vc_rows.index[0]

    early_stage_funding_rows = company_funding_rows.loc[:late_vc_index - 1]

    # Get the max_late_vc_stage rows
    # This should be all the rows after the late vc index and up to the the first row where the
    # the next round type is after the max late vc stage
    # So if max_late_vc_stage is Series B we'd want all rows that are between the first
    # Series B (Or Late VC if that comes first) and the first row where the next round is Series C or later
    after_max_late_vc_types = DISCLOSED_STAGES_ORDERED[
        DISCLOSED_STAGES_ORDERED.index(max_late_vc_stage) + 1:
    ]
    after_max_late_vc = company_funding_rows[company_funding_rows['round_type_nzi'].isin(after_max_late_vc_types)]
    if after_max_late_vc.shape[0] > 0:
        after_max_late_vc_index = after_max_late_vc.index[0] - 1
    else:
        after_max_late_vc_index = len(company_funding_rows) - 1

    after_max_late_vc_funding_rows = company_funding_rows.loc[late_vc_index:after_max_late_vc_index]

    return early_stage_funding_rows, after_max_late_vc_funding_rows
