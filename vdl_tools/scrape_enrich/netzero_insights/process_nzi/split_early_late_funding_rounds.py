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
    "Early VC",
    "Series A",
    "Series B",
    "Late VC",
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

# Stage classification sets — only equity venture rounds define boundaries
EARLY_STAGE_TYPES = {
    "Pre-Seed",
    "Seed",
    "Early VC",
    "Series A",
}

MIDDLE_STAGE_TYPES = {
    "Series B",
    "Late VC",
}

LATE_STAGE_TYPES = {
    "Series C",
    "Series D",
    "Series E",
    "Series F",
    "Series G",
    "Series H",
    "Series I",
    "Series J",
    "Growth equity",
}

EXIT_TYPES = {
    "IPO",
    "SPAC",
    "Post IPO",
    "Post IPO - Equity",
    "Merger",
    "Acquisition",
    "Buyout",
}


def _get_effective_stage(round_type):
    """Returns 'early', 'middle', 'late', 'exit', or None for non-boundary types."""
    if round_type in EARLY_STAGE_TYPES:
        return "early"
    if round_type in MIDDLE_STAGE_TYPES:
        return "middle"
    if round_type in LATE_STAGE_TYPES:
        return "late"
    if round_type in EXIT_TYPES:
        return "exit"
    return None


STAGE_ORDER = {"early": 0, "middle": 1, "late": 2, "exit": 3}


def raised_equity_round(company_funding_rows):
    financing_types = company_funding_rows['financing_type_nzi'].values
    if "Equity" in financing_types:
        return True
    if "Grant" in financing_types:
        return True
    return False


def divide_funding_rows(
    company_funding_rows,
    split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
):
    """Split a company's funding rounds into Early, Middle, Late, and Exit buckets.

    Returns (early, middle, late, exit) where each is a DataFrame or None.
    Exit includes both IPO/SPAC/Post-IPO and M&A events (Acquisition, Merger, Buyout).
    Only equity venture round types define stage boundaries; all other round types
    are absorbed into whichever bucket they fall into chronologically.
    """
    if not raised_equity_round(company_funding_rows):
        return None, None, None, None

    company_funding_rows = company_funding_rows.copy()
    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)
    company_funding_rows = company_funding_rows.reset_index(drop=True)

    if len(company_funding_rows) == 0:
        return None, None, None, None

    stages = company_funding_rows['round_type_nzi'].map(_get_effective_stage)

    if split_strategy == SPLIT_ON_FIRST_LATE_ROUND:
        return _split_on_first_late_round(company_funding_rows, stages)
    elif split_strategy == SPLIT_AFTER_LAST_EARLY_ROUND:
        return _split_after_last_early_round(company_funding_rows, stages)
    else:
        raise ValueError(
            "split_strategy must be "
            f"'{SPLIT_ON_FIRST_LATE_ROUND}' or '{SPLIT_AFTER_LAST_EARLY_ROUND}'"
        )


def _find_first_index_at_or_above(stages, min_stage):
    """Find the first index where the effective stage is >= min_stage."""
    min_order = STAGE_ORDER[min_stage]
    for idx, stage in stages.items():
        if stage is not None and STAGE_ORDER.get(stage, -1) >= min_order:
            return idx
    return None


def _find_last_index_at_stage(stages, target_stage):
    """Find the last index where the effective stage equals target_stage."""
    last = None
    for idx, stage in stages.items():
        if stage == target_stage:
            last = idx
    return last


def _has_stage_in_range(stages, target_stage, start_idx, end_idx):
    """Check if any row in [start_idx, end_idx] has the given effective stage."""
    for idx in range(start_idx, end_idx + 1):
        if idx in stages.index and stages[idx] == target_stage:
            return True
    return False


def _slice_or_none(df, start_idx, end_idx):
    """Return df.loc[start:end] or None if the slice would be empty."""
    if start_idx is None or end_idx is None or start_idx > end_idx:
        return None
    result = df.loc[start_idx:end_idx]
    if len(result) == 0:
        return None
    return result


def _split_on_first_late_round(company_funding_rows, stages):
    """Split where each stage begins at the first occurrence of that stage's round type."""
    n = len(company_funding_rows)
    last_idx = n - 1

    # Find boundary indices (first occurrence of each stage or higher)
    middle_start = _find_first_index_at_or_above(stages, "middle")
    late_start = _find_first_index_at_or_above(stages, "late")
    exit_start = _find_first_index_at_or_above(stages, "exit")

    # Determine early bucket
    early_end = None
    if middle_start is not None:
        early_end = middle_start - 1
    elif late_start is not None:
        early_end = late_start - 1
    elif exit_start is not None:
        early_end = exit_start - 1
    else:
        # No middle/late/exit found — everything is potentially early
        early_end = last_idx

    # Only emit early if there's at least one early-stage round in the range
    has_early = early_end >= 0 and _has_stage_in_range(stages, "early", 0, early_end)
    early = _slice_or_none(company_funding_rows, 0, early_end) if has_early else None

    # Determine middle bucket
    if middle_start is not None:
        middle_end = last_idx
        if late_start is not None:
            middle_end = late_start - 1
        elif exit_start is not None:
            middle_end = exit_start - 1
        middle = _slice_or_none(company_funding_rows, middle_start, middle_end)
    else:
        middle = None

    # Determine late bucket
    if late_start is not None:
        late_end = last_idx
        if exit_start is not None:
            late_end = exit_start - 1
        late = _slice_or_none(company_funding_rows, late_start, late_end)
    else:
        late = None

    # Determine post-equity bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    # If nothing was classified into any bucket, put everything in the appropriate
    # "remainder" — but only if there are equity/grant rounds
    if early is None and middle is None and late is None and exit is None:
        return None, None, None, None

    return early, middle, late, exit


def _split_after_last_early_round(company_funding_rows, stages):
    """Split where early stage extends through the last early-stage round."""
    n = len(company_funding_rows)
    last_idx = n - 1

    last_early = _find_last_index_at_stage(stages, "early")

    # Find boundary for late and post-equity using first occurrence
    late_start = _find_first_index_at_or_above(stages, "late")
    exit_start = _find_first_index_at_or_above(stages, "exit")

    # Early bucket: everything up to and including the last early-stage round
    if last_early is not None:
        early = _slice_or_none(company_funding_rows, 0, last_early)
        middle_start = last_early + 1
    else:
        early = None
        # No early rounds — check if there are middle rounds
        first_middle = _find_first_index_at_or_above(stages, "middle")
        if first_middle is not None:
            middle_start = first_middle
        elif late_start is not None:
            middle_start = None  # skip middle
        else:
            middle_start = None

    # Middle bucket
    if middle_start is not None and middle_start <= last_idx:
        middle_end = last_idx
        if late_start is not None and late_start > middle_start:
            middle_end = late_start - 1
        elif exit_start is not None and exit_start > middle_start:
            middle_end = exit_start - 1
        middle = _slice_or_none(company_funding_rows, middle_start, middle_end)
    else:
        middle = None

    # Late bucket
    if late_start is not None:
        late_end = last_idx
        if exit_start is not None:
            late_end = exit_start - 1
        late = _slice_or_none(company_funding_rows, late_start, late_end)
    else:
        late = None

    # Post-equity bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    if early is None and middle is None and late is None and exit is None:
        return None, None, None, None

    return early, middle, late, exit


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
