import datetime as dt
import pandas as pd


def did_raise_venture(company_funding_rows):
    """Check whether a company has raised venture (equity) funding.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding rounds for a single company. Must contain a
        ``financing_type_nzi`` column.

    Returns
    -------
    bool
        True if any row has ``financing_type_nzi == "Equity"``.
    """
    return "Equity" in company_funding_rows['financing_type_nzi'].values


M_AND_A_SUCCESS_STAGE = "Series A"

EARLY_VC_STAGES = [
    "Early VC",
    "Pre-Seed",
    "Seed",
    "Series A",
]

# Anything Series B or later is considered a late venture round
LATE_VC_CUTOFF = "Series B"

DISCLOSED_STAGES_ORDERED = [
    "Accelerator/incubator",
    "Grant",
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
    """Map a NZI round type to its effective stage category.

    Parameters
    ----------
    round_type : str
        The ``round_type_nzi`` value for a single funding round (e.g.
        ``"Series A"``, ``"IPO"``).

    Returns
    -------
    str or None
        One of ``"early"``, ``"middle"``, ``"late"``, ``"exit"``, or ``None``
        if the round type does not define a stage boundary.
    """
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
    """Check whether a company has raised equity, grant, or accelerator funding.

    This is a broader gate than ``did_raise_venture``; it also counts grants
    and accelerator/incubator rounds as qualifying equity-like funding.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding rounds for a single company. Must contain
        ``financing_type_nzi`` and ``round_type_nzi`` columns.

    Returns
    -------
    bool
        True if any row has ``financing_type_nzi`` of ``"Equity"`` or
        ``"Grant"``, or ``round_type_nzi`` of ``"Accelerator/incubator"``.
    """
    financing_types = company_funding_rows['financing_type_nzi'].values
    if "Equity" in financing_types:
        return True
    if "Grant" in financing_types:
        return True
    round_types = company_funding_rows['round_type_nzi'].values
    if "Accelerator/incubator" in round_types:
        return True
    return False


def divide_funding_rows(
    company_funding_rows,
    split_strategy=SPLIT_ON_FIRST_LATE_ROUND,
):
    """Split a company's funding rounds into early, middle, late, and exit buckets.

    Rounds are sorted chronologically and then partitioned using equity
    venture round types as stage boundaries. Non-boundary round types
    (e.g. debt, convertible notes) are absorbed into whichever bucket they
    fall into chronologically. Companies that never raised equity-like
    funding are skipped entirely.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding rounds for a single company. Expected columns include
        ``round_type_nzi``, ``round_date_nzi``, ``financing_type_nzi``,
        and ``round_amount_usd_nzi``.
    split_strategy : str, optional
        How to determine bucket boundaries. One of:

        - ``SPLIT_ON_FIRST_LATE_ROUND`` (default) — each stage begins at
          the first occurrence of that stage's round type.
        - ``SPLIT_AFTER_LAST_EARLY_ROUND`` — the early bucket extends
          through the last early-stage round.

    Returns
    -------
    dict of {str: pandas.DataFrame or None}
        Keys are ``"early"``, ``"middle"``, ``"late"``, and ``"exit"``.
        Values are the corresponding DataFrames, or ``None`` if the bucket
        has no rounds. Exit includes IPO/SPAC/Post-IPO as well as M&A events
        (Acquisition, Merger, Buyout).

    Raises
    ------
    ValueError
        If ``split_strategy`` is not a recognised value.
    """
    if not raised_equity_round(company_funding_rows):
        split = [None, None, None, None]

    company_funding_rows = company_funding_rows.copy()
    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)
    company_funding_rows = company_funding_rows.reset_index(drop=True)

    if len(company_funding_rows) == 0:
        split = [None, None, None, None]

    stages = company_funding_rows['round_type_nzi'].map(_get_effective_stage)

    if split_strategy == SPLIT_ON_FIRST_LATE_ROUND:
        split = _split_on_first_late_round(company_funding_rows, stages)
    elif split_strategy == SPLIT_AFTER_LAST_EARLY_ROUND:
        split = _split_after_last_early_round(company_funding_rows, stages)
    else:
        raise ValueError(
            "split_strategy must be "
            f"'{SPLIT_ON_FIRST_LATE_ROUND}' or '{SPLIT_AFTER_LAST_EARLY_ROUND}'"
        )

    return {
        "early": split[0],
        "middle": split[1],
        "late": split[2],
        "exit": split[3],
    }

def divided_funding_rows_and_flatten(
    processed_funding_rounds,
    id_col="client_id_nzi"
):
    """Divide every company's funding rounds into stage buckets and flatten.

    Groups ``processed_funding_rounds`` by company, calls
    ``divide_funding_rows`` on each group, then summarises each stage
    bucket into one row per company with aggregated metrics (date range,
    total amount raised, round count, and early-stage investor type counts).

    Parameters
    ----------
    processed_funding_rounds : pandas.DataFrame
        All funding rounds across companies. Must include the columns
        required by ``divide_funding_rows`` plus any ``has_*_investor_calced_nzi``
        columns for investor type counting.
    id_col : str, optional
        Column name used to group rows by company. Defaults to
        ``"client_id_nzi"``.

    Returns
    -------
    pandas.DataFrame
        One row per company with columns prefixed by stage name (e.g.
        ``early_first_round_date``, ``middle_amount_raised``,
        ``late_num_rounds``, ``exit_last_round_date``). Early-stage rows
        also include ``*_investor_calced_nzi_count`` columns.
    """
    divided_rounds = processed_funding_rounds.groupby(id_col).apply(
        divide_funding_rows,
        include_groups=False
    )

    investor_type_columns = [
        x for x in processed_funding_rounds.columns
        if x.startswith('has_') and x.endswith('_investor_calced_nzi')
    ]

    all_rows = []
    for company_id, company_divided_rounds in divided_rounds.items():
        company_round_groups_parsed = []
        company_round_groups_parsed_dict = {}
        for round_name, round_group_rounds in company_divided_rounds.items():
            round_group_dict = {
                "name": round_name,
                "first_round_date": None,
                "last_round_date": None,
                "amount_raised": None,
                "num_rounds": None,
                "all_funding_activity": None,
            }
            if round_name == 'early':
                round_group_dict.update({f"{col}_count": None for col in investor_type_columns})

            if round_group_rounds is None:
                company_round_groups_parsed.append(round_group_dict)
                continue
            round_group_dict["first_round_date"] = round_group_rounds['round_date_nzi'].min()
            round_group_dict["last_round_date"] = round_group_rounds['round_date_nzi'].max()
            round_group_dict["amount_raised"] = round_group_rounds['round_amount_usd_nzi'].sum()
            round_group_dict["num_rounds"] = round_group_rounds['round_type_nzi'].count()
            if round_name == 'early':
                for investor_type_col in investor_type_columns:
                    round_group_dict[f"{investor_type_col}_count"] = round_group_rounds[investor_type_col].sum()
            round_group_dict["all_funding_activity"] = round_group_rounds
            company_round_groups_parsed.append(round_group_dict)

        company_round_groups_parsed_dict = {
            "client_id_nzi": company_id
        }
        for round_group_dict in company_round_groups_parsed:
            for col, v in round_group_dict.items():
                if col == 'name':
                    continue
                company_round_groups_parsed_dict[f"{round_group_dict['name']}_{col}"] = v
        all_rows.append(company_round_groups_parsed_dict)
    return pd.DataFrame(all_rows)



def _find_first_index_at_or_above(stages, min_stage):
    """Find the first index whose effective stage is at or above a threshold.

    Parameters
    ----------
    stages : pandas.Series
        Effective stage labels (values from ``_get_effective_stage``) indexed
        to match the funding-rows DataFrame.
    min_stage : str
        Minimum stage to match, one of ``"early"``, ``"middle"``, ``"late"``,
        ``"exit"``.

    Returns
    -------
    int or None
        The first DataFrame index where the stage order is >= ``min_stage``,
        or ``None`` if no such index exists.
    """
    min_order = STAGE_ORDER[min_stage]
    for idx, stage in stages.items():
        if stage is not None and STAGE_ORDER.get(stage, -1) >= min_order:
            return idx
    return None


def _find_last_index_at_stage(stages, target_stage):
    """Find the last index whose effective stage matches exactly.

    Parameters
    ----------
    stages : pandas.Series
        Effective stage labels indexed to match the funding-rows DataFrame.
    target_stage : str
        Stage to match (e.g. ``"early"``).

    Returns
    -------
    int or None
        The last DataFrame index with ``stages[idx] == target_stage``, or
        ``None`` if not found.
    """
    last = None
    for idx, stage in stages.items():
        if stage == target_stage:
            last = idx
    return last


def _has_stage_in_range(stages, target_stage, start_idx, end_idx):
    """Check whether a stage appears within a contiguous index range.

    Parameters
    ----------
    stages : pandas.Series
        Effective stage labels indexed to match the funding-rows DataFrame.
    target_stage : str
        Stage to look for (e.g. ``"early"``).
    start_idx : int
        Inclusive start of the index range.
    end_idx : int
        Inclusive end of the index range.

    Returns
    -------
    bool
        True if any index in ``[start_idx, end_idx]`` has the target stage.
    """
    for idx in range(start_idx, end_idx + 1):
        if idx in stages.index and stages[idx] == target_stage:
            return True
    return False


def _slice_or_none(df, start_idx, end_idx):
    """Slice a DataFrame by label range, returning None for empty results.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to slice.
    start_idx : int or None
        Inclusive start label for ``df.loc``.
    end_idx : int or None
        Inclusive end label for ``df.loc``.

    Returns
    -------
    pandas.DataFrame or None
        The sliced DataFrame, or ``None`` if the inputs are ``None``,
        ``start_idx > end_idx``, or the resulting slice is empty.
    """
    if start_idx is None or end_idx is None or start_idx > end_idx:
        return None
    result = df.loc[start_idx:end_idx]
    if len(result) == 0:
        return None
    return result


def _split_on_first_late_round(company_funding_rows, stages):
    """Split funding rounds so each bucket starts at the first occurrence of its stage.

    Boundaries are drawn at the first round whose effective stage is
    ``"middle"``, ``"late"``, or ``"exit"`` respectively. Everything before
    the first middle-stage round is early, everything between middle and late
    is middle, and so on. Non-boundary round types are absorbed into
    whichever bucket they fall into chronologically.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Chronologically sorted funding rounds for one company (already
        filtered to rows with valid dates).
    stages : pandas.Series
        Effective stage label for each row, aligned with
        ``company_funding_rows``.

    Returns
    -------
    tuple of (pandas.DataFrame or None)
        ``(early, middle, late, exit)``. If no boundary-defining rounds
        exist but the company passed the equity gate, all rows are returned
        as the early bucket.
    """
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

    # If no boundary-defining round types were found but the company passed
    # the equity gate, treat all rows as early stage — these companies have
    # equity/grant funding through non-standard types (e.g. Equity crowdfunding,
    # Accelerator) and never reached a named venture round.
    if early is None and middle is None and late is None and exit is None:
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None
        return None, None, None, None

    return early, middle, late, exit


def _split_after_last_early_round(company_funding_rows, stages):
    """Split funding rounds so the early bucket extends through the last early-stage round.

    Unlike ``_split_on_first_late_round``, the early bucket here includes
    everything up to and including the **last** early-stage round (even if
    middle or late rounds are interleaved). The middle bucket begins
    immediately after, and late/exit boundaries are still drawn at the first
    occurrence of those stage types.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Chronologically sorted funding rounds for one company (already
        filtered to rows with valid dates).
    stages : pandas.Series
        Effective stage label for each row, aligned with
        ``company_funding_rows``.

    Returns
    -------
    tuple of (pandas.DataFrame or None)
        ``(early, middle, late, exit)``. If no boundary-defining rounds
        exist but the company passed the equity gate, all rows are returned
        as the early bucket.
    """
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
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None
        return None, None, None, None

    return early, middle, late, exit
