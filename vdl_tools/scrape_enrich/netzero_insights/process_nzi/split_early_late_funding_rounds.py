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
    "Series C",
    "Late VC",
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

# Stage classification sets — only equity venture rounds define boundaries.
# The pre-Series-B window is split into two finer buckets:
#   up_to_a = rounds before the first Series A or Early VC round
#   a_to_b  = rounds from first Series A / Early VC up to first Series B
UP_TO_A_TYPES = {
    "Pre-Seed",
    "Seed",
}

A_TO_B_TYPES = {
    "Series A",
    "Early VC",
}

MIDDLE_STAGE_TYPES = {
    "Series B",
    # "Late VC",  # moved to late stage - usually follows C or is synonymous with C
}

LATE_STAGE_TYPES = {
    "Late VC",
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
        One of ``"up_to_a"``, ``"a_to_b"``, ``"middle"``, ``"late"``,
        ``"exit"``, or ``None`` if the round type does not define a stage
        boundary.
    """
    if round_type in UP_TO_A_TYPES:
        return "up_to_a"
    if round_type in A_TO_B_TYPES:
        return "a_to_b"
    if round_type in MIDDLE_STAGE_TYPES:
        return "middle"
    if round_type in LATE_STAGE_TYPES:
        return "late"
    if round_type in EXIT_TYPES:
        return "exit"
    return None


STAGE_ORDER = {"up_to_a": 0, "a_to_b": 1, "middle": 2, "late": 3, "exit": 4}


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
    """Split a company's funding rounds into 5 stage buckets.

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
        - ``SPLIT_AFTER_LAST_EARLY_ROUND`` — the a_to_b bucket extends
          through the last Series A / Early VC round.

    Returns
    -------
    dict of {str: pandas.DataFrame or None}
        Keys are ``"up_to_a"``, ``"a_to_b"``, ``"middle"``, ``"late"``,
        and ``"exit"``. Values are the corresponding DataFrames, or ``None``
        if the bucket has no rounds. Exit includes IPO/SPAC/Post-IPO as
        well as M&A events (Acquisition, Merger, Buyout).

    Raises
    ------
    ValueError
        If ``split_strategy`` is not a recognised value.
    """
    empty_split = {"up_to_a": None, "a_to_b": None, "middle": None, "late": None, "exit": None}

    if not raised_equity_round(company_funding_rows):
        return empty_split

    company_funding_rows = company_funding_rows.copy()
    company_funding_rows = company_funding_rows[company_funding_rows['round_date_nzi'].notna()]
    company_funding_rows = company_funding_rows.sort_values(by='round_date_nzi', ascending=True)
    company_funding_rows = company_funding_rows.reset_index(drop=True)

    if len(company_funding_rows) == 0:
        return empty_split

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
        "up_to_a": split[0],
        "a_to_b":  split[1],
        "middle":  split[2],
        "late":    split[3],
        "exit":    split[4],
    }

# Buckets that get per-investor-type count columns. Pre-Series-B funding is
# the analytical focus, so we count investors in the two pre-B buckets plus
# `middle` (so callers can study Series-B-stage investors as well).
INVESTOR_COUNT_BUCKETS = {"up_to_a", "a_to_b", "middle"}

# Buckets that get equity / non-equity split columns. Same pre-B focus as
# INVESTOR_COUNT_BUCKETS — late and exit are excluded.
EQUITY_SPLIT_BUCKETS = {"up_to_a", "a_to_b", "middle"}

# Buckets that get a per-stage `{stage}_investors` list-of-dicts column.
INVESTOR_LIST_BUCKETS = {"up_to_a", "a_to_b", "middle"}


def _collect_investor_types(primary, secondary):
    """Return a deduped list of investor types from primary + secondary.

    Primary type comes first (when present), then any secondary types not
    already in the list. Non-string / empty values are ignored. NZI stores
    ``secondary_types_nzi`` as a list, but it can also be NaN / missing.
    """
    types = []
    if isinstance(primary, str) and primary:
        types.append(primary)
    if isinstance(secondary, (list, tuple)):
        for t in secondary:
            if isinstance(t, str) and t and t not in types:
                types.append(t)
    return types


def _build_investor_lookup(processed_investor_df):
    """Build {investor_id: {"id", "name", "investor_types"}} from the investor df.

    ``investor_types`` is a deduped list combining ``primary_type_nzi`` and
    ``secondary_types_nzi`` — an investor classified as both Venture Capital
    (primary) and Foundation (secondary) shows up under both labels.

    Returns None if ``processed_investor_df`` is None, signalling that
    per-stage investor lists should be skipped. Rows with a missing
    ``investor_id_nzi`` are dropped from the lookup.
    """
    if processed_investor_df is None:
        return None
    lookup = {}
    for _, row in processed_investor_df.iterrows():
        inv_id = row.get("investor_id_nzi")
        if pd.isna(inv_id):
            continue
        lookup[inv_id] = {
            "id": inv_id,
            "name": row.get("name_nzi"),
            "investor_types": _collect_investor_types(
                row.get("primary_type_nzi"),
                row.get("secondary_types_nzi"),
            ),
        }
    return lookup


def _bucket_investors(round_group_rounds, investor_lookup):
    """Return a deduped list of investor dicts for one stage bucket.

    Flattens ``round_investor_ids_nzi`` across all rounds in the bucket,
    preserves first-seen order, and attaches name + investor_types from
    ``investor_lookup``. Investor ids not present in the lookup still
    show up as ``{"id": ..., "name": None, "investor_types": []}`` so no
    data is silently dropped.
    """
    seen = set()
    investors = []
    if "round_investor_ids_nzi" not in round_group_rounds.columns:
        return investors
    for ids in round_group_rounds["round_investor_ids_nzi"]:
        if not isinstance(ids, (list, tuple)):
            continue
        for inv_id in ids:
            if pd.isna(inv_id) or inv_id in seen:
                continue
            seen.add(inv_id)
            meta = investor_lookup.get(inv_id)
            if meta is None:
                investors.append({"id": inv_id, "name": None, "investor_types": []})
            else:
                investors.append(meta)
    return investors


def divided_funding_rows_and_flatten(
    processed_funding_rounds,
    id_col="client_id_nzi",
    processed_investor_df=None,
):
    """Divide every company's funding rounds into stage buckets and flatten.

    Groups ``processed_funding_rounds`` by company, calls
    ``divide_funding_rows`` on each group, then summarises each stage
    bucket into one row per company with aggregated metrics (date range,
    total amount raised, round count, and pre-B investor type counts).

    Parameters
    ----------
    processed_funding_rounds : pandas.DataFrame
        All funding rounds across companies. Must include the columns
        required by ``divide_funding_rows`` plus any ``has_*_investor_calced_nzi``
        columns for investor type counting.
    id_col : str, optional
        Column name used to group rows by company. Defaults to
        ``"client_id_nzi"``.
    processed_investor_df : pandas.DataFrame, optional
        Investor metadata (output of ``process_nzi_investors``) with
        ``investor_id_nzi``, ``name_nzi``, ``primary_type_nzi``, and
        ``secondary_types_nzi``. When provided, each bucket in
        ``INVESTOR_LIST_BUCKETS`` gets a ``{stage}_investors`` column
        containing a deduped list of
        ``{"id", "name", "investor_types"}`` dicts (``investor_types``
        merges primary + secondary types). When ``None`` (default), the
        column is omitted.

    Returns
    -------
    pandas.DataFrame
        One row per company with columns prefixed by stage name (e.g.
        ``up_to_a_first_round_date``, ``a_to_b_amount_raised``,
        ``middle_num_rounds``, ``exit_last_round_date``). Buckets in
        ``INVESTOR_COUNT_BUCKETS`` also get
        ``*_has_<type>_investor_calced_nzi_count`` columns. Buckets in
        ``EQUITY_SPLIT_BUCKETS`` additionally get
        ``*_equity_raised`` (sum of ``Equity`` financing-type rounds),
        ``*_nonequity_raised`` (sum of all other rounds — grants, debt,
        convertibles, etc.), ``*_n_rounds_equity`` / ``*_n_rounds_nonequity``
        (counts of equity vs. non-equity rounds in the bucket), and
        ``*_nonequity_types`` (sorted list of unique ``round_type_nzi``
        values among non-equity rounds). When the bucket exists,
        ``equity_raised + nonequity_raised == amount_raised`` (ignoring
        the NaN guard, which fires when every round of a given type in
        the bucket was undisclosed). When
        ``processed_investor_df`` is provided, buckets in
        ``INVESTOR_LIST_BUCKETS`` also get ``*_investors`` — a deduped
        list of ``{"id", "name", "investor_types"}`` dicts covering every
        investor across the bucket's rounds.
    """
    divided_rounds = processed_funding_rounds.groupby(id_col).apply(
        divide_funding_rows,
        include_groups=False
    )

    investor_type_columns = [
        x for x in processed_funding_rounds.columns
        if x.startswith('has_') and x.endswith('_investor_calced_nzi')
    ]

    investor_lookup = _build_investor_lookup(processed_investor_df)
    include_investor_lists = investor_lookup is not None

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
            # Investor counts only for the two pre-B buckets.
            if round_name in INVESTOR_COUNT_BUCKETS:
                round_group_dict.update({f"{col}_count": None for col in investor_type_columns})
            # Equity / non-equity split only for pre-B buckets.
            if round_name in EQUITY_SPLIT_BUCKETS:
                round_group_dict["equity_raised"] = None
                round_group_dict["nonequity_raised"] = None
                round_group_dict["n_rounds_equity"] = None
                round_group_dict["n_rounds_nonequity"] = None
                round_group_dict["nonequity_types"] = None
            if include_investor_lists and round_name in INVESTOR_LIST_BUCKETS:
                round_group_dict["investors"] = None

            if round_group_rounds is None:
                company_round_groups_parsed.append(round_group_dict)
                continue
            round_group_dict["first_round_date"] = round_group_rounds['round_date_nzi'].min()
            round_group_dict["last_round_date"] = round_group_rounds['round_date_nzi'].max()
            # Sum dollars across the bucket's rounds. Pandas .sum() skips NaN
            # by default, so this is the sum of the *disclosed* portion only.
            round_group_dict["amount_raised"] = round_group_rounds['round_amount_usd_nzi'].sum()
            round_group_dict["num_rounds"] = round_group_rounds['round_type_nzi'].count()
            # Undisclosed-amount guard: a 0 sum combined with >0 rounds means
            # every round in the bucket had an undisclosed amount. Flip to NaN
            # so downstream means treat the bucket as missing, not as $0.
            if round_group_dict["num_rounds"] > 0 and round_group_dict["amount_raised"] == 0:
                round_group_dict["amount_raised"] = float("nan")
            if round_name in INVESTOR_COUNT_BUCKETS:
                for investor_type_col in investor_type_columns:
                    round_group_dict[f"{investor_type_col}_count"] = round_group_rounds[investor_type_col].sum()
            if round_name in EQUITY_SPLIT_BUCKETS:
                is_equity = round_group_rounds['financing_type_nzi'] == "Equity"
                equity_rows = round_group_rounds[is_equity]
                nonequity_rows = round_group_rounds[~is_equity]
                round_group_dict["equity_raised"] = float(equity_rows['round_amount_usd_nzi'].sum())
                round_group_dict["nonequity_raised"] = float(nonequity_rows['round_amount_usd_nzi'].sum())
                round_group_dict["n_rounds_equity"] = int(len(equity_rows))
                round_group_dict["n_rounds_nonequity"] = int(len(nonequity_rows))
                round_group_dict["nonequity_types"] = sorted(
                    nonequity_rows['round_type_nzi'].dropna().unique().tolist()
                )
                # Per-split undisclosed-amount guard. Gate each split by its
                # own round count so true zeros survive (e.g. equity_raised=0
                # for a company with only nonequity rounds stays 0, not NaN),
                # but splits where every round of that type was undisclosed
                # become NaN.
                if round_group_dict["n_rounds_equity"] > 0 and round_group_dict["equity_raised"] == 0:
                    round_group_dict["equity_raised"] = float("nan")
                if round_group_dict["n_rounds_nonequity"] > 0 and round_group_dict["nonequity_raised"] == 0:
                    round_group_dict["nonequity_raised"] = float("nan")
            if include_investor_lists and round_name in INVESTOR_LIST_BUCKETS:
                round_group_dict["investors"] = _bucket_investors(
                    round_group_rounds, investor_lookup
                )
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

    df_out = pd.DataFrame(all_rows)
    return df_out


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


def _find_first_index_at_stage(stages, target_stage):
    """Find the first index whose effective stage matches exactly.

    Parameters
    ----------
    stages : pandas.Series
        Effective stage labels indexed to match the funding-rows DataFrame.
    target_stage : str
        Stage to match (e.g. ``"a_to_b"``).

    Returns
    -------
    int or None
        The first DataFrame index with ``stages[idx] == target_stage``, or
        ``None`` if not found.
    """
    for idx, stage in stages.items():
        if stage == target_stage:
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
    ``"a_to_b"``, ``"middle"``, ``"late"``, or ``"exit"``. Everything before
    the first a_to_b round is up_to_a; from first a_to_b to first middle is
    a_to_b; and so on. Non-boundary round types (debt, convertible) are
    absorbed into whichever bucket they fall into chronologically.

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
        ``(up_to_a, a_to_b, middle, late, exit)``. If no boundary-defining
        rounds exist but the company passed the equity gate, all rows are
        returned as the up_to_a bucket.
    """
    n = len(company_funding_rows)
    last_idx = n - 1

    # Two distinct things we need to find:
    #   first_a_to_b  : first row whose stage IS "a_to_b" (Series A / Early VC).
    #                   Used to gate whether the a_to_b bucket exists at all.
    #   first_elevated: first row whose stage is a_to_b OR higher (anything past
    #                   pre-A). Used to mark where the up_to_a window ends.
    # These differ when a company has no Series A round — e.g. [Debt, Late VC,
    # IPO]: first_a_to_b is None but first_elevated points to the Late VC row.
    first_a_to_b   = _find_first_index_at_stage(stages, "a_to_b")
    first_elevated = _find_first_index_at_or_above(stages, "a_to_b")
    middle_start   = _find_first_index_at_or_above(stages, "middle")
    late_start     = _find_first_index_at_or_above(stages, "late")
    exit_start     = _find_first_index_at_or_above(stages, "exit")

    # up_to_a window: rows before the first elevated row.
    up_to_a_end = first_elevated - 1 if first_elevated is not None else last_idx

    # Only emit up_to_a if there's at least one Pre-Seed/Seed round in
    # the range. Otherwise, leading non-equity rows (debt, convertible):
    #   - are absorbed into a_to_b when a Series A round exists, matching
    #     the old behavior where a convertible before Series A bundled
    #     into the same bucket as the A.
    #   - are dropped when no Series A exists either (e.g.
    #     [Debt, Late VC, IPO]). Intentional: the old behavior
    #     over-counted these outliers by bundling leading debt rounds
    #     into the late bucket, inflating late totals by ~$1B across
    #     the dataset.
    has_up_to_a = up_to_a_end >= 0 and _has_stage_in_range(stages, "up_to_a", 0, up_to_a_end)
    up_to_a = _slice_or_none(company_funding_rows, 0, up_to_a_end) if has_up_to_a else None

    # a_to_b: only exists if the company actually had an a_to_b stage round.
    # When there's no up_to_a, leading non-equity rows get pulled into a_to_b
    # so they're not silently dropped (matches old "early" semantics).
    if first_a_to_b is not None:
        a_to_b_actual_start = first_a_to_b if has_up_to_a else 0
        a_to_b_end = last_idx
        if middle_start is not None:
            a_to_b_end = middle_start - 1
        elif late_start is not None:
            a_to_b_end = late_start - 1
        elif exit_start is not None:
            a_to_b_end = exit_start - 1
        a_to_b = _slice_or_none(company_funding_rows, a_to_b_actual_start, a_to_b_end)
    else:
        a_to_b = None

    # middle bucket
    if middle_start is not None:
        middle_end = last_idx
        if late_start is not None:
            middle_end = late_start - 1
        elif exit_start is not None:
            middle_end = exit_start - 1
        middle = _slice_or_none(company_funding_rows, middle_start, middle_end)
    else:
        middle = None

    # late bucket
    if late_start is not None:
        late_end = last_idx
        if exit_start is not None:
            late_end = exit_start - 1
        late = _slice_or_none(company_funding_rows, late_start, late_end)
    else:
        late = None

    # exit bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    # If no boundary-defining round types were found but the company passed
    # the equity gate, treat all rows as up_to_a — these companies have
    # equity/grant funding through non-standard types (e.g. Equity crowdfunding,
    # Accelerator) and never reached a named venture round.
    if up_to_a is None and a_to_b is None and middle is None and late is None and exit is None:
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None, None
        return None, None, None, None, None

    return up_to_a, a_to_b, middle, late, exit


def _split_after_last_early_round(company_funding_rows, stages):
    """Split funding rounds so the a_to_b bucket extends through the last
    Series A / Early VC round.

    Unlike ``_split_on_first_late_round``, the a_to_b bucket here includes
    everything from the first a_to_b-or-later row up to and including the
    **last** a_to_b round (even if middle / late rounds are interleaved
    in between). Middle begins immediately after the last a_to_b; late and
    exit boundaries are still drawn at the first occurrence of those stages.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Chronologically sorted funding rounds for one company.
    stages : pandas.Series
        Effective stage label for each row.

    Returns
    -------
    tuple of (pandas.DataFrame or None)
        ``(up_to_a, a_to_b, middle, late, exit)``. If no boundary-defining
        rounds exist but the company passed the equity gate, all rows are
        returned as the up_to_a bucket.
    """
    n = len(company_funding_rows)
    last_idx = n - 1

    last_a_to_b = _find_last_index_at_stage(stages, "a_to_b")
    a_to_b_start_at_or_above = _find_first_index_at_or_above(stages, "a_to_b")
    late_start = _find_first_index_at_or_above(stages, "late")
    exit_start = _find_first_index_at_or_above(stages, "exit")

    # up_to_a window: rows before first a_to_b / middle / late / exit row.
    if a_to_b_start_at_or_above is not None:
        up_to_a_end = a_to_b_start_at_or_above - 1
    elif late_start is not None:
        up_to_a_end = late_start - 1
    elif exit_start is not None:
        up_to_a_end = exit_start - 1
    else:
        up_to_a_end = last_idx

    has_up_to_a = up_to_a_end >= 0 and _has_stage_in_range(stages, "up_to_a", 0, up_to_a_end)
    up_to_a = _slice_or_none(company_funding_rows, 0, up_to_a_end) if has_up_to_a else None

    # a_to_b: extends through last_a_to_b round (interleaved middle stays in a_to_b).
    if last_a_to_b is not None:
        a_to_b_start = a_to_b_start_at_or_above if has_up_to_a else 0
        a_to_b = _slice_or_none(company_funding_rows, a_to_b_start, last_a_to_b)
        middle_start = last_a_to_b + 1
    else:
        a_to_b = None
        # No a_to_b stage — fall back to first-late semantics for middle.
        first_middle = _find_first_index_at_or_above(stages, "middle")
        middle_start = first_middle  # may be None

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

    # Exit bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    if up_to_a is None and a_to_b is None and middle is None and late is None and exit is None:
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None, None
        return None, None, None, None, None

    return up_to_a, a_to_b, middle, late, exit
