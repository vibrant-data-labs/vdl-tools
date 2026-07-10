import datetime as dt
import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.stage_constants import (
    A_TO_B_TYPES,
    EXIT_TYPES,
    LATE_STAGE_TYPES,
    MIDDLE_STAGE_TYPES,
    SPLIT_AFTER_LAST_EARLY_ROUND,
    SPLIT_ON_FIRST_LATE_ROUND,
    STAGE_ORDER,
    UP_TO_A_TYPES,
)

# Periods that get the per-investor-type count columns (one count column per investor type:
# ``{period}_has_<type>_investor_calced_nzi_count``). "exit" is intentionally excluded.
PERIODS_WITH_INVESTOR_TYPE_COUNTS = {"up_to_a", "a_to_b", "b_to_late", "late_to_exit"}

# Periods that get the per-financing-type breakdown columns (equity_raised / nonequity_raised /
# debt_raised / grant_raised / project_finance_raised / convertible_note_raised plus per-type
# round counts — 13 columns total). "exit" is intentionally excluded.
PERIODS_WITH_FINANCING_TYPE_COLS = {"up_to_a", "a_to_b", "b_to_late", "late_to_exit"}

# Periods that get the per-period ``{period}_investors`` list-of-dicts column (one dict per
# unique investor seen in any round in that period). "exit" is intentionally excluded.
PERIODS_WITH_INVESTOR_LIST = {"up_to_a", "a_to_b", "b_to_late", "late_to_exit"}


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
        One of ``"up_to_a"``, ``"a_to_b"``, ``"b_to_late"``, ``"late_to_exit"``,
        ``"exit"``, or ``None`` if the round type does not define a stage
        boundary. (Names: ``b_to_late`` = first Series B up to first late-stage
        round; ``late_to_exit`` = first late-stage round up to first exit.)
    """
    if round_type in UP_TO_A_TYPES:
        return "up_to_a"
    if round_type in A_TO_B_TYPES:
        return "a_to_b"
    if round_type in MIDDLE_STAGE_TYPES:
        return "b_to_late"
    if round_type in LATE_STAGE_TYPES:
        return "late_to_exit"
    if round_type in EXIT_TYPES:
        return "exit"
    return None


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
        Keys are ``"up_to_a"``, ``"a_to_b"``, ``"b_to_late"``, ``"late_to_exit"``,
        and ``"exit"``. Values are the corresponding DataFrames, or ``None``
        if the bucket has no rounds. ``b_to_late`` captures rounds from the
        first Series B up to (but not including) the first late-stage round
        (Series C/D/E/.../Late VC/Growth equity). ``late_to_exit`` then runs
        from that first late-stage round up to the first exit. Exit includes
        IPO/SPAC/Post-IPO as well as M&A events (Acquisition, Merger, Buyout).

    Raises
    ------
    ValueError
        If ``split_strategy`` is not a recognised value.
    """
    empty_split = {"up_to_a": None, "a_to_b": None, "b_to_late": None, "late_to_exit": None, "exit": None}

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
        "b_to_late":  split[2],
        "late_to_exit":    split[3],
        "exit":    split[4],
    }


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
        ``PERIODS_WITH_INVESTOR_LIST`` gets a ``{stage}_investors`` column
        containing a deduped list of
        ``{"id", "name", "investor_types"}`` dicts (``investor_types``
        merges primary + secondary types). When ``None`` (default), the
        column is omitted.

    Returns
    -------
    pandas.DataFrame
        One row per company with columns prefixed by stage name (e.g.
        ``up_to_a_first_round_date``, ``a_to_b_amount_raised``,
        ``b_to_late_num_rounds``, ``exit_last_round_date``). Buckets in
        ``PERIODS_WITH_INVESTOR_TYPE_COUNTS`` also get
        ``*_has_<type>_investor_calced_nzi_count`` columns. Buckets in
        ``PERIODS_WITH_FINANCING_TYPE_COLS`` additionally get
        ``*_equity_raised`` (sum of ``Equity`` financing-type rounds),
        ``*_nonequity_raised`` (sum of all other rounds — grants, debt,
        convertibles, etc.), ``*_n_rounds_equity`` / ``*_n_rounds_nonequity``
        (counts of equity vs. non-equity rounds in the bucket), and
        ``*_nonequity_types`` (sorted list of unique ``round_type_nzi``
        values among non-equity rounds). When the bucket exists,
        ``equity_raised + nonequity_raised == amount_raised`` (ignoring
        the NaN guard, which fires when every round of a given type in
        the bucket was undisclosed). The **a_to_b bucket** additionally
        gets ``a_to_b_first_a_raised`` (amount of the first Series A / Early VC
        round), ``a_to_b_post_first_a_equity_raised`` (sum of equity rounds after
        the first A — subsequent A bridges + any other equity before the first
        B), and ``a_to_b_n_rounds_post_first_a_equity`` (their count). These split
        ``a_to_b_equity_raised`` (= first_a + post, up to the undisclosed guard)
        and ``a_to_b_n_rounds_equity`` (= 1 + n_rounds_post_first_a_equity);
        there is no ``n_rounds_first_a`` because it is always 1. The **b_to_late
        bucket** gets the analogous split around the first **Series B** round:
        ``b_to_late_first_b_raised``, ``b_to_late_post_first_b_equity_raised``,
        ``b_to_late_n_rounds_post_first_b_equity`` (the b_to_late bucket always
        opens on the first Series B since the 2026-06 bucket-start fix, so the
        identity holds cleanly). When
        ``processed_investor_df`` is provided, buckets in
        ``PERIODS_WITH_INVESTOR_LIST`` also get ``*_investors`` — a deduped
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
            if round_name in PERIODS_WITH_INVESTOR_TYPE_COUNTS:
                round_group_dict.update({f"{col}_count": None for col in investor_type_columns})
            # Equity / non-equity split only for pre-B buckets. The
            # debt / grant / project-finance entries are extra line items
            # carved out of the nonequity rows so analyses can pull them
            # out separately (not a re-bucketing).
            if round_name in PERIODS_WITH_FINANCING_TYPE_COLS:
                round_group_dict["equity_raised"] = None
                round_group_dict["nonequity_raised"] = None
                round_group_dict["n_rounds_equity"] = None
                round_group_dict["n_rounds_nonequity"] = None
                round_group_dict["nonequity_types"] = None
                round_group_dict["debt_raised"] = None
                round_group_dict["grant_raised"] = None
                round_group_dict["project_finance_raised"] = None
                round_group_dict["convertible_note_raised"] = None
                round_group_dict["n_rounds_debt"] = None
                round_group_dict["n_rounds_grant"] = None
                round_group_dict["n_rounds_project_finance"] = None
                round_group_dict["n_rounds_convertible_note"] = None
                round_group_dict["accelerator_raised"] = None
                round_group_dict["n_rounds_accelerator"] = None
            # A-to-B ONLY: split the bucket's equity into the FIRST Series A round
            # vs every equity round after it (subsequent A bridges + any later
            # equity before the first B). There is deliberately no
            # ``n_rounds_first_a`` — by definition the first A is a single round,
            # and every A-to-B-cohort company has exactly one.
            if round_name == "a_to_b":
                round_group_dict["first_a_raised"] = None
                round_group_dict["post_first_a_equity_raised"] = None
                round_group_dict["n_rounds_post_first_a_equity"] = None
            # B-to-LATE ONLY: the same split, but around the FIRST Series B round vs every
            # equity round after it (subsequent B bridges + any equity before the first
            # late-stage round). No ``n_rounds_first_b`` — the first B is a single round.
            if round_name == "b_to_late":
                round_group_dict["first_b_raised"] = None
                round_group_dict["post_first_b_equity_raised"] = None
                round_group_dict["n_rounds_post_first_b_equity"] = None
            if include_investor_lists and round_name in PERIODS_WITH_INVESTOR_LIST:
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
            if round_name in PERIODS_WITH_INVESTOR_TYPE_COUNTS:
                for investor_type_col in investor_type_columns:
                    round_group_dict[f"{investor_type_col}_count"] = round_group_rounds[investor_type_col].sum()
            if round_name in PERIODS_WITH_FINANCING_TYPE_COLS:
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

                # Surface debt / grant / project-finance / convertible-note separately
                # so analyses can pull them out of the broader nonequity bucket. These
                # four are all subsets of the nonequity rows just summed — adding line
                # items, not re-bucketing.
                #   - debt and grant use financing_type_nzi (symmetric with equity).
                #   - project finance + convertible note use round_type_nzi because
                #     their financing_type is "Other" (NZI's catch-all), so they
                #     aren't distinguishable at the financing-type level.
                is_debt = round_group_rounds['financing_type_nzi'] == "Debt"
                is_grant = round_group_rounds['financing_type_nzi'] == "Grant"
                is_pf = round_group_rounds['round_type_nzi'] == "Project Finance"
                is_cn = round_group_rounds['round_type_nzi'] == "Convertible note"
                # Accelerator/incubator (round_type-based; NZI uses two casings).
                is_accel = round_group_rounds['round_type_nzi'].isin(
                    ["Accelerator/incubator", "Accelerator/Incubator"]
                )
                debt_rows = round_group_rounds[is_debt]
                grant_rows = round_group_rounds[is_grant]
                pf_rows = round_group_rounds[is_pf]
                cn_rows = round_group_rounds[is_cn]
                accel_rows = round_group_rounds[is_accel]
                round_group_dict["debt_raised"]  = float(debt_rows['round_amount_usd_nzi'].sum())
                round_group_dict["grant_raised"] = float(grant_rows['round_amount_usd_nzi'].sum())
                round_group_dict["project_finance_raised"] = float(pf_rows['round_amount_usd_nzi'].sum())
                round_group_dict["convertible_note_raised"] = float(cn_rows['round_amount_usd_nzi'].sum())
                round_group_dict["accelerator_raised"] = float(accel_rows['round_amount_usd_nzi'].sum())
                round_group_dict["n_rounds_debt"]  = int(len(debt_rows))
                round_group_dict["n_rounds_grant"] = int(len(grant_rows))
                round_group_dict["n_rounds_project_finance"] = int(len(pf_rows))
                round_group_dict["n_rounds_convertible_note"] = int(len(cn_rows))
                round_group_dict["n_rounds_accelerator"] = int(len(accel_rows))
                # Same undisclosed-amount NaN guard as equity/nonequity above.
                # Gate each split by its own count so true zeros survive.
                for _amt_key, _n_key in (
                    ("debt_raised", "n_rounds_debt"),
                    ("grant_raised", "n_rounds_grant"),
                    ("project_finance_raised", "n_rounds_project_finance"),
                    ("convertible_note_raised", "n_rounds_convertible_note"),
                    ("accelerator_raised", "n_rounds_accelerator"),
                ):
                    if round_group_dict[_n_key] > 0 and round_group_dict[_amt_key] == 0:
                        round_group_dict[_amt_key] = float("nan")

                # A-to-B ONLY: split equity into the first Series A round vs the
                # equity raised after it (before the first B). The bucket rounds
                # are already date-sorted (divide_funding_rows sorts + resets
                # index) and the a_to_b bucket starts at the first Series A /
                # Early VC round, so the "first A" is the earliest round whose
                # round_type is a Series-A-cohort type. Series A and Early VC are
                # interchangeable (A_TO_B_TYPES), so an Early-VC-only company's
                # first Early VC round counts as its first A.
                if round_name == "a_to_b":
                    # Positional view (index may be non-contiguous after slicing).
                    sorted_rounds = round_group_rounds.reset_index(drop=True)
                    is_first_a_type = sorted_rounds['round_type_nzi'].isin(A_TO_B_TYPES)
                    a_positions = [i for i, is_a in enumerate(is_first_a_type) if is_a]
                    if a_positions:
                        first_a_pos = a_positions[0]
                        # first_a_raised is a SINGLE round's amount — take it
                        # directly (NaN if undisclosed; don't .sum(), which would
                        # turn that NaN into 0).
                        round_group_dict["first_a_raised"] = float(
                            sorted_rounds['round_amount_usd_nzi'].iloc[first_a_pos]
                        )
                        # Equity rounds strictly after the first A: subsequent A
                        # bridges and any other equity rounds before the first B.
                        after_first_a = sorted_rounds.iloc[first_a_pos + 1:]
                        post_a_equity = after_first_a[
                            after_first_a['financing_type_nzi'] == "Equity"
                        ]
                        n_post = int(len(post_a_equity))
                        post_sum = float(post_a_equity['round_amount_usd_nzi'].sum())
                        # Same undisclosed-amount guard as equity_raised above:
                        # a 0 sum with >0 rounds means every post-A equity round
                        # was undisclosed → NaN, not a true $0.
                        if n_post > 0 and post_sum == 0:
                            post_sum = float("nan")
                        round_group_dict["n_rounds_post_first_a_equity"] = n_post
                        round_group_dict["post_first_a_equity_raised"] = post_sum

                # B-to-LATE ONLY: same split around the first Series B round. The b_to_late
                # bucket starts at the first Series-B-or-higher round and (since the 2026-06
                # bucket-start fix) always opens on the first Series B, so the "first B" is the
                # earliest round whose round_type is in MIDDLE_STAGE_TYPES (= {"Series B"}).
                if round_name == "b_to_late":
                    sorted_rounds = round_group_rounds.reset_index(drop=True)
                    is_first_b_type = sorted_rounds['round_type_nzi'].isin(MIDDLE_STAGE_TYPES)
                    b_positions = [i for i, is_b in enumerate(is_first_b_type) if is_b]
                    if b_positions:
                        first_b_pos = b_positions[0]
                        # first_b_raised is a SINGLE round's amount — take it directly (NaN if
                        # undisclosed; don't .sum(), which would turn that NaN into 0).
                        round_group_dict["first_b_raised"] = float(
                            sorted_rounds['round_amount_usd_nzi'].iloc[first_b_pos]
                        )
                        # Equity rounds strictly after the first B: subsequent B bridges and any
                        # other equity rounds before the first late-stage round.
                        after_first_b = sorted_rounds.iloc[first_b_pos + 1:]
                        post_b_equity = after_first_b[
                            after_first_b['financing_type_nzi'] == "Equity"
                        ]
                        n_post = int(len(post_b_equity))
                        post_sum = float(post_b_equity['round_amount_usd_nzi'].sum())
                        # Same undisclosed-amount guard as equity_raised: a 0 sum with >0 rounds
                        # means every post-B equity round was undisclosed → NaN, not a true $0.
                        if n_post > 0 and post_sum == 0:
                            post_sum = float("nan")
                        round_group_dict["n_rounds_post_first_b_equity"] = n_post
                        round_group_dict["post_first_b_equity_raised"] = post_sum
            if include_investor_lists and round_name in PERIODS_WITH_INVESTOR_LIST:
                round_group_dict["investors"] = _bucket_investors(
                    round_group_rounds, investor_lookup
                )
            if isinstance(round_group_rounds, pd.DataFrame):
                round_group_dict["all_funding_activity"] = round_group_rounds.to_dict(orient="records")
            else:
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
        Minimum stage to match, one of ``"up_to_a"``, ``"a_to_b"``,
        ``"b_to_late"``, ``"late_to_exit"``, ``"exit"``.

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
    ``"a_to_b"``, ``"b_to_late"``, ``"late_to_exit"``, or ``"exit"``. Everything
    before the first a_to_b round is up_to_a; from first a_to_b to first
    b_to_late is a_to_b; and so on. Non-boundary round types (debt, convertible)
    are absorbed into whichever bucket they fall into chronologically.

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
        ``(up_to_a, a_to_b, b_to_late, late_to_exit, exit)``. If no
        boundary-defining rounds exist but the company passed the equity
        gate, all rows are returned as the up_to_a bucket.
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
    b_to_late_start   = _find_first_index_at_or_above(stages, "b_to_late")
    late_to_exit_start     = _find_first_index_at_or_above(stages, "late_to_exit")
    exit_start     = _find_first_index_at_or_above(stages, "exit")

    # up_to_a window: rows before the first elevated row.
    up_to_a_end = first_elevated - 1 if first_elevated is not None else last_idx

    # Emit the up_to_a bucket when EITHER:
    #   (a) there's a Pre-Seed/Seed round in the window (the original anchor), OR
    #   (b) a Series A / Early VC round exists later — then by definition every
    #       round before that first A is the "up to A" period, even pre-A grants /
    #       accelerators / debt / convertibles that aren't Pre-Seed/Seed. This is
    #       the fix (2026-06): without it, those leading pre-A rounds leaked into
    #       the a_to_b bucket whenever the company had no Pre-Seed/Seed round, so
    #       the a_to_b period didn't actually start at the first A. We anchor on
    #       round POSITION (before the first A), not round TYPE, because grant /
    #       accelerator / award rounds recur at every stage (39% of A companies
    #       raise one AFTER their Series A) and so can't define the boundary.
    #
    # The no-Series-A case is unchanged: leading non-equity rows for a company
    # like [Debt, Late VC, IPO] are still dropped (not bundled into late_to_exit),
    # because first_a_to_b is None there and only the seed anchor (a) applies.
    has_seed_anchor = up_to_a_end >= 0 and _has_stage_in_range(stages, "up_to_a", 0, up_to_a_end)
    has_up_to_a = has_seed_anchor or (first_a_to_b is not None and up_to_a_end >= 0)
    up_to_a = _slice_or_none(company_funding_rows, 0, up_to_a_end) if has_up_to_a else None

    # a_to_b: only exists if the company actually had an a_to_b stage round, and it
    # ALWAYS starts at that first Series A / Early VC round — the rounds before it
    # are the up_to_a bucket (see above), never folded into a_to_b.
    if first_a_to_b is not None:
        a_to_b_actual_start = first_a_to_b
        a_to_b_end = last_idx
        if b_to_late_start is not None:
            a_to_b_end = b_to_late_start - 1
        elif late_to_exit_start is not None:
            a_to_b_end = late_to_exit_start - 1
        elif exit_start is not None:
            a_to_b_end = exit_start - 1
        a_to_b = _slice_or_none(company_funding_rows, a_to_b_actual_start, a_to_b_end)
    else:
        a_to_b = None

    # b_to_late bucket
    if b_to_late_start is not None:
        b_to_late_end = last_idx
        if late_to_exit_start is not None:
            b_to_late_end = late_to_exit_start - 1
        elif exit_start is not None:
            b_to_late_end = exit_start - 1
        b_to_late = _slice_or_none(company_funding_rows, b_to_late_start, b_to_late_end)
    else:
        b_to_late = None

    # late_to_exit bucket
    if late_to_exit_start is not None:
        late_to_exit_end = last_idx
        if exit_start is not None:
            late_to_exit_end = exit_start - 1
        late_to_exit = _slice_or_none(company_funding_rows, late_to_exit_start, late_to_exit_end)
    else:
        late_to_exit = None

    # exit bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    # If no boundary-defining round types were found but the company passed
    # the equity gate, treat all rows as up_to_a — these companies have
    # equity/grant funding through non-standard types (e.g. Equity crowdfunding,
    # Accelerator) and never reached a named venture round.
    if (up_to_a is None and a_to_b is None and b_to_late is None
            and late_to_exit is None and exit is None):
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None, None
        return None, None, None, None, None

    return up_to_a, a_to_b, b_to_late, late_to_exit, exit


def _split_after_last_early_round(company_funding_rows, stages):
    """Split funding rounds so the a_to_b bucket extends through the last
    Series A / Early VC round.

    Unlike ``_split_on_first_late_round``, the a_to_b bucket here includes
    everything from the first a_to_b-or-later row up to and including the
    **last** a_to_b round (even if b_to_late / late_to_exit rounds are
    interleaved in between). b_to_late begins immediately after the last
    a_to_b; late_to_exit and exit boundaries are still drawn at the first
    occurrence of those stages.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Chronologically sorted funding rounds for one company.
    stages : pandas.Series
        Effective stage label for each row.

    Returns
    -------
    tuple of (pandas.DataFrame or None)
        ``(up_to_a, a_to_b, b_to_late, late_to_exit, exit)``. If no boundary-defining
        rounds exist but the company passed the equity gate, all rows are
        returned as the up_to_a bucket.
    """
    n = len(company_funding_rows)
    last_idx = n - 1

    first_a_to_b = _find_first_index_at_stage(stages, "a_to_b")
    last_a_to_b = _find_last_index_at_stage(stages, "a_to_b")
    a_to_b_start_at_or_above = _find_first_index_at_or_above(stages, "a_to_b")
    late_to_exit_start = _find_first_index_at_or_above(stages, "late_to_exit")
    exit_start = _find_first_index_at_or_above(stages, "exit")

    # up_to_a window: rows before first a_to_b / b_to_late / late_to_exit / exit row.
    if a_to_b_start_at_or_above is not None:
        up_to_a_end = a_to_b_start_at_or_above - 1
    elif late_to_exit_start is not None:
        up_to_a_end = late_to_exit_start - 1
    elif exit_start is not None:
        up_to_a_end = exit_start - 1
    else:
        up_to_a_end = last_idx

    # Emit up_to_a on a Pre-Seed/Seed anchor OR (the 2026-06 fix) whenever a
    # Series A / Early VC round exists later — everything before the first A is the
    # up_to_a period by position, so pre-A grants / accelerators / debt no longer
    # leak into a_to_b. See _split_on_first_late_round for the full rationale.
    has_seed_anchor = up_to_a_end >= 0 and _has_stage_in_range(stages, "up_to_a", 0, up_to_a_end)
    has_up_to_a = has_seed_anchor or (first_a_to_b is not None and up_to_a_end >= 0)
    up_to_a = _slice_or_none(company_funding_rows, 0, up_to_a_end) if has_up_to_a else None

    # a_to_b: starts at the first Series A / Early VC round and extends through the
    # last a_to_b round (interleaved b_to_late stays in a_to_b).
    if last_a_to_b is not None:
        a_to_b_start = first_a_to_b
        a_to_b = _slice_or_none(company_funding_rows, a_to_b_start, last_a_to_b)
        b_to_late_start = last_a_to_b + 1
    else:
        a_to_b = None
        # No a_to_b stage — fall back to first-late semantics for b_to_late.
        first_b_to_late = _find_first_index_at_or_above(stages, "b_to_late")
        b_to_late_start = first_b_to_late  # may be None

    # b_to_late bucket
    if b_to_late_start is not None and b_to_late_start <= last_idx:
        b_to_late_end = last_idx
        if late_to_exit_start is not None and late_to_exit_start > b_to_late_start:
            b_to_late_end = late_to_exit_start - 1
        elif exit_start is not None and exit_start > b_to_late_start:
            b_to_late_end = exit_start - 1
        b_to_late = _slice_or_none(company_funding_rows, b_to_late_start, b_to_late_end)
    else:
        b_to_late = None

    # late_to_exit bucket
    if late_to_exit_start is not None:
        late_to_exit_end = last_idx
        if exit_start is not None:
            late_to_exit_end = exit_start - 1
        late_to_exit = _slice_or_none(company_funding_rows, late_to_exit_start, late_to_exit_end)
    else:
        late_to_exit = None

    # Exit bucket
    exit = _slice_or_none(company_funding_rows, exit_start, last_idx) if exit_start is not None else None

    if (up_to_a is None and a_to_b is None and b_to_late is None
            and late_to_exit is None and exit is None):
        has_any_boundary = stages.notna().any()
        if not has_any_boundary:
            return company_funding_rows, None, None, None, None
        return None, None, None, None, None

    return up_to_a, a_to_b, b_to_late, late_to_exit, exit
