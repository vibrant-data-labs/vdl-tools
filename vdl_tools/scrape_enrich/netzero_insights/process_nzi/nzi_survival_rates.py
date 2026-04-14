"""Aggregate survival-rate analysis for NZI companies.

Mirrors the Crunchbase pipeline in ``funding_calcations.py`` but adapted for
the NZI data model where funding data lives in a per-round DataFrame and
company details are in a separate DataFrame.

Pipeline layers:
    1. calculate_failure_rate      – per-stage failure rate across a cohort
    2. calculate_expected_survivals – chain Seed → Early VC → Series B survival
    3. compare_survival_rates      – permutation test for a boolean comparison column
"""

import numpy as np
import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_zombie_companies_fail import (
    STAGE_FAILURE_MAP,
    did_company_fail,
    did_company_succeed,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    EARLY_STAGE_TYPES,
    EXIT_TYPES,
    LATE_STAGE_TYPES,
    LATE_VC_CUTOFF,
    M_AND_A_SUCCESS_STAGE,
    MIDDLE_STAGE_TYPES,
    TWO_YEARS_IN_DAYS,
)
from vdl_tools.shared_tools.funding_calcations import create_plot_get_metrics

# Stages evaluated in the survival pipeline (maps to CB's seed → series_a → late_venture)
NZI_SURVIVAL_STAGES = ["Seed", "Early VC", "Series B"]

# Round types that count as being at "seed" level for cohort classification
_SEED_LEVEL_TYPES = {"Accelerator/incubator", "Grant", "Pre-Seed", "Seed"}

# Round types that indicate early VC level (Series A / Early VC)
_EARLY_VC_LEVEL_TYPES = {"Series A", "Early VC"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify_company_current_stage(company_funding_rows):
    """Classify a company into its highest-reached funding stage bucket.

    Examines the ``round_type_nzi`` values in the company's funding rounds
    and returns the highest stage bucket the company has reached, using
    the module-level ``_SEED_LEVEL_TYPES``, ``_EARLY_VC_LEVEL_TYPES``, and
    the stage sets from ``split_early_late_funding_rounds``.

    Parameters
    ----------
    company_funding_rows : pandas.DataFrame
        Funding round rows for a single company. Must contain a
        ``"round_type_nzi"`` column.

    Returns
    -------
    str or None
        ``"series_b+"`` if the company reached Series B or later (including
        late-stage, exit types).
        ``"early_vc"`` if the highest stage is Series A / Early VC.
        ``"seed"`` if the highest stage is seed-level (Accelerator, Grant,
        Pre-Seed, Seed).
        ``None`` if no recognized round types are present.
    """
    round_types = set(company_funding_rows["round_type_nzi"].values)

    if round_types & (MIDDLE_STAGE_TYPES | LATE_STAGE_TYPES | EXIT_TYPES):
        return "series_b+"
    if round_types & _EARLY_VC_LEVEL_TYPES:
        return "early_vc"
    if round_types & _SEED_LEVEL_TYPES:
        return "seed"
    return None


def _build_company_status_lookup(companies_df, company_id_col, company_status_col):
    """Build a lookup dict mapping company ID to its operating status string.

    Parameters
    ----------
    companies_df : pandas.DataFrame
        Company-level DataFrame containing at least ``company_id_col`` and
        ``company_status_col``.
    company_id_col : str
        Column name for the company identifier.
    company_status_col : str
        Column name for the operating status classification
        (e.g. ``"ensemble_operating_status_classification"``).

    Returns
    -------
    dict[str, str]
        Mapping of ``{company_id: status_string}``.
    """
    lookup = {}
    for _, row in companies_df.iterrows():
        cid = row[company_id_col]
        lookup[cid] = row[company_status_col]
    return lookup


def _precompute_company_classifications(
    funding_rounds_df,
    companies_df,
    stages=None,
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    company_id_col="client_id_nzi",
    company_status_col="ensemble_operating_status_classification",
):
    """Pre-compute succeed/fail classification per company per stage.

    Iterates over all companies in ``funding_rounds_df`` (grouped by
    ``company_id_col``), and for each company at each stage calls
    ``did_company_succeed`` and ``did_company_fail`` from
    ``nzi_zombie_companies_fail``.  Also classifies each company's current
    highest-reached stage bucket via ``_classify_company_current_stage``.

    This is the **expensive step** in the survival-rate pipeline — it touches
    every company × every stage.  The result is a pair of dicts that can be
    reused across many permutation rounds or comparison splits without
    re-running the per-company logic.

    Parameters
    ----------
    funding_rounds_df : pandas.DataFrame
        All funding rounds. Must contain ``company_id_col`` and
        ``"round_type_nzi"``.
    companies_df : pandas.DataFrame
        Company-level data. Must contain ``company_id_col`` and
        ``company_status_col``.
    stages : list[str] or None, default None
        Stages to evaluate (defaults to ``NZI_SURVIVAL_STAGES``).
    outlier_time : int, default TWO_YEARS_IN_DAYS
        Days since last funding to classify a company as zombie/stale.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold for early vs. late venture.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which M&A counts as success.
    company_id_col : str, default "client_id_nzi"
        Column name for the company identifier.
    company_status_col : str, default "ensemble_operating_status_classification"
        Column name for the operating status classification.

    Returns
    -------
    classifications : dict[str, dict[str, dict[str, bool]]]
        Nested dict: ``{company_id: {stage: {"succeeded": bool, "failed": bool}}}``.
        A company can be neither succeeded nor failed (still in progress).
    current_stages : dict[str, str | None]
        ``{company_id: "seed" | "early_vc" | "series_b+" | None}``.
    """
    if stages is None:
        stages = NZI_SURVIVAL_STAGES

    company_lookup = _build_company_status_lookup(
        companies_df, company_id_col, company_status_col,
    )
    grouped = funding_rounds_df.groupby(company_id_col)

    classifications = {}
    current_stages = {}

    for company_id, company_funding_rows in grouped:
        company_status = company_lookup.get(company_id, None)
        current_stages[company_id] = _classify_company_current_stage(company_funding_rows)

        per_stage = {}
        for stage in stages:
            mapping = STAGE_FAILURE_MAP[stage]
            succeeded = did_company_succeed(
                company_funding_rows,
                company_status,
                graduation_stages=mapping["graduation_stages"],
                late_venture_cutoff=late_venture_cutoff,
                m_and_a_success_stage=m_and_a_success_stage,
            )
            failed = did_company_fail(
                company_funding_rows,
                company_status,
                at_stage=stage,
                outlier_time=outlier_time,
                late_venture_cutoff=late_venture_cutoff,
                m_and_a_success_stage=m_and_a_success_stage,
            )
            per_stage[stage] = {"succeeded": succeeded, "failed": failed}
        classifications[company_id] = per_stage

    return classifications, current_stages


# ---------------------------------------------------------------------------
# Aggregate functions
# ---------------------------------------------------------------------------

def calculate_failure_rate(
    funding_rounds_df,
    companies_df,
    at_stage="Early VC",
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    company_id_col="client_id_nzi",
    company_status_col="ensemble_operating_status_classification",
    debug=False,
):
    """Calculate the failure rate for companies at a given funding stage.

    Iterates over all companies in ``funding_rounds_df`` (grouped by
    ``company_id_col``) and classifies each as succeeded, failed, or
    in-progress at the given stage using ``did_company_succeed`` /
    ``did_company_fail``.  Returns ``n_failed / (n_failed + n_succeeded)``.

    Note: companies that are still in progress (neither succeeded nor failed)
    are excluded from both numerator and denominator.

    Parameters
    ----------
    funding_rounds_df : pandas.DataFrame
        All funding rounds. Must contain ``company_id_col``,
        ``"round_type_nzi"``, ``"financing_type_nzi"``, and
        ``"round_date_nzi"``.
    companies_df : pandas.DataFrame
        Company-level data. Must contain ``company_id_col`` and
        ``company_status_col``.
    at_stage : str, default "Early VC"
        The stage to evaluate. Must be a key in ``STAGE_FAILURE_MAP``.
    outlier_time : int, default TWO_YEARS_IN_DAYS
        Days since last funding to classify a company as zombie/stale.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold for early vs. late venture.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which M&A counts as success.
    company_id_col : str, default "client_id_nzi"
        Column name for the company identifier.
    company_status_col : str, default "ensemble_operating_status_classification"
        Column name for the operating status classification.
    debug : bool, default False
        If True, return a tuple of ``(rate, failed_ids, succeeded_ids)``
        instead of just the rate.

    Returns
    -------
    float or tuple[float, list, list]
        The failure rate as ``n_failed / (n_failed + n_succeeded)``, or
        0.0 if no companies are classifiable.  If ``debug=True``, returns
        ``(rate, failed_ids, succeeded_ids)``.
    """
    if at_stage not in STAGE_FAILURE_MAP:
        raise ValueError(f"at_stage must be one of {list(STAGE_FAILURE_MAP)}")

    mapping = STAGE_FAILURE_MAP[at_stage]
    company_lookup = _build_company_status_lookup(
        companies_df, company_id_col, company_status_col,
    )
    grouped = funding_rounds_df.groupby(company_id_col)

    succeeded_ids = []
    failed_ids = []

    for company_id, company_funding_rows in grouped:
        company_status = company_lookup.get(company_id, None)

        if did_company_succeed(
            company_funding_rows,
            company_status,
            graduation_stages=mapping["graduation_stages"],
            late_venture_cutoff=late_venture_cutoff,
            m_and_a_success_stage=m_and_a_success_stage,
        ):
            succeeded_ids.append(company_id)
        elif did_company_fail(
            company_funding_rows,
            company_status,
            at_stage=at_stage,
            outlier_time=outlier_time,
            late_venture_cutoff=late_venture_cutoff,
            m_and_a_success_stage=m_and_a_success_stage,
        ):
            failed_ids.append(company_id)

    total = len(failed_ids) + len(succeeded_ids)
    rate = len(failed_ids) / total if total > 0 else 0.0

    if debug:
        return rate, failed_ids, succeeded_ids
    return rate


def _count_cohorts(current_stages):
    """Count the number of seed-stage and early-VC-stage companies.

    Parameters
    ----------
    current_stages : dict[str, str | None]
        Mapping of ``{company_id: stage_bucket}`` as produced by
        ``_precompute_company_classifications``.

    Returns
    -------
    n_seed : int
        Number of companies whose highest stage is ``"seed"``.
    n_early_vc : int
        Number of companies whose highest stage is ``"early_vc"``.
    """
    n_seed = sum(1 for s in current_stages.values() if s == "seed")
    n_early_vc = sum(1 for s in current_stages.values() if s == "early_vc")
    return n_seed, n_early_vc


def _aggregate_from_precomputed(classifications, company_ids, stages):
    """Aggregate pre-computed succeed/fail counts for a subset of companies.

    Takes the full ``classifications`` dict (from
    ``_precompute_company_classifications``) and sums up succeeded/failed
    counts for only the companies in ``company_ids``.

    Parameters
    ----------
    classifications : dict[str, dict[str, dict[str, bool]]]
        Full precomputed classifications as returned by
        ``_precompute_company_classifications``.
    company_ids : list[str]
        Subset of company IDs to aggregate over.
    stages : list[str]
        Stages to aggregate (e.g. ``NZI_SURVIVAL_STAGES``).

    Returns
    -------
    dict[str, dict[str, int]]
        ``{stage: {"n_succeeded": int, "n_failed": int}}``.
        Companies that are neither succeeded nor failed are not counted.
    """
    result = {stage: {"n_succeeded": 0, "n_failed": 0} for stage in stages}
    for cid in company_ids:
        if cid not in classifications:
            continue
        for stage in stages:
            cls = classifications[cid][stage]
            if cls["succeeded"]:
                result[stage]["n_succeeded"] += 1
            elif cls["failed"]:
                result[stage]["n_failed"] += 1
    return result


def _failure_rates_from_aggregated(aggregated, stages):
    """Compute failure rate per stage from aggregated succeed/fail counts.

    Parameters
    ----------
    aggregated : dict[str, dict[str, int]]
        Per-stage counts as returned by ``_aggregate_from_precomputed``:
        ``{stage: {"n_succeeded": int, "n_failed": int}}``.
    stages : list[str]
        Stages to compute rates for.

    Returns
    -------
    dict[str, float]
        ``{stage: failure_rate}`` where failure_rate is
        ``n_failed / (n_failed + n_succeeded)``, or 0.0 if no companies
        are classifiable at that stage.
    """
    rates = {}
    for stage in stages:
        total = aggregated[stage]["n_succeeded"] + aggregated[stage]["n_failed"]
        rates[stage] = aggregated[stage]["n_failed"] / total if total > 0 else 0.0
    return rates


def _compute_expected_survivals(failure_rates, n_seed, n_early_vc, stages):
    """Chain per-stage survival rates into an overall expected survival rate.

    Models the funding pipeline as a sequential funnel:
    Seed → Early VC → Series B.  At each stage, a fraction of companies
    survive (1 - failure_rate).  The "expected survived" count at each stage
    feeds into the next stage's input.

    The funnel logic:
      - ``expected_survived_seed = (1 - seed_failure_rate) * n_seed``
      - ``expected_survived_early_vc = (1 - early_vc_failure_rate) *
        (n_early_vc + expected_survived_seed)``
      - ``expected_survived_series_b = (1 - series_b_failure_rate) *
        expected_survived_early_vc``
      - ``overall_rate = expected_survived_series_b / (n_seed + n_early_vc)``

    Parameters
    ----------
    failure_rates : dict[str, float]
        Per-stage failure rates, keyed by stage name.
    n_seed : int
        Number of companies currently at the seed stage.
    n_early_vc : int
        Number of companies currently at the early VC stage.
    stages : list[str]
        Ordered list of 3 stages (e.g. ``["Seed", "Early VC", "Series B"]``).

    Returns
    -------
    dict[str, float | int]
        Keys include:
        - ``"n_seed"``, ``"n_early_vc"``, ``"n_total"``: cohort sizes
        - ``"expected_survived_seed"``, ``"expected_survived_early_vc"``,
          ``"expected_survived_series_b"``: expected survivors at each stage
        - ``"overall_expected_survival_rate"``: end-to-end survival rate
        - ``"seed_failure_rate"``, ``"early_vc_failure_rate"``,
          ``"series_b_failure_rate"``: input failure rates echoed back
    """
    seed_survival = 1 - failure_rates[stages[0]]
    early_vc_survival = 1 - failure_rates[stages[1]]
    series_b_survival = 1 - failure_rates[stages[2]]

    expected_survived_seed = seed_survival * n_seed
    expected_survived_early_vc = early_vc_survival * (n_early_vc + expected_survived_seed)
    expected_survived_series_b = series_b_survival * expected_survived_early_vc

    n_total = n_seed + n_early_vc
    overall = expected_survived_series_b / n_total if n_total > 0 else 0.0

    return {
        "n_seed": int(n_seed),
        "n_early_vc": int(n_early_vc),
        "n_total": int(n_total),
        "expected_survived_seed": expected_survived_seed,
        "expected_survived_early_vc": expected_survived_early_vc,
        "expected_survived_series_b": expected_survived_series_b,
        "overall_expected_survival_rate": overall,
        "seed_failure_rate": failure_rates[stages[0]],
        "early_vc_failure_rate": failure_rates[stages[1]],
        "series_b_failure_rate": failure_rates[stages[2]],
    }


def calculate_expected_survivals(
    funding_rounds_df,
    companies_df,
    alive_funding_rounds_df,
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    company_id_col="client_id_nzi",
    company_status_col="ensemble_operating_status_classification",
    stages=None,
):
    """Calculate expected survival rates through the full funding pipeline.

    Computes per-stage failure rates from the **full** dataset, then counts
    cohort sizes (how many companies are at seed vs early VC) from the
    **alive-only** dataset, and chains them through the survival funnel via
    ``_compute_expected_survivals``.

    This is the high-level entry point for a single-group survival
    calculation (no comparison / permutation test).

    Parameters
    ----------
    funding_rounds_df : pandas.DataFrame
        All funding rounds (used to compute failure rates across the full
        population, including failed companies).
    companies_df : pandas.DataFrame
        All company details (used to compute failure rates).
    alive_funding_rounds_df : pandas.DataFrame
        Funding rounds for **living companies only** (used to count how many
        companies are currently at each stage — these are the cohort sizes
        that enter the survival funnel).
    outlier_time : int, default TWO_YEARS_IN_DAYS
        Days since last funding to classify a company as zombie/stale.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold for early vs. late venture.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which M&A counts as success.
    company_id_col : str, default "client_id_nzi"
        Column name for the company identifier.
    company_status_col : str, default "ensemble_operating_status_classification"
        Column name for the operating status classification.
    stages : list[str] or None, default None
        Stages to evaluate (defaults to ``NZI_SURVIVAL_STAGES``).

    Returns
    -------
    dict[str, float | int]
        Survival results dict as returned by ``_compute_expected_survivals``.
    """
    if stages is None:
        stages = NZI_SURVIVAL_STAGES

    # Failure rates from the full dataset
    failure_rates = {}
    for stage in stages:
        failure_rates[stage] = calculate_failure_rate(
            funding_rounds_df,
            companies_df,
            at_stage=stage,
            outlier_time=outlier_time,
            late_venture_cutoff=late_venture_cutoff,
            m_and_a_success_stage=m_and_a_success_stage,
            company_id_col=company_id_col,
            company_status_col=company_status_col,
        )

    # Cohort sizes from alive companies
    alive_grouped = alive_funding_rounds_df.groupby(company_id_col)
    alive_stages = {
        cid: _classify_company_current_stage(rows)
        for cid, rows in alive_grouped
    }
    n_seed, n_early_vc = _count_cohorts(alive_stages)

    return _compute_expected_survivals(failure_rates, n_seed, n_early_vc, stages)


# ---------------------------------------------------------------------------
# Permutation test
# ---------------------------------------------------------------------------

def run_compare_survival_rates_rounds(
    funding_rounds_df,
    companies_df,
    comparison_column,
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    company_id_col="client_id_nzi",
    company_status_col="ensemble_operating_status_classification",
    stages=None,
    n_rounds=1000,
):
    """Run a permutation test comparing survival rates for two groups.

    Splits companies by a boolean column, computes observed survival rates
    for each group, then runs ``n_rounds`` random permutations of the
    boolean labels and recomputes survival rates each time.  This produces
    a null distribution of survival-rate differences (or ratios) against
    which the observed difference can be compared.

    Internally calls ``_precompute_company_classifications`` **once** to
    avoid redundant per-company succeed/fail logic, then uses
    ``_aggregate_from_precomputed`` for fast aggregation in each round.

    Parameters
    ----------
    funding_rounds_df : pandas.DataFrame
        All funding rounds.
    companies_df : pandas.DataFrame
        Company-level data. Must contain ``comparison_column`` as a boolean
        column, plus ``company_id_col`` and ``company_status_col``.
    comparison_column : str
        Name of a boolean column on ``companies_df`` that defines the
        True/False group split.
    outlier_time : int, default TWO_YEARS_IN_DAYS
        Days since last funding to classify a company as zombie/stale.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold for early vs. late venture.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which M&A counts as success.
    company_id_col : str, default "client_id_nzi"
        Column name for the company identifier.
    company_status_col : str, default "ensemble_operating_status_classification"
        Column name for the operating status classification.
    stages : list[str] or None, default None
        Stages to evaluate (defaults to ``NZI_SURVIVAL_STAGES``).
    n_rounds : int, default 1000
        Number of random permutation rounds.

    Returns
    -------
    observed_true : dict
        Survival results for the True group (see ``_compute_expected_survivals``).
    observed_false : dict
        Survival results for the False group.
    random_results : list[tuple[dict, dict]]
        List of ``(true_result, false_result)`` dicts from each permutation
        round, forming the null distribution.
    """
    if stages is None:
        stages = NZI_SURVIVAL_STAGES

    # Pre-compute all classifications once
    classifications, current_stages = _precompute_company_classifications(
        funding_rounds_df,
        companies_df,
        stages=stages,
        outlier_time=outlier_time,
        late_venture_cutoff=late_venture_cutoff,
        m_and_a_success_stage=m_and_a_success_stage,
        company_id_col=company_id_col,
        company_status_col=company_status_col,
    )

    company_ids = companies_df[company_id_col].values
    comparison_values = companies_df[comparison_column].values

    def _survival_for_split(mask):
        true_ids = [cid for cid, m in zip(company_ids, mask) if m]
        false_ids = [cid for cid, m in zip(company_ids, mask) if not m]

        true_agg = _aggregate_from_precomputed(classifications, true_ids, stages)
        false_agg = _aggregate_from_precomputed(classifications, false_ids, stages)

        true_rates = _failure_rates_from_aggregated(true_agg, stages)
        false_rates = _failure_rates_from_aggregated(false_agg, stages)

        n_seed_all, n_early_vc_all = _count_cohorts(current_stages)

        true_result = _compute_expected_survivals(true_rates, n_seed_all, n_early_vc_all, stages)
        false_result = _compute_expected_survivals(false_rates, n_seed_all, n_early_vc_all, stages)
        return true_result, false_result

    # Observed split
    observed_true, observed_false = _survival_for_split(comparison_values)

    # Permutation rounds
    random_results = []
    for i in range(n_rounds):
        shuffled = np.random.permutation(comparison_values)
        true_result, false_result = _survival_for_split(shuffled)
        random_results.append((true_result, false_result))
        if i % 100 == 0 and i > 0:
            print(f"Round {i} complete")

    return observed_true, observed_false, random_results


def compare_survival_rates(
    funding_rounds_df,
    companies_df,
    comparison_column,
    outlier_time=TWO_YEARS_IN_DAYS,
    late_venture_cutoff=LATE_VC_CUTOFF,
    m_and_a_success_stage=M_AND_A_SUCCESS_STAGE,
    company_id_col="client_id_nzi",
    company_status_col="ensemble_operating_status_classification",
    stages=None,
    n_rounds=1000,
    plot=False,
    title=None,
    annotation_title=None,
    absolute_difference=True,
):
    """Compare survival rates between two groups via a permutation test.

    High-level wrapper around ``run_compare_survival_rates_rounds``.
    Computes the observed difference (or ratio) in overall expected survival
    rate between the True and False groups, builds a null distribution from
    ``n_rounds`` permutations, and optionally plots the result.

    Parameters
    ----------
    funding_rounds_df : pandas.DataFrame
        All funding rounds.
    companies_df : pandas.DataFrame
        Company-level data. Must contain ``comparison_column`` as a boolean.
    comparison_column : str
        Boolean column on ``companies_df`` that splits companies into two
        groups.
    outlier_time : int, default TWO_YEARS_IN_DAYS
        Days since last funding to classify a company as zombie/stale.
    late_venture_cutoff : str, default LATE_VC_CUTOFF
        Stage threshold for early vs. late venture.
    m_and_a_success_stage : str, default M_AND_A_SUCCESS_STAGE
        Earliest stage at which M&A counts as success.
    company_id_col : str, default "client_id_nzi"
        Column name for the company identifier.
    company_status_col : str, default "ensemble_operating_status_classification"
        Column name for the operating status classification.
    stages : list[str] or None, default None
        Stages to evaluate (defaults to ``NZI_SURVIVAL_STAGES``).
    n_rounds : int, default 1000
        Number of random permutation rounds.
    plot : bool, default False
        If True, display a histogram of the null distribution with the
        observed value marked.
    title : str or None, default None
        Plot title. If None, auto-generated from ``comparison_column``.
    annotation_title : str or None, default None
        Annotation text on the plot. If None, shows the observed difference.
    absolute_difference : bool, default True
        If True, compute ``true_rate - false_rate`` (absolute difference).
        If False, compute ``true_rate / false_rate`` (ratio).

    Returns
    -------
    observed_difference : float
        The observed difference (or ratio) in overall expected survival rate
        between the True and False groups.
    random_differences : list[float]
        Null distribution: one difference (or ratio) per permutation round.
    observed_true : dict
        Survival results for the True group (see
        ``_compute_expected_survivals``).
    observed_false : dict
        Survival results for the False group.
    """
    observed_true, observed_false, random_results = run_compare_survival_rates_rounds(
        funding_rounds_df,
        companies_df,
        comparison_column,
        outlier_time=outlier_time,
        late_venture_cutoff=late_venture_cutoff,
        m_and_a_success_stage=m_and_a_success_stage,
        company_id_col=company_id_col,
        company_status_col=company_status_col,
        stages=stages,
        n_rounds=n_rounds,
    )

    if absolute_difference:
        observed_difference = (
            observed_true["overall_expected_survival_rate"]
            - observed_false["overall_expected_survival_rate"]
        )
        random_differences = [
            t["overall_expected_survival_rate"] - f["overall_expected_survival_rate"]
            for t, f in random_results
        ]
    else:
        observed_difference = (
            observed_true["overall_expected_survival_rate"]
            / observed_false["overall_expected_survival_rate"]
            if observed_false["overall_expected_survival_rate"] != 0
            else float("inf")
        )
        random_differences = [
            t["overall_expected_survival_rate"] / f["overall_expected_survival_rate"]
            if f["overall_expected_survival_rate"] != 0
            else float("inf")
            for t, f in random_results
        ]

    title = title or f"Survival Rates vs Random {comparison_column}"
    if annotation_title is None:
        annotation_title = f"Observed Difference: {observed_difference:.4f}"
    else:
        annotation_title = f"{annotation_title} ({observed_difference:.4f})"

    if plot:
        fig = create_plot_get_metrics(
            differences=random_differences,
            observed_difference=observed_difference,
            title=title,
            annotation_title=annotation_title,
        )
        fig.show()

    return observed_difference, random_differences, observed_true, observed_false
