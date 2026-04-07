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
    """Return the highest stage bucket a company has reached.

    Returns ``"seed"``, ``"early_vc"``, ``"series_b+"``, or ``None``.
    """
    round_types = set(company_funding_rows["round_type_nzi"].values)

    if round_types & (MIDDLE_STAGE_TYPES | LATE_STAGE_TYPES | EXIT_TYPES):
        return "series_b+"
    if round_types & _EARLY_VC_LEVEL_TYPES:
        return "early_vc"
    if round_types & _SEED_LEVEL_TYPES:
        return "seed"
    return None


def _build_company_lookup(companies_df, company_id_col, company_status_col):
    """Build a dict mapping company_id → company_row dict."""
    lookup = {}
    for _, row in companies_df.iterrows():
        cid = row[company_id_col]
        lookup[cid] = {company_status_col: row[company_status_col]}
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
    """Pre-compute succeed/fail per company per stage and current stage bucket.

    Returns
    -------
    classifications : dict
        ``{company_id: {stage: {"succeeded": bool, "failed": bool}}}``
    current_stages : dict
        ``{company_id: "seed" | "early_vc" | "series_b+" | None}``
    """
    if stages is None:
        stages = NZI_SURVIVAL_STAGES

    company_lookup = _build_company_lookup(
        companies_df, company_id_col, company_status_col,
    )
    grouped = funding_rounds_df.groupby(company_id_col)

    classifications = {}
    current_stages = {}

    for company_id, company_funding_rows in grouped:
        company_row = company_lookup.get(company_id, {company_status_col: None})
        current_stages[company_id] = _classify_company_current_stage(company_funding_rows)

        per_stage = {}
        for stage in stages:
            mapping = STAGE_FAILURE_MAP[stage]
            succeeded = did_company_succeed(
                company_funding_rows,
                company_row,
                graduation_stages=mapping["graduation_stages"],
                late_venture_cutoff=late_venture_cutoff,
                m_and_a_success_stage=m_and_a_success_stage,
            )
            failed = did_company_fail(
                company_funding_rows,
                company_row,
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
    """Calculate failure rate for companies at a given stage.

    Returns ``failures / (failures + successes)``.
    If *debug* is True, returns ``(rate, failed_ids, succeeded_ids)``.
    """
    if at_stage not in STAGE_FAILURE_MAP:
        raise ValueError(f"at_stage must be one of {list(STAGE_FAILURE_MAP)}")

    mapping = STAGE_FAILURE_MAP[at_stage]
    company_lookup = _build_company_lookup(
        companies_df, company_id_col, company_status_col,
    )
    grouped = funding_rounds_df.groupby(company_id_col)

    succeeded_ids = []
    failed_ids = []

    for company_id, company_funding_rows in grouped:
        company_row = company_lookup.get(company_id, {company_status_col: None})

        if did_company_succeed(
            company_funding_rows,
            company_row,
            graduation_stages=mapping["graduation_stages"],
            late_venture_cutoff=late_venture_cutoff,
            m_and_a_success_stage=m_and_a_success_stage,
        ):
            succeeded_ids.append(company_id)
        elif did_company_fail(
            company_funding_rows,
            company_row,
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
    """Count how many companies fall in each stage bucket."""
    n_seed = sum(1 for s in current_stages.values() if s == "seed")
    n_early_vc = sum(1 for s in current_stages.values() if s == "early_vc")
    return n_seed, n_early_vc


def _aggregate_from_precomputed(classifications, company_ids, stages):
    """Aggregate pre-computed classifications for a subset of companies.

    Returns ``{stage: {"n_succeeded": int, "n_failed": int}}``.
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
    """Compute failure rate per stage from aggregated counts."""
    rates = {}
    for stage in stages:
        total = aggregated[stage]["n_succeeded"] + aggregated[stage]["n_failed"]
        rates[stage] = aggregated[stage]["n_failed"] / total if total > 0 else 0.0
    return rates


def _compute_expected_survivals(failure_rates, n_seed, n_early_vc, stages):
    """Chain survival through stages and return results dict."""
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
    """Calculate expected survival rates through the funding pipeline.

    Parameters
    ----------
    funding_rounds_df : DataFrame
        All funding rounds (used to compute failure rates).
    companies_df : DataFrame
        All company details (used to compute failure rates).
    alive_funding_rounds_df : DataFrame
        Funding rounds for living companies (used to count cohort sizes).
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
    """Run permutation rounds and return observed + random survival rates.

    The ``comparison_column`` is a boolean column on *companies_df*.
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
    """Compare survival rates between two groups defined by a boolean column.

    Runs a permutation test to determine whether the observed difference in
    overall expected survival rate is statistically significant.

    Parameters
    ----------
    comparison_column : str
        Boolean column on *companies_df* that splits companies into two groups.
    n_rounds : int
        Number of permutation rounds (default 1000).
    absolute_difference : bool
        If True, compute difference; if False, compute ratio.

    Returns
    -------
    observed_difference : float
    random_differences : list[float]
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
    if plot:
        fig = create_plot_get_metrics(
            differences=random_differences,
            observed_difference=observed_difference,
            title=title,
            annotation_title=annotation_title,
        )
        fig.show()

    return observed_difference, random_differences, observed_true, observed_false
