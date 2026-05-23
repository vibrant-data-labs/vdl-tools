"""Tests for the NZI survival rate pipeline."""

import datetime as dt
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_survival_rates import (
    _classify_company_current_stage,
    _precompute_company_classifications,
    _aggregate_from_precomputed,
    _failure_rates_from_aggregated,
    _compute_expected_survivals,
    _count_cohorts,
    calculate_failure_rate,
    calculate_expected_survivals,
    compare_survival_rates,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.stage_constants import (
    NZI_SURVIVAL_STAGES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_rows(rounds, client_id=None):
    df = pd.DataFrame(rounds)
    if client_id is not None:
        df["client_id_nzi"] = client_id
    return df


FIXED_NOW = dt.datetime(2026, 4, 6)

DATETIME_PATCH_TARGET = (
    "vdl_tools.scrape_enrich.netzero_insights.process_nzi."
    "nzi_zombie_companies_fail.dt.datetime"
)


# ---------------------------------------------------------------------------
# Per-company funding fixtures (with client_id_nzi)
# ---------------------------------------------------------------------------

# Company 1: Seed-only, stale → should FAIL at Seed and Early VC
COMPANY_1_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-06-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
], client_id=1)

# Company 2: Seed → Series A (recent) → should SUCCEED at Seed, not yet fail at Early VC
COMPANY_2_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2024-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2025-10-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
], client_id=2)

# Company 3: Seed → Series A → Series B → should SUCCEED at Seed, Early VC, and be at Series B
COMPANY_3_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
], client_id=3)

# Company 4: Seed-only, shut down → should FAIL at Seed
COMPANY_4_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2021-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
], client_id=4)

# Company 5: Grant-only, stale → should FAIL at Seed
COMPANY_5_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
], client_id=5)

# Company 6: Accelerator → Seed → Series A (stale) → should FAIL at Early VC
COMPANY_6_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
], client_id=6)

# Company 7: Debt only → not venture, should not count
COMPANY_7_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
], client_id=7)

# Company 8: Seed → Series A → Series B → Series C → should SUCCEED at all stages
COMPANY_8_ROUNDS = make_rows([
    {"round_date_nzi": pd.Timestamp("2016-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2017-01-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
], client_id=8)

# Combined funding rounds
ALL_FUNDING_ROUNDS = pd.concat([
    COMPANY_1_ROUNDS,
    COMPANY_2_ROUNDS,
    COMPANY_3_ROUNDS,
    COMPANY_4_ROUNDS,
    COMPANY_5_ROUNDS,
    COMPANY_6_ROUNDS,
    COMPANY_7_ROUNDS,
    COMPANY_8_ROUNDS,
], ignore_index=True)

# Company details
ALL_COMPANIES = pd.DataFrame([
    {"client_id_nzi": 1, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 2, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 3, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 4, "ensemble_operating_status_classification": "Shut Down"},
    {"client_id_nzi": 5, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 6, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 7, "ensemble_operating_status_classification": "Operating"},
    {"client_id_nzi": 8, "ensemble_operating_status_classification": "Operating"},
])


# ===========================================================================
# _classify_company_current_stage tests
# ===========================================================================

class TestClassifyCompanyCurrentStage:

    def test_seed_only(self):
        rows = make_rows([
            {"round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "seed"

    def test_grant_only(self):
        rows = make_rows([
            {"round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
        ])
        assert _classify_company_current_stage(rows) == "seed"

    def test_accelerator_only(self):
        rows = make_rows([
            {"round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
        ])
        assert _classify_company_current_stage(rows) == "seed"

    def test_series_a(self):
        rows = make_rows([
            {"round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
            {"round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "early_vc"

    def test_early_vc_label(self):
        rows = make_rows([
            {"round_type_nzi": "Early VC", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "early_vc"

    def test_series_b(self):
        rows = make_rows([
            {"round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
            {"round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "series_b+"

    def test_late_vc(self):
        rows = make_rows([
            {"round_type_nzi": "Late VC", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "series_b+"

    def test_series_c(self):
        rows = make_rows([
            {"round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "series_b+"

    def test_ipo(self):
        rows = make_rows([
            {"round_type_nzi": "IPO", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "series_b+"

    def test_debt_only_returns_none(self):
        rows = make_rows([
            {"round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
        ])
        assert _classify_company_current_stage(rows) is None

    def test_pre_seed(self):
        rows = make_rows([
            {"round_type_nzi": "Pre-Seed", "financing_type_nzi": "Equity"},
        ])
        assert _classify_company_current_stage(rows) == "seed"


# ===========================================================================
# calculate_failure_rate tests
# ===========================================================================

class TestCalculateFailureRate:
    """Test failure rate computation with known company outcomes."""

    @patch(DATETIME_PATCH_TARGET)
    def test_seed_failure_rate(self, mock_dt):
        """Companies 1,4,5 fail at Seed; 2,3,8 succeed (raised Series A+); 6 succeeds; 7 not venture."""
        mock_dt.now.return_value = FIXED_NOW
        rate = calculate_failure_rate(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Seed",
        )
        # Succeed at Seed: companies that raised Series A or Early VC
        #   Company 2 (Seed→Series A) → succeed
        #   Company 3 (Seed→A→B) → succeed
        #   Company 6 (Accel→Seed→A) → succeed
        #   Company 8 (Seed→A→B→C) → succeed
        # Fail at Seed: raised seed-level but didn't graduate AND (stale or shut down)
        #   Company 1 (Seed+Grant, stale) → fail
        #   Company 4 (Seed, shut down) → fail
        #   Company 5 (Grant, stale) → fail
        # Company 7 (debt only) → not venture, excluded
        assert rate == pytest.approx(3 / 7)

    @patch(DATETIME_PATCH_TARGET)
    def test_early_vc_failure_rate(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        rate = calculate_failure_rate(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Early VC",
        )
        # Succeed at Early VC: raised Series B
        #   Company 3 (→B) → succeed
        #   Company 8 (→B→C) → succeed
        # Fail at Early VC: reached early VC, didn't graduate, stale/shut down
        #   Company 1 (Seed+Grant, stale) → fail
        #   Company 4 (Seed, shut down) → fail
        #   Company 5 (Grant, stale) → fail
        #   Company 6 (Accel→Seed→A, stale) → fail
        # Company 2 (Series A, recent) → neither succeed nor fail yet
        # Company 7 → not venture
        assert rate == pytest.approx(4 / 6)

    @patch(DATETIME_PATCH_TARGET)
    def test_series_b_failure_rate(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        rate = calculate_failure_rate(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Series B",
        )
        # Succeed at Series B: raised Series C
        #   Company 8 (→C) → succeed
        # Fail at Series B: reached Series B, stale
        #   Company 3 (Series B, last round 2020) → fail (stale)
        # Others don't reach Series B
        assert rate == pytest.approx(1 / 2)

    @patch(DATETIME_PATCH_TARGET)
    def test_debug_returns_ids(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        rate, failed_ids, succeeded_ids = calculate_failure_rate(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Seed", debug=True,
        )
        assert set(failed_ids) == {1, 4, 5}
        assert set(succeeded_ids) == {2, 3, 6, 8}

    def test_invalid_stage_raises(self):
        with pytest.raises(ValueError, match="at_stage must be one of"):
            calculate_failure_rate(
                ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Series Z",
            )


# ===========================================================================
# calculate_expected_survivals tests
# ===========================================================================

class TestCalculateExpectedSurvivals:

    @patch(DATETIME_PATCH_TARGET)
    def test_basic_survivals(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        # Use all companies as "alive" for simplicity
        result = calculate_expected_survivals(
            ALL_FUNDING_ROUNDS,
            ALL_COMPANIES,
            alive_funding_rounds_df=ALL_FUNDING_ROUNDS,
        )
        assert "overall_expected_survival_rate" in result
        assert "seed_failure_rate" in result
        assert "early_vc_failure_rate" in result
        assert "series_b_failure_rate" in result
        assert result["n_seed"] >= 0
        assert result["n_early_vc"] >= 0
        assert result["n_total"] == result["n_seed"] + result["n_early_vc"]

    @patch(DATETIME_PATCH_TARGET)
    def test_cohort_counts(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        result = calculate_expected_survivals(
            ALL_FUNDING_ROUNDS,
            ALL_COMPANIES,
            alive_funding_rounds_df=ALL_FUNDING_ROUNDS,
        )
        # Classify alive companies:
        #   1: Seed+Grant → seed
        #   2: Seed+Series A → early_vc
        #   3: Seed+A+B → series_b+ (excluded from cohort count)
        #   4: Seed → seed
        #   5: Grant → seed
        #   6: Accel+Seed+A → early_vc
        #   7: Debt → None (excluded)
        #   8: Seed+A+B+C → series_b+ (excluded)
        assert result["n_seed"] == 3  # companies 1, 4, 5
        assert result["n_early_vc"] == 2  # companies 2, 6

    @patch(DATETIME_PATCH_TARGET)
    def test_survival_chaining(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        result = calculate_expected_survivals(
            ALL_FUNDING_ROUNDS,
            ALL_COMPANIES,
            alive_funding_rounds_df=ALL_FUNDING_ROUNDS,
        )
        # Verify chaining math
        seed_survival = 1 - result["seed_failure_rate"]
        early_vc_survival = 1 - result["early_vc_failure_rate"]
        series_b_survival = 1 - result["series_b_failure_rate"]

        expected_seed = seed_survival * result["n_seed"]
        expected_early_vc = early_vc_survival * (result["n_early_vc"] + expected_seed)
        expected_series_b = series_b_survival * expected_early_vc

        assert result["expected_survived_seed"] == pytest.approx(expected_seed)
        assert result["expected_survived_early_vc"] == pytest.approx(expected_early_vc)
        assert result["expected_survived_series_b"] == pytest.approx(expected_series_b)
        assert result["overall_expected_survival_rate"] == pytest.approx(
            expected_series_b / result["n_total"]
        )


# ===========================================================================
# compare_survival_rates tests
# ===========================================================================

class TestCompareSurvivalRates:

    @patch(DATETIME_PATCH_TARGET)
    def test_output_shape(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        companies_with_flag = ALL_COMPANIES.copy()
        companies_with_flag["is_treated"] = [True, False, True, False, True, False, True, False]

        observed_diff, random_diffs, obs_true, obs_false = compare_survival_rates(
            ALL_FUNDING_ROUNDS,
            companies_with_flag,
            comparison_column="is_treated",
            n_rounds=5,
        )
        assert isinstance(observed_diff, float)
        assert len(random_diffs) == 5
        assert all(isinstance(d, float) for d in random_diffs)
        assert "overall_expected_survival_rate" in obs_true
        assert "overall_expected_survival_rate" in obs_false

    @patch(DATETIME_PATCH_TARGET)
    def test_ratio_mode(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        companies_with_flag = ALL_COMPANIES.copy()
        companies_with_flag["is_treated"] = [True, False, True, False, True, False, True, False]

        observed_diff, random_diffs, _, _ = compare_survival_rates(
            ALL_FUNDING_ROUNDS,
            companies_with_flag,
            comparison_column="is_treated",
            n_rounds=3,
            absolute_difference=False,
        )
        assert isinstance(observed_diff, float)
        assert len(random_diffs) == 3

    @patch(DATETIME_PATCH_TARGET)
    def test_deterministic_with_seed(self, mock_dt):
        """With a fixed random seed, results should be reproducible."""
        mock_dt.now.return_value = FIXED_NOW
        companies_with_flag = ALL_COMPANIES.copy()
        companies_with_flag["is_treated"] = [True, True, True, True, False, False, False, False]

        np.random.seed(42)
        obs1, rand1, _, _ = compare_survival_rates(
            ALL_FUNDING_ROUNDS,
            companies_with_flag,
            comparison_column="is_treated",
            n_rounds=5,
        )

        np.random.seed(42)
        obs2, rand2, _, _ = compare_survival_rates(
            ALL_FUNDING_ROUNDS,
            companies_with_flag,
            comparison_column="is_treated",
            n_rounds=5,
        )

        assert obs1 == pytest.approx(obs2)
        assert rand1 == pytest.approx(rand2)


# ===========================================================================
# _precompute_company_classifications tests
# ===========================================================================

class TestPrecomputeClassifications:

    @patch(DATETIME_PATCH_TARGET)
    def test_returns_all_companies(self, mock_dt):
        mock_dt.now.return_value = FIXED_NOW
        classifications, current_stages = _precompute_company_classifications(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES,
        )
        funded_ids = set(ALL_FUNDING_ROUNDS["client_id_nzi"].unique())
        assert set(classifications.keys()) == funded_ids
        assert set(current_stages.keys()) == funded_ids

    @patch(DATETIME_PATCH_TARGET)
    def test_classifications_match_direct_calls(self, mock_dt):
        """Pre-computed results should match calling calculate_failure_rate directly."""
        mock_dt.now.return_value = FIXED_NOW
        classifications, _ = _precompute_company_classifications(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES,
        )

        rate_direct, failed_ids, succeeded_ids = calculate_failure_rate(
            ALL_FUNDING_ROUNDS, ALL_COMPANIES, at_stage="Seed", debug=True,
        )

        # Count from pre-computed
        n_failed = sum(
            1 for cid, stages in classifications.items()
            if stages["Seed"]["failed"]
        )
        n_succeeded = sum(
            1 for cid, stages in classifications.items()
            if stages["Seed"]["succeeded"]
        )

        assert n_failed == len(failed_ids)
        assert n_succeeded == len(succeeded_ids)
