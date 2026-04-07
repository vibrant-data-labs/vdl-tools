from unittest.mock import patch
import datetime as dt

import pandas as pd
import pytest

from vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_zombie_companies_fail import (
    did_company_fail,
    did_company_succeed,
    raised_stage_or_earlier,
    time_since_last_funding,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    raised_equity_round,
)


def make_rows(rounds):
    return pd.DataFrame(rounds)


def make_company_row(ensemble_operating_status_classification):
    """Build a minimal company details dict with the ensemble status field."""
    return {"ensemble_operating_status_classification": ensemble_operating_status_classification}


# ---------------------------------------------------------------------------
# Real-data fixtures (based on actual companies in the dataset)
# ---------------------------------------------------------------------------

# Bowery Farming (id=62): Shut Down, reached Series D
BOWERY_FARMING = make_rows([
    {"round_date_nzi": pd.Timestamp("2017-02-22"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2017-06-13"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2019-09-28"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2019-11-11"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-03-24"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
    {"round_date_nzi": pd.Timestamp("2021-05-24"), "round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2021-07-31"), "round_type_nzi": "Growth equity", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2022-01-11"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
    {"round_date_nzi": pd.Timestamp("2022-02-24"), "round_type_nzi": "Late VC", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2023-09-30"), "round_type_nzi": "Series D", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2023-11-08"), "round_type_nzi": "Late VC", "financing_type_nzi": "Equity"},
])

# INFINIUM (id=16651): Shut Down, Series C, heavy grants
INFINIUM = make_rows([
    {"round_date_nzi": pd.Timestamp("2009-06-30"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2009-12-31"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2010-02-18"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2010-09-14"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2012-06-27"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2012-09-10"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2012-09-11"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
    {"round_date_nzi": pd.Timestamp("2012-11-04"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2013-04-15"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2014-07-14"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2015-10-12"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2016-12-14"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2019-09-26"), "round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-07-29"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
])

# AppHarvest (id=50): Restructured with SPAC exit
APPHARVEST = make_rows([
    {"round_date_nzi": pd.Timestamp("2018-02-13"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2019-05-20"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-31"), "round_type_nzi": "Early VC", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-31"), "round_type_nzi": "Early VC", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-09-15"), "round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2021-01-31"), "round_type_nzi": "PIPE", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2021-01-31"), "round_type_nzi": "SPAC", "financing_type_nzi": "Other"},
])

# Mantle (id=163844): Acquired, no M&A round type, reached Series C
MANTLE = make_rows([
    {"round_date_nzi": pd.Timestamp("2016-12-30"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2021-09-20"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2024-07-09"), "round_type_nzi": "Series C", "financing_type_nzi": "Equity"},
])

# Spero Foods (id=498): Acquired, no M&A round, Early VC only
SPERO_FOODS = make_rows([
    {"round_date_nzi": pd.Timestamp("2018-08-20"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
    {"round_date_nzi": pd.Timestamp("2020-06-15"), "round_type_nzi": "Early VC", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2022-03-24"), "round_type_nzi": "Early VC", "financing_type_nzi": "Equity"},
])


# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------

DEBT_ONLY = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
    {"round_date_nzi": pd.Timestamp("2021-01-01"), "round_type_nzi": "Debt", "financing_type_nzi": "Debt"},
])

SEED_ONLY_STALE = make_rows([
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-06-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
])

RECENT_SERIES_A = make_rows([
    {"round_date_nzi": pd.Timestamp("2024-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2025-10-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
])

SEED_PLUS_ACQUISITION_EARLY = make_rows([
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Acquisition", "financing_type_nzi": "Other"},
])

SERIES_A_PLUS_ACQUISITION = make_rows([
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Acquisition", "financing_type_nzi": "Other"},
])

IPO_COMPANY = make_rows([
    {"round_date_nzi": pd.Timestamp("2015-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2016-01-01"), "round_type_nzi": "Series A", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "IPO", "financing_type_nzi": "Equity"},
])

STRAIGHT_TO_IPO = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "IPO", "financing_type_nzi": "Equity"},
])

GRANT_ONLY = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2021-01-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
])

GRANT_ONLY_RECENT = make_rows([
    {"round_date_nzi": pd.Timestamp("2025-06-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
])

ACCELERATOR_ONLY_STALE = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
])

ACCELERATOR_PLUS_GRANT_STALE = make_rows([
    {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
    {"round_date_nzi": pd.Timestamp("2021-08-01"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
    {"round_date_nzi": pd.Timestamp("2022-01-18"), "round_type_nzi": "Grant", "financing_type_nzi": "Grant"},
])

ACCELERATOR_TO_SERIES_B = make_rows([
    {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Accelerator/incubator", "financing_type_nzi": "Other"},
    {"round_date_nzi": pd.Timestamp("2019-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
    {"round_date_nzi": pd.Timestamp("2021-01-01"), "round_type_nzi": "Series B", "financing_type_nzi": "Equity"},
])


# ---------------------------------------------------------------------------
# Company row fixtures (mimic company details records)
# ---------------------------------------------------------------------------

COMPANY_SHUT_DOWN = make_company_row("Shut Down")
COMPANY_ACQUIRED = make_company_row("Acquired / Merger")
COMPANY_RESTRUCTURED = make_company_row("Restructured")
COMPANY_OPERATING = make_company_row("Operating")


# ===========================================================================
# raised_equity_round tests
# ===========================================================================

class TestRaisedEquityRound:
    def test_equity_returns_true(self):
        assert raised_equity_round(RECENT_SERIES_A) is True

    def test_grant_returns_true(self):
        assert raised_equity_round(GRANT_ONLY) is True

    def test_accelerator_returns_true(self):
        assert raised_equity_round(ACCELERATOR_ONLY_STALE) is True

    def test_debt_only_returns_false(self):
        assert raised_equity_round(DEBT_ONLY) is False


# ===========================================================================
# raised_stage_or_earlier tests
# ===========================================================================

class TestRaisedStageOrEarlier:
    def test_exact_stage_match(self):
        assert raised_stage_or_earlier(RECENT_SERIES_A, stages=["Series A", "Early VC"]) is True

    def test_earlier_stage_present(self):
        # Seed is earlier than Series A
        assert raised_stage_or_earlier(SEED_ONLY_STALE, stages=["Series A"]) is True

    def test_only_later_stages(self):
        # Company with only Series D — asking if they raised Series A or earlier: no
        rows = make_rows([
            {"round_date_nzi": pd.Timestamp("2020-01-01"), "round_type_nzi": "Series D", "financing_type_nzi": "Equity"},
        ])
        assert raised_stage_or_earlier(rows, stages=["Series A"]) is False

    def test_grant_matches_early_vc(self):
        assert raised_stage_or_earlier(GRANT_ONLY, stages=["Early VC"]) is True

    def test_accelerator_matches_early_vc(self):
        assert raised_stage_or_earlier(ACCELERATOR_ONLY_STALE, stages=["Early VC"]) is True

    def test_no_disclosed_stages(self):
        assert raised_stage_or_earlier(DEBT_ONLY, stages=["Series A"]) is False


# ===========================================================================
# did_company_succeed tests
# ===========================================================================

class TestDidCompanySucceed:
    def test_ipo_is_success(self):
        assert did_company_succeed(IPO_COMPANY, COMPANY_OPERATING) is True

    def test_past_threshold_is_success(self):
        # Bowery has Series B+ which exceeds default threshold of Series B
        assert did_company_succeed(BOWERY_FARMING, COMPANY_SHUT_DOWN) is True

    def test_ma_after_series_a_is_success(self):
        assert did_company_succeed(SERIES_A_PLUS_ACQUISITION, COMPANY_OPERATING) is True

    def test_ma_before_series_a_is_not_success(self):
        # Seed + Acquisition — M&A before Series A is not success
        assert did_company_succeed(SEED_PLUS_ACQUISITION_EARLY, COMPANY_OPERATING) is False

    def test_no_equity_returns_false(self):
        assert did_company_succeed(DEBT_ONLY, COMPANY_OPERATING) is False

    def test_never_reached_threshold_returns_false(self):
        # Seed only, threshold is Series B — never made it there
        assert did_company_succeed(SEED_ONLY_STALE, COMPANY_OPERATING) is False

    def test_straight_to_ipo_guard(self):
        # Company with only IPO, no early stages — should not count as success
        assert did_company_succeed(STRAIGHT_TO_IPO, COMPANY_OPERATING) is False

    def test_acquired_status_without_ma_round_past_threshold(self):
        # Mantle: Seed → Series B → Series C, acquired per status but no M&A round
        # Series B exceeds default threshold, so success even with Operating status
        assert did_company_succeed(MANTLE, COMPANY_OPERATING) is True
        # With Acquired status, still success
        assert did_company_succeed(MANTLE, COMPANY_ACQUIRED) is True

    def test_acquired_status_without_ma_round_below_threshold(self):
        # Spero Foods: Early VC only, acquired per status but no M&A round
        # Below Series B threshold — but with Acquired company_row, treated as success
        # since they passed the stage gate (raised_stage_or_earlier checks for Early VC)
        assert did_company_succeed(SPERO_FOODS, COMPANY_OPERATING) is False
        assert did_company_succeed(SPERO_FOODS, COMPANY_ACQUIRED) is True

    def test_operating_company_row_no_effect(self):
        assert did_company_succeed(SEED_ONLY_STALE, COMPANY_OPERATING) is False

    def test_shut_down_does_not_make_success(self):
        # Shut Down status alone doesn't make a company succeed
        assert did_company_succeed(SEED_ONLY_STALE, COMPANY_SHUT_DOWN) is False


# ===========================================================================
# time_since_last_funding tests
# ===========================================================================

class TestTimeSinceLastFunding:
    def test_recent_funding(self):
        rows = make_rows([
            {"round_date_nzi": pd.Timestamp("2025-12-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
        ])
        days = time_since_last_funding(rows)
        assert days < 365

    def test_old_funding(self):
        rows = make_rows([
            {"round_date_nzi": pd.Timestamp("2018-01-01"), "round_type_nzi": "Seed", "financing_type_nzi": "Equity"},
        ])
        days = time_since_last_funding(rows)
        assert days > 365 * 2


# ===========================================================================
# did_company_fail tests
# ===========================================================================

# Use a fixed "now" so stale checks are deterministic
FIXED_NOW = dt.datetime(2026, 4, 6)


class TestDidCompanyFail:
    """Tests for the core failure logic."""

    def _patch_now(self):
        """Return a patch that fixes dt.datetime.now() in the zombie module."""
        return patch(
            "vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_zombie_companies_fail.dt.datetime",
            wraps=dt.datetime,
        )

    def test_debt_only_returns_false(self):
        """Equity gate: debt-only company cannot fail."""
        assert did_company_fail(DEBT_ONLY, COMPANY_OPERATING) is False

    def test_grant_only_stale_is_failure(self):
        """Grant-only company, stale for 2+ years → failure at Early VC threshold."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(GRANT_ONLY, COMPANY_OPERATING) is True

    def test_grant_only_recent_not_failure(self):
        """Grant-only company with recent funding → not yet classifiable."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(GRANT_ONLY_RECENT, COMPANY_OPERATING) is False

    def test_already_succeeded_returns_false(self):
        """Success override: Bowery succeeded past Series B."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(BOWERY_FARMING, COMPANY_OPERATING) is False

    def test_ma_before_success_stage_is_failure(self):
        """M&A before Series A is a failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(SEED_PLUS_ACQUISITION_EARLY, COMPANY_OPERATING) is True

    def test_stale_funding_is_failure(self):
        """Company with only Seed from 2019, stale for 6+ years → failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(SEED_ONLY_STALE, COMPANY_OPERATING) is True

    def test_recent_funding_not_failure(self):
        """Series A from 2025 — not stale yet."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(RECENT_SERIES_A, COMPANY_OPERATING) is False

    def test_ipo_company_not_failure(self):
        """IPO company succeeded, should not be failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(IPO_COMPANY, COMPANY_OPERATING) is False

    def test_accelerator_only_stale_is_failure(self):
        """Accelerator-only company, stale for 2+ years → failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(ACCELERATOR_ONLY_STALE, COMPANY_OPERATING) is True

    def test_accelerator_plus_grant_stale_is_failure(self):
        """Accelerator + Grant company, stale → failure (like client 65539)."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(ACCELERATOR_PLUS_GRANT_STALE, COMPANY_OPERATING) is True

    def test_accelerator_to_series_b_succeeds(self):
        """Company starting at accelerator that reached Series B → not failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(ACCELERATOR_TO_SERIES_B, COMPANY_OPERATING) is False


class TestDidCompanyFailThresholds:
    """Test failure at different stage thresholds."""

    def _patch_now(self):
        return patch(
            "vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_zombie_companies_fail.dt.datetime",
            wraps=dt.datetime,
        )

    def test_seed_threshold(self):
        """Seed threshold: has Seed, no Series A, stale → fail."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            result = did_company_fail(
                SEED_ONLY_STALE,
                COMPANY_OPERATING,
                at_stage="Seed",
            )
            assert result is True

    def test_series_a_threshold(self):
        """Series A threshold: INFINIUM has Series A but also Series B+C → succeeded."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            result = did_company_fail(
                INFINIUM,
                COMPANY_OPERATING,
                at_stage="Series A",
            )
            assert result is False

    def test_series_b_threshold(self):
        """Series B threshold: need Series C+ to succeed. INFINIUM has Series C → succeeded."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            result = did_company_fail(
                INFINIUM,
                COMPANY_OPERATING,
                at_stage="Series B",
            )
            assert result is False


class TestDidCompanyFailWithCompanyRow:
    """Tests for ensemble_operating_status_classification integration via company_row."""

    def _patch_now(self):
        return patch(
            "vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_zombie_companies_fail.dt.datetime",
            wraps=dt.datetime,
        )

    def test_shut_down_but_succeeded_is_not_failure(self):
        """Bowery Farming: Shut Down but reached Series D — succeeded past Early VC, not a failure."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(BOWERY_FARMING, COMPANY_SHUT_DOWN) is False

    def test_shut_down_seed_only_is_failure(self):
        """Seed-only company, Shut Down → failure (didn't succeed past threshold)."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(SEED_ONLY_STALE, COMPANY_SHUT_DOWN) is True

    def test_shut_down_recent_funding_is_failure(self):
        """Recent Series A but Shut Down → still failure if didn't succeed past threshold."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(RECENT_SERIES_A, COMPANY_SHUT_DOWN) is True

    def test_operating_status_no_effect(self):
        """'Operating' status doesn't change failure logic."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(SEED_ONLY_STALE, COMPANY_OPERATING) is True

    def test_acquired_status_prevents_failure(self):
        """Spero Foods: Early VC only, Acquired — should succeed (not fail) via company_row."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(SPERO_FOODS, COMPANY_ACQUIRED) is False

    def test_acquired_status_mantle(self):
        """Mantle: Series C, Acquired — already succeeds on funding alone."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(MANTLE, COMPANY_ACQUIRED) is False

    def test_restructured_does_not_auto_fail(self):
        """AppHarvest: Restructured with SPAC — should not auto-fail."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(APPHARVEST, COMPANY_RESTRUCTURED) is False

    def test_shut_down_infinium(self):
        """INFINIUM: Shut Down, Series C — succeeded past Early VC threshold."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            assert did_company_fail(INFINIUM, COMPANY_SHUT_DOWN) is False

    def test_shut_down_infinium_at_series_b_threshold(self):
        """INFINIUM: Shut Down, has Series C — succeeded past Series B threshold too."""
        with self._patch_now() as mock_dt:
            mock_dt.now.return_value = FIXED_NOW
            result = did_company_fail(
                INFINIUM,
                COMPANY_SHUT_DOWN,
                at_stage="Series B",
            )
            assert result is False

    def test_debt_only_shut_down_still_false(self):
        """Debt-only company, even if Shut Down, fails the equity gate."""
        assert did_company_fail(DEBT_ONLY, COMPANY_SHUT_DOWN) is False
