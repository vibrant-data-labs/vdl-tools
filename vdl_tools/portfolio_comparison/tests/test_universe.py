import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.matching.universe import (
    UniverseIndex,
    run_tier1,
)

ENRICHED = pd.DataFrame([
    {"uid": "u-solaris", "Organization": "Solaris Energy Inc",
     "url_homepage": "https://www.solarisenergy.com"},
    {"uid": "u-windward", "Organization": "Windward Climate Analytics",
     "url_homepage": "https://windward.io"},
    {"uid": "u-tidal", "Organization": "Tidal Power Co",
     "url_homepage": "https://tidalpower.com"},
    # In the enriched file but removed by the landscape's relevance filtering:
    {"uid": "u-filtered", "Organization": "Generic SaaS Corp",
     "url_homepage": "https://genericsaas.com"},
])
UNIVERSE_IDS = {"u-solaris", "u-windward", "u-tidal"}


@pytest.fixture
def index():
    return UniverseIndex(ENRICHED, UNIVERSE_IDS)


def make_rows(rows):
    defaults = {"entity_type": "for_profit", "disposition": "invested"}
    return pd.DataFrame([{**defaults, **r} for r in rows])


def test_domain_exact_auto_accepts(index):
    rows = make_rows([{
        "customer_row_id": "r1", "customer_name": "Solaris (they renamed)",
        "customer_url": "solarisenergy.com/about",
    }])
    result, _ = run_tier1(rows, index)
    row = result.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "u-solaris"
    assert row["match_method"] == "url_exact"
    assert row["in_universe"] is True or row["in_universe"] == True  # noqa: E712


def test_name_exact_auto_accepts_without_url(index):
    rows = make_rows([{
        "customer_row_id": "r2", "customer_name": "Windward Climate Analytics",
        "customer_url": None,
    }])
    result, _ = run_tier1(rows, index)
    row = result.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "u-windward"


def test_name_match_with_conflicting_domain_goes_to_review(index):
    # "Company" ≡ "Co" after suffix stripping, so the name matches exactly —
    # but the customer's URL points at a different domain. Same name,
    # different site must never auto-accept.
    rows = make_rows([{
        "customer_row_id": "r3", "customer_name": "Tidal Power Company",
        "customer_url": "https://different-domain.com",
    }])
    result, candidates = run_tier1(rows, index)
    row = result.iloc[0]
    assert row["status"] == "needs_review"
    assert candidates["r3"][0].matched_id == "u-tidal"


def test_name_match_without_url_still_auto_accepts(index):
    rows = make_rows([{
        "customer_row_id": "r3b", "customer_name": "Tidal Power Company",
        "customer_url": None,
    }])
    result, _ = run_tier1(rows, index)
    assert result.iloc[0]["status"] == "auto_matched"


def test_no_signal_leaves_status_na_for_tier2(index):
    rows = make_rows([{
        "customer_row_id": "r4", "customer_name": "Completely Unrelated Startup",
        "customer_url": "https://nowhere.example",
    }])
    result, _ = run_tier1(rows, index)
    assert pd.isna(result.iloc[0]["status"])


def test_filtered_out_org_flags_scope_not_identity(index):
    rows = make_rows([{
        "customer_row_id": "r5", "customer_name": "Generic SaaS",
        "customer_url": "https://www.genericsaas.com",
    }])
    result, _ = run_tier1(rows, index)
    row = result.iloc[0]
    # Confident identity (domain exact) but excluded from the universe:
    # never auto-accepts — review decides scope.
    assert row["matched_id"] == "u-filtered"
    assert row["status"] == "needs_review"
    assert row["out_of_universe_reason"] == "excluded_by_landscape_filter"
    assert not row["in_universe"]


def test_nonprofit_rows_also_match_tier1(index):
    # Multi-source universes (CFT = CB + Candid) contain nonprofits, so the
    # nonprofit lane runs Tier 1 too; the GT EIN lane confirms afterwards.
    rows = make_rows([{
        "customer_row_id": "r6", "customer_name": "Solaris Energy Inc",
        "customer_url": "solarisenergy.com", "entity_type": "nonprofit",
    }])
    result, _ = run_tier1(rows, index)
    row = result.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "u-solaris"
