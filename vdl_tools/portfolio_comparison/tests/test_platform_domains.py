import pandas as pd
import pytest

import vdl_tools.portfolio_comparison.intake.normalize as normalize
from vdl_tools.portfolio_comparison.intake.normalize import (
    domains_converge,
    identity_domain,
    linkedin_slug,
)
from vdl_tools.portfolio_comparison.matching.universe import UniverseIndex, run_tier1


def test_identity_domain_blanks_platforms():
    assert identity_domain("https://www.linkedin.com/company/replant-capital/about/") == ""
    assert identity_domain("https://facebook.com/somepage") == ""
    assert identity_domain("https://replant.capital") == "replant.capital"


def test_linkedin_slug_extraction():
    assert linkedin_slug("https://www.linkedin.com/company/replant-capital/about/") == "replant-capital"
    assert linkedin_slug("linkedin.com/company/Acme-Co?trk=x") == "acme-co"
    assert linkedin_slug("https://replant.capital") == ""


def test_platform_domains_never_converge(monkeypatch):
    monkeypatch.setattr(normalize, "_REDIRECT_CACHE", {})
    monkeypatch.setattr(normalize, "resolve_redirect", lambda d, timeout=10.0: d)
    assert not domains_converge("linkedin.com", "linkedin.com")


ENRICHED = pd.DataFrame([
    {"uid": "u-replant", "Organization": "RePlant Capital",
     "url_homepage": "https://www.linkedin.com/company/replant-capital/",
     "LinkedIn": "https://www.linkedin.com/company/replant-capital/"},
    {"uid": "u-geogen", "Organization": "GeoGenCo",
     "url_homepage": "https://www.linkedin.com/company/geogenco/",
     "LinkedIn": "https://www.linkedin.com/company/geogenco/"},
])


def make_rows(rows):
    defaults = {"entity_type": "for_profit", "disposition": "passed"}
    return pd.DataFrame([{**defaults, **r} for r in rows])


def test_linkedin_url_never_domain_matches_other_linkedin_orgs():
    index = UniverseIndex(ENRICHED, {"u-replant", "u-geogen"})
    rows = make_rows([{
        "customer_row_id": "r1", "customer_name": "RePlant Capital",
        "customer_url": "https://www.linkedin.com/company/replant-capital/about/",
    }])
    result, cands = run_tier1(rows, index)
    row = result.iloc[0]
    # Matches via the LinkedIn slug — its own record, not every LinkedIn org.
    assert row["matched_id"] == "u-replant"
    assert row["status"] == "auto_matched"
    assert all(c.matched_id == "u-replant" for c in cands["r1"])


def test_linkedin_url_without_slug_match_falls_to_name():
    index = UniverseIndex(ENRICHED, {"u-replant", "u-geogen"})
    rows = make_rows([{
        "customer_row_id": "r2", "customer_name": "Unrelated Startup",
        "customer_url": "https://www.linkedin.com/company/unrelated-startup/",
    }])
    result, cands = run_tier1(rows, index)
    # No slug hit, no name hit — and crucially, zero linkedin.com domain noise.
    assert pd.isna(result.iloc[0]["status"])
    assert "r2" not in cands
