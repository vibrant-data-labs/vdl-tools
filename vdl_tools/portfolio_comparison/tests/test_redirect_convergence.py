import pandas as pd
import pytest

import vdl_tools.portfolio_comparison.intake.normalize as normalize
from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate
from vdl_tools.portfolio_comparison.matching.tier2 import run_tier2
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


@pytest.fixture(autouse=True)
def fake_redirects(monkeypatch):
    # abalobi.info and abalobi.org both land on abalobi.org; everything else
    # resolves to itself.
    table = {"abalobi.info": "abalobi.org", "abalobi.org": "abalobi.org"}
    monkeypatch.setattr(normalize, "_REDIRECT_CACHE", {})
    monkeypatch.setattr(normalize, "resolve_redirect", lambda d, timeout=10.0: table.get(d, d))


def test_domains_converge_via_redirects():
    assert normalize.domains_converge("abalobi.info", "abalobi.org")
    assert normalize.domains_converge("abalobi.org", "abalobi.org")
    assert not normalize.domains_converge("abalobi.org", "other.com")
    assert not normalize.domains_converge("", "abalobi.org")


class FakeClient:
    source = "crunchbase"

    def __init__(self, results):
        self.results = results

    def search(self, name, url):
        return self.results.get(name, [])


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def test_tier2_redirect_convergence_auto_accepts():
    # CB record on abalobi.info, customer says abalobi.org, name exact:
    # redirect convergence upgrades review → auto.
    m = make_mapping([{
        "customer_row_id": "r1", "customer_name": "Abalobi",
        "customer_url": "https://abalobi.org/", "entity_type": "for_profit",
    }])
    cand = Candidate(
        matched_id="cb-abalobi", matched_name="Abalobi",
        matched_url="https://abalobi.info", score=1.0, method="api_search",
        evidence={"signal": "name", "domain": "abalobi.info", "in_universe": False},
    )
    client = FakeClient({"Abalobi": [cand]})
    m, cands, _ = run_tier2(m, client, {})
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "cb-abalobi"
    assert cands["r1"][0].evidence.get("redirect_confirmed") is True


def test_tier2_no_convergence_still_reviews():
    m = make_mapping([{
        "customer_row_id": "r2", "customer_name": "Acme",
        "customer_url": "https://acme.org/", "entity_type": "for_profit",
    }])
    cand = Candidate(
        matched_id="cb-acme", matched_name="Acme",
        matched_url="https://unrelated.com", score=1.0, method="api_search",
        evidence={"signal": "name", "domain": "unrelated.com", "in_universe": False},
    )
    client = FakeClient({"Acme": [cand]})
    m, _, _ = run_tier2(m, client, {})
    assert m.iloc[0]["status"] == "needs_review"
