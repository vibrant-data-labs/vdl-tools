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


def test_tier2_multi_candidate_convergence_picks_the_one(monkeypatch):
    # The Aquila case: name search returns many same-named companies; the
    # one whose domain redirects to the customer's site wins.
    table = {"aquila.earth": "aquila.space", "aquila.space": "aquila.space"}
    monkeypatch.setattr(normalize, "_REDIRECT_CACHE", {})
    monkeypatch.setattr(normalize, "resolve_redirect", lambda d, timeout=10.0: table.get(d, d))

    def aquila(cid, domain):
        return Candidate(
            matched_id=cid, matched_name="Aquila", matched_url=f"https://{domain}",
            score=1.0, method="api_search",
            evidence={"signal": "name", "domain": domain, "in_universe": False},
        )

    m = make_mapping([{
        "customer_row_id": "r3", "customer_name": "Aquila",
        "customer_url": "https://aquila.space/", "entity_type": "for_profit",
    }])
    cands = [aquila("cb-earth", "aquila.earth"), aquila("cb-za", "aquilaproductions.co.za"),
             aquila("cb-br", "aquila.com.br"), aquila("cb-ro", "aquila.ro")]
    m, out_cands, _ = run_tier2(m, FakeClient({"Aquila": cands}), {})
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "cb-earth"
    assert any(c.evidence.get("redirect_confirmed") for c in out_cands["r3"])


def test_two_converging_candidates_go_to_review(monkeypatch):
    table = {"a.com": "same.com", "b.com": "same.com", "same.com": "same.com"}
    monkeypatch.setattr(normalize, "_REDIRECT_CACHE", {})
    monkeypatch.setattr(normalize, "resolve_redirect", lambda d, timeout=10.0: table.get(d, d))

    def cand2(cid, domain):
        return Candidate(matched_id=cid, matched_name="Same Co",
                         matched_url=f"https://{domain}", score=1.0, method="api_search",
                         evidence={"signal": "name", "domain": domain, "in_universe": False})

    m = make_mapping([{
        "customer_row_id": "r4", "customer_name": "Same Co",
        "customer_url": "https://same.com/", "entity_type": "for_profit",
    }])
    m, _, _ = run_tier2(m, FakeClient({"Same Co": [cand2("c1", "a.com"), cand2("c2", "b.com")]}), {})
    assert m.iloc[0]["status"] == "needs_review"


def test_queued_row_with_converging_candidate_auto_resolves(monkeypatch):
    # A row the baseline matcher already sent to review must still escape
    # the queue when one of its candidates redirect-converges.
    table = {"old-site.org": "current.org", "current.org": "current.org"}
    monkeypatch.setattr(normalize, "_REDIRECT_CACHE", {})
    monkeypatch.setattr(normalize, "resolve_redirect", lambda d, timeout=10.0: table.get(d, d))

    m = make_mapping([{
        "customer_row_id": "r5", "customer_name": "Current Org",
        "customer_url": "https://current.org/", "entity_type": "for_profit",
        "status": "needs_review",
    }])
    tier1_cand = Candidate(
        matched_id="u-old", matched_name="Current Org",
        matched_url="https://old-site.org", score=1.0, method="name_exact",
        evidence={"domain": "old-site.org", "in_universe": True},
    )
    m, cands, _ = run_tier2(m, FakeClient({"Current Org": []}), {"r5": [tier1_cand]})
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "u-old"
    assert row["in_universe"] == True  # noqa: E712  (Tier-1 origin keeps universe flag)
    assert cands["r5"][0].evidence["redirect_confirmed"] is True


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
