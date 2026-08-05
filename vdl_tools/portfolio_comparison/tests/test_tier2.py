import pandas as pd

from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate
from vdl_tools.portfolio_comparison.matching.tier2 import run_tier2
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


class FakeClient:
    source = "crunchbase"

    def __init__(self, results):
        self.results = results
        self.searched = []

    def search(self, name, url):
        self.searched.append(name)
        return self.results.get(name, [])


def cand(cid, name, signal, score=0.97):
    return Candidate(
        matched_id=cid, matched_name=name, matched_url=f"https://{cid}.com",
        score=score, method="api_search",
        evidence={"signal": signal, "in_universe": False},
    )


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def test_sole_domain_hit_auto_accepts_out_of_universe():
    m = make_mapping([{
        "customer_row_id": "r1", "customer_name": "Loa Carbon",
        "customer_url": "loacarbon.com", "entity_type": "for_profit",
    }])
    client = FakeClient({"Loa Carbon": [cand("cb-loa", "Loa Carbon", "domain")]})
    m, cands, n = run_tier2(m, client, {})
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "cb-loa"
    assert row["in_universe"] == False  # noqa: E712
    assert row["match_method"] == "api_search"
    assert "r1" in cands


def test_name_signal_never_auto_accepts():
    m = make_mapping([{
        "customer_row_id": "r2", "customer_name": "Acme Climate",
        "customer_url": None, "entity_type": "for_profit",
    }])
    client = FakeClient({"Acme Climate": [cand("cb-acme", "Acme Climate", "name", 0.99)]})
    m, cands, _ = run_tier2(m, client, {})
    assert m.iloc[0]["status"] == "needs_review"
    assert cands["r2"][0].matched_id == "cb-acme"


def test_review_rows_get_api_candidates_attached_without_status_change():
    m = make_mapping([{
        "customer_row_id": "r3", "customer_name": "Fuzzy Co",
        "customer_url": "fuzzy.co", "entity_type": "for_profit",
        "status": "needs_review",
    }])
    existing = {"r3": [cand("base-1", "Fuzzy Corp", "tier1", 0.8)]}
    client = FakeClient({"Fuzzy Co": [cand("cb-fuzzy", "Fuzzy Co", "domain")]})
    m, cands, _ = run_tier2(m, client, existing)
    assert m.iloc[0]["status"] == "needs_review"
    assert [c.matched_id for c in cands["r3"]] == ["base-1", "cb-fuzzy"]


def test_decided_and_nonprofit_rows_untouched():
    m = make_mapping([
        {"customer_row_id": "r4", "customer_name": "Done Co",
         "customer_url": "done.co", "entity_type": "for_profit",
         "status": "vdl_reviewed"},
        {"customer_row_id": "r5", "customer_name": "Charity",
         "customer_url": None, "entity_type": "nonprofit"},
    ])
    client = FakeClient({})
    m, _, n = run_tier2(m, client, {})
    assert n == 0
    assert client.searched == []
