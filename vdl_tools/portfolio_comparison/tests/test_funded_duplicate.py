"""Duplicate source profiles on one domain: prefer the one reporting
financial data (Zein's ruling, 2026-08-07)."""

import pandas as pd

from vdl_tools.portfolio_comparison.matching.source_adapter import (
    Candidate,
    pick_funded_duplicate,
)
from vdl_tools.portfolio_comparison.matching.tier2 import run_tier2, _supplement_ids
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


def cand(cid, domain="acme.com", signal="domain", funded=False, score=0.97):
    return Candidate(
        matched_id=cid, matched_name="Acme", matched_url=f"https://{domain}",
        score=score, method="api_search",
        evidence={"signal": signal, "domain": domain, "has_financials": funded,
                  "in_universe": False},
    )


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


class FakeClient:
    source = "crunchbase"

    def __init__(self, results):
        self.results = results

    def search(self, name, url):
        return self.results.get(name, [])


def test_exactly_one_funded_wins():
    picked = pick_funded_duplicate([cand("dead"), cand("live", funded=True)])
    assert picked.matched_id == "live"
    assert picked.evidence["funded_duplicate_pick"] is True


def test_no_pick_when_ambiguous_or_inapplicable():
    # Both funded / both unfunded: no mechanical basis.
    assert pick_funded_duplicate([cand("a", funded=True), cand("b", funded=True)]) is None
    assert pick_funded_duplicate([cand("a"), cand("b")]) is None
    # Different domains: not the duplicate-profile case.
    assert pick_funded_duplicate([cand("a"), cand("b", domain="other.com", funded=True)]) is None
    # Name-signal hits never qualify.
    assert pick_funded_duplicate([cand("a", signal="name"), cand("b", funded=True)]) is None
    # A single candidate is the sole-hit rule's job.
    assert pick_funded_duplicate([cand("a", funded=True)]) is None


def test_tier2_auto_accepts_funded_duplicate():
    # Two CB profiles on the customer's own domain; the funded one is listed
    # SECOND to prove the pick is by financials, not order.
    m = make_mapping([{
        "customer_row_id": "r1", "customer_name": "Acme",
        "customer_url": "https://acme.com/", "entity_type": "for_profit",
    }])
    client = FakeClient({"Acme": [cand("dead"), cand("live", funded=True)]})
    m, _, _ = run_tier2(m, client, {})
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "live"


def test_supplement_fills_funded_duplicate():
    m = make_mapping([{
        "customer_row_id": "r2", "customer_name": "Acme",
        "customer_url": "https://acme.com/", "entity_type": "for_profit",
        "matched_id": "108278", "matched_name": "Acme",
        "matched_url": "https://acme.com", "status": "auto_matched",
    }])
    client = FakeClient({"Acme": [cand("cb-dead"), cand("cb-live", funded=True)]})
    m = _supplement_ids(m, client, "cb_id", r"[0-9a-fA-F-]{36}")
    assert m.iloc[0]["cb_id"] == "cb-live"
