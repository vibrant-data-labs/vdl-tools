import pandas as pd

from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate
from vdl_tools.portfolio_comparison.review_apps.customer_export import (
    RESPONSE_COLUMNS,
    _resolve_response,
    build_export_frame,
)
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def test_export_selects_unresolved_and_customer_review_rows():
    m = make_mapping([
        {"customer_row_id": "r1", "customer_name": "Ghost Co",
         "entity_type": "for_profit"},                       # NA -> exported
        {"customer_row_id": "r2", "customer_name": "Maybe Org",
         "entity_type": "nonprofit", "status": "customer_review",
         "matched_name": "Maybe Organization", "matched_url": "maybe.org"},
        {"customer_row_id": "r3", "customer_name": "Done Co",
         "entity_type": "for_profit", "status": "auto_matched"},
    ])
    frame = build_export_frame(m)
    assert list(frame["ID (do not edit)"]) == ["r1", "r2"]
    assert all(col in frame.columns for col in RESPONSE_COLUMNS)
    asks = list(frame["What we need"])
    assert "couldn't find" in asks[0]
    assert "best guess: Maybe Organization" in asks[1]


class FakeCB:
    def __init__(self, cands):
        self.cands = cands

    def search(self, name, url):
        return self.cands


def cb_cand(signal="domain"):
    return Candidate(matched_id="cb-1", matched_name="Found Co",
                     matched_url="https://found.co", score=0.97,
                     method="api_search", evidence={"signal": signal})


def test_response_resolution_order():
    universe_domains = {"known.org": {"id": "u-known", "name": "Known Org",
                                      "url": "https://known.org", "in_universe": True}}
    # EIN wins outright
    r = _resolve_response("X", "", "12-3456789", universe_domains, {"12-3456789"}, None)
    assert r["status"] == "auto_matched" and r["in_universe"]
    # Universe domain beats CB
    r = _resolve_response("X", "known.org", "", universe_domains, set(), FakeCB([cb_cand()]))
    assert r["matched_id"] == "u-known" and r["in_universe"]
    # CB sole domain hit accepts, out of universe
    r = _resolve_response("X", "found.co", "", {}, set(), FakeCB([cb_cand()]))
    assert r["matched_id"] == "cb-1" and r["in_universe"] is False
    # Name-signal CB hit does not auto-accept
    r = _resolve_response("X", "found.co", "", {}, set(), FakeCB([cb_cand(signal="name")]))
    assert r == {"status": "needs_review"}
    # No usable signals -> final
    r = _resolve_response("", "", "", {}, set(), None)
    assert r == {"status": "unmatched_final"}
