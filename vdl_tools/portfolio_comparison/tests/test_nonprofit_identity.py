from dataclasses import dataclass

import pandas as pd

from vdl_tools.portfolio_comparison.matching.nonprofit import (
    apply_identity_matches,
    match_identity,
)
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


@dataclass
class FakeHit:
    ein: str
    name: str
    name_secondary: str | None
    city: str | None
    state: str | None
    latest_taxyear: int | None
    org_type: str
    rank: float
    signal: str


class FakeGtClient:
    def __init__(self, results):
        self.results = results
        self.calls = []

    def search_identity(self, name=None, url=None, limit=5):
        self.calls.append((name, url))
        return self.results.get(name, [])


def hit(ein, name, signal):
    return FakeHit(ein=ein, name=name, name_secondary=None, city="Portland",
                   state="OR", latest_taxyear=2024, org_type="nonprofit",
                   rank=1_500_000, signal=signal)


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def np_row(row_id, name, url=None):
    return {"customer_row_id": row_id, "customer_name": name,
            "customer_url": url, "entity_type": "nonprofit"}


def test_sole_url_exact_auto_accepts_with_universe_flag():
    m = make_mapping([np_row("r1", "River Trust", "rivertrust.org")])
    client = FakeGtClient({"River Trust": [hit("12-3456789", "River Trust Inc", "url_exact")]})
    matches = match_identity(m, gt_client=client, universe_ids={"12-3456789"})
    m = apply_identity_matches(m, matches)
    row = m.iloc[0]
    assert row["status"] == "auto_matched"
    assert row["matched_id"] == "12-3456789"
    assert row["in_universe"] == True  # noqa: E712
    assert client.calls == [("River Trust", "rivertrust.org")]


def test_name_signal_goes_to_review():
    m = make_mapping([np_row("r2", "Ocean Fund")])
    client = FakeGtClient({"Ocean Fund": [hit("98-7654321", "Ocean Fund of Maine", "name")]})
    matches = match_identity(m, gt_client=client, universe_ids=set())
    m = apply_identity_matches(m, matches)
    assert m.iloc[0]["status"] == "needs_review"
    assert matches["r2"][0].evidence["in_universe"] is False


def test_fiscal_sponsor_rows_search_without_ein():
    # EIN was blanked at intake for sponsor rows; only name/url signals go out.
    m = make_mapping([{**np_row("r3", "Sponsored Project", "project.org"),
                       "customer_ein": ""}])
    client = FakeGtClient({"Sponsored Project": []})
    matches = match_identity(m, gt_client=client)
    assert matches == {}
    assert client.calls == [("Sponsored Project", "project.org")]


def test_decided_rows_keep_status():
    m = make_mapping([{**np_row("r4", "Decided Org", "decided.org"),
                       "status": "vdl_reviewed"}])
    client = FakeGtClient({"Decided Org": [hit("11-1111111", "Decided Org", "url_exact")]})
    matches = match_identity(m, gt_client=client)
    m = apply_identity_matches(m, matches)
    assert m.iloc[0]["status"] == "vdl_reviewed"
    assert pd.isna(m.iloc[0]["matched_id"])
