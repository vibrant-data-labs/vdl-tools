"""Sourcing stage: solution-matched landscape companies and concentration-ranked backers."""

import json

import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.sourcing import run_sourcing

YAML = (
    "engagement:\n  customer: test-co\n  vertical: climate\n  match_objective: text\n"
    "  baseline_run:\n    name: x\n    version: v\n    enriched_uri: e.json\n"
    "    network_nodes_uri: n.json\n    source: crunchbase\n    taxonomy: oneearth\n"
    "    taxonomy_version: \"1\"\n  inputs:\n    companies: c.xlsx\n"
    "  sourcing:\n    pillars: [Nature Conservation]\n"
    "    min_backer_companies: 2\n    min_backer_share: 0.5\n"
)


def _node(uid, name, pillar, solutions, investors, website="", kind="For Profit"):
    return {"uid": uid, "Name": name, "For-Profit vs Non-Profit": kind, "Website": website,
            "One Earth Pillars": [pillar], "One Earth Sub-Pillars": ["Ecosystem Restoration"],
            "One Earth Solutions Only": solutions, "Investors": investors,
            "Funding Stage": "Seed", "Total Funding": 100, "Year Last Funded": 2025,
            "HQ State": "CA", "Summary": "s"}


@pytest.fixture
def engagement(tmp_path):
    (tmp_path / "engagement.yaml").write_text(YAML)
    results = tmp_path / "data" / "results"
    (results / "baseline").mkdir(parents=True)
    nodes = [
        # aligned (Reforestation): two unseen, one already in the customer's files by uid,
        # one by website domain
        _node("u1", "Trees Inc", "Nature Conservation", ["Reforestation"], ["Specialist", "Generalist"]),
        _node("u2", "Canopy", "Nature Conservation", ["Reforestation", "Conserved Lands"], ["Specialist"]),
        _node("u3", "Seen By Id", "Nature Conservation", ["Reforestation"], ["Specialist"]),
        _node("u4", "Seen By Site", "Nature Conservation", ["Reforestation"], ["Specialist"],
              website="https://www.seen.example/"),
        # not aligned: energy companies the generalist also backs
        _node("u5", "Volts", "Energy Transition", ["Solar"], ["Generalist"]),
        _node("u6", "Amps", "Energy Transition", ["Solar"], ["Generalist"]),
        _node("u7", "Watts", "Energy Transition", ["Solar"], ["Generalist"]),
        # a nonprofit in the same solution must not appear
        _node("u8", "Trees Org", "Nature Conservation", ["Reforestation"], ["Specialist"], kind="Non Profit"),
    ]
    (results / "baseline" / "cft_network_cleaned.json").write_text(json.dumps({"nodes": nodes}))
    port = pd.DataFrame([
        # the customer's own nature investment seeds the solution set
        {"customer_row_id": "r1", "entity_type": "for_profit", "disposition": "invested",
         "matched_id": "m-own", "customer_url": "own.example", "matched_url": None,
         "level0_one_earth_category": "Nature Conservation",
         "level1_one_earth_category": "Ecosystem Restoration",
         "level2_one_earth_category": "Reforestation"},
        # a passed deal already seen (by uid) and one seen by website
        {"customer_row_id": "r2", "entity_type": "for_profit", "disposition": "passed",
         "matched_id": "u3", "customer_url": None, "matched_url": None,
         "level0_one_earth_category": None, "level1_one_earth_category": None,
         "level2_one_earth_category": None},
        {"customer_row_id": "r3", "entity_type": "for_profit", "disposition": "passed",
         "matched_id": None, "customer_url": "seen.example", "matched_url": None,
         "level0_one_earth_category": None, "level1_one_earth_category": None,
         "level2_one_earth_category": None},
        # an energy investment does not seed anything
        {"customer_row_id": "r4", "entity_type": "for_profit", "disposition": "invested",
         "matched_id": "m-e", "customer_url": None, "matched_url": None,
         "level0_one_earth_category": "Energy Transition",
         "level1_one_earth_category": "Renewable Power", "level2_one_earth_category": "Solar"},
    ])
    port.to_parquet(results / "enriched_portfolio.parquet")
    return tmp_path


def test_sourcing_companies_and_backers(engagement):
    out = run_sourcing(engagement)
    companies = out["sourcing_companies"]
    assert list(out["solutions"]) == ["Reforestation"]
    assert set(companies["company"]) == {"Trees Inc", "Canopy"}  # seen rows + nonprofit excluded
    backers = out["sourcing_backers"].set_index("backer")
    # Specialist: 2 aligned of 4 landscape companies (50%) -> passes; Generalist: 1 of 4 -> fails
    # on both count and share (the high-volume-generalist case).
    assert list(backers.index) == ["Specialist"]
    assert backers.at["Specialist", "n_aligned_companies"] == 2
    assert backers.at["Specialist", "n_landscape_companies"] == 4
    assert backers.at["Specialist", "aligned_share_pct"] == 50.0
    state = json.loads((engagement / "pipeline_state.json").read_text())["stages"]["sourcing"]
    assert state["n_seed_investments"] == 1 and state["n_companies"] == 2 and state["n_backers"] == 1
    assert (engagement / "data/results/sourcing_backers.csv").exists()


def test_sourcing_requires_seed_pillars(tmp_path):
    (tmp_path / "engagement.yaml").write_text(YAML.replace("    pillars: [Nature Conservation]\n", ""))
    with pytest.raises(ValueError, match="sourcing.pillars"):
        run_sourcing(tmp_path)
