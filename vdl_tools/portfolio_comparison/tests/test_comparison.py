"""Phase 3 compare stage: shares, tilt, conversion."""

import json

import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.comparison import run_compare


@pytest.fixture
def engagement(tmp_path):
    (tmp_path / "engagement.yaml").write_text(
        "engagement:\n  customer: test-co\n  vertical: climate\n"
        "  match_objective: text\n"
        "  baseline_run:\n    name: x\n    version: v\n"
        "    enriched_uri: e.json\n    network_nodes_uri: n.json\n"
        "    source: crunchbase\n    taxonomy: oneearth\n"
        "    taxonomy_version: \"1\"\n"
        "  inputs:\n    companies: c.xlsx\n"
    )
    results = tmp_path / "data" / "results"
    (results / "baseline").mkdir(parents=True)
    # Ecosystem: 4 orgs — 2 Energy (repr-list encoded), 2 Nature.
    eco = pd.DataFrame([
        {"uid": f"u{i}",
         "level0_one_earth_category": lvl0, "level1_one_earth_category": lvl1}
        for i, (lvl0, lvl1) in enumerate([
            ("['Energy Transition']", "['Renewable Power']"),
            ("['Energy Transition']", "['Energy Efficiency']"),
            ("['Nature Conservation']", "['Land Conservation']"),
            ("['Nature Conservation']", "['Land Conservation']"),
        ])])
    eco.to_json(results / "baseline" / "cb_cd_li_meta.json")
    (results / "baseline_universe.json").write_text(
        json.dumps([f"u{i}" for i in range(4)]))
    # Portfolio: 3 orgs (one duplicated matched_id) — 2 Energy (1 inv, 1 pass),
    # 1 Nature (inv).
    port = pd.DataFrame([
        {"customer_row_id": "r1", "matched_id": "m1", "disposition": "invested",
         "level0_one_earth_category": "Energy Transition",
         "level1_one_earth_category": "Renewable Power"},
        {"customer_row_id": "r1b", "matched_id": "m1", "disposition": "invested",
         "level0_one_earth_category": "Energy Transition",
         "level1_one_earth_category": "Renewable Power"},
        {"customer_row_id": "r2", "matched_id": "m2", "disposition": "passed",
         "level0_one_earth_category": "Energy Transition",
         "level1_one_earth_category": "Energy Efficiency"},
        {"customer_row_id": "r3", "matched_id": "m3", "disposition": "invested",
         "level0_one_earth_category": "Nature Conservation",
         "level1_one_earth_category": "Land Conservation"},
    ])
    port.to_parquet(results / "enriched_portfolio.parquet")
    return tmp_path


def test_compare_shares_tilt_and_conversion(engagement):
    tables = run_compare(engagement)
    pillar = tables["comparison_pillar"]
    # Ecosystem 50/50; portfolio (deduped to 3) = 2/3 Energy, 1/3 Nature.
    assert pillar.at["Energy Transition", "ecosystem_pct"] == 50.0
    assert pillar.at["Energy Transition", "portfolio_pct"] == 66.7
    assert pillar.at["Energy Transition", "tilt_vs_eco"] == 16.7
    assert pillar.at["Nature Conservation", "n_portfolio"] == 1

    conv = tables["comparison_conversion"]
    assert conv.at["Energy Transition", "n_invested"] == 1  # dedupe held
    assert conv.at["Energy Transition", "n_passed"] == 1
    assert conv.at["Energy Transition", "conversion_rate"] == 0.5
    assert conv.at["Nature Conservation", "conversion_rate"] == 1.0

    sub = tables["comparison_subpillar"]
    assert sub.at["Land Conservation", "ecosystem_pct"] == 50.0

    state = json.loads((engagement / "pipeline_state.json").read_text())
    assert state["stages"]["compare"]["n_portfolio_with_pillar"] == 3
    assert (engagement / "data/results/comparison_pillar.csv").exists()
