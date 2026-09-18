"""Map input: customer organizations merged into the landscape schema and tagged."""

import json

import pandas as pd

from vdl_tools.portfolio_comparison.mapping import build_map_input

YAML = (
    "engagement:\n  customer: test-co\n  vertical: climate\n  match_objective: text\n"
    "  baseline_run:\n    name: x\n    version: v\n    enriched_uri: e.json\n"
    "    network_nodes_uri: n.json\n    source: crunchbase\n    taxonomy: oneearth\n"
    "    taxonomy_version: \"1\"\n  inputs:\n    companies: c.xlsx\n"
    "  mapping:\n    customer_label: TC\n    portfolio_tag: TC Portfolio\n"
)


def test_map_input_merges_and_tags(tmp_path):
    (tmp_path / "engagement.yaml").write_text(YAML)
    results = tmp_path / "data" / "results"
    (results / "baseline").mkdir(parents=True)
    eco = pd.DataFrame([
        {"uid": "u1", "profile_name": "Trees Inc", "Website": "https://trees.example",
         "Org Type": "For Profit", "Summary": "trees", "Data Source": "Crunchbase",
         "level0_one_earth_category": "['Nature Conservation']",
         "level1_one_earth_category": "['Ecosystem Restoration']", "Country": "United States"},
        {"uid": "u2", "profile_name": "Volts", "Website": "https://volts.example",
         "Org Type": "For Profit", "Summary": "volts", "Data Source": "Crunchbase",
         "level0_one_earth_category": "['Energy Transition']",
         "level1_one_earth_category": "['Renewable Power']", "Country": "United States"},
    ])
    eco.to_json(results / "baseline" / "cb_cd_li_meta.json")
    (results / "baseline_universe.json").write_text(json.dumps(["u1", "u2"]))
    port = pd.DataFrame([
        # already in the landscape by uid -> tagged, no new row
        {"customer_row_id": "r1", "customer_name": "Trees Inc", "entity_type": "for_profit",
         "disposition": "invested", "matched_id": "u1", "customer_url": None, "matched_url": None,
         "Summary": "x", "text_for_taxonomy": "x", "level0_one_earth_category": "Nature Conservation",
         "level1_one_earth_category": "Ecosystem Restoration", "Country": "United States"},
        # in the landscape by website domain only
        {"customer_row_id": "r2", "customer_name": "Volts Co", "entity_type": "for_profit",
         "disposition": "passed", "matched_id": None, "customer_url": "volts.example/about",
         "matched_url": None, "Summary": "y", "text_for_taxonomy": "y",
         "level0_one_earth_category": None, "level1_one_earth_category": None, "Country": None},
        # new to the landscape, has text -> added
        {"customer_row_id": "r3", "customer_name": "Alpine", "entity_type": "nonprofit",
         "disposition": "invested", "matched_id": None, "customer_url": "alpine.example",
         "matched_url": None, "Summary": "forest reserve", "text_for_taxonomy": "forest reserve",
         "level0_one_earth_category": "Nature Conservation",
         "level1_one_earth_category": "Land Conservation", "Country": "Costa Rica"},
        # no text at all -> listed, not added
        {"customer_row_id": "r4", "customer_name": "Ghost", "entity_type": "nonprofit",
         "disposition": "invested", "matched_id": None, "customer_url": None, "matched_url": None,
         "Summary": None, "text_for_taxonomy": None, "level0_one_earth_category": None,
         "level1_one_earth_category": None, "Country": None},
    ])
    port.to_parquet(results / "enriched_portfolio.parquet")

    out = build_map_input(tmp_path)
    assert len(out) == 3  # 2 landscape rows + Alpine; Ghost skipped
    tags = dict(zip(out["uid"], out["TC Portfolio"]))
    assert tags["u1"] == ["TC Portfolio", "Funded by TC"]
    assert tags["u2"] == ["TC Portfolio", "Evaluated by TC, passed"]
    new = out[out["uid"] == "customer:r3"].iloc[0]
    assert new["profile_name"] == "Alpine" and new["Org Type"] == "Non Profit"
    assert new["Data Source"] == "TC" and new["Country"] == "Costa Rica"
    assert list(new["TC Portfolio"]) == ["TC Portfolio", "Funded by TC"]
    # landscape rows keep their own name/summary
    assert out.loc[out["uid"] == "u2", "profile_name"].iloc[0] == "Volts"
    state = json.loads((tmp_path / "pipeline_state.json").read_text())["stages"]["map_input"]
    assert state["n_customer_in_landscape"] == 2 and state["n_customer_added"] == 1
    assert state["customer_textless"] == ["Ghost"]
    assert (results / "map_input.json").exists()
