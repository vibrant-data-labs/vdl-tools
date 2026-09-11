"""Finalize stage + manual source-id entry (set-id)."""

import json
from pathlib import Path

import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.finalize import (
    FINAL_BASENAME,
    find_row,
    run_finalize,
    set_manual_id,
)
from vdl_tools.portfolio_comparison.matching.queue import (
    load_id_mapping,
    record_decision,
    replay_decisions,
    save_id_mapping,
)
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS

CB_UUID = "151d5f17-1348-4f5d-91ee-e28da195b1ec"


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


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
    results.mkdir(parents=True)
    m = make_mapping([
        {"customer_row_id": "r1", "customer_name": "Resolved Co",
         "customer_url": "https://resolved.com", "entity_type": "for_profit",
         "disposition": "invested", "matched_id": "12345",
         "matched_name": "Resolved Co", "status": "auto_matched",
         "enrichment_ready": True},
        {"customer_row_id": "r2", "customer_name": "Mystery Org",
         "entity_type": "nonprofit", "disposition": "invested"},
    ])
    save_id_mapping(m, results)
    return tmp_path, results


def test_set_id_fills_column_and_replays(engagement):
    root, results = engagement
    set_manual_id(root, decided_by="vdl:zein", name="Resolved Co",
                  nzi_id="98765", verify=False)
    m = load_id_mapping(results)
    row = m[m["customer_row_id"] == "r1"].iloc[0]
    assert row["nzi_id"] == "98765"
    assert row["matched_id"] == "12345"  # primary untouched on a matched row

    # Survives a full rebuild.
    fresh = make_mapping([{
        "customer_row_id": "r1", "customer_name": "Resolved Co",
        "entity_type": "for_profit", "matched_id": "12345",
        "status": "auto_matched",
    }])
    fresh = replay_decisions(fresh, results)
    assert fresh.iloc[0]["nzi_id"] == "98765"


def test_set_id_resolves_unresolved_row(engagement):
    root, results = engagement
    set_manual_id(
        root, decided_by="vdl:zein", name="Mystery Org", cb_id=CB_UUID,
        verify=True, verifiers={"cb_id": lambda i: "Mystery Org Inc (https://mystery.org)"},
    )
    m = load_id_mapping(results)
    row = m[m["customer_row_id"] == "r2"].iloc[0]
    assert row["cb_id"] == CB_UUID
    assert row["matched_id"] == CB_UUID
    assert row["matched_name"] == "Mystery Org Inc"
    assert row["matched_url"] == "https://mystery.org"
    assert row["match_method"] == "manual"
    assert row["status"] == "vdl_reviewed"
    entry = json.loads(Path(results / "decisions.jsonl").read_text().splitlines()[-1])
    assert entry["gate"] == "manual_id"
    assert "verified" in entry["reason"]


def test_set_id_layers_with_review_decision(engagement):
    # A review accept and a later manual id on the SAME row both survive
    # replay — the old last-decision-wins replay would have eaten one.
    root, results = engagement
    m = load_id_mapping(results)
    m = record_decision(
        m, results, "r2", decided_by="vdl:zein", status="vdl_reviewed",
        reason="accepted", matched_id="55555", matched_name="Mystery Org",
    )
    save_id_mapping(m, results)
    set_manual_id(root, decided_by="vdl:zein", name="Mystery Org",
                  nzi_id="77777", verify=False)

    fresh = make_mapping([{
        "customer_row_id": "r2", "customer_name": "Mystery Org",
        "entity_type": "nonprofit",
    }])
    fresh = replay_decisions(fresh, results)
    row = fresh.iloc[0]
    assert row["matched_id"] == "55555"  # review accept survived
    assert row["nzi_id"] == "77777"      # manual id survived


def test_set_id_rejects_malformed_ids(engagement):
    root, _ = engagement
    with pytest.raises(ValueError, match="cb_id"):
        set_manual_id(root, decided_by="vdl:zein", name="Resolved Co",
                      cb_id="not-a-uuid", verify=False)


def test_find_row_ambiguity():
    m = make_mapping([
        {"customer_row_id": "a", "customer_name": "Acme Solar"},
        {"customer_row_id": "b", "customer_name": "Acme Wind"},
    ])
    with pytest.raises(ValueError, match="matches 2 rows"):
        find_row(m, name="Acme")
    assert find_row(m, name="acme solar")["customer_row_id"] == "a"


def test_finalize_blocks_on_pending_review(engagement):
    root, results = engagement
    m = load_id_mapping(results)
    m.loc[m["customer_row_id"] == "r2", "status"] = "needs_review"
    save_id_mapping(m, results)
    with pytest.raises(RuntimeError, match="await VDL review"):
        run_finalize(root)


def test_finalize_emits_contract_artifact(engagement):
    root, results = engagement
    out = run_finalize(root)
    assert out.name == f"{FINAL_BASENAME}.parquet"
    final = pd.read_parquet(out)
    assert list(final.columns[:8]) == [
        "customer_row_id", "customer_ein", "customer_name", "customer_url",
        "customer_description", "cb_id", "nzi_id", "coresignal_id",
    ]
    assert len(final) == 2
    assert (Path(results) / f"{FINAL_BASENAME}.csv").exists()
    state = json.loads((Path(root) / "pipeline_state.json").read_text())
    assert state["stages"]["finalize"]["sha256"]


def test_finalize_blocks_on_malformed_ids(engagement):
    root, results = engagement
    m = load_id_mapping(results)
    m.loc[m["customer_row_id"] == "r1", "cb_id"] = "garbage"
    save_id_mapping(m, results)
    with pytest.raises(ValueError, match="malformed"):
        run_finalize(root)
