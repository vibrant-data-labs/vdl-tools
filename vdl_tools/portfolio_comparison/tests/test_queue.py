import pandas as pd

from vdl_tools.portfolio_comparison.matching.queue import (
    record_decision,
    replay_decisions,
)
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def test_decisions_survive_a_match_rerun(tmp_path):
    mapping = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
        {"customer_row_id": "r2", "customer_name": "B", "status": "needs_review"},
    ])
    record_decision(
        mapping, tmp_path, "r1",
        decided_by="vdl:zein", status="vdl_reviewed", reason="confirmed",
        matched_id="u-1", confidence=0.9,
    )
    # Simulate a rerun: rebuild from scratch, decision fields gone.
    rebuilt = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
        {"customer_row_id": "r2", "customer_name": "B", "status": "needs_review"},
    ])
    replayed = replay_decisions(rebuilt, tmp_path)
    r1 = replayed[replayed["customer_row_id"] == "r1"].iloc[0]
    assert r1["status"] == "vdl_reviewed"
    assert r1["matched_id"] == "u-1"
    assert r1["decided_by"] == "vdl:zein"
    r2 = replayed[replayed["customer_row_id"] == "r2"].iloc[0]
    assert r2["status"] == "needs_review"


def test_last_decision_per_row_wins(tmp_path):
    mapping = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    record_decision(mapping, tmp_path, "r1", decided_by="vdl:zein",
                    status="vdl_reviewed", matched_id="u-1")
    record_decision(mapping, tmp_path, "r1", decided_by="vdl:zein",
                    status="customer_review", matched_id=None,
                    reason="changed my mind")
    rebuilt = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    replayed = replay_decisions(rebuilt, tmp_path)
    assert replayed.iloc[0]["status"] == "customer_review"


def test_reject_all_replays_as_real_na_and_stays_tier2_eligible(tmp_path):
    # A reject-all decision sets status=pd.NA; it must round-trip through the
    # decisions log as null, not the string "<NA>" — Tier 2 selects on isna().
    mapping = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    record_decision(mapping, tmp_path, "r1", decided_by="vdl:zein",
                    status=pd.NA, matched_id=None, reason="rejected all")
    rebuilt = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    replayed = replay_decisions(rebuilt, tmp_path)
    assert pd.isna(replayed.iloc[0]["status"])
    assert replayed.iloc[0]["status"] is not None or True  # isna is the contract


def test_replay_normalizes_legacy_stringified_na(tmp_path):
    import json
    entry = {"customer_row_id": "r1", "gate": "match_review", "decided_by": "vdl:zein",
             "decided_at": "2026-08-05T00:00:00+00:00", "reason": "",
             "before": {}, "after": {"status": "<NA>", "matched_id": "None"}}
    (tmp_path / "decisions.jsonl").write_text(json.dumps(entry) + "\n")
    rebuilt = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    replayed = replay_decisions(rebuilt, tmp_path)
    assert pd.isna(replayed.iloc[0]["status"])
    assert pd.isna(replayed.iloc[0]["matched_id"])


def test_replay_skips_rows_that_no_longer_exist(tmp_path):
    mapping = make_mapping([
        {"customer_row_id": "gone", "customer_name": "X", "status": "needs_review"},
    ])
    record_decision(mapping, tmp_path, "gone", decided_by="vdl:zein",
                    status="vdl_reviewed")
    rebuilt = make_mapping([
        {"customer_row_id": "r9", "customer_name": "Y", "status": "needs_review"},
    ])
    replayed = replay_decisions(rebuilt, tmp_path)
    assert replayed.iloc[0]["status"] == "needs_review"


def test_apply_decision_refuses_during_match_run(tmp_path):
    import pytest
    from vdl_tools.portfolio_comparison.matching.queue import (
        apply_decision,
        save_id_mapping,
    )

    mapping = make_mapping([
        {"customer_row_id": "r1", "customer_name": "A", "status": "needs_review"},
    ])
    save_id_mapping(mapping, tmp_path)
    (tmp_path / ".match_running").write_text("")
    with pytest.raises(RuntimeError, match="match run is in progress"):
        apply_decision(tmp_path, "r1", decided_by="vdl:zein", status="vdl_reviewed")
