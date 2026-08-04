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
