import uuid

import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.baseline import (
    _find_id_column,
    _validate_source_ids,
)


def _cb_uuid():
    return str(uuid.uuid4())


def make_mixed_universe(n_cb=5, n_candid=5):
    """CFT-shaped frame: CB rows carry uuids, Candid rows carry EIN-ish uids."""
    rows = [
        {"uid": _cb_uuid(), "Data Source": "Crunchbase"} for _ in range(n_cb)
    ] + [
        {"uid": f"1{i:d}-345678{i:d}", "Data Source": "Candid"} for i in range(n_candid)
    ]
    return pd.DataFrame(rows)


def test_find_id_column_rejects_positional_ids():
    # Player-cleaned network files carry a positional "id" plus the real uid.
    nodes = pd.DataFrame({
        "id": range(10),
        "uid": [_cb_uuid() for _ in range(10)],
    })
    assert _find_id_column(nodes, "nodes", "crunchbase") == "uid"


def test_find_id_column_errors_when_nothing_matches_source_shape():
    nodes = pd.DataFrame({"id": range(10)})
    with pytest.raises(ValueError, match="does not match"):
        _find_id_column(nodes, "nodes", "crunchbase")


def test_mixed_source_universe_validates_by_source_rows():
    df = make_mixed_universe()
    # Only ~50% of ids are CB-shaped, but 100% of CB rows are — passes.
    _validate_source_ids(df, "uid", "crunchbase", "enriched file")


def test_declared_source_with_wrong_ids_fails():
    df = make_mixed_universe()
    df.loc[df["Data Source"] == "Crunchbase", "uid"] = "not-a-uuid"
    with pytest.raises(ValueError, match="declared source"):
        _validate_source_ids(df, "uid", "crunchbase", "enriched file")
