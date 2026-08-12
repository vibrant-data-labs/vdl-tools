"""Reviewed taxonomy overrides apply as data and survive reruns."""

import json

import pandas as pd

from vdl_tools.portfolio_comparison.enrichment.overrides import (
    OVERRIDES_FILENAME,
    apply_taxonomy_overrides,
)


def test_overrides_apply_levels_lists_and_deepest(tmp_path):
    out = pd.DataFrame([{
        "customer_row_id": "r1",
        **{f"level{i}_one_earth_category": pd.NA for i in range(4)},
        **{f"all_level{i}_one_earth_category": [] for i in range(4)},
        "one_earth_category": "NoMatch",
        "cat_level_one_earth_category": pd.NA,
    }, {
        "customer_row_id": "r2",
        **{f"level{i}_one_earth_category": "Keep" for i in range(4)},
        **{f"all_level{i}_one_earth_category": [["Keep"]] for i in range(4)},
        "one_earth_category": "Keep",
        "cat_level_one_earth_category": 3,
    }])
    (tmp_path / OVERRIDES_FILENAME).write_text(json.dumps([{
        "customer_row_id": "r1",
        "level0": "Energy Transition", "level1": "Energy Efficiency",
        "level2": "Industries & Services: Water & Waste Utilities",
        "level3": None, "cat_level": 2,
        "by": "test", "reason": "test",
    }]))
    got = apply_taxonomy_overrides(out, tmp_path).set_index("customer_row_id")
    assert got.at["r1", "level2_one_earth_category"] == \
        "Industries & Services: Water & Waste Utilities"
    assert pd.isna(got.at["r1", "level3_one_earth_category"])
    assert got.at["r1", "all_level1_one_earth_category"] == ["Energy Efficiency"]
    assert got.at["r1", "all_level3_one_earth_category"] == []
    assert got.at["r1", "one_earth_category"] == \
        "Industries & Services: Water & Waste Utilities"
    assert got.at["r1", "cat_level_one_earth_category"] == 2
    assert got.at["r2", "one_earth_category"] == "Keep"  # untouched


def test_no_overrides_file_is_a_noop(tmp_path):
    out = pd.DataFrame([{"customer_row_id": "r1", "one_earth_category": "X"}])
    got = apply_taxonomy_overrides(out, tmp_path)
    assert got.at[0, "one_earth_category"] == "X"
