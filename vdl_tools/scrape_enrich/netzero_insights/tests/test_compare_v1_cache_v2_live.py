"""The pure comparison helpers behind the v1-cache-vs-v2-live cohort script."""

import math

import pandas as pd

from vdl_tools.scrape_enrich.netzero_insights.scripts.compare_v1_cache_v2_live import (
    _equal, _norm, all_na_columns, diff_frames,
)


def test_equal_tolerates_float_noise_but_not_real_differences():
    assert _equal(206726268.0, 206726268.00001)
    assert not _equal(100.0, 101.0)
    assert _equal(3, 3.0)


def test_equal_treats_nan_none_and_nat_as_the_same_missing_value():
    assert _equal(math.nan, None)
    assert _equal(pd.NaT, None)
    assert not _equal(math.nan, 0)


def test_equal_compares_lists_as_sets_and_dates_by_day():
    # v1 and v2 return roundInvestorIDs in different orders.
    assert _equal([59651, 19382, 7721], [7721, 19382, 59651])
    assert not _equal([1, 2], [1, 2, 3])
    assert _equal(pd.Timestamp("2025-01-07 08:37:00"), pd.Timestamp("2025-01-07 00:00:00"))
    assert _norm(pd.Timestamp("2025-01-07 08:37:00")) == "2025-01-07"


def test_equal_does_not_treat_bools_as_numbers_with_tolerance():
    assert not _equal(True, False)
    assert _equal(True, True)


def test_diff_frames_reports_only_one_side_and_per_field_mismatches():
    v1 = pd.DataFrame({"k": [1, 2, 3], "a": ["x", "y", "z"], "b": [1.0, 2.0, 3.0]})
    v2 = pd.DataFrame({"k": [1, 2, 4], "a": ["x", "Y", "w"], "b": [1.0, 2.0000001, 4.0]})

    d = diff_frames(v1, v2, "k", ["a", "b", "c"])

    assert d["n_common"] == 2 and d["only_v1"] == [3] and d["only_v2"] == [4]
    assert d["mismatches"] == {"a": 1, "b": 0}
    assert d["examples"]["a"] == [(2, "y", "Y")]
    assert d["missing_fields"] == {"v1": ["c"], "v2": []}


def test_all_na_columns():
    df = pd.DataFrame({"a": [None, None], "b": [1, None]})
    assert all_na_columns(df) == ["a"]
