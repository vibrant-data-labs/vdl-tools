"""The D3 measures: the numbers the adoption decision rests on come from these helpers."""

import numpy as np
import pandas as pd

from vdl_tools.scrape_enrich.scraper.firecrawl.validation import paired_llm_test as d3


def test_jaccard_treats_two_empty_sets_as_identical():
    assert d3._jaccard([], []) == 1.0
    assert d3._jaccard(["a"], []) == 0.0
    assert d3._jaccard(["a", "b"], ["b", "c"]) == 1 / 3


def test_level_values_parse_from_lists_json_and_pipes():
    assert d3._as_list(["Solar", " Wind ", "Solar"]) == ["Solar", "Wind"]
    assert d3._as_list('["Wind", "Solar"]') == ["Solar", "Wind"]
    assert d3._as_list("Wind|Solar") == ["Solar", "Wind"]
    assert d3._as_list(None) == [] and d3._as_list(float("nan")) == [] and d3._as_list("") == []


def test_membership_needs_relevance_and_the_adaptation_filter():
    assert d3._member(None, {"label": "both"}) is True                       # relevance not re-read
    assert d3._member({"prediction": 0}, {"label": "mitigation"}) is False
    assert d3._member({"prediction": 1}, {"label": "neither"}) is False
    assert d3._member({"prediction": 1}, None) is None


def test_compare_reports_the_paired_difference_and_noise_tail():
    rng = np.random.default_rng(0)
    noise = rng.uniform(0.95, 1.0, 400)
    vendor = noise - 0.05
    df = pd.DataFrame({"v": vendor, "n": noise})
    out = d3._compare(df, "cos", "v", "n", higher_is_closer=True)
    assert out["n"] == 400
    assert abs(out["diff"] + 0.05) < 1e-9
    assert out["ci_low"] <= out["diff"] <= out["ci_high"]
    assert out["beyond_noise_p05"] > 0.5      # most vendor values sit below the noise floor's 5th percentile
    flips = pd.DataFrame({"v": [True, False, False, False], "n": [False, False, False, False]})
    out = d3._compare(flips, "flip", "v", "n", higher_is_closer=False)
    assert out["vendor"] == 0.25 and out["noise"] == 0.0 and np.isnan(out["beyond_noise_p05"])


def test_order_key_is_stable():
    assert d3._order_key("abc") == d3._order_key("abc") != d3._order_key("abd")
