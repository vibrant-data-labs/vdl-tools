"""Threshold networks, ensemble aggregation, and monte-carlo thinning on toy data."""
import pandas as pd
import pytest

from vdl_tools.causal_networks import ensemble


def _voted_triangle():
    nodes = pd.DataFrame({"id": [0, 1, 2], "Label": list("ABC")})
    # A->B is a link at every threshold, B->C only up to 50%, C->A only at 30%
    links = pd.DataFrame({"Source": [0, 1, 2], "Target": [1, 2, 0], "fromName": list("ABC"),
                          "toName": list("BCA"), "yes": [8, 6, 4], "no": [2, 4, 6], "votes": [10, 10, 10],
                          "fracYes": [0.8, 0.6, 0.4], "note": ["x", "y", "z"]})
    return nodes, links


def test_threshold_networks_and_aggregate_ensemble_by_hand():
    nodes, links = _voted_triangle()
    nodes_all, links_all = ensemble.build_threshold_networks(
        nodes, links, min_votes=3, min_pct=30, max_pct=70, pct_step=20, max_connectance=1.0, max_isolated=3)
    assert sorted(nodes_all["pct_consensus"].unique()) == [30, 50, 70]
    assert len(nodes_all) == 9                       # 3 nodes x 3 networks
    nodes_agg, links_agg = ensemble.aggregate_ensemble(nodes_all, links_all, link_frac_thresh=0.5)
    kept = set(zip(links_agg["fromName"], links_agg["toName"]))
    assert kept == {("A", "B"), ("B", "C")}          # C->A is in 1 of 3 networks -> dropped
    assert links_agg.set_index("fromName").loc["A", "frac_networks"] == 1.0
    assert links_agg.set_index("fromName").loc["B", "frac_networks"] == pytest.approx(2 / 3)
    assert links_agg["note"].tolist() == ["x", "y"]  # text metadata carried through
    assert {"Keystone_Index_mean", "Keystone_Index_std", "n_networks"} <= set(nodes_agg.columns)
    assert nodes_agg["n_networks"].unique().tolist() == [3]


def test_monte_carlo_is_seeded_and_keeps_all_nodes():
    nodes = pd.DataFrame({"id": range(6), "Label": list("ABCDEF")})
    links = pd.DataFrame({"Source": [0, 0, 0, 1, 2, 3, 4], "Target": [1, 2, 3, 4, 4, 5, 5]})
    s1, all1 = ensemble.monte_carlo_thinning(nodes, links, n_trials=8, del_frac=0.3, seed=7)
    s2, _ = ensemble.monte_carlo_thinning(nodes, links, n_trials=8, del_frac=0.3, seed=7)
    s3, _ = ensemble.monte_carlo_thinning(nodes, links, n_trials=8, del_frac=0.3, seed=8)
    pd.testing.assert_frame_equal(s1, s2)
    assert not s1["Keystone_Index_mean"].equals(s3["Keystone_Index_mean"])
    assert len(all1) == 6 * 8                            # every node in every trial
    assert s1["n_trials"].unique().tolist() == [8]
    assert not s1.filter(like="_mean").isna().any().any()
