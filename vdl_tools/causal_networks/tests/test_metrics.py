"""Hand-checkable metric tests on toy directed graphs."""
import networkx as nx
import numpy as np
import pandas as pd
import pytest

from vdl_tools.causal_networks import metrics
from vdl_tools.causal_networks.load_data import build_graph


def _nodes(n):
    return pd.DataFrame({"id": range(n), "Label": [f"F{i}" for i in range(n)]})


def test_chain_trophic_level_increases_downstream():
    # F0 -> F1 -> F2 -> F3: F0 is the root cause, F3 the most downstream
    nodes = _nodes(4)
    links = pd.DataFrame({"Source": [0, 1, 2], "Target": [1, 2, 3]})
    nw = build_graph(nodes, links)
    tl = metrics.rooted_trophic_level(nw)
    assert tl[0] == pytest.approx(1.0)          # minimum is shifted to 1
    assert tl[0] < tl[1] < tl[2] < tl[3]
    scored = metrics.add_node_metrics(nodes, nw)
    assert scored["Trophic_Level"].tolist()[0] == 0.0
    assert scored["Trophic_Level"].tolist()[-1] == 1.0
    assert scored["Upstream_Score"].tolist() == pytest.approx((1 - scored["Trophic_Level"]).tolist())


def test_hub_is_the_keystone():
    # hub 0 -> 1..5, each i -> i+5 (6..10); node 11 isolated
    nodes = _nodes(12)
    links = pd.DataFrame({"Source": [0] * 5 + [1, 2, 3, 4, 5], "Target": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    nw = build_graph(nodes, links)
    assert metrics.two_hop_out_degree(nw)[0] == 5
    assert metrics.two_hop_in_degree(nw)[0] == 0
    scored = metrics.add_node_metrics(nodes, nw)
    hub = scored.loc[scored["id"] == 0].iloc[0]
    assert hub["2_Degree_Asymmetry"] == 1.0
    assert hub["2_Degree_Reach"] == pytest.approx(100 * 5 / 12)   # percent of all nodes
    assert hub["Keystone_Index"] == scored["Keystone_Index"].max() == 1.0
    assert hub["Keystone_Pctl"] == 100.0
    # the isolated node gets zeros, never NaN
    iso = scored.loc[scored["id"] == 11].iloc[0]
    assert iso[["total_links", "outout_degree", "2_Degree_Asymmetry", "2_Degree_Reach", "Keystone_Index"]].tolist() == [0, 0, 0, 0, 0]
    assert not scored[metrics.NODE_METRIC_COLUMNS].isna().any().any()


def test_percentile_rank_ties_take_top_of_group():
    ranks = metrics.percentile_rank(pd.Series([1, 2, 2, 3]))
    assert ranks.tolist() == pytest.approx([0.0, 200 / 3, 200 / 3, 100.0])


def test_min_max_all_equal_is_zero():
    assert metrics.min_max(pd.Series([2.0, 2.0, 2.0])).tolist() == [0.0, 0.0, 0.0]


def test_clusters_and_layout_keep_ids_aligned():
    # two triangles joined by one link; node order in the table is deliberately shuffled
    nodes = _nodes(6).sample(frac=1, random_state=3).reset_index(drop=True)
    links = pd.DataFrame({"Source": [0, 1, 2, 3, 4, 5, 2], "Target": [1, 2, 0, 4, 5, 3, 3]})
    nw = build_graph(nodes, links)
    clustered = metrics.add_clusters(nodes, nw)
    by_id = clustered.set_index("id")["Cluster"]
    assert by_id[0] == by_id[1] == by_id[2]
    assert by_id[3] == by_id[4] == by_id[5]
    assert by_id[0] != by_id[3]
    laid_out = metrics.add_group_layout(clustered, links, group_attr="Cluster", seed=0)
    assert {"x", "y"} <= set(laid_out.columns)
    assert laid_out["id"].tolist() == nodes["id"].tolist()   # row order untouched


def test_node_pair_similarities_flags_duplicates():
    nodes = _nodes(4)
    # 0 and 1 have identical causes and effects
    links = pd.DataFrame({"Source": [2, 2, 0, 1], "Target": [0, 1, 3, 3]})
    nw = build_graph(nodes, links)
    sims = metrics.node_pair_similarities(nw, dict(zip(nodes["id"], nodes["Label"])))
    top = sims.iloc[0]
    assert {top["label_1"], top["label_2"]} == {"F0", "F1"}
    assert top["jaccard_similarity"] == 1.0
