"""
Node-level structural metrics for a directed causal network, plus clusters and layouts.

Every function takes the standard `nodes` table (see load_data.py) and a networkx DiGraph built
with `load_data.build_graph`, so the graph contains every node including isolated ones.

The two core ideas, borrowed from ecology:

* Keystone leverage. A keystone species is controlled by few others but influences many, directly
  and one step removed (the "1-2 punch"). For a factor we count nodes reachable in 2 outgoing hops
  (outout_degree) vs nodes that reach it in 2 incoming hops (inin_degree):
      2_Degree_Asymmetry = (outout - inin) / (outout + inin)      -1 .. 1
      2_Degree_Reach     = 100 * outout / N                       % of the network reached in 2 hops
      Keystone_Index     = minmax(reach) * minmax(asymmetry)      0 .. 1 within this network

* Upstream position (trophic level). In a food web, trophic level = 1 + mean trophic level of what
  you eat, so producers sit at the bottom and top predators at the top. Here "eating" is "being
  caused by": a factor's level is 1 + the mean level of the factors that cause it, so root causes
  have LOW trophic level and downstream symptoms have HIGH trophic level. To handle feedback loops
  a synthetic root node is attached below every factor before solving.
      Trophic_Level  = minmax(rooted trophic level)   0 = most upstream .. 1 = most downstream
      Upstream_Score = 1 - Trophic_Level              1 = most upstream

Ranks (`*_Pctl`) are percentiles 0..100 within the network being scored.
"""

import numpy as np
import pandas as pd
import networkx as nx

from vdl_tools.tag2network.Network import LayoutNetwork as ln
from vdl_tools.tag2network.Network import ComputeClustering as cc


# the columns add_node_metrics() adds, in order (used by ensemble.py to know what to average)
NODE_METRIC_COLUMNS = [
    "total_links", "in_degree", "out_degree", "betweenness",
    "outout_degree", "inin_degree",
    "1_Degree_Asymmetry", "2_Degree_Asymmetry", "2_Degree_Reach",
    "Keystone_Index", "Keystone_Pctl",
    "Trophic_Level", "Upstream_Score", "Upstream_Pctl",
]

# columns add_clusters() adds
CLUSTER_COLUMNS = ["Cluster", "InterclusterFraction", "ClusterDiversity", "ClusterBridging", "ClusterCentrality"]


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

def two_hop_out_degree(nw):
    """Number of distinct nodes reachable from each node in exactly 2 outgoing hops. {node: count}"""
    result = {}
    for n in nw.nodes():
        second_hop = set()
        for n2 in nw.successors(n):
            second_hop.update(nw.successors(n2))
        result[n] = len(second_hop)
    return result


def two_hop_in_degree(nw):
    """Number of distinct nodes that reach each node in exactly 2 incoming hops. {node: count}"""
    result = {}
    for n in nw.nodes():
        second_hop = set()
        for n2 in nw.predecessors(n):
            second_hop.update(nw.predecessors(n2))
        result[n] = len(second_hop)
    return result


def rooted_trophic_level(nw):
    """
    Trophic level of each node, treating incoming links as "is caused by". {node: level}

    Level(i) = 1 + mean level of i's causes. A synthetic root node is attached beneath every node
    (as an extra cause) so the linear system is solvable even with feedback loops; it is removed
    afterwards and levels are shifted so the minimum is 1. Root causes and isolated nodes share
    the minimum level.
    """
    # reverse so that arrows point from effect to cause: "prey" of a node = its causes
    rev = nw.reverse()
    root = "__root__"
    rev.add_edges_from((n, root) for n in list(rev.nodes()))
    order = list(rev.nodes())
    a = nx.to_numpy_array(rev, nodelist=order)          # a[i, j] = 1 if i is caused by j
    row_sums = a.sum(axis=1)
    row_sums[row_sums == 0] = 1                          # root has no causes; avoid 0/0
    b = a / row_sums[:, np.newaxis]                      # each row averages over the node's causes
    levels = np.linalg.solve(np.eye(len(order)) - b, np.ones(len(order)))  # level = 1 + mean(cause levels)
    levels = dict(zip(order, levels))
    del levels[root]
    shift = min(levels.values()) - 1.0
    return {n: levels[n] - shift for n in nw.nodes()}


def percentile_rank(series):
    """Percentile 0..100 of each value within the series (ties share the top rank of their group)."""
    n = len(series)
    if n <= 1:
        return pd.Series(0.0, index=series.index)
    return 100 * (series.rank(method="max") - 1) / (n - 1)


def min_max(series):
    """Scale a series to 0..1. All-equal values become 0."""
    span = series.max() - series.min()
    if pd.isna(span) or span == 0:
        return pd.Series(0.0, index=series.index)
    return (series - series.min()) / span


def connectance(n_nodes, n_links):
    """Fraction of all possible directed links (excluding self-links) that are present."""
    return n_links / (n_nodes * (n_nodes - 1))


def _safe_ratio(numerator, denominator):
    """Elementwise division that returns 0 where the denominator is 0."""
    return (numerator / denominator.replace(0, np.nan)).fillna(0)


# ---------------------------------------------------------------------------
# Node metrics
# ---------------------------------------------------------------------------

def add_node_metrics(nodes, nw):
    """
    Add every structural metric in NODE_METRIC_COLUMNS to a copy of the nodes table.
    Reach is a percent of ALL nodes in the table (constant denominator across trials).
    """
    nodes = nodes.copy()
    ids = nodes["id"]
    n_nodes = len(nodes)

    # 1-hop counts (every node is in the graph, but fillna keeps this safe if not)
    nodes["total_links"] = ids.map(dict(nw.degree())).fillna(0).astype(int)
    nodes["in_degree"] = ids.map(dict(nw.in_degree())).fillna(0).astype(int)
    nodes["out_degree"] = ids.map(dict(nw.out_degree())).fillna(0).astype(int)
    nodes["betweenness"] = ids.map(nx.betweenness_centrality(nw)).fillna(0)

    # 2-hop counts and the keystone index
    nodes["outout_degree"] = ids.map(two_hop_out_degree(nw)).fillna(0).astype(int)
    nodes["inin_degree"] = ids.map(two_hop_in_degree(nw)).fillna(0).astype(int)
    nodes["1_Degree_Asymmetry"] = _safe_ratio(nodes["out_degree"] - nodes["in_degree"],
                                              nodes["out_degree"] + nodes["in_degree"])
    nodes["2_Degree_Asymmetry"] = _safe_ratio(nodes["outout_degree"] - nodes["inin_degree"],
                                              nodes["outout_degree"] + nodes["inin_degree"])
    nodes["2_Degree_Reach"] = 100 * nodes["outout_degree"] / n_nodes
    nodes["Keystone_Index"] = min_max(nodes["2_Degree_Reach"]) * min_max(nodes["2_Degree_Asymmetry"])
    nodes["Keystone_Pctl"] = percentile_rank(nodes["Keystone_Index"])

    # trophic level: 0 = most upstream, 1 = most downstream
    nodes["Trophic_Level"] = min_max(ids.map(rooted_trophic_level(nw)))
    nodes["Upstream_Score"] = 1 - nodes["Trophic_Level"]
    nodes["Upstream_Pctl"] = percentile_rank(nodes["Upstream_Score"])
    return nodes


# ---------------------------------------------------------------------------
# Clusters and layouts (thin wrappers around tag2network)
# ---------------------------------------------------------------------------

def _index_by_id(nodes):
    """
    tag2network keys nodes by the DataFrame index, this package keys them by the `id` column.
    Return a copy whose index IS the id column so the two agree. Callers reset the index after.
    """
    if not nodes["id"].is_unique:
        raise ValueError("node ids must be unique")
    nodes = nodes.copy()
    nodes.index = nodes["id"].to_numpy()
    return nodes


def add_clusters(nodes, nw, cluster_attr="Cluster"):
    """
    Add directed Louvain clusters (`Cluster`) and per-node cluster metrics
    (bridging, centrality, ...) to a copy of the nodes table.
    """
    nodes = _index_by_id(nodes)
    cc.addLouvainClusters(nodes, nw, prefix=cluster_attr)
    cc.add_cluster_metrics(nodes, nw, [cluster_attr])
    return nodes.reset_index(drop=True)


def add_group_layout(nodes, links,
                     group_attr="Cluster",   # categorical node column to group by
                     layout="cluster",       # 'cluster' (packed tSNE groups) or 'circle' (one circle per group)
                     overlap_frac=0.2, max_expansion=1.5, scale_factor=1,
                     seed=None,              # tag2network layouts use numpy's global random state
                     ):
    """Add x, y coordinates that visually group nodes by `group_attr`. Returns a copy."""
    nodes = _index_by_id(nodes)
    if seed is not None:
        np.random.seed(seed)
    if layout == "cluster":
        params = ln.ClusterLayoutParams(group_attr=group_attr, overlap_frac=overlap_frac,
                                        max_expansion=max_expansion, scale_factor=scale_factor)
    elif layout == "circle":
        params = ln.CircleLayoutParams(group_attr=group_attr, overlap_frac=overlap_frac,
                                       max_expansion=max_expansion, scale_factor=scale_factor)
    else:
        raise ValueError("layout must be 'cluster' or 'circle'")
    ln.add_layout(nodes, links, params=params)
    return nodes.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Node similarity (which factors have near-identical causes and effects?)
# ---------------------------------------------------------------------------

def node_pair_similarities(nw, id_to_label):
    """
    Jaccard similarity of the combined in- and out-neighbour sets for every pair of nodes.
    High values flag factors that behave as near-duplicates and might be merged.
    Returns a DataFrame sorted by similarity, descending.
    """
    ids = list(nw.nodes())
    neighbours = {n: (set(nw.predecessors(n)), set(nw.successors(n))) for n in ids}
    rows = []
    for i, a in enumerate(ids):
        in_a, out_a = neighbours[a]
        for b in ids[i + 1:]:
            in_b, out_b = neighbours[b]
            union = len(in_a | in_b) + len(out_a | out_b)
            shared = len(in_a & in_b) + len(out_a & out_b)
            rows.append((a, b, shared / union if union else 0.0))
    sims = pd.DataFrame(rows, columns=["id_1", "id_2", "jaccard_similarity"])
    sims["label_1"] = sims["id_1"].map(id_to_label)
    sims["label_2"] = sims["id_2"].map(id_to_label)
    sims["avg_frac_overlap"] = 2 * sims["jaccard_similarity"] / (1 + sims["jaccard_similarity"])
    return sims.sort_values("jaccard_similarity", ascending=False).reset_index(drop=True)
