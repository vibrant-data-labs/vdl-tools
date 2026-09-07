"""
Run the whole causal-network analysis in one call and write the results to Excel.

    nodes, links = load_data.from_undercurrent("survey.json")      # or from_tables / from_kumu_weighted
    nodes, links = pipeline.run_causal_network_analysis(nodes, links, "results.xlsx")

`run_causal_network_analysis` needs `yes`/`no` vote counts on the links (threshold networks ->
ensemble -> monte-carlo). `analyze_single_network` is the path for links that are already decided
(a weight, or nothing at all): one network, optional monte-carlo thinning.

Plots, node similarities and the player are separate calls (see plots.py, metrics.py, player.py)
so a run script can include or skip them by adding or deleting a line.
"""

import warnings

import numpy as np
import pandas as pd

from vdl_tools.causal_networks.load_data import build_graph, add_node_attributes
from vdl_tools.causal_networks.metrics import (add_node_metrics, add_clusters, add_group_layout,
                                               NODE_METRIC_COLUMNS, CLUSTER_COLUMNS)
from vdl_tools.causal_networks.ensemble import (build_threshold_networks, aggregate_ensemble,
                                                monte_carlo_thinning)


# internal column name -> name shown in Excel and the player
DISPLAY_NAMES = {
    "Label": "Root Factor",
    "Short Name": "Name",
    "Top_Keystone": "Top Keystone",
    "Keystone_Index": "Keystone Score",
    "Keystone_Pctl": "Keystone Rank",
    "Keystone_Pctl_std": "Keystone Rank (Std Dev)",
    "Trophic_Level": "Trophic Level",
    "Upstream_Score": "Upstream Score",
    "Upstream_Pctl": "Upstream Rank",
    "Upstream_Pctl_std": "Upstream Rank (Std Dev)",
    "total_links": "Degree",
    "in_degree": "Incoming Links",
    "out_degree": "Outgoing Links",
    "2_Degree_Reach": "Reach in 2 Hops (Pct)",
    "betweenness": "Betweenness",
    "Cluster": "Causal Cluster",
    "ClusterBridging": "Cluster Bridging",
    "ClusterCentrality": "Cluster Centrality",
}

# display columns in output order (the grouping column is inserted after Name); missing ones are skipped
FINAL_COLUMNS_FRONT = [
    "Root Factor", "Name",
    "Catalytic Score", "Top Keystone",
    "Keystone Rank", "Keystone Score",
    "Upstream Rank", "Upstream Score", "Trophic Level",
    "Degree", "Incoming Links", "Outgoing Links", "Reach in 2 Hops (Pct)", "Betweenness",
    "Causal Cluster", "Cluster Bridging", "Cluster Centrality",
    "Keystone Rank (Std Dev)", "Upstream Rank (Std Dev)", "n_trials",
]
FINAL_COLUMNS_TAIL = ["label", "x", "y", "id"]  # needed by the player, hidden there

# intermediate columns that are not written to the display output
INTERNAL_COLUMNS = ["outout_degree", "inin_degree", "1_Degree_Asymmetry", "2_Degree_Asymmetry",
                    "InterclusterFraction", "ClusterDiversity", "n_networks", "del_frac"]

DEFAULT_CLUSTER_LAYOUT = dict(overlap_frac=0.2, max_expansion=1.5, scale_factor=2)  # packed tSNE groups
DEFAULT_GROUP_LAYOUT = dict(overlap_frac=0.1, max_expansion=1.5, scale_factor=1)    # one circle per group


def write_network_excel(nodes, links, path):
    """Write nodes and links to one Excel file with sheets 'Nodes' and 'Links'."""
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        writer.book.strings_to_urls = False  # don't turn url-like text into links
        nodes.to_excel(writer, sheet_name="Nodes", index=False)
        links.to_excel(writer, sheet_name="Links", index=False)
    print(f"Wrote {path}")


def finalize_for_display(nodes, links,
                         node_attributes=None,   # DataFrame or file with hand-curated node columns (Short Name, Pillar, ...)
                         node_attribute_cols=None,  # which of its columns to add (default: all except Label)
                         top_keystone_pctl=80,   # Keystone Rank at or above this is flagged 'Top Keystone'
                         group_attr="Pillar",    # categorical node column used to group the layout and color the player
                         cluster_layout=None,    # layout settings when grouping by Causal Cluster
                         group_layout=None,      # layout settings when grouping by `group_attr`
                         seed=42,
                         ):
    """
    Turn a scored nodes table (means across trials, or a single network) into the display table:
    headline values, Top Keystone flag, curated names/themes, x/y layout, display names,
    Catalytic Score, tidy column order. Returns a new DataFrame sorted by Catalytic Score.
    """
    cluster_layout = cluster_layout or DEFAULT_CLUSTER_LAYOUT
    group_layout = group_layout or DEFAULT_GROUP_LAYOUT
    nodes = nodes.copy()

    # 1. means are the headline values; keep only the rank standard deviations as uncertainty
    keep_std = {"Keystone_Pctl_std", "Upstream_Pctl_std"}
    nodes = nodes.drop(columns=[c for c in nodes.columns if c.endswith("_std") and c not in keep_std])
    nodes.columns = [c[:-5] if c.endswith("_mean") else c for c in nodes.columns]

    # 2. flag the top keystones
    nodes["Top_Keystone"] = np.where(nodes["Keystone_Pctl"] >= top_keystone_pctl, "Yes", "No")

    # 3. hand-curated short names / themes, and a display label
    if node_attributes is not None:
        nodes = add_node_attributes(nodes, node_attributes, keep=node_attribute_cols)
    if "label" not in nodes.columns:
        nodes["label"] = nodes["Label"].str[:40]

    # 4. layout: one circle per group if every node has a group, else packed causal clusters
    has_groups = group_attr in nodes.columns and nodes[group_attr].notna().all()
    if group_attr in nodes.columns and not has_groups:
        warnings.warn(f"Some nodes have no '{group_attr}'; laying out by Causal Cluster instead.")
    if has_groups:
        nodes = add_group_layout(nodes, links, group_attr=group_attr, layout="circle", seed=seed, **group_layout)
    elif "Cluster" in nodes.columns:
        nodes = add_group_layout(nodes, links, group_attr="Cluster", layout="cluster", seed=seed, **cluster_layout)
    else:
        nodes["x"], nodes["y"] = 0.0, 0.0

    # 5. display names and the catalytic score (high keystone leverage AND upstream)
    nodes = nodes.rename(columns=DISPLAY_NAMES)
    nodes["Catalytic Score"] = (nodes["Keystone Rank"] * nodes["Upstream Score"]).round(0)

    # 6. column order: standard columns, then any extra input columns, then player columns
    front = FINAL_COLUMNS_FRONT.copy()
    if group_attr in nodes.columns:
        front.insert(2, group_attr)
    front = [c for c in front if c in nodes.columns]
    extras = [c for c in nodes.columns
              if c not in front + FINAL_COLUMNS_TAIL and c not in INTERNAL_COLUMNS and c not in DISPLAY_NAMES]
    nodes = nodes[front + extras + FINAL_COLUMNS_TAIL]
    return nodes.sort_values("Catalytic Score", ascending=False).reset_index(drop=True)


def run_causal_network_analysis(nodes, links,
                                out_xlsx=None,          # write 'Nodes' and 'Links' sheets here (optional)
                                node_attributes=None,   # DataFrame or file with Short Name / Pillar etc. (optional)
                                node_attribute_cols=None,  # which of its columns to add, e.g. ("Short Name", "Pillar")
                                # which links count: one network per "% yes" threshold
                                min_votes=3, min_pct=20, max_pct=80, pct_step=1,
                                max_connectance=0.4, max_isolated=1,
                                link_frac_thresh=0.5,   # ensemble keeps links present in >= this fraction of networks
                                # robustness to voting error: random link deletion on the ensemble
                                n_trials=100, del_frac=0.15, seed=42,
                                # display
                                top_keystone_pctl=80, group_attr="Pillar",
                                cluster_layout=None, group_layout=None,
                                ):
    """
    Full analysis for a voted causal network (links need `yes` and `no` counts):

      1. build one network per "% yes" threshold and score every node in each
      2. average into an ensemble network (metric means; links present in most networks)
      3. find causal clusters on the ensemble network
      4. monte-carlo: delete `del_frac` of the ensemble links, rescore, repeat `n_trials` times
      5. finalize for display (names, layout, Catalytic Score) and write Excel

    Returns (nodes, links) for the ensemble network, ready for plots.py and player.py.
    """
    nodes_all, links_all = build_threshold_networks(nodes, links, min_votes=min_votes, min_pct=min_pct,
                                                    max_pct=max_pct, pct_step=pct_step,
                                                    max_connectance=max_connectance, max_isolated=max_isolated)
    nodes_ens, links_ens = aggregate_ensemble(nodes_all, links_all, link_frac_thresh=link_frac_thresh)

    nw_ens = build_graph(nodes, links_ens)
    nodes_ens = add_clusters(nodes_ens, nw_ens)

    summary, _ = monte_carlo_thinning(nodes, links_ens, n_trials=n_trials, del_frac=del_frac, seed=seed)

    # bring clusters (from the ensemble network) and any extra input node columns onto the summary
    summary = summary.merge(nodes_ens[["id"] + CLUSTER_COLUMNS], on="id", how="left")
    extra_input_cols = [c for c in nodes.columns if c not in ("id", "Label")]
    summary = summary.merge(nodes[["id"] + extra_input_cols], on="id", how="left")

    nodes_final = finalize_for_display(summary, links_ens, node_attributes=node_attributes,
                                       node_attribute_cols=node_attribute_cols,
                                       top_keystone_pctl=top_keystone_pctl, group_attr=group_attr,
                                       cluster_layout=cluster_layout, group_layout=group_layout, seed=seed)
    if out_xlsx is not None:
        write_network_excel(nodes_final, links_ens, out_xlsx)
    _print_top(nodes_final)
    return nodes_final, links_ens


def analyze_single_network(nodes, links,
                           out_xlsx=None, node_attributes=None, node_attribute_cols=None,
                           n_trials=0, del_frac=0.15, seed=42,   # n_trials=0 skips the monte-carlo
                           top_keystone_pctl=80, group_attr="Pillar",
                           cluster_layout=None, group_layout=None,
                           ):
    """
    Analysis for links that are already decided (a `weight`, or no link attributes at all):
    score one network, find clusters, optionally monte-carlo thin it, finalize and write.
    Returns (nodes, links).
    """
    nw = build_graph(nodes, links)
    scored = add_node_metrics(nodes, nw)
    scored = add_clusters(scored, nw)
    if n_trials > 0:
        summary, _ = monte_carlo_thinning(nodes, links, n_trials=n_trials, del_frac=del_frac, seed=seed)
        extra_cols = [c for c in scored.columns if c not in NODE_METRIC_COLUMNS and c != "Label"]
        scored = summary.merge(scored[extra_cols], on="id", how="left")
    nodes_final = finalize_for_display(scored, links, node_attributes=node_attributes,
                                       node_attribute_cols=node_attribute_cols,
                                       top_keystone_pctl=top_keystone_pctl, group_attr=group_attr,
                                       cluster_layout=cluster_layout, group_layout=group_layout, seed=seed)
    if out_xlsx is not None:
        write_network_excel(nodes_final, links, out_xlsx)
    _print_top(nodes_final)
    return nodes_final, links


def _print_top(nodes_final, n=10):
    cols = [c for c in ["label", "Catalytic Score", "Keystone Rank", "Upstream Rank"] if c in nodes_final.columns]
    print(f"\nTop {n} factors by Catalytic Score:")
    print(nodes_final[cols].head(n).to_string(index=False))
