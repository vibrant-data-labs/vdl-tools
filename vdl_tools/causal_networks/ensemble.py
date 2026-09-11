"""
Deal with uncertainty in the votes: build many networks, average them.

Two sources of uncertainty are handled separately (see slides 24-25 of the methods deck):

1. Where to draw the line for "this is a link"? Every link has several votes. Counting a link
   only at 100% consensus is too strict; counting it at 1 yes vote is too loose. So we build one
   network per "% yes" threshold (20%, 21%, ... 80%), score every node in each, and average.
   The ensemble network keeps a link if it is present in at least half of those networks.

2. What if some votes are simply wrong? On the ensemble network we randomly delete a fraction of
   the links, rescore every node, and repeat many times. The reported metrics are the mean (and
   standard deviation) across trials. Ranks are the mean of the per-trial ranks, by design.
"""

import numpy as np
import pandas as pd
import networkx as nx

from vdl_tools.causal_networks.load_data import threshold_links, build_graph
from vdl_tools.causal_networks.metrics import add_node_metrics, connectance, NODE_METRIC_COLUMNS


# ---------------------------------------------------------------------------
# 1. One network per vote threshold
# ---------------------------------------------------------------------------

def build_threshold_networks(nodes, links,
                             min_votes=3,          # a link needs at least this many votes to count at all
                             min_pct=20,           # lowest "% yes" threshold to try
                             max_pct=80,           # highest "% yes" threshold to try
                             pct_step=1,           # step between thresholds
                             max_connectance=0.4,  # drop networks denser than this (fraction of possible links)
                             max_isolated=1,       # drop networks with more isolated nodes than this
                             ):
    """
    Build and score one network for each "% yes" threshold, then keep only the networks that are
    neither too dense (low thresholds) nor too fragmented (high thresholds).

    Returns (nodes_all, links_all): every kept network's nodes and links stacked, each row tagged
    with `pct_consensus`, `connectance`, `n_isolated`.
    """
    nodes_list, links_list, summary = [], [], []
    for pct in range(min_pct, max_pct + 1, pct_step):
        kept = threshold_links(links, min_votes=min_votes, pct_yes=pct)
        if len(kept) == 0:
            break  # thresholds only get stricter from here
        nw = build_graph(nodes, kept)
        scored = add_node_metrics(nodes, nw)
        c = connectance(len(nodes), len(kept))
        n_isolated = int((scored["total_links"] == 0).sum())
        for df in (scored, kept):
            df["pct_consensus"] = pct
            df["connectance"] = c
            df["n_isolated"] = n_isolated
        nodes_list.append(scored)
        links_list.append(kept)
        summary.append(dict(pct_yes=pct, n_links=len(kept), connectance=round(c, 3), n_isolated=n_isolated,
                            kept=(c <= max_connectance) and (n_isolated <= max_isolated)))

    nodes_all = pd.concat(nodes_list, ignore_index=True)
    links_all = pd.concat(links_list, ignore_index=True)
    ok = (nodes_all["connectance"] <= max_connectance) & (nodes_all["n_isolated"] <= max_isolated)
    nodes_all = nodes_all[ok].reset_index(drop=True)
    ok = (links_all["connectance"] <= max_connectance) & (links_all["n_isolated"] <= max_isolated)
    links_all = links_all[ok].reset_index(drop=True)

    summary = pd.DataFrame(summary)
    kept_pcts = summary.loc[summary["kept"], "pct_yes"]
    print(f"Built {len(summary)} networks for {min_pct}%..{max_pct}% yes; "
          f"kept {len(kept_pcts)} (from {kept_pcts.min()}% to {kept_pcts.max()}% yes) "
          f"with connectance <= {max_connectance} and <= {max_isolated} isolated nodes.")
    return nodes_all, links_all


# ---------------------------------------------------------------------------
# 2. Aggregate the kept networks into one ensemble network
# ---------------------------------------------------------------------------

def _mean_and_std(df, group_cols, value_cols):
    """Group by `group_cols`; mean and std of each value column as `<col>_mean`, `<col>_std`."""
    agg = df.groupby(group_cols)[value_cols].agg(["mean", "std"])
    agg.columns = [f"{col}_{stat}" for col, stat in agg.columns]
    agg = agg.fillna(0)  # std is NaN when there is a single network/trial
    return agg.reset_index()


def aggregate_ensemble(nodes_all, links_all,
                       link_frac_thresh=0.5,  # keep a link if present in at least this fraction of networks
                       ):
    """
    Average node metrics across the threshold networks and build the ensemble link list.

    Nodes: mean and std of every metric column (`<metric>_mean`, `<metric>_std`), plus `n_networks`.
    Links: one row per Source->Target present in >= `link_frac_thresh` of the networks, with
    `frac_networks`. Numeric link columns are averaged across networks, text columns keep their
    first value, so any extra link metadata survives for display.
    """
    n_networks = nodes_all["pct_consensus"].nunique()

    # nodes
    metric_cols = [c for c in NODE_METRIC_COLUMNS if c in nodes_all.columns]
    nodes_agg = _mean_and_std(nodes_all, ["id", "Label"], metric_cols)
    nodes_agg["n_networks"] = n_networks
    nodes_agg = nodes_agg.sort_values("Keystone_Index_mean", ascending=False).reset_index(drop=True)

    # links
    key_cols = ["Source", "Target", "fromName", "toName"]
    tag_cols = ["pct_consensus", "connectance", "n_isolated"]  # per-network tags, not link data
    data_cols = [c for c in links_all.columns if c not in key_cols + tag_cols]
    how = {c: ("mean" if pd.api.types.is_numeric_dtype(links_all[c]) else "first") for c in data_cols}
    how["pct_consensus"] = "count"  # number of networks the link appears in
    links_agg = links_all.groupby(key_cols).agg(how).reset_index()
    links_agg = links_agg.rename(columns={"pct_consensus": "n_networks_with_link"})
    links_agg["frac_networks"] = links_agg["n_networks_with_link"] / n_networks
    links_agg = links_agg[links_agg["frac_networks"] >= link_frac_thresh].reset_index(drop=True)

    print(f"Ensemble network: {len(links_agg)} links present in >= {link_frac_thresh:.0%} of "
          f"{n_networks} networks (connectance {connectance(len(nodes_agg), len(links_agg)):.3f}).")
    return nodes_agg, links_agg


# ---------------------------------------------------------------------------
# 3. Monte-carlo link deletion
# ---------------------------------------------------------------------------

def monte_carlo_thinning(nodes, links,
                         n_trials=100,   # number of random networks
                         del_frac=0.15,  # fraction of links deleted in each trial
                         seed=None,      # set for reproducible results
                         ):
    """
    Randomly delete `del_frac` of the links, rescore every node, repeat `n_trials` times.

    Every trial graph contains every node, so a node that happens to lose all its links in a
    trial is scored as isolated (reach 0, asymmetry 0) rather than dropped, and `n_trials` is
    the same for every node.

    Returns (summary, all_trials):
      summary    one row per node: `<metric>_mean`, `<metric>_std`, `n_trials`, `del_frac`
      all_trials one row per node per trial (for plotting distributions), with a `trial` column
    """
    rng = np.random.default_rng(seed)
    link_pairs = links[["Source", "Target"]].to_numpy()
    n_keep = int(round((1 - del_frac) * len(link_pairs)))
    base_nodes = nodes[["id", "Label"]]
    print(f"Monte-carlo: {n_trials} trials, deleting {del_frac:.0%} of {len(link_pairs)} links each time"
          + (f" (seed {seed})" if seed is not None else " (no seed: not reproducible)"))

    trials = []
    for trial in range(n_trials):
        keep_idx = rng.choice(len(link_pairs), size=n_keep, replace=False)
        nw = nx.DiGraph()
        nw.add_nodes_from(base_nodes["id"].tolist())
        nw.add_edges_from(map(tuple, link_pairs[keep_idx]))
        scored = add_node_metrics(base_nodes, nw)
        scored["trial"] = trial
        trials.append(scored)
    all_trials = pd.concat(trials, ignore_index=True)

    summary = _mean_and_std(all_trials, ["id", "Label"], NODE_METRIC_COLUMNS)
    summary["n_trials"] = n_trials
    summary["del_frac"] = del_frac
    summary = summary.sort_values("Keystone_Pctl_mean", ascending=False).reset_index(drop=True)
    return summary, all_trials
