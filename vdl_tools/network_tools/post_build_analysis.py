"""
Post-build analysis for a network with clusters and keywords
1) Performs cluster stability analysis and plots results
2) Computes keyword entropy across clusters, plots resuots and saves keyword stats to a csv file
"""

import os
import pathlib as pl
from collections import defaultdict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

import vdl_tools.network_tools.optimize_clusters as oc


def _make_cluster_prob_plot(raw_ndf, tag_attr, name, clus_name, links_per, n_iter=10):
    # run the cluster stability analysis

    ndf, ldf, all_clusters, entity_df, nw_df, entity_probs, nw_clus\
        = oc.analyze_clusters(raw_ndf,
                              tag_attr,
                              links_per,
                              [],
                              None,
                              name,
                              clus_name,
                              n_iter=n_iter,
                              clus_size_thr=10
                              )

    # analyze each entity's probability of cluster membership
    # get most probable cluster of each node
    def _analyze_entity_prob_by_cluster(entity_probs, orig_nw):
        all_cl_names = [cl['name'] for cl in orig_nw]
        entity_cl_idx_order = entity_probs.argsort(axis=0)
        cl_max = entity_cl_idx_order[-1, :]
        # order nodes by max prob entity
        entity_order = cl_max.argsort()
        ordered_clus = cl_max[entity_order]
        ordered_probs = entity_probs.T[entity_order, :]
        # sort each cluster's nodes and get their indices
        cl_orders = []
        for idx in np.arange(cl_max.max() + 1):
            indices = np.argwhere(ordered_clus == idx).flatten()
            probs = ordered_probs[indices, idx]
            cl_orders.append(probs.argsort() + indices.min())
        # concatenate each cluster's orders and re-sort the probabilities
        ordered_probs = ordered_probs[np.concatenate(cl_orders), :]
        return ordered_probs, all_cl_names

    # plot fraction of times an entity lands in each cluster during the randomization
    def _plot_entity_prob_by_cluster(ordered_probs, all_cl_names):
        # plot prob of each entity landing in each cluster
        fig, ax = plt.subplots(1, 1, figsize=(16, 10))
        ax.pcolormesh(ordered_probs.T, cmap='jet')
        plt.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=0, vmax=1), cmap='jet'),
                     ax=[ax], location='right', shrink=0.7, fraction=0.1,
                     pad=0.02, anchor=(0.0, 1), label='Entity Prob')
        ticks = np.arange(ordered_probs.shape[1])
        labels = [f"{all_cl_names[idx][:40]}" for idx in ticks]
        ax.set_yticks(ticks)
        ax.set_yticklabels(labels)
        for label in ax.get_yticklabels():
            label.set_verticalalignment('bottom')
        ax.grid(True, axis='y')
        ax.set_title("Entity probability of occurence in cluster")
        return fig

    ordered_probs, all_cl_names = _analyze_entity_prob_by_cluster(entity_probs, all_clusters[0])
    return _plot_entity_prob_by_cluster(ordered_probs, all_cl_names)


def _get_keyword_stats(raw_ndf, tag_attr, clus_name):
    # keyword entropy across clusters
    tag_list = tag_attr + '_list'
    raw_ndf[tag_list] = raw_ndf[tag_attr].str.split('|')
    kwd_clus_counts = {}
    all_counts = defaultdict(int)
    for idx, row in raw_ndf.iterrows():
        clus = row[clus_name]
        kwds = row[tag_list]
        for kwd in kwds:
            if kwd not in kwd_clus_counts:
                kwd_clus_counts[kwd] = defaultdict(int)
            kwd_clus_counts[kwd][clus] += 1
            all_counts[kwd] += 1

    kwd_info = []
    for kwd, clus_cnts in kwd_clus_counts.items():
        cnts = np.array(list(clus_cnts.values()))
        cnt = cnts.sum()
        p = cnts / cnt
        h = np.abs(-(p * np.log(p)).sum())
        n_kwd = len(cnts)
        hmax = -np.log(1/n_kwd)
        kwd_info.append({'kwd': kwd,
                         'count': cnt,
                         'entropy': h,
                         'n_clus': n_kwd,
                         'max_entropy': hmax,
                         'common_high_entropy': h * cnt
                         })
        # common_high_entropy is high for keywords that are common and have high entropy
    kdf = pd.DataFrame(kwd_info)
    mask = kdf.common_high_entropy > 0
    kdf.common_high_entropy[mask] = np.log10(kdf.common_high_entropy[mask])
    kdf.sort_values('entropy', inplace=True, ascending=False)
    return kdf


def run_post_build_analysis(nw_path, results_path, tag_attr, name, clus_name, links_per, test_clusters=True):
    """
    Analyze clusters and keywords of final network.

    The cluster stability analysis only makes sense to perform on a network where the nodes
    are linked by keyword similarity

    Results are figures and a data table written to results_path

    Parameters
    ----------
    nw_path : string
        Path to network Excel file.
    results_path : string
        Path to write results to.
    tag_attr : string
        Keyword (tag) column name in network nodes table.
    name: string
        Label column name in network nodes table.
    clus_name : string
        Cluster column name in network nodes table..
    links_per : int
        Links per species when making network.
    test_clusters : Boolean, optional
        Set true to do cluster stability analysis. The default is True.

    Returns
    -------
    None.

    """
    raw_ndf = pd.read_excel(nw_path, sheet_name='Nodes')
    # raw_ldf = pd.read_excel(nw_path, sheet_name='Links')

    if test_clusters:
        fig = _make_cluster_prob_plot(raw_ndf, tag_attr, name, clus_name, links_per)
        fig.savefig(results_path + "/ClusterProbs.png")

    kdf = _get_keyword_stats(raw_ndf, tag_attr, clus_name)

    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    kdf.plot.scatter('entropy', 'count', logy=True, s=2, ax=ax)
    ax.set_title("Keyword Entropy vs. Count")
    fig.savefig(results_path + "/KeywordEntropy.png")

    kdf.to_csv(results_path + "/KeywordStats.csv", index=False)
    return kdf


# %%
# for testing
if __name__ == '__main__':
    prjpath = pl.Path(os.getcwd() + "/LocalData/projects/ClimateLandscape")

    version = "2022-09"
    cft_path = prjpath / "cft-published" / version
    sector_suffix = ''
    nw_name_cleaned = cft_path / ("cft_network_cleaned" + sector_suffix + ".xlsx")

    tag_attr = "Keywords"  # "tags"  tag attrib to use for linking
    clus_name = 'Keyword Theme'
    links_per = 6  # target average links per node - thinner network typically makes more smaller clusters

    kdf = run_post_build_analysis(nw_name_cleaned, '.', tag_attr, clus_name, links_per)
