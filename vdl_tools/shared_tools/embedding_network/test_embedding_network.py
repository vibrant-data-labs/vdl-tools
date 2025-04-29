# run from embedding_network folder
#
# build a network using embedding similarity

import pathlib as pl
import pandas as pd

import embedding_network as en

import vdl_tools.tag2network.Network.BuildNetwork as bn
import vdl_tools.tag2network.Network.LayoutNetwork as ln
import vdl_tools.tag2network.Network.ComputeClustering as cc
import vdl_tools.tag2network.Network.DrawNetwork as dn


# %%
def get_cft_data(filename="cft_network.xlsx",
                 datapath='../climate-landscape/data/cft-published/US',
                 keepcols=None,
                 sheet='Nodes'):
    if type(datapath) is str:
        datapath = pl.Path(datapath)
    if sheet is not None:
        raw_cft_df = pd.read_excel(datapath / filename, sheet_name=sheet)
    else:
        raw_cft_df = pd.read_excel(datapath / filename)
    return raw_cft_df


funding = 'Total_Funding_$'

keep_cols = ['Organization', 'climate_kwds_list', funding, 'P_vs_V',
             'Stage_Category', 'Any Equity-Justice Mention',  # 'Mitigation vs Adaptation',
             'uid', 'Description', 'Summary', 'Cluster', 'cluster_name']

raw_cft_df = get_cft_data(keepcols=keep_cols)

df = raw_cft_df[keep_cols].rename(columns={'Cluster': 'OrigCluster', 'cluster_name': 'OrigClusterName'})

# %%
results_file = 'CFTEmbeddingNetwork.xlsx'

params = bn.BuildEmbeddingNWParams(linksPer=6,
                                   n_tags=5,
                                   clusName='ClusterName',
                                   labelcol=None,
                                   layout_params=ln.ClusterLayoutParams(),
                                   clus_params=cc.ClusteringParams(method='leiden', merge_tiny=True))

nodesdf, edgesdf, sims = en.build_embedding_network(df, params, debug=True)
print(nodesdf.Cluster.nunique())

# output network nodes
nodesdf.to_excel(results_file, index=False)

# plot
dn.plot_network(nodesdf, edgesdf, plotfile="EmbeddingNetwork.png", draw_edges=False)

# %%
# analyze cluster naming in old and new networks
print(nodesdf.OrigClusterName.value_counts())
print(nodesdf.ClusterName.value_counts())

nodesdf['ClusNamePairs'] = nodesdf.OrigClusterName + ' ' + nodesdf.ClusterName.astype(str)

print(nodesdf.ClusNamePairs.value_counts()[0:20])
