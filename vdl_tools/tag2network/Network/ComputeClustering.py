import pandas as pd
import networkx as nx
import igraph as ig
import vdl_tools.tag2network.Network.BuildNetwork as bn
from vdl_tools.tag2network.Network.louvain import generate_dendrogram
from vdl_tools.tag2network.Network.louvain import partition_at_level
from vdl_tools.tag2network.Network.ClusteringProperties import basicClusteringProperties
from vdl_tools.tag2network.Network.ClusteringParams import ClusteringParams


def addLouvainClusters(nodesdf, nw, clusterLevel=0, prefix='Cluster'):
    """
    Compute and add Louvain clusters to node dataframe
    One of linksdf and nw must not be None
    Add just the specified Louvain dendrogram level or adds all levels

    Parameters
    ----------
    nodesdf : pandas.DataFrame
        nodes dataframe.
    linksdf : pandas.DataFrame, optional
        links dataframe. The default is None.
    nw : networkx.Graph, optional
        graph object, either linksdf or nw must be present. The default is None.
    clusterLevel : int, optional
        The clustering level to return, or all levels if None. The default is 0.

    Returns
    -------
    None
    """
    def mergePartitionData(g, p, name):
        return {node: (name + '_' + str(p[node]) if node in p else None) for node in g.nodes()}

    def getPartitioning(i, g, dendo, clusterings, clustering=prefix):
        p = partition_at_level(dendo, len(dendo) - 1 - i)
        vals = mergePartitionData(g, p, clustering)
        clusterings[clustering] = vals

    print("Computing Louvain clustering")
    clusterings = {}
    dendo = generate_dendrogram(nw)
    max_depth = len(dendo) - 1
    if clusterLevel is None:
        for i in range(len(dendo)):
            clus = f'L{i}' if i > 0 else prefix
            getPartitioning(i, nw, dendo, clusterings, clustering=clus)
    else:
        depth = min(clusterLevel, max_depth)
        getPartitioning(depth, nw, dendo, clusterings)
    # add cluster attr to dataframe
    for grp, vals in clusterings.items():
        bn.add_network_attr(nodesdf, grp, vals)
        nodesdf[grp].fillna('No Cluster', inplace=True)


def addLeidenClusters(nodesdf, nw, resolution=1.0, prefix='Cluster', min_clus_size=100, id_attr='__id__'):
    """
    Compute and add Leiden clusters to node dataframe
    One of linksdf and nw must not be None
    Clustering is hierarchical if resolution parameter is a list of values. The first value controls the coarsest
    clustering

    Parameters
    ----------
    nodesdf : pandas.DataFrame
        nodes dataframe.
    linksdf : pandas.DataFrame, optional
        links dataframe. The default is None.
    nw : networkx.Graph, optional
        graph object, either linksdf or nw must be present. The default is None.
    resolution: float or list, optional
        either a single value of clsutering resolution or a list of values, in which case clustering is hierarchical
    prefix: str, optional
        clustering attribute anme and value name prefix
    min_clus_size: int
        cluster size below which hierarchical clustering is not computed

    Returns
    -------
    None
    """

    def _leiden_helper(ndf, _nw, res, _prefix):
        print("Computing Leiden clustering")
        gg = ig.Graph.from_networkx(_nw)
        comm = gg.community_leiden(objective_function='modularity', resolution_parameter=res, n_iterations=-1)
        clus = comm.subgraphs()
        ndf[prefix] = None
        for idx, subg in enumerate(clus):
            nodes = subg.vs['_nx_name']
            ndf.loc[nodes, _prefix] = f"{_prefix}_{idx}"

    clusters = []
    if type(resolution) is list:
        prior_clus = None
        for idx, res in enumerate(resolution):
            _new_clus = f'{prefix}_L{idx+1}' if idx > 0 else prefix
            if idx == 0:
                # add top-level clusters
                _leiden_helper(nodesdf, nw, res, _new_clus)
            else:
                min_cl = min_clus_size[idx - 1] if type(min_clus_size) is list else min_clus_size
                # add subclusters of current level
                ndf = nodesdf[[id_attr, prior_clus]]
                all_dfs = []
                for clus, cnt in ndf[prior_clus].value_counts().items():
                    print(f"{clus} {cnt}")
                    clus_df = ndf[ndf[prior_clus] == clus].copy()
                    if cnt >= min_cl:    # current cluster is large enough - compute subclusters
                        nodes = clus_df[id_attr].values
                        clus_nw = nw.subgraph(nodes)
                        # add clusters of cluster, name with outer clsuter name
                        _leiden_helper(clus_df, clus_nw, res, clus)
                        clus_df.rename(columns={clus: _new_clus}, inplace=True)
                    else:                       # current cluster is small, next level is a single cluster
                        clus_df[_new_clus] = clus_df[prior_clus] + '_0'
                    all_dfs.append(clus_df)
                ndf = pd.concat(all_dfs)
                # add results into the main dataframe
                nodesdf[_new_clus] = ndf[_new_clus]
            clusters.append(_new_clus)
            prior_clus = _new_clus
    else:
        _leiden_helper(nodesdf, nw, resolution, prefix)
        clusters.append(prefix)
    return clusters


def add_cluster_metrics(nodesdf, nw, groupVars):
    # add bridging, cluster centrality etc. for one or more grouping variables
    for groupVar in groupVars:
        if len(nx.get_node_attributes(nw, groupVar)) == 0:
            vals = {k: v for k, v in dict(zip(nodesdf.index, nodesdf[groupVar])).items() if k in nw}
            nx.set_node_attributes(nw, vals, groupVar)
        grpprop = basicClusteringProperties(nw, groupVar)
        for prop, vals in grpprop.items():
            nodesdf[prop] = nodesdf.index.map(vals).values


# re-assign small clusters to similar large clusters
def reassign_small_clusters(nodes_df, edges_df, sims, size_ratio=10, top_n=5, max_size=40):
    # compute intra- and inter-cluster similarities
    clusters = nodes_df.Cluster.unique()
    cluster_similarities = []
    for idx, clus1 in enumerate(clusters):
        for jdx, clus2 in enumerate(clusters):
            idx1 = nodes_df[nodes_df.Cluster == clus1].index
            idx2 = nodes_df[nodes_df.Cluster == clus2].index
            clus_sims = sims[idx1][:, idx2]
            cluster_similarities.append({'idx': idx,
                                         'jdx': jdx,
                                         'clus1': clus1,
                                         'clus2': clus2,
                                         'size1': len(idx1),
                                         'size2': len(idx2),
                                         'interclus_mean_sim': clus_sims.mean()
                                         })
    # make cluster similarities dataframe
    sim_df = pd.DataFrame(cluster_similarities).sort_values(['clus1', 'interclus_mean_sim'])
    # get top 5 most similar smaller clusters of each cluster
    # keep only rows where clus2 is significantly (10x) smaller or bigger than max_size
    sim_df = (sim_df[(sim_df.clus1 == sim_df.clus2)
                     | ((sim_df.size1 > (sim_df.size2 * (size_ratio or 0)))
                     & (sim_df.size2 < (max_size or 0)))
                     ])
    sim_dfs = []
    for clus, cdf in sim_df.groupby('clus1'):
        sim_dfs.append(cdf.sort_values(['interclus_mean_sim', 'size2'], ascending=[False, True]).iloc[0:top_n])
    top_sim_df = pd.concat(sim_dfs)
    # for debugging/evaluation, output top similarity clusters
    top_sim_df.to_excel("TopSimilarityClusters.xlsx", index=False)

    # for each small clus2 value, get the most-similar clus1 value
    # this creates a list of pairs of cluster values to reassign
    clus_pairs = []
    for clus2, df in top_sim_df[top_sim_df.clus1 != top_sim_df.clus2].groupby('clus2'):
        clus_pairs.append(df.sort_values('interclus_mean_sim').iloc[-1:])
    # sort so hierarchical reassignment works: if merge pairs are a->b and b->c, have to do a->b first
    if len(clus_pairs) > 0:
        pairs_df = pd.concat(clus_pairs).sort_values(['size2', 'size1'])
        # for evaluation, output cluster pairs that will be merged
        print("Merge Clusters")
        print(pairs_df)

        # reassign clusters
        # first remove old cluster properties
        dropcol = [col for col in ['InterclusterFraction', 'ClusterDiversity',
                                   'ClusterBridging', 'ClusterCentrality'] if col in nodes_df]
        nodes_df = nodes_df.drop(columns=dropcol)
        # then reassign small clusters to most similar large clusters
        for idx, row in pairs_df.iterrows():
            mask = nodes_df['Cluster'] == row['clus2']
            nodes_df.loc[mask, 'Cluster'] = row['clus1']

    return nodes_df


def add_clustering(nodesdf, linksdf=None, nw=None, sims=None, params=ClusteringParams()):
    if nw is None:
        nw = bn.buildNetworkX(linksdf)
    if isinstance(nw, nx.DiGraph):
        nw = nx.Graph(nw)
    if params.method == 'leiden':
        clusters = addLeidenClusters(nodesdf, nw, prefix=params.name_prefix, resolution=params.resolution)
    elif params.method == 'louvain':
        addLouvainClusters(nodesdf, nw, prefix=params.name_prefix)
        clusters = [params.name_prefix]
    else:
        return
    if sims is not None and params.merge_tiny:
        nodesdf = reassign_small_clusters(nodesdf, linksdf, sims,
                                          size_ratio=params.reassign_size_ratio,
                                          top_n=params.reassign_top_n,
                                          max_size=params.reassign_max_size,
                                          )
        # recompute cluster metrics
#        add_cluster_metrics(nodesdf, nw, [params.name_prefix])
    return nodesdf, clusters
