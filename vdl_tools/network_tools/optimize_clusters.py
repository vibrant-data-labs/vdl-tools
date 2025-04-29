"""
Test stability of clusters across similar networks and find 'core' stable clusters

1) Build n (50?) networks that each sample 95% of the entities in the full dataset
2) Compute clustering for each network
3) Pick a network as the 'base' network and compute pairwise similarity between
clusters in the base network and clusters in the other 49 networks
4) Identify the highest similarity cluster pairs
5) Track the nodes that are in the most similar cluster

Use this information to identify small, unstable clusters and reassign nodes
to build network with optimized clusters
"""
import pandas as pd
import numpy as np

import vdl_tools.network_tools.network_functions as net


def build_and_cluster(data_df, idx, params, frac=0.95):
    print(f'Building network {idx + 1} linksPer={params.linksPer}')
    keep_cols = ['uid', 'Cluster', 'cluster_name',  # params.clusName,
                 'Degree', 'InterclusterFraction',
                 'ClusterDiversity', 'ClusterBridging', 'ClusterCentrality']
    df_ = data_df.sample(frac=frac) if (idx > 0 and frac < 1) else data_df
    ndf, ldf = net.build_network(df_, params.tag_attr, idf=False, linksPer=params.linksPer,
                                 clus_params=params.clus_params,
                                 blacklist=params.blacklist, minTags=1)
    ndf = ndf[keep_cols]
    ndf = ndf[ndf.Cluster != 'No Cluster']
    clusters = []
    for cl, df in ndf.groupby('Cluster'):
        cl_info = {'cluster': cl,
                   'size': len(df),
                   'node_ids': list(df['uid'].values),
                   'name': df.cluster_name.iloc[0],
                   # 'cl_name': df[params.clusName].iloc[0]
                   }
        clusters.append(cl_info)
    # on first iteration, return network nodes and links as well as the original clustering data
    if idx == 0:
        return ndf, ldf, clusters
    return clusters


# compare two lists
def _jaccard_sim(l1, l2):
    s1 = set(l1)
    s2 = set(l2)
    return len(s1 & s2) / len(s1 | s2)


# compare two lists, normalized by max possible sim
def _normalized_sim(l1, l2):
    n1 = len(l1)
    n2 = len(l2)
    return _jaccard_sim(l1, l2) * max(n1, n2) / min(n1, n2)


# return an array of length l1 that has element i equal 1 if element i is in l2
def _overlap(l1, l2):
    s2 = set(l2)
    return np.array([v in s2 for v in l1], dtype=int)


def _run_cluster_randomization(df, params, n_iter=100):
    # build the ensemble of networks
    print(f'Building ensemble of networks for linksPer={params.linksPer}')
    # all_results = Parallel(n_jobs=10)(
    #    delayed(build_and_cluster)(
    #        df, linksPer, idx, del_list=del_list,
    #        tag_attr=tag_attr, label_col=label_col,
    #        finalNodeAttrs=finalNodeAttrs, clusName=clusName)
    #    for idx in range(n_iter + 1)
    # )
    # save the non-parallel version for debugging
    all_results = [build_and_cluster(df, idx, params) for idx in range(n_iter + 1)]
    # first iteration includes nodes and links as well as cluster info
    ndf = all_results[0][0]
    ldf = all_results[0][1]
    all_results[0] = all_results[0][2]
    return ndf, ldf, all_results


def _process_randomization_results(all_clusters, df, label_col):
    print("Processing results")
    # compare the first (full) network to each other (95% sampled) networks
    # first value is the target network with all nodes
    n1 = all_clusters[0]
    # get original cluster of each entity
    orig_data = []
    for idx, cl_info in enumerate(n1):
        cl_info['counts'] = np.zeros(len(cl_info['node_ids']))
        cl_info['all_counts'] = np.zeros(len(df))
        cl_info['all_counts'] += df.uid.isin(cl_info['node_ids'])
        cl_info['size_ratio'] = 0
        cl_df = pd.DataFrame({'uid': cl_info['node_ids']})
        cl_df['orig_cluster_idx'] = idx
        cl_df['orig_cluster_name'] = cl_info['name']
        cl_df['orig_cluster'] = cl_info['cluster']
        orig_data.append(cl_df)
    orig_df = pd.concat(orig_data).reset_index(drop=True)

    # get max similarity between nodes in randomized cluster and nodes in target network cluster
    all_max_sim = []
    # get max sim normalized by max possible sim given size difference between cluster
    all_max_norm_sim = []
    # iterate through the subsampled networks
    for n2 in all_clusters[1:]:
        max_sim = {}
        max_norm_sim = {}
        # for each cluster in n1
        for cl_info1 in n1:
            cl1 = cl_info1['cluster']
            # get the similarity to each cluster in n2
            sims = np.array(
                [_jaccard_sim(cl_info1['node_ids'], cl_info2['node_ids']) for cl_info2 in n2])
            # find the highest similarity
            max_sim[cl1] = sims.max()
            # find the cluster in n2 that this n1 cluster it is most similar to
            sim_idx = sims.argmax()
            cl_info2 = n2[sim_idx]
            # get the normalized sim of the most similar cluster
            max_norm_sim[cl1] = _normalized_sim(
                cl_info1['node_ids'], cl_info2['node_ids'])
            # and then find the nodes that overlap between the two clusters
            cl_info1['counts'] += _overlap(cl_info1['node_ids'],
                                           cl_info2['node_ids'])
            # accumlate counts of all nodes in best-matching clusters
            cl_info1['all_counts'] += df.uid.isin(cl_info2['node_ids'])
            cl_info1['size_ratio'] += len(cl_info1['node_ids']) / \
                (len(cl_info2['node_ids']) * (len(all_clusters) - 1))
        all_max_sim.append(max_sim)
        all_max_norm_sim.append(max_norm_sim)
    # get data on node overlap between best-matching clusters
    sdf = pd.DataFrame(all_max_sim)
    # count nodes in the 'target' network
    ndf = pd.DataFrame(all_max_norm_sim)

    # build dataframe of info about each cluster in the original netowrk
    nw_info = [{'cluster': cl['cluster'],
                'cl_name': cl['name'],
                'size': cl['size'],
                'size_ratio': cl['size_ratio'],
                'mean_count': cl['counts'].mean()} for cl in n1]
    nw_df = pd.DataFrame(nw_info).set_index('cluster')
    nw_df['mean_max_sim'] = sdf.mean(axis=0)
    nw_df['stddev_max_sim'] = sdf.std(axis=0)
    nw_df['mean_max_norm_sim'] = ndf.mean(axis=0)
    nw_df['stddev_max_norm_sim'] = ndf.std(axis=0)
    nw_df.reset_index(inplace=True)

    # get data on all nodes in best matching clusters
    all_cl_counts = [cl['all_counts'] for cl in n1]
    # get counts of each node in each master cluster
    all_cl_counts = np.stack(all_cl_counts)
    entity_probs = all_cl_counts / np.clip(all_cl_counts.sum(axis=0), 1, None)

    # get cluster index order and max, 2nd highest prob cluster for each entity
    entity_cl_idx_order = entity_probs.argsort(axis=0)
    entity_cl_idx_max = entity_cl_idx_order[-1, :]
    entity_cl_idx_max_2 = entity_cl_idx_order[-2, :]

    max_probs = entity_probs[entity_cl_idx_max,
                             np.arange(len(entity_cl_idx_max))]
    max_probs_2 = entity_probs[entity_cl_idx_max_2,
                               np.arange(len(entity_cl_idx_max))]

    edf = pd.DataFrame(entity_probs)
    h_ent = (-np.log(edf) * edf).fillna(0).sum(axis=0)
    # for each entity, save most probable and next most probable cluster
    entity_df = pd.DataFrame({label_col: df[label_col].values,
                              'uid': df.uid.values,
                              'max_cluster': entity_cl_idx_max,
                              'alt_cluster': entity_cl_idx_max_2,
                              'max_cluster_prob': max_probs,
                              'alt_cluster_prob': max_probs_2,
                              'entropy': h_ent
                              })
    entity_df['cluster'] = entity_df.max_cluster.map(nw_df['cluster'])
    entity_df['cl_name'] = entity_df.max_cluster.map(nw_df['cl_name'])
    entity_df['cluster_2'] = entity_df.alt_cluster.map(nw_df['cluster'])
    entity_df['cl_name_2'] = entity_df.alt_cluster.map(nw_df['cl_name'])
    entity_df = entity_df.merge(orig_df, on='uid', how='outer')
    # set true if entity most common cluster is different from original cluster
    entity_df['reclustered'] = entity_df.cluster != entity_df.orig_cluster
    return (entity_df, nw_df, entity_probs, n1)


def get_small_cluster_nodes(nw_clus, entity_df, clus_sz_thr=10):
    # get all nodes in initial clusters with <= 10 nodes
    small_cl = [{'cluster': cl['cluster'],
                 'cl_name': cl['name'],
                 # 'name': cl['nodes'],
                 'uid': cl['node_ids']} for cl in nw_clus if cl['size'] <= clus_sz_thr]
    if len(small_cl) > 0:
        small_df = pd.DataFrame(small_cl).explode('uid').reset_index(drop=True)
        return small_df[['uid']].merge(entity_df, on='uid', how='inner')
    return None


def analyze_clusters(df, params, n_iter):
    ndf, ldf, all_clusters = _run_cluster_randomization(df, params, n_iter=n_iter)
    entity_df, nw_df, entity_probs, nw_clus = _process_randomization_results(all_clusters, df, params.labelcol)
    return ndf, ldf, all_clusters, entity_df, nw_df, entity_probs, nw_clus


def build_optimized_network(df, params, n_iter=10):
    ndf, ldf, all_clusters, entity_df, nw_df, entity_probs, nw_clus = analyze_clusters(df, params, n_iter)
    if params.min_clus_size > 0:
        # get nodes that are members of small clusters
        sdf = get_small_cluster_nodes(nw_clus, entity_df, clus_sz_thr=params.min_clus_size)
        if sdf is not None:
            # re-assign nodes in small cluster to their next-most likely cluster
            print('Reassigning nodes in small clusters')
            # filter nodes to reassign - do not reassign if new assignment is to a small cluster
            reassign_df = sdf[sdf.alt_cluster_prob > 0]
            print("\nInitial cluster sizes:")
            print(f"{ndf.Cluster.value_counts()}")
            ndf.set_index('uid', inplace=True)
            # don't reassign nodes that are reassigned-to
            assigned_to = reassign_df.cluster_2.unique()
            reassign_df = reassign_df[~reassign_df.cluster.isin(assigned_to)]
            # do the cluster re-assignment
            reassign_df.set_index('uid', inplace=True)
            ndf.loc[reassign_df.index, 'Cluster'] = reassign_df.cluster_2
            ndf.loc[reassign_df.index, 'cluster_name'] = reassign_df.cl_name_2
            ndf = pd.concat([ndf, df.set_index('uid', drop=True)], axis=1).reset_index()
            ndf['id'] = range(len(ndf))
            ndf[params.tag_attr + '_list'] = ndf[params.tag_attr].apply(lambda x: x.split('|'))
    print("\nFinal cluster sizes:")
    print(f"{ndf.Cluster.value_counts()}")
    return ndf, ldf
