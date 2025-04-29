# -*- coding: utf-8 -*-

import networkx as nx
from collections import defaultdict
import pandas as pd
from vdl_tools.network_tools.trophiclevel import rootedTL
from vdl_tools.tag2network.Network import BuildNetwork as bn  # for build network and layout functions
# from tag2network.Network import ClusterLayout as cl   # new cluster layout function
# from tag2network.Network.BuildNetwork import addLouvainClusters  # directed louvain
from vdl_tools.tag2network.Network.ClusteringProperties import basicClusteringProperties
from vdl_tools.tag2network.Network.DrawNetwork import draw_network_categorical
from vdl_tools.tag2network.Network import ComputeClustering as cc

# ---------------------------------------------
# Custom Metrics
# ---------------------------------------------


def buildNetworkX(linksdf, id1='Source', id2='Target', directed=True):
    # build networkX graph object from links dataframe with 'Source' and 'Target' ids
    # generate list of links from dataframe
    linkdata = [(getattr(link, id1), getattr(link, id2)) for link in linksdf.itertuples()]
    g = nx.DiGraph() if directed else nx.Graph()
    g.add_edges_from(linkdata)
    return g


# compute number of second degree outgoing neighbors
def outoutdegree(nw):
    outout = {n: set() for n in nw.nodes()}
    for n in nw.nodes():
        for n2 in nw.successors(n):
            outout[n].update(nw.successors(n2))
    return {n: len(outout[n]) for n in nw.nodes()}


# compute number of second degree incoming neighbors
def inindegree(nw):
    inin = {n: set() for n in nw.nodes()}
    for n in nw.nodes():
        for n2 in nw.predecessors(n):
            inin[n].update(nw.predecessors(n2))
    return {n: len(inin[n]) for n in nw.nodes()}


# summarize fraction of positive incoming and outgoung links
# beware that in_edges and out_edges return data in different formats, looks like a networkx bug
def fracoutpositive(nw):
    result = {}
    for n in nw.nodes():
        e = list(nw.out_edges(n, data='ispos'))
        if len(e) > 0:
            ispos = list(zip(*e))[2]  # *e zips element by element [(A,B,1), (X,Y,2)] >>  [(A,X), (B,Y), (1,2)]
            result[n] = sum(ispos)/float(len(ispos))
        else:
            result[n] = 0
    return result


def fracinpositive(nw):
    result = {}
    for n in nw.nodes():
        e = list(nw.in_edges(n, data='ispos'))
        if len(e) > 0:
            ispos = list(zip(*e))[2]
            result[n] = sum(ispos)/float(len(ispos))
        else:
            result[n] = 0
    return result


def fracOutNeg(nw):
    result = {}
    for n in nw.nodes():
        e = list(nw.out_edges(n, data='isneg'))
        if len(e) > 0:
            isneg = list(zip(*e))[2]  # *e zips element by element [(A,B,1), (X,Y,2)] >>  [(A,X), (B,Y), (1,2)]
            result[n] = sum(isneg)/float(len(isneg))
        else:
            result[n] = 0
    return result


def fracInNeg(nw):
    result = {}
    for n in nw.nodes():
        e = list(nw.in_edges(n, data='isneg'))
        if len(e) > 0:
            isneg = list(zip(*e))[2]
            result[n] = sum(isneg)/float(len(isneg))
        else:
            result[n] = 0
    return result


def avg_votes(nw):
    result = {}
    for n in nw.nodes():
        e = list(nw.edges(n, data='votes'))
        if len(e) > 0:
            votes = list(zip(*e))[2]
            result[n] = sum(votes)/float(len(votes))
        else:
            result[n] = 0
    return result


def add_cluster_metrics(nodesdf, nw, groupVars):
    # add bridging, cluster centrality etc. for one or more grouping variables
    for groupVar in groupVars:
        if len(nx.get_node_attributes(nw, groupVar)) == 0:
            vals = {k: v for k, v in dict(zip(nodesdf['id'], nodesdf[groupVar])).items() if k in nw}
            nx.set_node_attributes(nw, vals, groupVar)
        grpprop = basicClusteringProperties(nw, groupVar)
        for prop, vals in grpprop.items():
            nodesdf[prop] = nodesdf['id'].map(vals)


def _jaccardianSim(nw, identicalThresh=0.3, deleteIdentical=False):
    print("Computing similarities")

    def linkSim(nw, n1, n2):
        incoming1 = set([p for p in nw.predecessors(n1)])
        incoming2 = set([p for p in nw.predecessors(n2)])
        outgoing1 = set([s for s in nw.successors(n1)])
        outgoing2 = set([s for s in nw.successors(n2)])
        in_int = len(incoming1.intersection(incoming2))
        out_int = len(outgoing1.intersection(outgoing2))
        in_union = len(incoming1.union(incoming2))
        out_union = len(outgoing1.union(outgoing2))
        return float(in_int+out_int)/(in_union+out_union)

    identical = []
    sims = []
    nwnodes = list(nw.nodes())
    for i, n1 in enumerate(nwnodes):
        for j in range(i+1, len(nwnodes)):
            n2 = nwnodes[j]
            sim = linkSim(nw, n1, n2)
            if sim >= identicalThresh:
                identical.append((n1, n2))
            sims.append((n1, n2, sim))

    if deleteIdentical:
        merged = defaultdict(list)
        for (n1, n2) in identical:
            delnode = max(n1, n2)
            if delnode in nw:
                nw.remove_node(delnode)
                merged[n1].append(n2)
                print("Merged %d %d" % (n1, n2))

    return sims


def compute_node_pair_similarities(nw, id2labelDict, deleteIdentical=False):
    # used externally in Kumu causal network code
    # calcualte similarity between all pairs of nodes in their incomign and outgoing links
    # returns dataframe sorty by most similar node pairs
    sims = _jaccardianSim(nw, deleteIdentical=False)
    simdf = pd.DataFrame(sims, columns=['id_1', 'id_2', 'jaccard_similarity'])
    simdf['label_1'] = simdf['id_1'].map(id2labelDict)
    simdf['label_2'] = simdf['id_2'].map(id2labelDict)
    simdf['avg_frac_overlap'] = simdf['jaccard_similarity'].apply(lambda x: (2*x)/(1+x))
    simdf.sort_values('jaccard_similarity', ascending=False, inplace=True)
    return simdf


def _map_node_labels_to_id(ndf, nodeLabel='Label'):
    # add numeric ids
    label2id_Dict = dict(zip(ndf[nodeLabel], ndf['id']))
    id2label_Dict = dict(zip(ndf['id'], ndf[nodeLabel]))
    return label2id_Dict, id2label_Dict


def find_similar_nodes(ndf, ldf,
                       simfile,  # output filename
                       nodeLabel='label'
                       ):
    nw = buildNetworkX(ldf, id1='Source', id2='Target', directed=True)
    # calculate node similarities in incoming and outgoing links
    label2id_Dict, id2label_Dict = _map_node_labels_to_id(ndf, nodeLabel=nodeLabel)  # get id>>label mapping
    df_node_sims = compute_node_pair_similarities(nw, id2label_Dict)
    df_node_sims.to_excel(simfile, index=False)


def keystone_influences_overlap(ndf, ldf,
                                Keystone,  # column name for boolean top keystones
                                thresh=2   # keep if influence at least thresh keystones (if 1 = all influences)
                                ):
    # Keystone: boolean for which nodes are Keystone
    # find all root causes that are direct influenes to more at least n keystone nodes
    keystones = ndf[ndf[Keystone]]['id'].tolist()
    df_to_keystones = ldf[ldf['Target'].isin(keystones)][['Source', 'Target', 'fromName', 'toName']]
    from_counts = df_to_keystones.groupby('fromName')['Source'].agg('count').reset_index()
    df_overlap = from_counts[from_counts.Source >= thresh]
    df_overlap.columns = ['fromName', 'count']
    df_overlap = df_overlap.sort_values(['count'], ascending=False)

    return df_overlap  # dataframe of From nodes that occur more than once


def add_percentile(df, col):
    # returns a series
    return df[col].rank(method='max').apply(lambda x: 100*(x-1)/(len(df)-1))  # percentile linear interpolation (0-100)


def min_max_normalize_column(df, col):
    return (df[col] - df[col].min()) / (df[col].max() - df[col].min())


def min_max_normalize_series(series):
    # same as above but for a series not dataframe
    return (series - series.min()) / (series.max() - series.min())


def keystone_index(df, reach='2_Degree_Reach', leverage='2_Degree_Leverage'):
    '''
    Description: scale leverage and reach from 0 to 1, then multiply
    Returns: series
    '''
    reach_normalized = min_max_normalize_column(df, reach)
    leverage_normalized = min_max_normalize_column(df, leverage)
    keystone = reach_normalized * leverage_normalized
    return keystone  # series


def keystone(nw, ndf, id='id'):
    '''
    nw: networkX object
    ndf: pandas dataframe of nodes with node metadata
    id: string: name of column in ndf that is the node id

    this function calculates a 'keystone index' as
    high outgoing 2 degree reach weighted by outgoing reach.
    so it is a node with very few incoming controls but it reaches a high fraction of the network in 2 hops.
    '''
    ndf['outout_degree'] = ndf['id'].map(outoutdegree(nw))
    ndf['inin_degree'] = ndf['id'].map(inindegree(nw))
    ndf['outout_reach'] = (ndf['outout_degree']/float(len(ndf)))*100  # % of network reached in 2 hops
    ndf['outout_inin_leverage'] = ((ndf['outout_degree'] - ndf['inin_degree']) /
                                   (ndf['outout_degree'] + ndf['inin_degree']))  # normalized difference of outout vs inin
    reach_normalized = min_max_normalize_series(ndf['outout_reach'])  # normalize 0-1
    leverage_normalized = min_max_normalize_column(ndf['outout_inin_leverage'])  # normalize 0-1
    ndf['keystone index'] = reach_normalized * leverage_normalized
    return ndf['keystone index']


def trophic_level_normalized(ndf, nw):
    '''
    DESCRIPTION. compute rooted trophic level - root node added to bottom and connected to all to address looping.
    Then normalized the values from 0-1
    Returns: series
    '''
    ndf['Trophic_Level'] = ndf['id'].map(rootedTL(nw))  # rooted trophic level (root node added to base to deal with looping
    ndf['Trophic_Level'] = min_max_normalize_column(ndf, 'Trophic_Level')  # normalize 0-1
    return ndf['Trophic_Level']


# #####################################
# ## network level stats
def compute_connectance(ndf, ldf):
    possible_links = len(ndf)*(len(ndf)-1)  # possible links
    realized_links = len(ldf)  # links with consensus votes
    return realized_links/possible_links


def compute_frac_isolated_nodes(ndf):
    n_isolated = sum(ndf['total_links'] == 0)
    return n_isolated/len(ndf)  # frac isolated


# #######################
# ## Layouts coordinates


def plot_network(ndf, edf, plot_name, x='x_tsne', y='y_tsne', colorBy='Cluster', sizeBy='Keystone_Index', sizeScale=100):
    # draw network colored by creative style and save image
    nw = bn.buildNetworkX(edf)
    node_sizes = ndf.loc[:, sizeBy] * sizeScale
    node_sizes_array = node_sizes.values  # convert sizeBy col to array for sizing
    draw_network_categorical(nw, ndf, node_attr=colorBy, plotfile=plot_name, x=x, y=y, node_size=node_sizes_array)


def add_network_metrics(nw, ndf, ldf,
                        sign=True,  # links have pos vs neg sign
                        add_trophic_level=True,  # add rooted trophic level
                        add_clusters=True,  # add directed louvain clusters
                        ):
    # ---------------------------------------------
    # add metrics to node metadata
    # ---------------------------------------------
    print("Calculating network metrics")
    # Add standard metrics
    ndf['total_links'] = ndf['id'].map(dict(nx.degree(nw)))  # calculate metric and map to column in dataframe
    ndf['betweenness'] = ndf['id'].map(dict(nx.betweenness_centrality(nw)))
    ndf['in_degree'] = ndf['id'].map(dict(nw.in_degree()))   # note that in and out degree have diff format that the previous
    ndf['out_degree'] = ndf['id'].map(dict(nw.out_degree()))

    # Add 'leverage' metrics
    ndf['outout_degree'] = ndf['id'].map(outoutdegree(nw))
    ndf['inin_degree'] = ndf['id'].map(inindegree(nw))
    ndf['2_Degree_Asymmetry'] = ((ndf['outout_degree'] - ndf['inin_degree'])/(ndf['outout_degree'] + ndf['inin_degree']))
    ndf['1_Degree_Asymmetry'] = ((ndf['out_degree'] - ndf['in_degree']) /
                                 (ndf['out_degree'] + ndf['in_degree']))  # normalized diff out vs in
    ndf['2_Degree_Reach'] = (ndf['outout_degree']/float(len(ndf))) * 100  # % of network reached in 2 hops
    ndf['Keystone_Index'] = keystone_index(ndf, reach='2_Degree_Reach', leverage='2_Degree_Asymmetry')
    ndf['Keystone_Pctl'] = add_percentile(ndf, 'Keystone_Index')

    ndf.fillna(0, inplace=True)

    if add_trophic_level:
        # Add trophic level - scaled 0-1 (rooting the network to a basal node)
        ndf['Trophic_Level'] = trophic_level_normalized(ndf, nw)

    if sign:
        # Add summaries of frac positive out and in and avg votes
        ndf['frac_negative_out'] = ndf['id'].map(fracOutNeg(nw))
        ndf['frac_negative_in'] = ndf['id'].map(fracInNeg(nw))
        ndf['avg_LinkVotes'] = ndf['id'].map(avg_votes(nw))

    # Add louvain clusters with directed modularity (from Tag2Network)
    if add_clusters:
        cc.addLouvainClusters(ndf, nw)
        add_cluster_metrics(ndf, nw, ['Cluster'])
