# -*- coding: utf-8 -*-
#
#
# Build network from similarity matrix by thresholding
# or from matrix of possibly weighted connections
#
# Add data to nodes - cluster, netowrk proerties, layout coordinates
#

from dataclasses import dataclass, field
import numpy as np
import pandas as pd
import math
from collections import defaultdict
from collections import Counter
from scipy.sparse import dok_matrix
from scipy.sparse import csr_matrix
import networkx as nx

from vdl_tools.tag2network.Network.ClusteringParams import ClusteringParams
from vdl_tools.tag2network.Network import ClusteringProperties as cp
from vdl_tools.tag2network.Network import LayoutNetwork as ln
from vdl_tools.tag2network.Network import ComputeClustering as cc


@dataclass
class BuildNWParams:
    tag_attr: str = None  # tag col for linking
    linksPer: float = 1  # links per node
    blacklist: list = field(default_factory=list)  # tags to blacklist for linking
    nw_name: str = None  # final filename for network
    labelcol: str = 'Name'   # attribute to use for mappr player node labels
    clusName: str = "Keyword Theme"  # name of clusters
    size_attr: str = None  # "Relative Funding" - if None, then sized by number of nodes
    n_tags: int = 3  # number of tags to keep in cluster names
    minTags: int = 1  # minimum number of tags an entity must have
    removeSingletons: bool = True  # remove tags that only occur in one entity (these can't inform linking)
    finalNodeAttrs: list = None  # custom list of final columns, if None keep all
    # todo check if these are ever used
    tagcols_nodata: list = field(default_factory=list)  # tag columns to replace empty with 'no data'
    add_nodata: bool = False  # for columns in tagcols_nodata, add the words 'no data' when nan
    layout_params: object = field(default_factory=ln.ClusterLayoutParams)  # None for no layout
    clus_params: object = field(default_factory=ClusteringParams)


@dataclass
class BuildEmbeddingNWParams:
    linksPer: float = 6  # links per node
    labelcol: str = None   # attribute to use for mappr player node labels
    clusName: str = "ClusterName"  # name of clusters
    n_tags: int = 5  # number of tags to keep in cluster names
    uid: str = 'uid'
    textcol: str = "Summary"
    nw_name: str = None
    tagcols_nodata: list = field(default_factory=list)  # tag columns to replace empty with 'no data'
    add_nodata: bool = False  # for columns in tagcols_nodata, add the words 'no data' when nan
    layout_params: object = field(default_factory=ln.ClusterLayoutParams)  # None for no layout
    clus_params: object = field(default_factory=ClusteringParams)


@dataclass
class BuildSimNWParams:
    linksPer: float = 1  # links per node
    labelcol: str = 'Name'   # attribute to use for mappr player node labels
    clusName: str = "ClusterName"  # name of clusters
    n_tags: int = 5  # number of tags to keep in cluster names
    layout_params: object = field(default_factory=ln.ClusterLayoutParams)  # None for no layout
    clus_params: object = field(default_factory=ClusteringParams)


def save_network_to_csv(nodes_df, edges_df, nodesfile, edgesfile):
    print("Writing nodes and edges to files")
    nodes_df.to_csv(nodesfile, index=False)
    edges_df.to_csv(edgesfile, index=False)


def save_network_to_excel(nodes_df, edges_df, outfile):
    print("Writing network to file")
    writer = pd.ExcelWriter(outfile)
    nodes_df.to_excel(writer, 'Nodes', index=False)
    edges_df.to_excel(writer, 'Links', index=False)
    writer.close()


def remove_singleton_tags(df, taglist_attr):
    # remove singleton tags and return tag lists without weights
    print("Removing singleton tags")
    # remove singleton tags
    # across entire dataset, count tag hist and remove singleton tags
    tags = df[taglist_attr].apply(lambda x: [val[0] for val in x])
    # build master histogram of tags that occur at least twice
    tagHist = dict([item for item in
                    Counter([k for kwList in tags for k in kwList]).most_common() if item[1] > 1])
    # filter tags to only include 'active' tags - tags which occur twice or more in the entire dataset
    df[taglist_attr] = df[taglist_attr].apply(lambda x: [k[0] for k in x if k[0] in tagHist])
    # double check to remove spaces and empty elements
    # df[taglist_attr] = df[taglist_attr].apply(lambda x: [(s[0].strip(), s[1]) for s in x if len(s[0]) > 0])


def prepare_tag_data(df, params):
    taglist_attr = params.tag_attr + '_stripped'
    # remove keywords in blacklist
    df[taglist_attr] = df[params.tag_attr].apply(lambda x: [s for s in x if s[0] not in params.blacklist])
    if params.removeSingletons:
        remove_singleton_tags(df, taglist_attr)
    return taglist_attr


# build sparse feature matrix with optional idf weighting
# each row is a document, each column is a tag
# weighting assumes each term occurs once in each doc it appears in
def build_features(taglists, tagHist, idf):
    allTags = tagHist.keys()
    # build tag-index mapping
    tagIdx = dict(zip(allTags, range(len(allTags))))
    # build feature matrix
    print("Build feature matrix")
    nDoc = len(taglists)
    features = dok_matrix((nDoc, len(tagIdx)), dtype=float)
    row = 0
    for tagList in taglists:
        tagList = [k for k in tagList if k in tagIdx]
        if len(tagList) > 0:
            for tag in tagList:
                if idf:
                    docFreq = tagHist[tag]
                    features[row, tagIdx[tag]] = math.log(nDoc/float(docFreq), 2.0)
                else:
                    features[row, tagIdx[tag]] = 1.0
        else:
            print("Document with no tags")
        row += 1
    return csr_matrix(features)


# compute cosine similarity
# f is a (sparse) feature matrix
def simCosine(f):
    # compute feature matrix dot product
    fdot = np.array(f.dot(f.T).todense())
    # get inverse feature vector magnitudes
    invMag = np.sqrt(np.divide(1.0, np.diag(fdot)))
    # set NaN to zero
    invMag[np.isinf(invMag)] = 0
    # get cosine sim by elementwise multiply by inverse magnitudes
    sim = (fdot * invMag).T * invMag
    return sim


def threshold(sim, linksPer=4, connect_isolated_pairs=True):
    '''
    threshold similarity matrix - threshold by minimum maximum similarity of each node. This leaves at least one
    link per node.  Then if there are too many links, thin links of each node proportional to their abundance,
    but leave at least one link from each node.
    Parameters:
        sim - the similarity matrix
        linksPer - target connectivity
        connect_isolated_pairs - if true, connect isolated reciprocal pairs of nodes to their next
        most similar neighbors
    '''
    if connect_isolated_pairs:
        simvals = sim.copy()    # presesrve original for connecting reciprocal pairs
    sim = sim.copy()        # duplicate original so input matrix is preserved
    nnodes = sim.shape[0]
    targetL = nnodes*linksPer
    # threshold on minimum max similarity in each row, this keeps at least one link per row
    mxsim = sim.max(axis=0)
    thr = mxsim[mxsim > 0].min()
    sim[sim < thr] = 0.0
    nL = (sim > 0).sum()
    # if too many links, keep equal fraction of links in each row,
    # minimally 1 per row, keep highest similarity links
    if nL > targetL:
        # get fraction to keep
        frac = targetL/float(nL)
        # sort the rows
        indices = np.argsort(sim, axis=1)
        # get number of elements in each row
        nonzero = np.round(np.maximum((frac*((sim > 0).sum(axis=1))), 1)).astype(int)
        # get minimum index to keep in each row
        minelement = (nnodes - nonzero).astype(int)
        # in each row, set values below number to keep to zero
        for i in range(nnodes):
            sim[i][indices[i][:minelement[i]]] = 0.0
        if connect_isolated_pairs:
            # for isolated reciprocal pairs, keep next lower similarity link
            # fisrt find all reciprocal pairs
            upper = np.triu(sim)
            recip = np.argwhere((upper > 0) & (np.isclose(upper, np.tril(sim).T, 1e-14)))
            # get isolated reciprocal pairs
            links = sim > 0
            isolated = (links[recip[:, 0]].sum(axis=1) == 1) & (links[recip[:, 1]].sum(axis=1) == 1)
            # get all nodes involved in isolated pairs
            isolated_recip = recip[isolated].flatten()
            # add next most similar link
            sim[isolated_recip, indices[isolated_recip, -2]] = simvals[isolated_recip, indices[isolated_recip, -2]]
    return sim


# build cluster name based on keywords that occur commonly in the cluster
# if wtd, then weigh keywords based on local frequency relative to global freq
def build_cluster_names_from_tags(df, allTagHist, tagAttr, tag_wt_attr=None,
                                  clAttr='Cluster', clusterName='cluster_name',
                                  topTags='top_tags', n_tags=5, wtd=True):

    def merge_tag_weight_tuples(col):
        # Each item has a list of (tag, weight) tuples.
        # Sum and normalize these to get the histogram of tag weights in the cluster
        dd = defaultdict(float)

        def merge_tuples(vals):
            for k, v in vals:
                dd[k] += v
        col.apply(merge_tuples)
        norm = sum(list(dd.values()))
        return [(k, v / norm) for k, v in dd.items()]

    if tag_wt_attr is not None:
        # use (tag, weight) tuples
        allTagHist = dict([item for item in merge_tag_weight_tuples(df[tag_wt_attr])])
        taglists = df[tag_wt_attr].apply(lambda x: [val[0] for val in x])
    else:
        # this function requires tagAttr values are lists, not | delimited strings
        if type(df[tagAttr].iloc[0]) is str:
            taglists = df[tagAttr].str.split('|')
        else:
            taglists = df[tagAttr]
        if allTagHist is None:
            allTagHist = dict([item for item in Counter([k for kwList in taglists
                                                         for k in kwList]).most_common() if item[1] > 1])
    allVals = np.array(list(allTagHist.values()), dtype=float)
    allFreq = dict(zip(allTagHist.keys(), allVals/allVals.sum()))
    clusters = df[clAttr].unique()
    df[clusterName] = ''
    df[topTags] = ''
    clusInfo = []
    for clus in clusters:
        clusRows = df[clAttr] == clus
        nRows = clusRows.sum()
        if nRows > 0:
            if tag_wt_attr is None:
                tagHist = Counter([k for tagList in taglists[clusRows] for k in tagList if k in allTagHist])
            else:
                tagHist = Counter(dict([item for item in merge_tag_weight_tuples(df.loc[clusRows, tag_wt_attr])]))
            if wtd:
                vals = np.array(list(tagHist.values()), dtype=float)
                freq = dict(zip(tagHist.keys(), vals/vals.sum()))
                # weight tags, only include tag if it is more common than global
                wtdTag = [(item[0], item[1]*math.sqrt(freq[item[0]]/allFreq[item[0]]))
                          for item in tagHist.most_common() if freq[item[0]] > allFreq[item[0]]]
                wtdTag.sort(key=lambda x: x[1], reverse=True)
                topTag = [item[0] for item in wtdTag[:10]]
            else:
                topTag = [item[0] for item in tagHist.most_common()][:10]
            # remove unigrams that make up n-grams with n > 1
            topSet = set(topTag)
            removeTag = set()
            for tag in topTag:
                tags = tag.split(' ')
                if len(tags) > 1:
                    for tag in tags:
                        if tag in topSet:
                            removeTag.add(tag)
            topTag = [k for k in topTag if k not in removeTag]
            # build and store name
            clName = ', '.join(topTag[:n_tags])
            df.loc[clusRows, clusterName] = clName
            df.loc[clusRows, topTags] = ', '.join(topTag)
            clusInfo.append((clus, nRows, clName))
    df[topTags] = df[topTags].str.split(',')
    clusInfo.sort(key=lambda x: x[1], reverse=True)
    for info in clusInfo:
        print("Cluster %s, %d nodes, name: %s" % info)


def buildNetworkFromNodesAndEdges(nodesdf, edgesdf, sims=None, directed=True,
                                  layout_params=ln.ClusterLayoutParams(),
                                  clus_params=ClusteringParams()):
    if '__id__' in nodesdf.columns:
        raise(Exception("__id__ column already exists in nodesdf. Please rename or remove it before re-running."))
    nodesdf['__id__'] = range(len(nodesdf))
    # add clusters and attributes
    nw = buildNetworkX(edgesdf, directed=directed)
    if clus_params is not None:
        nodesdf, clusters = cc.add_clustering(nodesdf, nw=nw, sims=sims, params=clus_params)
        addNetworkAttributes(nodesdf, nw=nw, groupVars=clusters)
    else:
        clusters = None
    # add layout
    if layout_params is not None:
        ln.add_layout(nodesdf, nw=nw, params=layout_params)
    return nodesdf, edgesdf, clusters


# build link dataframe from matrix where non-zero element is a link
def matrixToLinkDataFrame(mat, undirected=False, include_weights=True):
    if undirected:  # make symmetric then take upper triangle
        mat = np.triu(np.maximum(mat, mat.T))
    links = np.transpose(np.nonzero(mat))
    if include_weights:
        linkList = [{'Source': li[0], 'Target': li[1], 'weight': mat[li[0], li[1]]} for li in links]
    else:
        linkList = [{'Source': li[0], 'Target': li[1]} for li in links]
    return pd.DataFrame(linkList)


def buildNetworkX(linksdf, id1='Source', id2='Target', directed=False):
    linkdata = [(getattr(link, id1), getattr(link, id2)) for link in linksdf.itertuples()]
    g = nx.DiGraph() if directed else nx.Graph()
    g.add_edges_from(linkdata)
    return g


# add a computed network attribute to the node attribute table
#
def add_network_attr(nodesdf, attr, vals):
    nodesdf[attr] = nodesdf.index.map(vals).values


# add network structural attributes to nodesdf
# clusVar is the attribute to use for computing bridging etc
def addNetworkAttributes(nodesdf, linksdf=None, nw=None, groupVars=["Cluster"]):
    if nw is None:
        nw = buildNetworkX(linksdf)
    if type(nw) is nx.DiGraph:
        add_network_attr(nodesdf, "InDegree", dict(nw.in_degree()))
        add_network_attr(nodesdf, "OutDegree", dict(nw.out_degree()))
    add_network_attr(nodesdf, "Degree", dict(nw.degree()))

    # add bridging, cluster centrality etc. for one or more grouping variables
    for groupVar in groupVars:
        if len(nx.get_node_attributes(nw, groupVar)) == 0:
            vals = {k: v for k, v in dict(zip(nodesdf.index, nodesdf[groupVar])).items() if k in nw}
            nx.set_node_attributes(nw, vals, groupVar)
        grpprop = cp.basicClusteringProperties(nw, groupVar)
        for prop, vals in grpprop.items():
            add_network_attr(nodesdf, prop, vals)
        # add counts, use Degree since it was just added so must be in the ddataframe
        nodesdf[f'{groupVar}_count'] = nodesdf.groupby([groupVar])['Degree'].transform('count')


#################################################################################
# main build network functions, one to build a tag network,
# the other to build a network from a similarity matrix

# build network given node dataframe and similarity matrix
# if layout is in params run layout
# optionally pass in tag histogram for cluster naming
# thresholding can create asymmetry in links so network is directed
#
def buildSimilarityNetwork(df, sims, params):
    # add node label
    if params.labelcol is not None and params.labelcol in df:
        df['label'] = df[params.labelcol]
    # threshold
    if params.linksPer > 0:
        print("Threshold similarity")
        thr_sims = threshold(sims, linksPer=params.linksPer)
    # make edge dataframe
    edgesdf = matrixToLinkDataFrame(thr_sims)

    return buildNetworkFromNodesAndEdges(df, edgesdf, sims=sims, directed=True,
                                         layout_params=params.layout_params, clus_params=params.clus_params)


#
# build network, linking based on common tags
# tag lists in column named tagAttr 
# adds clusters and other attributes
# doLayout - if true, run layout
# return nodesdf, edgedf
def buildTagNetwork(df, params, dropCols=[], idf=False):
    df = df.copy()  # so passed-in dataframe is not altered, not a recursive deep copy so list elements aren't copied
    taglist_attr = prepare_tag_data(df, params)
    print("Building tag/keyword network")
    # make histogram of tag frequencies, only include tags with > 1 occurence
    tagHist = dict([item for item in Counter([k for kwList in df[taglist_attr]
                                              for k in kwList]).most_common() if item[1] > 1])
    # filter tags to only include 'active' tags - tags which occur twice or more in the doc set
    # df[taglist_attr] = df[taglist_attr].apply(lambda x: [k for k in x if k in tagHist])
    # filter docs to only include docs with a minimum number of 'active' tags
    # TODO: @rich - consider making this optional?
    df = df[df[taglist_attr].apply(lambda x: len(x) >= params.minTags)].reset_index(drop=True)
    # build document-keywords feature matrix
    features = build_features(df[taglist_attr], tagHist, idf)
    # compute similarity
    print("Compute similarity")
    sim = simCosine(features)
    # avoid self-links
    np.fill_diagonal(sim, 0)
    df.drop(dropCols, axis=1, inplace=True)
    # build similarity network with clustering and layout
    nodesdf, edgesdf, clusters = buildSimilarityNetwork(df, sim, params)
    # add cluster names from tag histogram at each cluster level
    if clusters is not None:
        for idx, clus in enumerate(clusters):
            clus_name = params.clusName if idx == 0 else f"{params.clusName}_L{idx + 1}"
            build_cluster_names_from_tags(nodesdf, tagHist, taglist_attr, n_tags=params.n_tags,
                                          clAttr=clus, clusterName=clus_name)
    nodesdf.drop(columns=[taglist_attr], inplace=True)
    return nodesdf, edgesdf
