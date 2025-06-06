# -*- coding: utf-8 -*-

import numpy as np
# from scipy import identity
from scipy.linalg import inv
import networkx as nx


# computes trophic level and returns dict of {nodeId:TLValue}
def computeTL(network):
    if not isinstance(network, nx.DiGraph):
        return None
    n = len(network.nodes)
    a = nx.adjacency_matrix(network).astype('float64')
    # normalize the adjacency matrix (divide each row by row sum)
    asum = a.sum(axis=1)
    asum[asum == 0] = 1     # set zeros to 1 so division works (0 / 1 = 0)
    b = a / asum[:, np.newaxis]
    try:
        # use normalized matrix b in tl computation
        # m = identity(n) - b
        m = np.identity(n) - b
        mInv = inv(m)
        tl = mInv.sum(axis=1)
        # return results as {node, tl}
        return dict(zip(network.nodes(), tl))
    except:
        return None


def rootedTL(nw):
    revnw = nw.reverse()
    rootNode = "rootNode"
    for n in list(revnw.nodes()):
        revnw.add_edge(n, rootNode)
    tls = computeTL(revnw)
    del tls[rootNode]
    mn = min(tls.values()) - 1.0
    return {n: (tls[n]-mn) for n in nw.nodes()} if tls is not None else {}


def min_max_normalize_column(df, col):
    return (df[col] - df[col].min()) / (df[col].max() - df[col].min())


def trophic_level_normalized(ndf, nw):
    '''
    DESCRIPTION. compute rooted trophic level - root node added to bottom and connected to all to address looping.
    Then normalized the values from 0-1
    Returns: series
    '''
    # rooted trophic level (root node added to base to deal with looping)
    ndf['Trophic_Level_rooted'] = ndf['id'].map(rootedTL(nw))
    ndf['Trophic_Level_rooted'] = min_max_normalize_column(ndf, 'Trophic_Level')  # normalize 0-1
    return ndf['Trophic_Level_rooted']
