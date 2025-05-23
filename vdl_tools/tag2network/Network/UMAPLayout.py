
import umap
from vdl_tools.tag2network.Network.tSNELayout import setup_layout_distances
#from tSNELayout import setup_layout_dists

def runUMAPlayout(nw, nodesdf=None, dists=None, maxdist=5, cluster=None):
    print("Running UMAP layout")
    dists, clus = setup_layout_distances(nw, nodesdf, dists, maxdist, cluster)
    model = umap.UMAP(metric='precomputed')
    layout = model.fit_transform(dists)
    # build the output data structure
    nodes = nw.nodes()
    nodeMap = dict(zip(nodes, range(len(nodes))))
    layout_dict = {node: layout[nodeMap[node]] for node in nodes}
    print("Done running UMAP layout")
    # return both networkx style dict and array of positions
    return layout_dict, layout
