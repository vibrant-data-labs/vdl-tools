"""
Modified tSNE layout to pull clusters together into visually coherent groups.

1) tSNE on whole network
2) Kamada-Kawai on each cluster
3) Pull 'distant nodes in to limit total radius of each cluster
4) (Optional) GTree to move clsuters to eliminate overlap
"""
from vdl_tools.tag2network.Network.tSNELayout import runTSNELayout
import networkx as nx
import igraph as ig
import numpy as np
import scipy as sp


def _remove_overlap(nodes, overlap_frac):
    """Implement GTree algorithm https://arxiv.org/pdf/1608.02653.pdf."""
    nodes = [n.copy() for n in nodes]

    def dist(idx1, idx2, pos, nodes):
        d = pos[idx1] - pos[idx2]
        center_to_center = np.sqrt((d * d).sum())
        return center_to_center - (1.0 - overlap_frac) * (nodes[idx1]['radius'] + nodes[idx2]['radius'])

    def get_next(mst, previous, current):
        edges = list(mst.edges(current))
        next_nodes = []
        for e in edges:
            if previous is None or previous not in e:
                next_nodes.append(e[1] if e[0] == current else e[0])
        return next_nodes

    def shift_nodes(nodes, mst, source, target, delta_x, delta_y):
        # shift the target
        trg_node = nodes[target]
        trg_node['x'] += delta_x
        trg_node['y'] += delta_y
        # shift nodes recursively
        next_nodes = get_next(mst, source, target)
        for next_n in next_nodes:
            shift_nodes(nodes, mst, target, next_n, delta_x, delta_y)

    def process_tree(nodes, mst, previous, current):
        # process mst recursively
        next_nodes = get_next(mst, previous, current)
        for next_n in next_nodes:
            wt = mst.edges[(current, next_n)]['weight']
            if wt < 0:
                # compute the shift x, y
                src_node = nodes[current]
                trg_node = nodes[next_n]
                dx = trg_node['x'] - src_node['x']
                dy = trg_node['y'] - src_node['y']
                dist = np.sqrt(dx ** 2 + dy ** 2)
                frac_x = dx / dist if dist != 0 else 0
                frac_y = dy / dist if dist != 0 else 0
                wt = mst.edges[(current, next_n)]['weight']
                delta_x = -wt * frac_x
                delta_y = -wt * frac_y
                # shift target and its children
                shift_nodes(nodes, mst, current, next_n, delta_x, delta_y)
            process_tree(nodes, mst, current, next_n)

    if len(nodes) > 4:
        max_steps = 10
        for step in range(max_steps):
            # extract position data to numpy
            pos = np.array([[n['x'], n['y']] for n in nodes])
            # build delauney triangulation
            tri = sp.spatial.Delaunay(pos)
            # build weighted networkx graph. Weight is distance between node edges. Compute overlap of each pair
            raw_edges = set()
            for sim in tri.simplices:
                raw_edges.add((sim[0], sim[1]))
                raw_edges.add((sim[1], sim[2]))
                raw_edges.add((sim[2], sim[0]))
            nw = nx.Graph()
            n_overlap = 0
            for e in raw_edges:
                d = dist(e[0], e[1], pos, nodes)
                nw.add_edge(e[0], e[1], weight=d)
                if d < 0:
                    n_overlap += 1
            print(f'Step {step} n_overlap = {n_overlap}')
            # quit looping if all weights are positive (no overlap)
            if n_overlap == 0:
                break
            # get minimal spanning tree of weighted graph
            mst = nx.minimum_spanning_tree(nw)
            # roots have degree == 1
            root = [n for n, d in mst.degree if d == 1][0]
            # recursively process mst from root
            process_tree(nodes, mst, None, root)
    return {n['name']: np.array([n['x'], n['y']]) for n in nodes}


def compress_groups(nodes_df, layout_dict, cluster_attr, overlap_frac,
                    max_expansion, scale_factor, aspect_ratio=1.0):
    clus_nodes = nodes_df.groupby(cluster_attr)
    subgraphs = {clus: cdf.index.to_list() for clus, cdf in clus_nodes}
    new_positions = {}
    clusters = []
    clus_id = 0
    for clus, subg in subgraphs.items():
        # get starting positions (from tSNE/KK layout)
        pos = {k: layout_dict[k] for k in subg if k in layout_dict}
        if len(pos) > 0:
            clus_pos = np.array(list(pos.values()))
            # get cluster centroid, final scale and distance from center
            center = np.median(clus_pos, axis=0)
            scale = np.sqrt(len(subg)) * scale_factor
            # add cluster node
            clusters.append({'id': clus_id,
                             'name': clus,
                             'x': center[0],
                             'y': center[1],
                             'radius': scale})
            clus_id += 1
    # move cluster centers to remove overlap
    centers = {cl['name']: (cl['x'], cl['y']) for cl in clusters}
    if overlap_frac < 1.0:
        print("Repositioning cluster centers")
        new_centers = _remove_overlap(clusters, overlap_frac)
    else:
        new_centers = centers
    print("Compressing layout of nodes in clusters")
    for clus, subg in subgraphs.items():
        # get starting positions (from tSNE/KK layout)
        pos = {k: layout_dict[k] for k in subg if k in layout_dict}
        if len(pos) > 0:
            if len(pos) > 1:
                clus_pos = np.array(list(pos.values()))
                # get cluster centroid, final scale and distance from center
                center = np.median(clus_pos, axis=0)
                scale = np.sqrt(len(subg)) * scale_factor
                dists = np.sqrt(((clus_pos - center) ** 2).sum(axis=1))
                # use a truncated, normalized Mechelis-Menten function
                # to rescale distance from center
                om = max(dists)  # current max distance
                nm = scale  # desired max distance
                k = om * nm / (om - nm / 2)
                rescale = np.clip(k / (k / 2 + dists), None, max_expansion)
                center = centers[clus]
                new_center = new_centers[clus]
                # change plot aspect ratio - aspect > 1 -> stretch x-axis]
                if aspect_ratio != 1.0:
                    new_center = new_center.copy()
                    if aspect_ratio > 1.0:
                        new_center[0] = new_center[0] * aspect_ratio
                    else:
                        new_center[1] = new_center[1] / aspect_ratio
                new_pos = new_center + ((clus_pos - center) * rescale.reshape(-1, 1))
                if np.isnan(new_pos).sum() > 0:
                    new_pos = new_center
            else:
                new_pos = list(pos.values())
            new_positions.update(dict(zip(pos.keys(), new_pos)))
    return new_positions


# test whether a cluster subgraph has multiple components and if so add
# minimal links to connect into a single component
# this connected subgraph can then be laid out using a force-directed method
# without large separation between components
def connect_subgraph(subg):
    # get component list with largest component last
    comps = sorted(nx.connected_components(subg), key=len)
    if len(comps) > 1:
        print("Connecting cluster components")
        # for each component, order node by degree
        subg = nx.Graph(subg)  # copy subgraph to get a mutable graph
        comp_degs = []         # degrees of nodes in each component
        for idx, comp in enumerate(comps):
            # get and sort node degrees foer the component
            degs = [(node, subg.degree[node]) for node in comp]
            degs.sort(key=lambda x: x[1])
            comp_degs.append(degs)
        # connect each smaller component to the largest component in the cluster
        for idx, info in enumerate(comp_degs[0:-1]):   # for each component that is not the largest
            # get the nodes to connect in each component pair
            idx1 = info[0][0]
            min_idx = min(len(comp_degs[-1]) - 1, idx)
            idx2 = comp_degs[-1][min_idx][0]
            # connect the components
            subg.add_edge(idx1, idx2)
    return subg


def layout_single_cluster(g, pos, scale, center, jitter_frac=0.03):
    gg = ig.Graph.from_networkx(g)
    ig_pos = gg.layout_kamada_kawai()
    ig_pos.fit_into((scale, scale))
    ig_pos.center(center)
    new_pos = dict(zip(g.nodes, np.array(ig_pos.coords)))
    
    #new_pos = nx.kamada_kawai_layout(g, pos=pos, weight=None,
    #                                 scale=scale,
    #                                 center=center)
#            new_pos = nx.spring_layout(subg, pos=pos, weight=None, iterations=500,
#                                       scale=scale,
#                                       center=center)
    if jitter_frac > 0:
        pts = np.array(list(new_pos.values()))
        rng = pts.ptp(axis=0)
        jitter = np.random.uniform(low=-1, high=1, size=pts.shape) * rng * jitter_frac
        new_pts = pts + jitter
        new_pos = {node: new_pts[idx] for idx, node in enumerate(new_pos.keys())}
    return new_pos


def run_cluster_layout(nw, nodes_df, dists=None, maxdist=5, cluster_attr='Cluster',
                       size_attr=None, overlap_frac=0.2,
                       max_expansion=1.5, scale_factor=1.0):
    """
    Pull nodes in clusters in tSNE layout into visually coherent groups.

    1) tSNE on whole network
    2) Kamada-Kawai on each cluster
    3) Pull 'distant nodes in to limit total radius of each cluster
    4) (Optional) GTree to minimally move clsuters to eliminate overlap


    Args:
        nw (TYPE): The network to lay out as a networkx Graph.
        nodes_df (TYPE): The node data in a pandas DataFrame.
        dists (TYPE, optional): Distance matrix, if none path length is used to compute distances.
                                Defaults to None.
        maxdist (TYPE, optional): Maximum path length to consider when computing path-based distances.
                                  Defaults to 5.
        cluster_attr (TYPE, optional): The attribute used to define the clusters. Defaults to 'Cluster'.
        overlap_frac (float, optional): Fraction of overlap that is left. If 1, no overlap removal. Defaults to 0.
        max_expansion (float, optional): Internode distance expansion at the center of each cluster. Defaults to 1.5.
        scale_factor (float, optional): Cluster area scale factor. Defaults to 1.0.

    Returns
    -------
        new_positions (TYPE): DESCRIPTION.
        layout (TYPE): DESCRIPTION.

    """
    nw = nw.to_undirected()
    layout_dict, layout = runTSNELayout(nw, nodes_df, dists, maxdist, cluster_attr)
    clus_nodes = nodes_df.groupby(cluster_attr)
    subgraphs = {clus: nw.subgraph(cdf.index.to_list()) for clus, cdf in clus_nodes}
    # compute sizing scale factor for each cluster
    if size_attr is not None and nodes_df[size_attr].min() < 0:
        print("Warning: you cannot use a size_attr with minimum < 0; setting size_attr to None")
        size_attr = None
    if size_attr is not None:
        mean_size = nodes_df[size_attr].mean()
        clus_scale = {clus: cdf[size_attr].mean() / mean_size for clus, cdf in clus_nodes}
    else:
        clus_scale = {clus: 1 for clus in subgraphs.keys()}
    new_positions = {}
    for clus, subg in subgraphs.items():
        if len(subg) > 0:
            # connect subgraph components if there are multiple components
            subg = connect_subgraph(subg)
            print(f"Laying out subgraph for {clus}")
            # get starting positions (from tSNE layout)
            pos = {k: layout_dict[k] for k in subg.nodes}
            clus_pos = np.array(list(pos.values()))
            # get cluster centroid
            center = np.median(clus_pos, axis=0)
            scale = np.sqrt(len(subg) * clus_scale[clus])
            new_pos = layout_single_cluster(subg, pos, scale, center)
            new_positions.update(new_pos)
    new_positions = compress_groups(nodes_df, new_positions, cluster_attr,
                                    overlap_frac, max_expansion, scale_factor)
    layout = [list(new_positions[idx]) for idx in layout_dict.keys()]
    return new_positions, layout


if __name__ == "__main__":
    # build test dataset
    init_nodes = [
        {'id': 1,
         'name': 'node1',
         'x': -0.5,
         'y': 0,
         'radius': 0.3
         },
        {'id': 2,
         'name': 'node2',
         'x': 0,
         'y': 0,
         'radius': 0.3
         },
        {'id': 3,
         'name': 'node3',
         'x': 0.5,
         'y': 0.1,
         'radius': 0.2
         },
        {'id': 4,
         'name': 'node4',
         'x': 1.0,
         'y': 0.1,
         'radius': 0.4
         }
        ]

    new_centers = _remove_overlap(init_nodes, 0)
