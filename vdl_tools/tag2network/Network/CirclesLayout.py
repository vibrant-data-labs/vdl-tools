"""
Grouped layout
1) Split into groups using a categorical variable
2) Set group node size based on number of nodes in the group
3) Layout the groups as packed circles
4) Force-directed (Kamada-Kawai) or circle pack  on each group
5) Pull 'distant' nodes in to limit total radius of each cluster
6) (Optional) GTree to move clusters to eliminate overlap
"""
import networkx as nx
import pandas as pd
import numpy as np
import circlify as circ

import vdl_tools.tag2network.Network.ClusterLayout as cl


def circle_pack(subg, center, mean_size, sizes=None):
    # return {nodeid: array(x, y)}
    if sizes is not None:
        circ_data = [{'id': node, 'datum': sizes[node]} for node in subg.nodes]
        circ_data.sort(key=lambda x: x['datum'], reverse=True)
    else:
        circ_data = [{'id': node, 'datum': 1} for node in subg.nodes]
    circ_pos = circ.circlify(circ_data,
                             circ.Circle(x=center[0], y=center[1], r=np.sqrt(len(subg) * mean_size) / 2))
    new_pos = {cp.ex['id']: np.array([cp.x, cp.y]) for cp in circ_pos}
    return new_pos


def run_circles_layout(nw, nodes_df, group_attr='Cluster',
                       packed_groups=False, compressed_groups=True,
                       size_attr=None, overlap_frac=0.2,
                       max_expansion=1.0, scale_factor=4.0, area_factor=1.0, aspect_ratio=1.0):
    """
    Group by some attribute, layout each group using kamada-kawai or close-packed circles

    Args:
        nw (TYPE): The network to lay out as a networkx Graph.
        nodes_df (TYPE): The node data in a pandas DataFrame.
        packed_groups (bool, optional): Layout of nodes in a group is circle packed (if True) or force-directed.
                                        Defaults to False
        compressed_groups (bool, optional): If not packed_groups, compress forced-directed layout outliers to pull
                                        towards circle center. Defaults to True
        group_attr (str, optional): The attribute used to define the clusters. Defaults to 'Cluster'.
        overlap_frac (float, optional): Fraction of overlap that is left. If 1, no overlap removal. Defaults to 0.2
        max_expansion (float, optional): Internode distance expansion at the center of each cluster. Defaults to 1.0.
        scale_factor (float, optional): Cluster area scale factor. Defaults to 4.0.
        area_factor (float, optional): circle pack area_factor. Defaults to 1.0.
        aspect_ratio (float, optional): circle pack aspect_ratio. Defaults to 1.0.

    Returns
    -------
        new_positions (TYPE): DESCRIPTION.
        layout (TYPE): DESCRIPTION.

    """

    # split the dataset into subgraphs
    group_nodes = nodes_df.groupby(group_attr)
    subgraphs = {grp: nw.subgraph(gdf.index.to_list()) for grp, gdf in group_nodes}
    # compute sizing scale factor for each cluster
    if size_attr is not None:
        mean_size = nodes_df[size_attr].mean()
        group_scale = {grp: np.sqrt((gdf[size_attr] * gdf[size_attr]).sum() / len(gdf)) for grp, gdf in group_nodes}
    else:
        mean_size = 1
        group_scale = {grp: 1 for grp in subgraphs.keys()}
    # layout the group circles
    circ_data = [{'id': grp, 'datum': group_scale[grp] * len(subg)}
                 for grp, subg in subgraphs.items()
                 if len(subg) > 0]
    circ_data.sort(key=lambda x: x['datum'], reverse=True)
    circ_pos = circ.circlify(circ_data,
                             circ.Circle(0, 0, np.sqrt(area_factor * len(nodes_df) * mean_size) / 2))
    pos_map = {pos.ex['id']: pos for pos in circ_pos}
    new_positions = {}
    for grp, subg in subgraphs.items():
        if len(subg) > 0:
            print(f"Laying out subgraph for {grp} {len(subg)}")
            # get cluster centroid
            pos = pos_map[grp]
            center = [pos.x, pos.y]
            if packed_groups or len(subg) < 5:
                sizes = group_nodes.get_group(grp)[size_attr] if size_attr else None
                new_pos = circle_pack(subg, center, scale_factor * mean_size, sizes)
            else:
                subg = cl.connect_subgraph(subg)
                scale = np.sqrt(len(subg) * group_scale[grp]) / 2
                new_pos = cl.layout_single_cluster(subg, None, scale, center)
            new_positions.update(new_pos)
    if compressed_groups and not packed_groups:
        new_positions = cl.compress_groups(nodes_df, new_positions, group_attr,
                                           overlap_frac, max_expansion, scale_factor,
                                           aspect_ratio)
    # build the output data structure
    nodes = nw.nodes()
    layout_dict = {node: new_positions[idx] for idx, node in enumerate(nodes)}
    layout = [list(new_positions[idx]) for idx in layout_dict.keys()]
    return new_positions, layout


def run_multi_circles_layout(nw, nodes_df, group_attrs=('level0', 'level1'),
                             packed_groups=False, compressed_groups=True,
                             size_attr=None, scale_factor=1.0,
                             outer_area_factor=0.4, inner_area_factor=1.0,
                             aspect_ratio=1.0):
    """
    Hierarchical grouping by two attributes in list, layout immer group using kamada-kawai or close-packed circles

    Args:
        nw (TYPE): The network to lay out as a networkx Graph.
        nodes_df (TYPE): The node data in a pandas DataFrame.
        group_attrs (list, optional): The attribute used to define the clusters. Defaults to 'Cluster'.
        packed_groups (bool, optional): Layout of nodes in a group is circle packed (if True) or force-directed.
                                        Defaults to False
        compressed_groups (bool, optional): If not packed_groups, compress forced-directed layout outliers to pull
                                        towards circle center. Defaults to True
        scale_factor (float, optional): Inner group layout compression scale factor, no effect if set too high. Defaults to 1.0.
        outer_area_factor (float, optional): outer circle pack area_factor - decrease to increase innter group overlap.. Defaults to 0.8.
        inner_area_factor (float, optional): innter circle pack area_factor - decrease to increase outer group overlap.. Defaults to 0.3
        aspect_ratio (float, optional): circle pack aspect_ratio. Defaults to 1.0.

    Returns
    -------
        new_positions (TYPE): DESCRIPTION.
        layout (TYPE): DESCRIPTION.

    """
    overlap_frac = 1.0      # no overlap removal needed
    max_expansion = 1.0     # no cluster center expansion

    # make sure all entities have grouping attribute values
    for idx, attr in enumerate(group_attrs):
        mask = nodes_df[attr].isna()
        if idx > 0:
            nodes_df.loc[mask, attr] = f'No_Level_{idx}_' + nodes_df.loc[mask, group_attrs[idx - 1]]
        else:
            nodes_df.loc[mask, attr] = f'No_Level_{idx}'
    # split the dataset into subgraphs
    group_attrs = list(group_attrs)  # cast tuple as list
    group_nodes = nodes_df.groupby(group_attrs)
    subgraphs = {grp: nw.subgraph(gdf.index.values) for grp, gdf in group_nodes}
    # compute sizing scale factor for each subgraph
    if size_attr is not None:
        mean_size = nodes_df[size_attr].mean()
        group_scale = {grp: np.sqrt((gdf[size_attr].astype(float) * gdf[size_attr].astype(float)).sum() / len(gdf))
                       for grp, gdf in group_nodes}
    else:
        mean_size = 1
        group_scale = {grp: 1 for grp in subgraphs.keys()}
    # compute scale of the innermost circles
    circ_data = pd.Series({grp: group_scale[grp] * len(subg) for grp, subg in subgraphs.items() if len(subg) > 0})
    # computer outer circle positions
    outer_circs = circ_data.index.levels[0]
    outer_df = circ_data.reset_index().groupby('level_0')[0].sum()
    outer_circ_data = [{'id': k, 'datum': v} for k, v in outer_df.items()]
    outer_circ_data.sort(key=lambda x: x['datum'], reverse=True)
    outer_circ_pos = circ.circlify(outer_circ_data,
                                   circ.Circle(0, 0, np.sqrt(len(nodes_df)) * mean_size))
    outer_scale = np.sqrt(outer_area_factor)
    outer_pos = {cc.ex['id']: (outer_scale * cc.x, outer_scale * cc.y) for cc in outer_circ_pos}
    # compute inner circle position and node layout for each outer circle
    inner_pos = {}
    new_positions = {}
    for outer_val in outer_circs:
        print(f"Laying out {outer_val}")
        op = outer_pos[outer_val]
        icd = circ_data[outer_val]
        indices = [val for val in circ_data.index.values if val[0] == outer_val]
        inner_subg = {val[1]: subgraphs[val] for val in indices}
        # layout the inner circles
        inner_circ_data = [{'id': k, 'datum': v} for k, v in icd.items()]
        inner_circ_data.sort(key=lambda x: x['datum'], reverse=True)
        inner_circ_pos = circ.circlify(inner_circ_data, circ.Circle(0, 0, np.sqrt(outer_df[outer_val] * mean_size)))
        # layout the nodes in the inner circles
        pos_map = {pos.ex['id']: pos for pos in inner_circ_pos}
        inner_pos[outer_val] = pos_map
        inner_scale = np.sqrt(inner_area_factor)
        outer_new_pos = {}
        for grp, subg in inner_subg.items():
            if len(subg) > 0:
                print(f"Laying out subgraph for {grp} {len(subg)}")
                # get cluster centroid
                pos = pos_map[grp]  # position within the outer circle
                center = [inner_scale * pos.x + op[0], inner_scale * pos.y + op[1]]
                if packed_groups or len(subg) < 5:  # use circle-=packing
                    sizes = group_nodes.get_group((outer_val, grp))[size_attr] if size_attr else None
                    new_pos = circle_pack(subg, center, scale_factor * mean_size, sizes)
                else:   # use foce-directed
                    subg = cl.connect_subgraph(subg)
                    scale = np.sqrt(len(subg) / 4 * mean_size * group_scale[(outer_val, grp)])
                    new_pos = cl.layout_single_cluster(subg, None, scale, center)
                outer_new_pos.update(new_pos)
        # reposition one level-0 subgraph at a time
        if compressed_groups and not packed_groups:
            ndf = nodes_df[nodes_df[group_attrs[0]] == outer_val]
            outer_new_pos = cl.compress_groups(ndf, outer_new_pos, group_attrs[1],
                                               overlap_frac, max_expansion,
                                               scale_factor * mean_size, aspect_ratio)
        new_positions.update(outer_new_pos)
    # build the output data structure
    nodes = nw.nodes()
    layout_dict = {node: new_positions[node] for node in nodes}
    layout = [list(new_positions[idx]) for idx in layout_dict.keys()]
    return new_positions, (layout, inner_pos, outer_pos)
