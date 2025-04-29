# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
import numbers
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
import networkx as nx

from vdl_tools.tag2network.Network.tSNELayout import runTSNELayout
from vdl_tools.tag2network.Network.UMAPLayout import runUMAPlayout
from vdl_tools.tag2network.Network.ClusterLayout import run_cluster_layout
from vdl_tools.tag2network.Network.CirclesLayout import run_circles_layout
from vdl_tools.tag2network.Network.CirclesLayout import run_multi_circles_layout


@dataclass
class BaseLayoutParams:
    orientation: float = 0     # angle from x-axis of long dimension of cluster layout
    x: str = 'x'  # x attrib for layout
    y: str = 'y'  # y attrib for layout


@dataclass
class ClusterLayoutParams(BaseLayoutParams):
    layout: str = 'cluster'
    group_attr: str = 'Cluster'
    overlap_frac: float = 0.2  # for cluster or circles layout, fraction circle overlap
    max_expansion: float = 1.5  # for cluster or circles layout, default = 1.5
    scale_factor: float = 1     # for cluster or circles layout, default = 1
    maxdist: float = 5
    size_attr: str = None


@dataclass
class CircleLayoutParams(BaseLayoutParams):
    layout: str = 'circle'
    group_attr: str = 'Cluster'
    size_attr: str = None
    overlap_frac: float = 0.25  # fraction circle overlap
    max_expansion: float = 1.5  # for cluster or circles layout, default = 1.5
    scale_factor: float = 1     # for cluster or circles layout, default = 1
    area_factor: float = 1.2    # for circles layout
    aspect_ratio: float = 1.0   # for circles layout


@dataclass
class MultiCircleLayoutParams(BaseLayoutParams):
    layout: str = 'multicircle'
    group_attrs: list = field(default_factory=list)
    size_attr: str = None
    overlap_frac: float = 0.25  # fraction circle overlap
    scale_factor: float = 1     # for cluster or circles layout, default = 1
    aspect_ratio: float = 1.0   # for circles layout
    outer_area_factor: float = 0.4
    inner_area_factor: float = 1.0


@dataclass
class ForceDirectedLayoutParams(BaseLayoutParams):
    layout: str = 'forcedirected'
    fd_iterations: int = 1000  # number of iterations if running force-directed layout


@dataclass
class RandomLayoutParams(BaseLayoutParams):
    layout: str = 'random'
    pass


@dataclass
class TSNELayoutParams(BaseLayoutParams):
    layout: str = 'tsne'
    pass


@dataclass
class UMAPLayoutParams(BaseLayoutParams):
    layout: str = 'umap'
    pass


def _rotate_layout(df, xnm, ynm, orientation):
    """
    Rotate a layout

    Parameters
    ----------
    df : pandas.DataFrame
        Contains x, y coordinate columns.
    x: str
        x column name.
    y: str
        y column name.
    orientation : int or float
        Final orientation angle from x-axis.

    Returns
    -------
    None.

    """
    if not isinstance(orientation, numbers.Number):
        return
    # convert to radians
    orientation *= np.pi / 180
    # get angle from first principal component
    pca = PCA(n_components=2).fit(df[[xnm, ynm]].to_numpy())
    comp = pca.components_[0]
    theta = orientation - np.arctan(comp[1] / comp[0])
    # do the rotation
    x = df[xnm]
    y = df[ynm]
    x_mn = x.mean()
    y_mn = y.mean()
    df[xnm] = x_mn + np.cos(theta) * (x - x_mn) - np.sin(theta) * (y - y_mn)
    df[ynm] = y_mn + np.sin(theta) * (x - x_mn) + np.cos(theta) * (y - y_mn)


def add_layout(nodesdf, linksdf=None, dists=None, nw=None, params=ClusterLayoutParams(), rename_xy=None):
    # get attributes shared across all layouts
    layout_name = params.layout
    x_name = params.x
    y_name = params.y
    print(f"Running graph layout {layout_name}")
    if nw is None:
        # build networkx model, linksdf must not be None
        linkdata = [(getattr(link, 'Source'), getattr(link, 'Target')) for link in linksdf.itertuples()]
        nw = nx.Graph()
        nw.add_edges_from(linkdata)
    if layout_name == 'cluster':
        layout, _ = run_cluster_layout(nw, nodesdf, dists=dists,
                                       maxdist=params.maxdist,
                                       size_attr=params.size_attr,
                                       cluster_attr=params.group_attr,
                                       overlap_frac=params.overlap_frac,
                                       max_expansion=params.max_expansion,
                                       scale_factor=params.scale_factor)
    elif layout_name == 'circle':
        layout, _ = run_circles_layout(nw, nodes_df=nodesdf,
                                       group_attr=params.group_attr,
                                       size_attr=params.size_attr,
                                       overlap_frac=params.overlap_frac,
                                       max_expansion=params.max_expansion,
                                       scale_factor=params.scale_factor,
                                       area_factor=params.area_factor,
                                       aspect_ratio=params.aspect_ratio)
    elif layout_name == 'multicircle':
        layout, _ = run_multi_circles_layout(nw, nodes_df=nodesdf,
                                             group_attrs=params.group_attrs,
                                             packed_groups=False, compressed_groups=True,
                                             size_attr=params.size_attr,
                                             scale_factor=params.scale_factor,
                                             outer_area_factor=params.outer_area_factor,
                                             inner_area_factor=params.inner_area_factor,
                                             aspect_ratio=params.aspect_ratio)
    elif layout_name == 'forcedirected':
        # remove isolated nodes and clusters for layout
        giant_component_nodes = max(nx.connected_components(nw), key=len)
        giant_component = nw.subgraph(giant_component_nodes)
        layout = nx.spring_layout(giant_component, k=0.2,
                                  weight='weight', iterations=params.fd_iterations)  # k is spacing 0-1, default 0.1
        x = {n: layout[n][0] for n in giant_component.nodes()}
        y = {n: layout[n][1] for n in giant_component.nodes()}
        nodesdf[x_name] = nodesdf.index.map(x)
        nodesdf[y_name] = nodesdf.index.map(y)
        layout = dict(zip(nodesdf.index, list(zip(nodesdf[x_name], nodesdf[y_name]))))
    elif layout_name == 'tsne':
        layout, _ = runTSNELayout(nw, nodesdf=nodesdf, cluster=params.group_attr)
    elif layout_name == 'umap':
        layout, _ = runUMAPlayout(nwnodesdf=nodesdf, cluster=params.group_attr)
    elif layout_name == 'random':
        rho = np.sqrt(np.random.uniform(0, 1, len(nodesdf)))
        phi = np.random.uniform(0, 2*np.pi, len(nodesdf))
        x = rho * np.cos(phi)
        y = rho * np.sin(phi)
        layout = dict(zip(nodesdf.index, list(zip(x, y))))
    else:
        layout = None
    if layout:
        idx_series = pd.Series(nodesdf.index, index=nodesdf.index)
        nodesdf[x_name] = idx_series.apply(lambda x: layout[x][0] if x in layout else 0.0).fillna(0)
        nodesdf[y_name] = idx_series.apply(lambda x: layout[x][1] if x in layout else 0.0).fillna(0)
        # rotate to make principal axis horizontal
        if params.orientation is not None and layout_name != 'random':
            _rotate_layout(nodesdf, x_name, y_name, params.orientation)
        if rename_xy:
            nodesdf.rename(columns={x_name: f"x_{layout_name}", y_name: f"y_{layout_name}"}, inplace=True)
    return layout
