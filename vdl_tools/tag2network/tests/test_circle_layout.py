# run from vdl-tools/vdl_tools/tag2network

import pandas as pd
import matplotlib.pyplot as plt

import networkx as nx
from vdl_tools.tag2network.Network.CirclesLayout import run_circles_layout, run_multi_circles_layout
import vdl_tools.tag2network.Network.LayoutNetwork as ln

# %%
cft_file = "../../../climate-landscape/data/cft-published/US/cft_network_cleaned.xlsx"

nodesdf = pd.read_excel(cft_file, sheet_name='Nodes')
edgesdf = pd.read_excel(cft_file, sheet_name='Links')

nodesdf['Cluster'] = nodesdf['Keyword Theme']

#nodesfile = "tag2network/Data/Example/ExampleNodes.csv"
#edgesfile = "tag2network/Data/Example/ExampleEdges.csv"
#nodesdf = pd.read_csv(nodesfile)
#edgesdf = pd.read_csv(edgesfile)


def buildNetworkX(linksdf, id1='Source', id2='Target', directed=False):
    linkdata = [(getattr(link, id1), getattr(link, id2)) for link in linksdf.itertuples()]
    g = nx.DiGraph() if directed else nx.Graph()
    g.add_edges_from(linkdata)
    return g


nw = buildNetworkX(edgesdf)

# %%
size_attr = 'ClusterCentrality'
size_attr = 'Total Funding'
min_sz = 1
max_sz = 100
rng = nodesdf[size_attr].max() - nodesdf[size_attr].min()
nodesdf['size'] = min_sz + (max_sz - min_sz) * (nodesdf[size_attr] - nodesdf[size_attr].min()) / rng


# %%
def plot_circles_layout(nodesdf):
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    _max = nodesdf[['x', 'y']].values.max()
    _min = nodesdf[['x', 'y']].values.min()

    color_map = pd.factorize(nodesdf.Cluster)
    nodesdf.plot.scatter('x', 'y', c=color_map[0], cmap='jet', s='size',
                         xlim=(_min, _max), ylim=(_min, _max), ax=ax, colorbar=None)


def run_plot_circles(nodesdf, nw):
    layout, _ = run_circles_layout(nw, nodes_df=nodesdf, group_attr='Cluster',
                                   size_attr='size', aspect_ratio=1.0, scale_factor=1.0,
                                   area_factor=0.2, packed_groups=False, compressed_groups=True)

    nodesdf['x'] = nodesdf['id'].apply(lambda x: layout[x][0] if x in layout else 0.0)
    nodesdf['y'] = nodesdf['id'].apply(lambda x: layout[x][1] if x in layout else 0.0)

    plot_circles_layout(nodesdf)


# %%
def run_plot_multicircles(attrs):
    group_attrs = ['Solution Pillar', 'Cluster']
    layout, _ = run_multi_circles_layout(nw, nodesdf, group_attrs=group_attrs,
                                         packed_groups=False, compressed_groups=True,
                                         size_attr='size', scale_factor=attrs[0],
                                         outer_area_factor=attrs[1], inner_area_factor=attrs[2],
                                         aspect_ratio=1.0)


    nodesdf['x'] = nodesdf['id'].apply(lambda x: layout[x][0] if x in layout else 0.0)
    nodesdf['y'] = nodesdf['id'].apply(lambda x: layout[x][1] if x in layout else 0.0)

    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    _max = nodesdf[['x', 'y']].values.max()
    _min = nodesdf[['x', 'y']].values.min()

    color_map = pd.factorize(nodesdf[group_attrs[0]])
    nodesdf.plot.scatter('x', 'y', c=color_map[0], cmap='jet', s='size',
                         xlim=(_min, _max), ylim=(_min, _max), ax=ax, colorbar=None)
    ax.set_title(str(attrs))


# %%
run_plot_circles(nodesdf, nw)

# %%
# attrs are scale, outer, inner
attrs_list = [(10, 1, 3),
              (5, 0.4, 1),
              ]

for attrs in attrs_list:
    run_plot_multicircles(attrs)
    
# %%
params = ln.CircleLayoutParams()

_ = ln.add_layout(nodesdf, nw=nw, params=params)
plot_circles_layout(nodesdf)