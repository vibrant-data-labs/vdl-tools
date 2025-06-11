# run from embedding_network folder (or any non-climate-landscape folder)
# run folder needs to contain paths.ini and config.ini
#
# build a cft using embedding similarity

import pathlib as pl

import vdl_tools.shared_tools.embedding_network.embedding_network as en
import vdl_tools.network_tools.network_functions as net
import vdl_tools.tag2network.Network.BuildNetwork as bn
import vdl_tools.tag2network.Network.LayoutNetwork as ln
import vdl_tools.tag2network.Network.ComputeClustering as cc
import vdl_tools.tag2network.Network.DrawNetwork as dn

import vdl_tools.py2mappr as mappr
import vdl_tools.py2mappr.publish as publisher

# %%

datapath = pl.Path('../climate-landscape/data/cft-published/US')
funding = 'Total_Funding_$'
clus_name = "Keyword Theme"

keep_cols = ['Organization', 'climate_kwds', funding, 'Philanthropy_vs_Venture',
             'Stage_Category', 'Any Equity-Justice Mention',  # 'Mitigation vs Adaptation',
             'uid', 'Description', 'Summary', 'Cluster', clus_name]

raw_cft_df, ldf = net.open_network_from_json(datapath / "cft_network.json")
df = raw_cft_df[keep_cols].rename(columns={'Cluster': 'KwdCluster', clus_name: 'KwdClusterName'})

# %%
results_file = 'CFTEmbeddingNetwork.xlsx'

cluster_params = cc.ClusteringParams(method='leiden',
                                     resolution=[0.3, 0.4, 0.3],
                                     min_clus_size=[200, 400],
                                     merge_tiny=True)

layout_params = ln.ClusterLayoutParams(group_attr="Cluster_L2")

params = bn.BuildEmbeddingNWParams(linksPer=6,
                                   n_tags=5,
                                   clusName='ClusterName',
                                   labelcol=None,
                                   layout_params=layout_params,
                                   clus_params=cluster_params)

nodesdf, edgesdf, sims = en.build_embedding_network(df.copy(), params, debug=True)

# output network nodes for easy review
nodesdf.to_excel(results_file, index=False)

# %%
# plot
plot_local = False

if plot_local:
    dn.plot_network(nodesdf, edgesdf, plotfile="EmbeddingNetwork_L2.png", draw_edges=False,
                    node_attr='Cluster_L2', legend_min_count=100,
                    title=f"Embedding Network. Cluster resolution={params.clus_params.resolution}")

    # plot
    dn.plot_network(nodesdf, edgesdf, plotfile="EmbeddingNetwork.png", draw_edges=False,
                    node_attr='Cluster', legend_min_count=100,
                    title=f"Embedding Network. Cluster resolution={params.clus_params.resolution}")

# %%
# to review cluster names
review_names = False

if review_names:
    clNames = (nodesdf[['Cluster', 'ClusterName', 'Cluster_L2', 'ClusterName_L2']]
               .drop_duplicates()
               .sort_values('Cluster_L2'))
    # analyze cluster naming in old and new networks
    clName = 'ClusterName' if 'ClusterName' in nodesdf else 'Cluster'
    print(nodesdf.KwdClusterName.value_counts())
    print(nodesdf[clName].value_counts())

    nodesdf['ClusNamePairs'] = nodesdf.KwdClusterName + ' ' + nodesdf[clName].astype(str)

    print(nodesdf.ClusNamePairs.value_counts()[0:20])

# %%
# build player and either publish or display locally
build_player = True
publish = True

if build_player:
    keep = ['Name', 'climate_kwds', 'Total_Funding_$',
            'Philanthropy_vs_Venture', 'Stage_Category',
            'Any Equity-Justice Mention', 'Description', 'Summary',
            'KwdCluster', 'KwdClusterName',
            'InDegree', 'OutDegree', 'Degree', 'InterclusterFraction',
            'ClusterDiversity', 'ClusterBridging', 'ClusterCentrality',
            'Cluster_count', 'fracIntergroup_Cluster_L2', 'diversity_Cluster_L2',
            'bridging_Cluster_L2', 'centrality_Cluster_L2', 'Cluster_L2_count',
            'wtd_cc', 'ClusterName', 'clus_summary', 'ClusterName_L2', 'ClusterName_L3']

    filters = ['climate_kwds', 'Total_Funding_$',
               'Philanthropy_vs_Venture', 'Stage_Category',
               'Any Equity-Justice Mention',
               'KwdClusterName', 'ClusterName', 'ClusterName_L2', 'ClusterName_L3',
               ]

    profile = ['climate_kwds', 'Total_Funding_$',
               'Stage_Category', 'Summary',
               'KwdClusterName', 'ClusterName', 'ClusterName_L2', 'ClusterName_L3',
               ]

    attrib_settings = {'Attribute': [],
                       'Keep': keep,
                       'visible_filters': filters,
                       'visible_profile': profile,
                       'visible_search': [],
                       'free_text': ['Name', 'Summary', 'Description'],
                       'tag_list': ['climate_kwds'],
                       }

    rename = {'Organization': 'Name'}

    plot_df = nodesdf.rename(columns=rename)
    plot_df.ClusterName = plot_df.ClusterName.apply(lambda x: ', '.join(x))
    plot_df.ClusterName_L2 = plot_df.ClusterName_L2.apply(lambda x: ', '.join(x))
    plot_df.ClusterName_L3 = plot_df.ClusterName_L3.apply(lambda x: ', '.join(x))

    project, original = mappr.create_map(plot_df, None)
    original.set_nodes(node_color="ClusterName",)
    original.set_links()
    original.set_clusters(cluster_attr='ClusterName_L2')  # , subcluster_attr='ClusterName_L2')

    original.settings.update({
        "drawLabels": True,
        "drawGroupLabels": True,
        "nodeSizeScaleStrategy": "log",  # "linear"
        "drawClustersCircle": True
    })

    project.snapshots = [
        original,
    ]

    original.set_display_data(
        title="CFT Embedding Network",
        subtitle="",
        description="""
                    Two-level clustering using Leiden algroithm
                    """
    )

    project.set_display_data(
        title="CFT Embedding Network",
        logo_image_url=None,
        description="""
                    CFT built using entity description embedding similarities
                    """,
                    )

    project.update_attributes(
        visible_filters=attrib_settings['visible_filters'],
        visible_profile=attrib_settings['visible_profile'],
        visible_search=attrib_settings['visible_search'],
        text_str=attrib_settings['free_text'],
        list_string=attrib_settings['tag_list'],
        )

    if publish:
        publisher.run([
            # publish to S3  http://cft-embedding-network-3-level.s3-website-us-east-1.amazonaws.com/
            publisher.s3("cft-embedding-network-3-level"),
        ])
    else:
        mappr.show()
