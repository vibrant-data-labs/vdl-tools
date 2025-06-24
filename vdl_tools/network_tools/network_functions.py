#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 31 08:14:03 2021
@author: ericberlow

network analysis functions to
* build network from tags
* decorate nodes with network metrics
* decorate nodes with layout coordinates
* plot pdf of network
* convert network nodes and links file  into openmappr format
* create template of node attribute settigns for openmappr

"""

import json
import pandas as pd
import numpy as np

from vdl_tools.tag2network.Network import BuildNetwork as bn  # build network functions
from vdl_tools.tag2network.Network import DrawNetwork as dn  # plot network function
from vdl_tools.tag2network.Network.ClusteringProperties import basicClusteringProperties

import networkx as nx


# %%
def add_theme_fracs(df, cluster, cat_col, cat_col_value, normalized=False, id_attr='__id__'):
    # add cluster level fraction summaries to each node
    # cat_col: categorical column to summarize (e.g. funding category)
    # cat_col_value: which catogory value to summarize (e.g. early stage venture)
    # rel frac = cluster freq / global freq
    # 'normalized' = True  is the normalized difference (scales from -1 to 1)
    df_fracs = add_group_relative_fracs(df, cluster, cat_col, cat_col_value,
                                        normalized=normalized)  # relative frac cat_col_value
    df_fracs.drop([cat_col], axis=1, inplace=True)
    df = df.merge(df_fracs, on=[id_attr, cluster])
    return df


def add_cluster_metrics(nodesdf, nw, groupVars, id_attr='__id__'):
    # add bridging, cluster centrality etc. for one or more grouping variables
    for groupVar in groupVars:
        if len(nx.get_node_attributes(nw, groupVar)) == 0:
            vals = {k: v for k, v in dict(zip(nodesdf[id_attr], nodesdf[groupVar])).items() if k in nw}
            nx.set_node_attributes(nw, vals, groupVar)
        grpprop = basicClusteringProperties(nw, groupVar)
        for prop, vals in grpprop.items():
            nodesdf[prop] = nodesdf[id_attr].map(vals)


def plot_network(ndf, edf, plot_name, x='x', y='y',
                 colorBy='Cluster', sizeBy='ClusterCentrality', sizeScale=100):
    # draw network colored by creative style and save image
    # ndf = nodes dataframe
    # ldf = links dataframe
    # plotname = name of file to save image (pdf)
    ndf['x'] = ndf[x]
    ndf['y'] = ndf[y]
    nw = bn.buildNetworkX(edf)  # build networkX graph object
    node_sizes = ndf.loc[:, sizeBy] * sizeScale
    node_sizes_array = node_sizes.values  # convert sizeBy col to array for sizing
    dn.draw_network_categorical(nw, ndf, node_attr=colorBy,
                                plotfile=plot_name, node_size=node_sizes_array)


def write_network_to_excel(ndf, ldf, outname):
    writer = pd.ExcelWriter(outname, engine='xlsxwriter')
    # Don't convert url-like strings to urls.
    writer.book.strings_to_urls = False
    ndf.to_excel(writer, sheet_name='Nodes', index=False)
    ldf.to_excel(writer, sheet_name='Links', index=False)
    writer.close()


def write_network_to_json(ndf, ldf, outname):
    ndf_json = ndf.to_dict(orient='records')
    ldf_json = ldf.to_dict(orient='records')
    full = {
        "nodes": ndf_json,
        "links": ldf_json
    }
    json.dump(full, open(outname, 'w', encoding='utf-8'), default=str)


def open_network_from_json(filename):
    full = json.load(open(filename))
    ndf = pd.DataFrame(full['nodes'])
    ldf = pd.DataFrame(full['links'])
    return ndf, ldf


def write_network_to_excel_simple(ndf, ldf, outname):
    writer = pd.ExcelWriter(outname)
    ndf.to_excel(writer, 'Nodes', index=False)
    ldf.to_excel(writer, 'Links', index=False)
    writer.close()


def normalized_difference(df, attr):
    # compute normalizd difference relative to the mean
    avg_attr = df[attr].mean()
    normalized_diff = ((df[attr]-avg_attr)/(df[attr]+avg_attr)).round(4)
    return normalized_diff


def max_min_normalize(df, attr):
    max_min = (df[attr]-df[attr].min())/(df[attr].max()-df[attr].min())
    return max_min


def add_group_fracs(ndf,
                    group_col,  # grouping attribute (e.g. 'cluster')
                    attr,  # column with value compute % (e.g. 'org typ')
                    value):  # value to tally if present (e.g. 'non-profit')
    # summarize fraction of nodes each cluster where a value is present
    groups = list(ndf[group_col].unique())
    df = ndf[[group_col, attr]]
    grp_fracs = {}
    for group in groups:
        df_grp = df[df[group_col] == group]  # subset cluster
        nodata = df_grp[attr].apply(lambda x: ((x is None) or x == ''))
        df_grp = df_grp[~nodata]  # remove missing data
        grp_size = len(df_grp)
        n_value = sum(df_grp[attr] == value)  # total cases where value is true
        frac_value = np.round(n_value/grp_size, 2)
        grp_fracs[group] = frac_value
    group_value_fracs = df[group_col].map(grp_fracs)
    return group_value_fracs  # series


def add_group_relative_fracs(ndf,
                             group_col,  # grouping attribute (e.g. 'cluster')
                             attr,  # column with value compute % (e.g. 'org typ')
                             value,  # value to tally if present (e.g. 'non-profit')
                             normalized=True,  # convert relative fract to normalized difference
                             id_attr='__id__'
                             ):
    # summarize fraction (relative to global frac)
    # of nodes each cluster where a value is present
    total = sum(ndf[attr].apply(lambda x: (x is not None) and (x != '') and pd.notnull(x)))
    n_value = sum(ndf[attr] == value)
    global_frac = n_value/total

    groups = list(ndf[group_col].unique())
    # subset the dataframe columns
    df = ndf[[id_attr, group_col, attr]]

    grp_fracs = {}  # dict to hold fracs for each group
    grp_rel_fracs = {}  # dict to hold relative fracs for each group

    for group in groups:
        df_grp = df[df[group_col] == group]  # subset cluster
        w_data = df_grp[attr].apply(lambda x: (x is not None) and (x != '') and pd.notnull(x))
        df_grp = df_grp[w_data]  # remove missing data
        grp_size = len(df_grp)
        n_value = sum(df_grp[attr] == value)  # total cases where value is true
        # compute values for the group
        if grp_size == 0:
            frac = 0
        else:
            frac = np.round(n_value/grp_size, 2)  # fraction of cases where value is true
        if normalized:
            rel_frac = np.round(((frac - global_frac) / (frac + global_frac)), 4)  # normalized to global
        elif global_frac != 0:
            rel_frac = np.round((frac/global_frac), 2)  # frac relative to global
        else:
            rel_frac = 0
        # map the values to the group dictionary
        grp_fracs[group] = frac  # dict to hold fracs for each group
        grp_rel_fracs[group] = rel_frac  # relative fracs for each group
    # map the group values to the dataframe
    df = df.reset_index(drop=True)
    df[group_col + '_frac_' + value] = df[group_col].map(grp_fracs)
    df[group_col + '_rel_frac_' + value] = df[group_col].map(grp_rel_fracs)
    group_value_fracs = df[group_col].map(grp_fracs)
    group_rel_fracs = df[group_col].map(grp_rel_fracs)
    return group_value_fracs, group_rel_fracs  # series of group fracs and rel fracs


def add_group_sums(df,
                   group_col,  # grouping attribute (e.g. 'cluster')
                   attr,  # column with value summarize % (e.g. 'total funding')
                   sum_type  # 'sum', 'mean', 'median'
                   ):
    # summarize total, mean, median value for each cluster
    group_means = df.groupby(group_col)[attr].transform(sum_type)
    group_means = np.round(group_means, 2)
    return group_means  # series


def add_group_frac_sums(ndf,
                        group_col,  # grouping attribute (e.g. 'cluster')
                        cat_col,  # col with categorical attr to summarize by (e.g. 'funding type')
                        cat_value,  # category value to compute frac of total for (e.g. 'venture')
                        num_attr,  # column with numberic value compute frac of total (e.g. total funding)
                        ):
    # summarize fraction of total value for a group in each cluster
    # for example, for each cluster, what is the fraction of total funding that is funding type = venture
    groups = list(ndf[group_col].unique())
    df = ndf[[group_col, cat_col, num_attr]]
    grp_fracs = {}
    for group in groups:
        df_grp = df[df[group_col] == group]  # subset cluster
        nodata = df_grp[cat_col].apply(lambda x: ((x is None) or x == ''))
        df_grp = df_grp[~nodata]  # remove missing data
        grp_tot = df_grp[num_attr].sum()  # sum of numeric attribute
        df_cat = df_grp[df_grp[cat_col] == cat_value]  # subset those whith category value
        n_value = df_cat[num_attr].sum()  # sum of numeric attribute for subset within category
        frac_value = np.round(n_value/grp_tot, 2)
        grp_fracs[group] = frac_value
    group_value_fracs = df[group_col].map(grp_fracs)
    return group_value_fracs  # series
