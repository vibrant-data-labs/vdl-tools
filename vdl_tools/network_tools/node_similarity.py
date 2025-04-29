# -*- coding: utf-8 -*-

from collections import defaultdict
import pandas as pd


def jaccardianSim(nw, identicalThresh=0.3, deleteIdentical=False):
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


def keystone_influences_overlap(ndf, ldf,
                                Keystone,  # column name for boolean top keystones
                                thresh=2   # keep if influence at least thresh keystones (if 1 = all influences)
                                ):
    # Keystone is boolean flag if Keystone is in top n percentile
    # find all root causes that are direct influenes to more than one keystone
    keystones = ndf[ndf[Keystone]]['id'].tolist()
    df_to_keystones = ldf[ldf['Target'].isin(keystones)][['Source', 'Target', 'fromName', 'toName']]
    from_counts = df_to_keystones.groupby('fromName')['Source'].agg('count').reset_index()
    df_overlap = from_counts[from_counts.Source >= thresh]
    df_overlap.columns = ['fromName', 'count']
    df_overlap = df_overlap.sort_values(['count'], ascending=False)

    return df_overlap  # dataframe of From nodes that occur more than once


if __name__ == '__main__':
    # test script here
    datapath = "./data/"
    ndf = pd.read_excel(datapath + "test_network.xlsx", sheet_name="nodes")
    ldf = pd.read_excel(datapath + "test_network.xlsx", sheet_name="links")

    kdf = keystone_influences_overlap(ndf, ldf)
