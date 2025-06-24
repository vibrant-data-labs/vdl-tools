#
# build similarity network from Web of Science data
#
# run from VDL/vdl-tools/tag2network

import os

from vdl_tools.tag2network.WOSTags.MergeWOSFiles import mergeWOSFiles
from vdl_tools.tag2network.WOSTags.ProcessReferences import processRawWoSData
from vdl_tools.tag2network.WOSTags.ProcessReferences import loadAndFilterDocuments
from vdl_tools.tag2network.Tags.BuildKeywords import buildKeywords
import vdl_tools.tag2network.Network.BuildNetwork as bn
import vdl_tools.tag2network.Network.DrawNetwork as dn

# %%
# columns to delete in the output
dropCols = ['text', 'AuKeywords', 'KeywordsPlus', 'keywords']

namebase = "Example"         # dataset/project name
rawfilebase = "savedrecs"   # name base of raw, unjoined WoS data files
outfile = namebase+"Raw.txt"
basepath = "tag2network"
datapath = "Example"

# concatentate multiple WOS data files
mergeWOSFiles(datapath, rawfilebase, outfile)
# extract smaller set of useful data from raw WOS output
processRawWoSData(datapath, namebase)

# if desired, make keyword blacklist and whitelist
blacklist = set([])
whitelist = set([])

fname = os.path.join(datapath, namebase+"Final.txt")

# too many documents - keep a fraction of them by year and citation rate
# df = loadAndFilterDocuments(fname, 0.9)
df = loadAndFilterDocuments(fname, 0.1)    # for debugging so it runs faster

# set up output file names
nwname = os.path.join(datapath, namebase+".xlsx")
nodesname = os.path.join(datapath, namebase+"Nodes.csv")
edgesname = os.path.join(datapath, namebase+"Edges.csv")
plotname = os.path.join(datapath, namebase+"Plot.pdf")

# build and enhance the keywords, add to df
kwAttr = buildKeywords(df, blacklist, whitelist)

# %%
# build network linked by keyword similarity
params = bn.BuildNWParams(tag_attr=kwAttr)
nodes_df, edges_df = bn.buildTagNetwork(df, params, dropCols=dropCols)
# save network
bn.save_network_to_csv(nodes_df, edges_df, nodesname, edgesname)
bn.save_network_to_excel(nodes_df, edges_df, nwname)
# %%
# plot network
dn.plot_network(nodes_df, edges_df, legend_min_count=100, plotfile=None)
