import pandas as pd

from vdl_tools.tag2network.Network.InteractiveNetworkViz import drawInteractiveNW

nodesfile = "tag2network/Data/Example/ExampleNodes.csv"
edgesfile = "tag2network/Data/Example/ExampleEdges.csv"

nodesdf = pd.read_csv(nodesfile)
edgesdf = pd.read_csv(edgesfile)

drawInteractiveNW(nodesdf, edgesdf=edgesdf, color_attr="Cluster", label_attr="name",
                  title="Interactive Network Visualization",
                  plotfile="tag2network/Data/Example/TestNetwork", inline=False)
