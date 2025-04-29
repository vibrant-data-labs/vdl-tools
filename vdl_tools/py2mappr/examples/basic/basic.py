import pandas as pd
import vdl_tools.py2mappr as mappr
import os

datapoints_path = os.path.join(os.path.dirname(__file__), '.', 'datapoints.csv')
edges_path = os.path.join(os.path.dirname(__file__), '.', 'edges.csv')

datapoints = pd.read_csv(datapoints_path)
edges = pd.read_csv(edges_path)

# prepare the project
project, original = mappr.create_map(datapoints, edges)

# by default 'Cluster Diversity' is selected for node color
# Change it to 'Journal'
original.set_nodes(node_color="Journal")

# enabling links
original.set_links()

mappr.show()
