"""
Run the full causal-network analysis on the synthetic example data.
Open this file in PyCharm and hit Run. Output goes to examples/output/.

Steps: load two tables -> ensemble of threshold networks -> monte-carlo -> Excel + plot + player.
Swap the two CSVs for your own nodes/links tables to run it on your data (see README.md).
"""

from pathlib import Path

import pandas as pd

from vdl_tools.causal_networks import load_data, pipeline, plots, player, metrics

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"
OUT.mkdir(exist_ok=True)

# 1. load: any nodes table (Label + optional columns) and links table (Source, Target, yes, no)
nodes_df = pd.read_csv(HERE / "example_nodes.csv")
links_df = pd.read_csv(HERE / "example_links.csv")
nodes, links = load_data.from_tables(nodes_df, links_df)

# 2. analyse: ensemble of vote thresholds + monte-carlo link deletion, written to Excel
nodes, links = pipeline.run_causal_network_analysis(
    nodes, links,
    out_xlsx=OUT / "example_results.xlsx",
    n_trials=20,          # small for speed; use 100-1000 for real projects
    seed=1,
    group_attr="Theme",   # the categorical node column to group and color by
)

# 3. plot: keystone leverage vs upstream position (most catalytic = upper right)
plots.keystone_vs_upstream_scatter(nodes, out_html=OUT / "example_scatter.html", color="Theme")

# 4. which factors have near-identical causes and effects? (candidates to merge)
nw = load_data.build_graph(nodes, links)
sims = metrics.node_pair_similarities(nw, dict(zip(nodes["id"], nodes["Root Factor"])))
sims.to_csv(OUT / "example_node_similarities.csv", index=False)

# 5. interactive openmappr player (open output/player/index.html via its run_local.sh)
player.build_player(nodes, links, OUT / "player", title="Example causal network", group_attr="Theme")
