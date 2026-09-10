"""
Analyse a causal network whose links are already decided (no votes): a plain link list.
Open this file in PyCharm and hit Run. Output goes to examples/output/.
"""

from pathlib import Path

import pandas as pd

from vdl_tools.causal_networks import load_data, pipeline

HERE = Path(__file__).resolve().parent
OUT = HERE / "output"
OUT.mkdir(exist_ok=True)

nodes_df = pd.read_csv(HERE / "example_nodes.csv")
links_df = pd.read_csv(HERE / "example_links_unweighted.csv")   # just Source, Target
nodes, links = load_data.from_tables(nodes_df, links_df)

# one network; n_trials > 0 adds monte-carlo link deletion for robustness
nodes, links = pipeline.analyze_single_network(
    nodes, links,
    out_xlsx=OUT / "example_unweighted_results.xlsx",
    n_trials=20, del_frac=0.15, seed=1,
    group_attr="Theme",
)
