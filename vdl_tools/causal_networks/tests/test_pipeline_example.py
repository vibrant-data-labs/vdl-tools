"""End-to-end run on the committed example data."""
from pathlib import Path

import pandas as pd

from vdl_tools.causal_networks import load_data, pipeline

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def test_full_pipeline_on_example_data(tmp_path):
    nodes_df = pd.read_csv(EXAMPLES / "example_nodes.csv")
    links_df = pd.read_csv(EXAMPLES / "example_links.csv")
    links_df["sign"] = "+"                                    # extra link metadata must survive
    nodes, links = load_data.from_tables(nodes_df, links_df)
    out = tmp_path / "results.xlsx"
    nodes_out, links_out = pipeline.run_causal_network_analysis(
        nodes, links, out_xlsx=out, n_trials=5, seed=1, group_attr="Theme")

    expected = ["Root Factor", "Theme", "Catalytic Score", "Top Keystone", "Keystone Rank",
                "Upstream Rank", "Trophic Level", "Reach in 2 Hops (Pct)", "Causal Cluster", "label", "x", "y", "id"]
    assert all(c in nodes_out.columns for c in expected)
    assert nodes_out["Catalytic Score"].between(0, 100).all()
    assert nodes_out["Reach in 2 Hops (Pct)"].between(0, 100).all()
    assert set(nodes_out["Top Keystone"]) <= {"Yes", "No"}
    # the planted upstream drivers should rank as upstream
    upstream = nodes_out.set_index("Root Factor")["Upstream Rank"]
    assert upstream["Funding"] > upstream["Community trust"]

    sheets = pd.read_excel(out, sheet_name=None)
    assert set(sheets) == {"Nodes", "Links"}
    assert "sign" in sheets["Links"].columns


def test_single_network_path(tmp_path):
    nodes_df = pd.read_csv(EXAMPLES / "example_nodes.csv")
    links_df = pd.read_csv(EXAMPLES / "example_links_unweighted.csv")
    nodes, links = load_data.from_tables(nodes_df, links_df)
    nodes_out, _ = pipeline.analyze_single_network(nodes, links, n_trials=0, group_attr="Theme")
    assert len(nodes_out) == 10
    assert nodes_out["Catalytic Score"].between(0, 100).all()
    nodes_mc, _ = pipeline.analyze_single_network(nodes, links, n_trials=5, seed=2, group_attr="Theme")
    assert nodes_mc["n_trials"].unique().tolist() == [5]
