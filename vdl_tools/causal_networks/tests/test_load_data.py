"""Loading any table pair, and the Undercurrent export formats, into the standard two tables."""
import json

import pandas as pd
import pytest

from vdl_tools.causal_networks import load_data


def test_from_tables_with_label_endpoints_and_extra_columns():
    nodes_df = pd.DataFrame({"Label": ["b", "a", "c"], "Theme": ["T1", "T2", "T1"]})
    links_df = pd.DataFrame({"Source": ["a", "b"], "Target": ["b", "c"], "sign": ["+", "-"], "weight": [1.0, 2.0]})
    nodes, links = load_data.from_tables(nodes_df, links_df)
    assert nodes["Label"].tolist() == ["a", "b", "c"]          # sorted by label
    assert nodes["id"].tolist() == [0, 1, 2]
    assert nodes["Theme"].tolist() == ["T2", "T1", "T1"]        # extra node column carried through
    assert links[["Source", "Target"]].values.tolist() == [[0, 1], [1, 2]]
    assert links["fromName"].tolist() == ["a", "b"]
    assert links["sign"].tolist() == ["+", "-"]                 # extra link columns carried through
    assert list(links.columns[:4]) == ["Source", "Target", "fromName", "toName"]


def test_from_tables_with_id_endpoints():
    nodes_df = pd.DataFrame({"name": ["x", "y"], "code": ["n1", "n2"]})
    links_df = pd.DataFrame({"from": ["n1"], "to": ["n2"]})
    nodes, links = load_data.from_tables(nodes_df, links_df, label_col="name", source_col="from",
                                         target_col="to", node_id_col="code")
    assert "Label" in nodes.columns and "name" not in nodes.columns
    assert links[["Source", "Target"]].values.tolist() == [[0, 1]]


def test_from_tables_rejects_unknown_endpoint_and_drops_self_links():
    nodes_df = pd.DataFrame({"Label": ["a", "b"]})
    with pytest.raises(ValueError, match="matches no node"):
        load_data.from_tables(nodes_df, pd.DataFrame({"Source": ["a"], "Target": ["zzz"]}))
    with pytest.warns(UserWarning, match="self-links"):
        _, links = load_data.from_tables(nodes_df, pd.DataFrame({"Source": ["a", "a"], "Target": ["a", "b"]}))
    assert len(links) == 1


def _undercurrent_export():
    # labels chosen so that sorting "Label" and "Label <uuid>" would disagree if ids came from sorting
    elements = [{"id": "u-1", "label": "Housing"}, {"id": "u-2", "label": "Housing-related costs"},
                {"id": "u-3", "label": "Childcare"}]
    connections = [
        {"id": "l1", "from": "u-1", "to": "u-3", "votes": 9, "sum": 3, "yes": 5, "no": 2},   # 2 skips
        {"id": "l2", "from": "u-2", "to": "u-1", "votes": 4, "sum": -2, "yes": 1, "no": 3},
        {"id": "l3", "from": "u-3", "to": "u-2", "votes": 2, "sum": 0, "yes": 1, "no": 1},
    ]
    return {"elements": elements, "connections": connections}


def test_from_undercurrent_json_maps_uuids_and_recomputes_votes(tmp_path):
    path = tmp_path / "export.json"
    path.write_text(json.dumps(_undercurrent_export()))
    nodes, links = load_data.from_undercurrent(path)
    assert nodes["Label"].tolist() == ["Childcare", "Housing", "Housing-related costs"]
    assert links.loc[0, ["fromName", "toName"]].tolist() == ["Housing", "Childcare"]
    assert links["votes"].tolist() == [7, 4, 2]            # yes + no, skips excluded
    assert "sum" not in links.columns and "link_uuid" not in links.columns
    assert links["fracYes"].tolist() == pytest.approx([5 / 7, 1 / 4, 1 / 2])


def test_from_undercurrent_xlsx_matches_json(tmp_path):
    export = _undercurrent_export()
    (tmp_path / "export.json").write_text(json.dumps(export))
    labels = {e["id"]: e["label"] for e in export["elements"]}
    elements = pd.DataFrame({"ID": labels.keys(), "Label": labels.values()})
    connections = pd.DataFrame([{"ID": c["id"], "From": f'{labels[c["from"]]} <{c["from"]}>',
                                 "To": f'{labels[c["to"]]} <{c["to"]}>', "votes": c["votes"], "sum": c["sum"],
                                 "yes": c["yes"], "no": c["no"]} for c in export["connections"]])
    with pd.ExcelWriter(tmp_path / "export.xlsx") as w:
        elements.to_excel(w, sheet_name="Elements", index=False)
        connections.to_excel(w, sheet_name="Connections", index=False)
    nodes_j, links_j = load_data.from_undercurrent(tmp_path / "export.json")
    nodes_x, links_x = load_data.from_undercurrent(tmp_path / "export.xlsx")
    pd.testing.assert_frame_equal(nodes_j, nodes_x)
    pd.testing.assert_frame_equal(links_j, links_x)


def test_threshold_links_and_build_graph():
    nodes = pd.DataFrame({"id": [0, 1, 2], "Label": list("abc")})
    links = pd.DataFrame({"Source": [0, 1, 2], "Target": [1, 2, 0], "votes": [5, 3, 2],
                          "fracYes": [0.8, 0.5, 1.0]})
    kept = load_data.threshold_links(links, min_votes=3, pct_yes=60)
    assert kept[["Source", "Target"]].values.tolist() == [[0, 1]]   # 1->2 too few yes, 2->0 too few votes
    assert kept["pct_consensus"].tolist() == [60]
    nw = load_data.build_graph(nodes, kept)
    assert set(nw.nodes()) == {0, 1, 2}          # isolated node 2 is still in the graph


def test_add_node_attributes_strips_whitespace_and_sets_label():
    nodes = pd.DataFrame({"id": [0, 1], "Label": ["a", "b"]})
    attrs = pd.DataFrame({"Label": ["a ", "b"], "Short Name": ["A", None], "Pillar": ["P1 ", "P1"], "junk": [1, 2]})
    out = load_data.add_node_attributes(nodes, attrs, keep=("Short Name", "Pillar"))
    assert out["Pillar"].tolist() == ["P1", "P1"]
    assert out["label"].tolist() == ["A", "b"]
    assert "junk" not in out.columns
