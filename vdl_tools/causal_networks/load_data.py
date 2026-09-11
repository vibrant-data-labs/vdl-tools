"""
Load causal-network data into the two tables that every analysis function expects.

The analysis does not care where the data came from. It only needs:

    nodes : one row per factor
        id       integer 0..N-1 (assigned here, sorted by label)
        Label    the factor text
        ...      any other node columns are carried through untouched

    links : one row per directed causal link (source -> target)
        Source, Target    integer node ids
        fromName, toName  the node labels (for readability)
        yes, no           vote counts per link (e.g. Kumu's Undercurrent)  -> full ensemble pipeline
        weight            a pre-computed link strength                 -> single-network analysis
        (nothing)         just the link list                           -> single-network analysis
        ...               any other link columns are carried through untouched, for display only

Links are unsigned: a link means "if the source improves, the target improves too".
A `sign` column (or any other link metadata) is carried through to the outputs but not analysed.
"""

import json
import re
import warnings
from pathlib import Path

import networkx as nx
import pandas as pd


# ---------------------------------------------------------------------------
# Generic entry point: any two tables
# ---------------------------------------------------------------------------

def from_tables(nodes_df, links_df,
                label_col="Label",       # column in nodes_df holding the factor text
                source_col="Source",     # column in links_df holding the link start
                target_col="Target",     # column in links_df holding the link end
                node_id_col=None,        # column in nodes_df that link endpoints may refer to (e.g. a uuid)
                allow_self_links=False,  # keep links from a node to itself
                ):
    """
    Convert any nodes table and links table into the standard two-table format.

    Link endpoints may be node labels, or values of `node_id_col` (e.g. survey uuids).
    Every node gets an integer `id` (0..N-1, sorted by label); links get integer `Source`/`Target`
    plus `fromName`/`toName`. If the links have `yes` and `no` vote counts, `votes` (= yes + no) and
    `fracYes` are added. All other columns in both tables are kept unchanged.

    Raises ValueError if labels are not unique or a link endpoint matches no node.
    """
    nodes = nodes_df.copy()
    links = links_df.copy()

    # --- nodes: clean labels, sort, assign integer ids
    nodes["Label"] = nodes[label_col].astype(str).str.strip()
    if label_col != "Label":
        nodes = nodes.drop(columns=[label_col])
    if nodes["Label"].duplicated().any():
        dupes = nodes.loc[nodes["Label"].duplicated(), "Label"].tolist()
        raise ValueError(f"Node labels must be unique. Duplicates: {dupes[:5]}")
    nodes = nodes.sort_values("Label").reset_index(drop=True)
    nodes["id"] = range(len(nodes))
    other_node_cols = [c for c in nodes.columns if c not in ("id", "Label")]
    nodes = nodes[["id", "Label"] + other_node_cols]

    # --- links: map endpoints (labels, or original node ids) to the new integer ids
    endpoint_to_id = dict(zip(nodes["Label"], nodes["id"]))
    if node_id_col is not None:
        endpoint_to_id.update(dict(zip(nodes[node_id_col].astype(str).str.strip(), nodes["id"])))
    source_ids = links[source_col].astype(str).str.strip().map(endpoint_to_id)
    target_ids = links[target_col].astype(str).str.strip().map(endpoint_to_id)
    unmapped = source_ids.isna() | target_ids.isna()
    if unmapped.any():
        examples = links.loc[unmapped, [source_col, target_col]].head(5).values.tolist()
        raise ValueError(f"{unmapped.sum()} links have an endpoint that matches no node. "
                         f"First few: {examples}")
    links = links.drop(columns=[c for c in (source_col, target_col) if c in links.columns])
    links["Source"] = source_ids.astype(int)
    links["Target"] = target_ids.astype(int)

    # --- self links: usually a data error, drop unless asked to keep
    is_self = links["Source"] == links["Target"]
    if is_self.any() and not allow_self_links:
        warnings.warn(f"Dropping {is_self.sum()} self-links (source == target).")
        links = links[~is_self]

    # --- duplicate links: keep them but say so (they will double count in the graph)
    n_dupes = links.duplicated(["Source", "Target"]).sum()
    if n_dupes:
        warnings.warn(f"{n_dupes} duplicate Source->Target links found; consider aggregating them first.")

    # --- readable endpoint names
    id_to_label = dict(zip(nodes["id"], nodes["Label"]))
    links["fromName"] = links["Source"].map(id_to_label)
    links["toName"] = links["Target"].map(id_to_label)

    # --- voted links: votes = yes + no (skips are not votes), fracYes = share of yes
    if "yes" in links.columns and "no" in links.columns:
        links["votes"] = links["yes"] + links["no"]
        links["fracYes"] = links["yes"] / links["votes"]  # NaN when nobody voted; never passes min_votes

    # --- put the standard columns first
    standard = ["Source", "Target", "fromName", "toName"]
    links = links[standard + [c for c in links.columns if c not in standard]]
    links = links.reset_index(drop=True)
    return nodes, links


# ---------------------------------------------------------------------------
# Loaders for specific export formats
# ---------------------------------------------------------------------------

def _endpoint_from_kumu_string(text):
    """Kumu writes link endpoints as 'Label <uuid>'. Return the uuid if present, else the label."""
    match = re.search(r"<([^<>]+)>\s*$", str(text))
    return match.group(1).strip() if match else str(text).strip()


def _label_from_kumu_string(text):
    """Kumu writes link endpoints as 'Label <code>'. Return just the label part."""
    return str(text).split(" <")[0].strip()


def from_undercurrent(path):
    """
    Load a raw export from Undercurrent (Kumu's pairwise-voting survey tool, kumu.io) into the standard two tables.

    Accepts the raw .json export ({"elements": [...], "connections": [...]}) or the two-sheet .xlsx
    (sheet 'Elements' with ID/Label, sheet 'Connections' with ID/From/To/yes/no, where From/To
    look like 'Label <uuid>').

    Votes are recomputed as votes = yes + no. The raw `votes` column counts everyone who was shown
    the pair including people who skipped it, so it is not a vote count and is dropped. The raw
    `sum` column (yes - no) is dropped as redundant. Skips are not votes.
    """
    path = Path(path)
    if path.suffix.lower() == ".json":
        with open(path) as f:
            data = json.load(f)
        elements = pd.DataFrame(data["elements"]).rename(columns={"id": "uuid", "label": "Label"})
        connections = pd.DataFrame(data["connections"]).rename(
            columns={"id": "link_uuid", "from": "Source", "to": "Target"})
    elif path.suffix.lower() in (".xlsx", ".xls"):
        elements = pd.read_excel(path, sheet_name="Elements").rename(columns={"ID": "uuid"})
        connections = pd.read_excel(path, sheet_name="Connections").rename(
            columns={"ID": "link_uuid", "From": "Source", "To": "Target"})
        for col in ("Source", "Target"):  # 'Label <uuid>' -> uuid (or the label when no uuid)
            connections[col] = connections[col].map(_endpoint_from_kumu_string)
    else:
        raise ValueError(f"Expected a .json or .xlsx Undercurrent export, got {path.name}")

    # drop the raw columns that would mislead (see docstring); from_tables recomputes votes = yes + no
    connections = connections.drop(columns=[c for c in ("votes", "sum", "link_uuid") if c in connections.columns])
    return from_tables(elements, connections, node_id_col="uuid")


def from_kumu_weighted(path):
    """
    Load a Kumu 'Elements'/'Connections' Excel export whose links carry a `weight` instead of votes.
    From/To may be plain labels or 'Label <code>'. Returns the standard two tables with `weight`.

    NOTE: this format was used for the Montenegro project; no example file is available, so this
    loader is untested against real data.
    """
    elements = pd.read_excel(path, sheet_name="Elements")
    connections = pd.read_excel(path, sheet_name="Connections").rename(columns={"From": "Source", "To": "Target"})
    for col in ("Source", "Target"):
        connections[col] = connections[col].map(_label_from_kumu_string)
    return from_tables(elements, connections)


# ---------------------------------------------------------------------------
# From links to a network
# ---------------------------------------------------------------------------

def threshold_links(links, min_votes=3, pct_yes=50):
    """
    Keep the links that count as a consensus causal link:
    at least `min_votes` votes and at least `pct_yes` % of them 'yes'.
    Adds a `pct_consensus` column recording the threshold used.
    """
    enough_votes = links["votes"] >= min_votes
    enough_yes = links["fracYes"] >= pct_yes / 100
    kept = links[enough_votes & enough_yes].copy()
    kept["pct_consensus"] = pct_yes
    return kept.reset_index(drop=True)


def threshold_weights(links, min_weight):
    """Keep links whose `weight` is at least `min_weight`."""
    return links[links["weight"] >= min_weight].reset_index(drop=True)


def build_graph(nodes, links):
    """
    Build a directed networkx graph from the two tables.
    Every node is added first, so isolated nodes stay in the graph (and get metrics of zero)
    instead of silently disappearing.
    """
    nw = nx.DiGraph()
    nw.add_nodes_from(nodes["id"].tolist())
    nw.add_edges_from(zip(links["Source"], links["Target"]))
    return nw


# ---------------------------------------------------------------------------
# Optional hand-curated node metadata (short names, themes, ...)
# ---------------------------------------------------------------------------

def add_node_attributes(nodes, attributes,
                        on="Label",   # column to match on (in both tables)
                        keep=None,    # attribute columns to add; default = all columns except `on`
                        ):
    """
    Left-merge hand-curated node metadata (e.g. 'Short Name', 'Pillar') onto the nodes table.

    `attributes` is a DataFrame, a .csv path, or an .xlsx path (sheet 'Elements' if it exists,
    otherwise the first sheet). Text is stripped of surrounding whitespace before matching and
    before use, because a stray trailing space silently turns one theme into two.
    Nodes with no match get NaN and a warning listing them.
    If a 'Short Name' column is added, a `label` column (short name, else Label[:40]) is set for display.
    """
    if isinstance(attributes, (str, Path)):
        path = Path(attributes)
        if path.suffix.lower() in (".xlsx", ".xls"):
            sheets = pd.read_excel(path, sheet_name=None)
            attrs = sheets["Elements"] if "Elements" in sheets else next(iter(sheets.values()))
        else:
            attrs = pd.read_csv(path)
    else:
        attrs = attributes.copy()

    if keep is None:
        keep = [c for c in attrs.columns if c != on]
    attrs = attrs[[on] + list(keep)].copy()
    for col in [on] + list(keep):  # strip whitespace from text columns
        if attrs[col].dtype == object:
            attrs[col] = attrs[col].where(attrs[col].isna(), attrs[col].astype(str).str.strip())
    attrs = attrs.drop_duplicates(subset=[on])

    nodes = nodes.drop(columns=[c for c in keep if c in nodes.columns])  # avoid _x/_y on re-run
    merged = nodes.merge(attrs, on=on, how="left", indicator=True)

    unmatched_nodes = merged.loc[merged["_merge"] == "left_only", on].tolist()
    merged = merged.drop(columns=["_merge"])
    if unmatched_nodes:
        warnings.warn(f"{len(unmatched_nodes)} nodes have no matching row in the attributes table: "
                      f"{unmatched_nodes[:5]}")
    unmatched_attrs = set(attrs[on]) - set(nodes[on])
    if unmatched_attrs:
        warnings.warn(f"{len(unmatched_attrs)} attribute rows match no node: {sorted(unmatched_attrs)[:5]}")

    if "Short Name" in merged.columns:
        merged["label"] = merged["Short Name"].fillna(merged["Label"].str[:40])
    return merged
