"""Drawdown taxonomy for JEV mapping: workbook loader, question wording, policy, export.

This package resolves only Drawdown inputs. It never imports the One Earth package,
and the generic engine in ``taxonomy_mapping.activity_jev`` never imports this one.
"""

import pandas as pd

from ..activity_jev.engine import MatchPolicy
from ..activity_jev.taxonomies import load_workbook
from . import questions as dd

COLUMNS = ("Sector", "Cluster", "Solution")
SHEETS = ("Sectors", "SectorClusters", "Solutions")
LEVEL_NAMES = ("Sector", "SectorCluster", "Solution")
DEFAULT_VARIANT = "stewardship_yes"

# Solution-sheet attributes carried onto nodes and exported as legacy list columns.
SOLUTION_PROPERTIES = ("CATEGORY", "CLIMATE POLLUTANTS", "SPEED OF ACTION")
PROPERTY_COLUMNS = (
    "drawdown_solution_category",
    "drawdown_climate_pollutants",
    "drawdown_speed_of_action",
)


def default_policy():
    """Development match policy for Drawdown activities.

    Returns
    -------
    MatchPolicy
        Tiers 0.7 then 0.5, then recursive descent into children scoring at least 0.6
        under a matched parent, as in the inspected drawdown prototype. Not a
        validated production threshold (Q5 / VIB-76 is open).
    """
    return MatchPolicy(descend=0.6)


def load_taxonomy(path, *, variant=None):
    """Load the Drawdown workbook as a three-level ``Taxonomy`` with built questions.

    Parameters
    ----------
    path : str or pathlib.Path
        The reviewed ``DrawdownTaxonomy_VDL_<date>.xlsx`` workbook from shared-data.
        Sheets ``Sectors``, ``SectorClusters`` and ``Solutions`` are read. A row with
        ``Exclude == 1`` is dropped together with all of its descendants.
    variant : str, optional
        Question wording from ``questions.QUESTION_VARIANTS``: ``base``,
        ``stewardship`` or ``stewardship_yes`` (default).

    Returns
    -------
    Taxonomy
        ``columns == ("Sector", "Cluster", "Solution")`` and
        ``subject_kind == "activity"``. ``nodes`` has one row per included node with
        ``node_id``, ``level``, ``depth``, ``name``, ``Definition``, the three path
        columns, the ``SOLUTION_PROPERTIES`` attributes (None above Solution) and a
        ``question`` dict (``type``, ``instructions``, optional ``criteria``).

    Raises
    ------
    ValueError
        Unknown variant, a variant naming a node that is absent or ambiguous in the
        workbook, or a workbook that fails the structural checks in ``load_workbook``.
    """
    variant = variant or DEFAULT_VARIANT
    if variant not in dd.QUESTION_VARIANTS:
        raise ValueError(f"Unknown drawdown question variant: {variant}")
    return load_workbook(
        "drawdown",
        path,
        columns=COLUMNS,
        sheets=SHEETS,
        level_names=LEVEL_NAMES,
        variant=variant,
        question_builder=_drawdown_questions,
        attributes=SOLUTION_PROPERTIES,
        subject_kind="activity",
    )


def _drawdown_questions(nodes, columns, variant):
    """Build one Jev question per node, applying the per-node changes of ``variant``."""
    nodes = nodes.copy()
    questions, applied = [], set()
    changes = dd.QUESTION_VARIANTS[variant]
    for r in nodes.to_dict("records"):
        ancestors = [f'{c} "{r[c]}"' for c in columns[: r["depth"]]]
        parent = f" (within {', '.join(ancestors)})" if ancestors else ""
        question = {
            "type": "noul",
            "instructions": dd.NODE_QUESTION.format(
                level=r["level"],
                name=r["name"],
                parent=parent,
                definition=r["Definition"],
            ),
        }
        key = (r["level"], r["name"])
        if key in changes:
            if key in applied:
                raise ValueError(f"Question variant matches an ambiguous node name: {key}")
            applied.add(key)
            change = changes[key]
            if "condition" in change:
                question["instructions"] += " " + change["condition"]
            if "criteria" in change:
                question["criteria"] = dict(change["criteria"])
        questions.append(question)
    if set(changes) - applied:
        raise ValueError(
            f"Question variant {variant} does not fit taxonomy: {set(changes) - applied}"
        )
    nodes["question"] = questions
    return nodes


def export_extraction(
    organizations, extraction, taxonomy, mapped, *, id_col="id", name_col="Organization"
):
    """Attach the legacy Drawdown tracker columns and level-2 funding to an organization frame.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization; ``id_col`` must be unique and non-null.
    extraction : ExtractionResult
        The saved extraction whose ``activities`` were mapped. Its organization
        cohort must equal ``organizations[id_col]``.
    taxonomy : Taxonomy
        The Drawdown taxonomy the activities were scored against.
    mapped : SubjectMappingResult
        Output of ``map_subjects`` for ``extraction.activities`` with ``activity_id``
        renamed to ``subject_id``.
    id_col, name_col : str
        Organization ID and display-name columns in ``organizations``.

    Returns
    -------
    MappingResult
        ``organizations`` gains ``level{i}_drawdown_category`` and
        ``all_level{i}_drawdown_category`` for i in 0..3 (level 3 is always empty),
        ``drawdown_category``, ``cat_level_drawdown_category``,
        ``drawdown_mapping_status`` and the three ``PROPERTY_COLUMNS`` lists aligned
        1:1 with ``all_level2_drawdown_category``, exactly as the legacy
        ``drawdown_hierarchical_taxonomy_mapping`` emits them. ``paths`` carries the
        ``Sector: Cluster`` display prefix, an empty ``Activity`` column and the
        Solution property values. ``funding`` is allocated with ``max_level=2``.

    Raises
    ------
    ValueError
        Duplicate Solution labels, which the property lookup cannot disambiguate.
    """
    from ..activity_jev.exports import export_extraction as export

    result = export(
        organizations,
        extraction,
        taxonomy,
        mapped,
        id_col=id_col,
        name_col=name_col,
        mapping_name="drawdown_category",
        path_formatter=format_paths,
        pad_to_n_levels=4,
        max_funding_level=2,
    )
    solutions = taxonomy.nodes[taxonomy.nodes.depth == 2]
    if solutions.Solution.duplicated().any():
        raise ValueError("Legacy Drawdown property columns require unique Solution labels")
    for source, target in zip(SOLUTION_PROPERTIES, PROPERTY_COLUMNS, strict=True):
        lookup = solutions.set_index("Solution")[source].to_dict()
        result.organizations[target] = result.organizations["all_level2_drawdown_category"].map(
            lambda values, lookup=lookup: [lookup.get(v) for v in values]
        )
        result.paths[source] = result.paths.Solution.map(lookup)
        result.lineage["export_columns"].append(target)
    return result


def format_paths(paths):
    """Prefix clusters with their sector, as the legacy display expects, and add ``Activity``."""
    paths = paths.copy()
    paths["Cluster"] = [
        f"{sector}: {cluster}"
        if pd.notna(cluster) and not str(cluster).startswith(f"{sector}: ")
        else cluster
        for sector, cluster in zip(paths.Sector, paths.Cluster, strict=True)
    ]
    paths["Activity"] = None
    return paths
