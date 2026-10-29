"""Independent drawdown taxonomy loading, policy and compatibility export."""

import pandas as pd

from . import drawdown_questions as dd
from .engine import MatchPolicy
from .taxonomies import load_workbook

SOLUTION_PROPERTIES = ("CATEGORY", "CLIMATE POLLUTANTS", "SPEED OF ACTION")


def default_policy():
    return MatchPolicy(descend=0.6)


def load_taxonomy(path, *, variant=None):
    variant = variant or "stewardship_yes"
    if variant not in dd.QUESTION_VARIANTS:
        raise ValueError(f"Unknown drawdown question variant: {variant}")
    return load_workbook(
        "drawdown",
        path,
        columns=("Sector", "Cluster", "Solution"),
        sheets=("Sectors", "SectorClusters", "Solutions"),
        level_names=("Sector", "SectorCluster", "Solution"),
        variant=variant,
        question_builder=_drawdown_questions,
        attributes=SOLUTION_PROPERTIES,
        subject_kind="activity",
    )


def _drawdown_questions(nodes, columns, variant):
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
    from .exports import export_extraction as export

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
    for source, target in zip(
        SOLUTION_PROPERTIES,
        ("drawdown_solution_category", "drawdown_climate_pollutants", "drawdown_speed_of_action"),
        strict=True,
    ):
        lookup = solutions.set_index("Solution")[source].to_dict()
        result.organizations[target] = result.organizations["all_level2_drawdown_category"].map(
            lambda values, lookup=lookup: [lookup.get(v) for v in values]
        )
        result.paths[source] = result.paths.Solution.map(lookup)
        result.lineage["export_columns"].append(target)
    return result


def format_paths(paths):
    paths = paths.copy()
    paths["Cluster"] = [
        f"{sector}: {cluster}"
        if pd.notna(cluster) and not str(cluster).startswith(f"{sector}: ")
        else cluster
        for sector, cluster in zip(paths.Sector, paths.Cluster, strict=True)
    ]
    paths["Activity"] = None
    return paths
