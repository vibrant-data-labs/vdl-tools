"""Pinned taxonomy adapters; question wording comes from the two source repos."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ...activity_extraction.pipeline import fingerprint
from . import drawdown_questions as dd
from . import oneearth_questions as oe

SOLUTION_PROPERTIES = ("CATEGORY", "CLIMATE POLLUTANTS", "SPEED OF ACTION")


@dataclass
class ActivityTaxonomy:
    name: str
    columns: tuple[str, ...]
    levels: list[dict]
    nodes: pd.DataFrame
    sha256: str
    path: str
    variant: str


def _activity_wording(template: str) -> str:
    for old, new in oe.ACTIVITY_WORDING:
        template = template.replace(old, new)
    if "organization" in template.lower():
        raise ValueError("Organization wording remains in an activity question template")
    return template


def load_taxonomy(name: str, path, *, variant: str | None = None) -> ActivityTaxonomy:
    """Read only the first three sheets; exclude full descendant branches consistently.

    Full paths identify nodes, including repeated child names in different branches.
    Inputs are explicit paths, never latest-file globs or source-repo imports.
    """
    if name == "one_earth":
        columns = ("Pillar", "Sub-Pillar", "Solution")
        sheets = ("Pillars", "SubPillars", "Solutions")
        level_names = ("Pillar", "SubPillar", "Solution")
        variant = variant or "cc_routing"
        if variant not in oe.QUESTION_VARIANTS:
            raise ValueError(f"Unknown One Earth question variant: {variant}")
    elif name == "drawdown":
        columns = ("Sector", "Cluster", "Solution")
        sheets = ("Sectors", "SectorClusters", "Solutions")
        level_names = ("Sector", "SectorCluster", "Solution")
        variant = variant or "stewardship_yes"
        if variant not in dd.QUESTION_VARIANTS:
            raise ValueError(f"Unknown Drawdown question variant: {variant}")
    else:
        raise ValueError(f"Unknown taxonomy: {name}")
    path = Path(path)
    payload = path.read_bytes()
    if payload.startswith(b"version https://git-lfs.github.com/spec/"):
        raise ValueError(f"Taxonomy is an unresolved Git LFS pointer: {path}")
    rows, prefixes = [], set()
    with pd.ExcelFile(path) as book:
        for depth, sheet in enumerate(sheets):
            table = pd.read_excel(book, sheet_name=sheet)
            required = [*columns[: depth + 1], "Definition"]
            if missing := set(required) - set(table):
                raise ValueError(f"{sheet} is missing columns: {sorted(missing)}")
            for raw in table.to_dict("records"):
                excluded = pd.to_numeric(raw.get("Exclude", 0), errors="coerce")
                if excluded == 1:
                    continue
                if any(pd.isna(raw[c]) or not str(raw[c]).strip() for c in required):
                    raise ValueError(f"Blank path or definition in {sheet}")
                branch = tuple(str(raw[c]).strip() for c in columns[: depth + 1])
                if depth and branch[:-1] not in prefixes:
                    # An explicitly excluded ancestor excludes its descendants.
                    # An orphan otherwise indicates a malformed taxonomy.
                    parent_table = pd.read_excel(book, sheet_name=sheets[depth - 1])
                    parent_rows = parent_table
                    for c, value in zip(columns, branch[:-1], strict=False):
                        parent_rows = parent_rows[parent_rows[c].astype(str).str.strip() == value]
                    if parent_rows.empty:
                        raise ValueError(f"Orphan taxonomy path: {branch}")
                    continue
                if branch in prefixes:
                    raise ValueError(f"Duplicate taxonomy path: {branch}")
                prefixes.add(branch)
                row = {
                    "node_id": fingerprint([name, branch])[:24],
                    "level": columns[depth],
                    "depth": depth,
                    "name": branch[-1],
                    "Definition": str(raw["Definition"]).strip(),
                    **{c: branch[i] if i <= depth else None for i, c in enumerate(columns)},
                }
                for c in SOLUTION_PROPERTIES:
                    row[c] = raw.get(c) if pd.notna(raw.get(c)) else None
                rows.append(row)
    if not rows:
        raise ValueError("Taxonomy has no included nodes")
    nodes = pd.DataFrame(rows)
    if name == "one_earth":
        nodes = _one_earth_questions(nodes, columns, variant)
    else:
        nodes = _drawdown_questions(nodes, columns, variant)
    levels = [
        {
            "idx": i,
            "name": level_names[i],
            "key_col": c,
            "output_col": c,
            "sheet": sheets[i],
            "parent_filters": list(columns[:i]),
        }
        for i, c in enumerate(columns)
    ]
    return ActivityTaxonomy(
        name, columns, levels, nodes, hashlib.sha256(payload).hexdigest(), str(path), variant
    )


def _one_earth_questions(nodes, columns, variant):
    nodes = nodes.copy()
    definitions = {
        tuple(r[c] for c in columns[:2]): r["Definition"]
        for r in nodes.to_dict("records")
        if r["depth"] == 1
    }
    questions = []
    for r in nodes.to_dict("records"):
        ancestors = [f'{c} "{r[c]}"' for c in columns[: r["depth"]]]
        parent = f" (within {', '.join(ancestors)})" if ancestors else ""
        text = _activity_wording(oe.NODE_QUESTION).format(
            level=r["level"],
            name=r["name"],
            parent=parent,
            definition=r["Definition"],
        )
        criteria = None
        if variant != "base" and r["depth"] == 2 and r["Sub-Pillar"].startswith(oe.CC_PREFIX):
            fill = {
                "name": r["name"],
                "subpillar": r["Sub-Pillar"],
                "subpillar_definition": definitions[(r["Pillar"], r["Sub-Pillar"])],
            }
            if variant in ("cc_condition", "cc_routing"):
                text += _activity_wording(oe.CC_CONDITION).format(**fill)
            else:
                criteria = {
                    "true": _activity_wording(oe.CC_CRITERIA_TRUE).format(**fill),
                    "false": _activity_wording(oe.CC_CRITERIA_FALSE).format(**fill),
                }
        if variant == "cc_routing" and (r["level"], r["name"]) in oe.ROUTING_CRITERIA:
            criteria = {
                k: _activity_wording(v)
                for k, v in oe.ROUTING_CRITERIA[(r["level"], r["name"])].items()
            }
        question = {"type": "noul", "instructions": text}
        if criteria:
            question["criteria"] = criteria
        questions.append(question)
    nodes["question"] = questions
    return nodes


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
