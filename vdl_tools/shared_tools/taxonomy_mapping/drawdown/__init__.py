"""Drawdown taxonomy for JEV mapping: workbook loader, question wording and default policy.

This package resolves only Drawdown inputs. It never imports the One Earth package, and
the generic engine in ``taxonomy_mapping.activity_jev`` never imports this one. Tracker
output conventions belong to the consumer (Climate Landscape), not here.
"""

from ..activity_jev.engine import MatchPolicy
from ..activity_jev.taxonomies import load_workbook
from . import questions as dd

COLUMNS = ("Sector", "Cluster", "Solution")
SHEETS = ("Sectors", "SectorClusters", "Solutions")
# Solutions-sheet attributes carried onto nodes; the Climate Landscape export lists them.
SOLUTION_PROPERTIES = ("CATEGORY", "CLIMATE POLLUTANTS", "SPEED OF ACTION")
# Tiers 0.7 then 0.5, then descent into children at 0.6 under a matched parent, as in the
# drawdown prototype. A development default.
DEFAULT_POLICY = MatchPolicy(descend=0.6)


def load_taxonomy(path):
    """Load the Drawdown workbook as a three-level ``Taxonomy`` with built questions.

    Parameters
    ----------
    path : str or pathlib.Path
        The reviewed ``DrawdownTaxonomy_VDL_<date>.xlsx`` workbook from shared-data.
        Sheets ``Sectors``, ``SectorClusters`` and ``Solutions`` are read. A row with
        ``Exclude == 1`` is dropped together with all of its descendants.

    Returns
    -------
    Taxonomy
        ``columns == ("Sector", "Cluster", "Solution")``; ``nodes`` has one row per
        included node with ``node_id``, ``level``, ``depth``, ``name``, ``Definition``,
        the three path columns, the ``SOLUTION_PROPERTIES`` (None above Solution) and
        a ``question`` dict.

    Raises
    ------
    ValueError
        The workbook lacks, or has more than one of, a node the stewardship criteria
        name; the pinned wording would otherwise silently stop applying.
    """
    return load_workbook(
        "drawdown",
        path,
        columns=COLUMNS,
        sheets=SHEETS,
        question_builder=build_questions,
        attributes=SOLUTION_PROPERTIES,
    )


def build_questions(nodes, columns):
    """One Jev question per node in the prototype's ``stewardship_yes`` wording."""
    nodes = nodes.copy()
    questions, applied = [], set()
    for r in nodes.to_dict("records"):
        ancestors = [f'{c} "{r[c]}"' for c in columns[: r["depth"]]]
        parent = f" (within {', '.join(ancestors)})" if ancestors else ""
        question = {
            "type": "noul",
            "instructions": dd.NODE_QUESTION.format(
                level=r["level"], name=r["name"], parent=parent, definition=r["Definition"]
            ),
        }
        key = (r["level"], r["name"])
        if key in dd.STEWARDSHIP_CRITERIA:
            if key in applied:
                raise ValueError(f"Stewardship criteria match an ambiguous node name: {key}")
            applied.add(key)
            question["criteria"] = dict(dd.STEWARDSHIP_CRITERIA[key])
        questions.append(question)
    if missing := set(dd.STEWARDSHIP_CRITERIA) - applied:
        raise ValueError(f"Stewardship criteria name nodes absent from the workbook: {missing}")
    nodes["question"] = questions
    return nodes
