"""One Earth taxonomy for JEV mapping: workbook loader, question wording and default policy.

This package resolves only One Earth inputs. It never imports the Drawdown package, and
the generic engine in ``taxonomy_mapping.activity_jev`` never imports this one. Tracker
output conventions belong to the consumer (Climate Landscape), not here.
"""

from ..activity_jev.engine import MatchPolicy
from ..activity_jev.taxonomies import load_workbook
from . import questions as oe

COLUMNS = ("Pillar", "Sub-Pillar", "Solution")
SHEETS = ("Pillars", "SubPillars", "Solutions")
# Tiers 0.7 then 0.5 with no descent, as in the oneearth prototype. A development default.
DEFAULT_POLICY = MatchPolicy()


def load_taxonomy(path):
    """Load the One Earth workbook as a three-level ``Taxonomy`` with built questions.

    Parameters
    ----------
    path : str or pathlib.Path
        The reviewed ``OE Solutions Terms ... .xlsx`` workbook from shared-data. Sheets
        ``Pillars``, ``SubPillars`` and ``Solutions`` are read. A row with
        ``Exclude == 1`` is dropped together with all of its descendants.

    Returns
    -------
    Taxonomy
        ``columns == ("Pillar", "Sub-Pillar", "Solution")``; ``nodes`` has one row per
        included node with ``node_id``, ``level``, ``depth``, ``name``,
        ``Definition``, the three path columns and a ``question`` dict.
    """
    return load_workbook(
        "one_earth", path, columns=COLUMNS, sheets=SHEETS, question_builder=build_questions
    )


def build_questions(nodes, columns):
    """One Jev question per node in the prototype's ``cc_routing`` wording.

    A Solution under a Cross-Cutting Sub-Pillar must also qualify for that Sub-Pillar,
    and three nodes carry explicit true/false routing criteria.
    """
    nodes = nodes.copy()
    definitions = {
        (r["Pillar"], r["Sub-Pillar"]): r["Definition"]
        for r in nodes.to_dict("records")
        if r["depth"] == 1
    }
    questions = []
    for r in nodes.to_dict("records"):
        ancestors = [f'{c} "{r[c]}"' for c in columns[: r["depth"]]]
        parent = f" (within {', '.join(ancestors)})" if ancestors else ""
        text = oe.NODE_QUESTION.format(
            level=r["level"], name=r["name"], parent=parent, definition=r["Definition"]
        )
        if r["depth"] == 2 and r["Sub-Pillar"].startswith(oe.CC_PREFIX):
            text += oe.CC_CONDITION.format(
                subpillar=r["Sub-Pillar"],
                subpillar_definition=definitions[(r["Pillar"], r["Sub-Pillar"])],
            )
        question = {"type": "noul", "instructions": text}
        if criteria := oe.ROUTING_CRITERIA.get((r["level"], r["name"])):
            question["criteria"] = dict(criteria)
        questions.append(question)
    nodes["question"] = questions
    return nodes
