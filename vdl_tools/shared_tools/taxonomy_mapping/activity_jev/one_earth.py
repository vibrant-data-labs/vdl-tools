"""Independent one_earth taxonomy loading, policy and compatibility export."""

from . import oneearth_questions as oe
from .engine import MatchPolicy
from .taxonomies import load_workbook


def default_policy():
    return MatchPolicy(descend=None)


def load_taxonomy(path, *, variant=None):
    variant = variant or "cc_routing"
    if variant not in oe.QUESTION_VARIANTS:
        raise ValueError(f"Unknown one_earth question variant: {variant}")
    return load_workbook(
        "one_earth",
        path,
        columns=("Pillar", "Sub-Pillar", "Solution"),
        sheets=("Pillars", "SubPillars", "Solutions"),
        level_names=("Pillar", "SubPillar", "Solution"),
        variant=variant,
        question_builder=_one_earth_questions,
        attributes=(),
        subject_kind="activity",
    )


def _activity_wording(template: str) -> str:
    for old, new in oe.ACTIVITY_WORDING:
        template = template.replace(old, new)
    if "organization" in template.lower():
        raise ValueError("Organization wording remains in an activity question template")
    return template


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


def export_extraction(
    organizations, extraction, taxonomy, mapped, *, id_col="id", name_col="Organization"
):
    from .exports import export_extraction as export

    return export(
        organizations,
        extraction,
        taxonomy,
        mapped,
        id_col=id_col,
        name_col=name_col,
        mapping_name="one_earth_category",
        path_formatter=format_paths,
        pad_to_n_levels=4,
        max_funding_level=2,
    )


def format_paths(paths):
    return paths.assign(**{"Sub-Term": None})
