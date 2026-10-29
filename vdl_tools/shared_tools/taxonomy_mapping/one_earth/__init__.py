"""One Earth taxonomy for JEV mapping: workbook loader, question wording, policy, export.

This package resolves only One Earth inputs. It never imports the Drawdown package,
and the generic engine in ``taxonomy_mapping.activity_jev`` never imports this one.
"""

from ..activity_jev.engine import MatchPolicy
from ..activity_jev.taxonomies import load_workbook
from . import questions as oe

COLUMNS = ("Pillar", "Sub-Pillar", "Solution")
SHEETS = ("Pillars", "SubPillars", "Solutions")
LEVEL_NAMES = ("Pillar", "SubPillar", "Solution")
DEFAULT_VARIANT = "cc_routing"


def default_policy():
    """Development match policy for One Earth activities.

    Returns
    -------
    MatchPolicy
        Tiers 0.7 then 0.5 with no descendant recovery, as in the inspected
        oneearth prototype. Not a validated production threshold (Q5 / VIB-76 is open).
    """
    return MatchPolicy(descend=None)


def load_taxonomy(path, *, variant=None):
    """Load the One Earth workbook as a three-level ``Taxonomy`` with built questions.

    Parameters
    ----------
    path : str or pathlib.Path
        The reviewed ``OE Solutions Terms ... .xlsx`` workbook from shared-data.
        Sheets ``Pillars``, ``SubPillars`` and ``Solutions`` are read. A row with
        ``Exclude == 1`` is dropped together with all of its descendants.
    variant : str, optional
        Question wording from ``questions.QUESTION_VARIANTS``: ``base``,
        ``cc_condition``, ``cc_criteria`` or ``cc_routing`` (default).

    Returns
    -------
    Taxonomy
        ``columns == ("Pillar", "Sub-Pillar", "Solution")`` and
        ``subject_kind == "activity"``. ``nodes`` has one row per included node with
        ``node_id``, ``level``, ``depth``, ``name``, ``Definition``, the three path
        columns and a ``question`` dict (``type``, ``instructions``, optional
        ``criteria``).

    Raises
    ------
    ValueError
        Unknown variant, or a workbook that fails the structural checks in
        ``load_workbook`` (blank path or definition, duplicate or orphan path).
    """
    variant = variant or DEFAULT_VARIANT
    if variant not in oe.QUESTION_VARIANTS:
        raise ValueError(f"Unknown one_earth question variant: {variant}")
    return load_workbook(
        "one_earth",
        path,
        columns=COLUMNS,
        sheets=SHEETS,
        level_names=LEVEL_NAMES,
        variant=variant,
        question_builder=_one_earth_questions,
        attributes=(),
        subject_kind="activity",
    )


def _activity_wording(template: str) -> str:
    """Rewrite the prototype's organization-level wording to address one activity."""
    for old, new in oe.ACTIVITY_WORDING:
        template = template.replace(old, new)
    if "organization" in template.lower():
        raise ValueError("Organization wording remains in an activity question template")
    return template


def _one_earth_questions(nodes, columns, variant):
    """Build one Jev question per node, applying the Cross-Cutting rules of ``variant``."""
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
    """Attach the legacy One Earth tracker columns and level-2 funding to an organization frame.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization; ``id_col`` must be unique and non-null.
    extraction : ExtractionResult
        The saved extraction whose ``activities`` were mapped. Its organization
        cohort must equal ``organizations[id_col]``.
    taxonomy : Taxonomy
        The One Earth taxonomy the activities were scored against.
    mapped : SubjectMappingResult
        Output of ``map_subjects`` for ``extraction.activities`` with ``activity_id``
        renamed to ``subject_id``.
    id_col, name_col : str
        Organization ID and display-name columns in ``organizations``.

    Returns
    -------
    MappingResult
        ``organizations`` gains ``level{i}_one_earth_category`` and
        ``all_level{i}_one_earth_category`` for i in 0..3 (level 3 is always empty),
        ``one_earth_category``, ``cat_level_one_earth_category`` and
        ``one_earth_mapping_status``. ``paths`` carries the legacy ``Sub-Term``
        column (always None). ``funding`` is allocated with ``max_level=2``.
    """
    from ..activity_jev.exports import export_extraction as export

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
    """Add the empty ``Sub-Term`` column that legacy One Earth consumers expect."""
    return paths.assign(**{"Sub-Term": None})
