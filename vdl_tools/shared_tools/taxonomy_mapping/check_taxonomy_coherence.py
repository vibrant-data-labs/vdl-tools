"""
Hierarchical taxonomy coherence checker
=======================================

General audit tool for any hierarchical taxonomy where every node carries
a text Definition (e.g. One Earth Pillar/SubPillar/Solution/SubTerm,
Drawdown Sector/Solution, etc.).

The check answers three questions at every parent->children transition:

1. **Coverage**: is every child's scope contained within the parent's
   definition? (no orphan children — children that escape the parent's
   stated scope)
2. **Completeness / reflection**: does the parent's definition enumerate
   or describe the categories that its children represent? (no hidden
   children — children whose existence is not signposted by the parent)
3. **Cohesion**: do the parent and its children form a single coherent
   category, or has the parent definition been stretched to cover
   heterogeneous children (separate scope paragraphs, sibling children
   that are different KINDS of thing, or a child that sits awkwardly
   under the parent's name)? (no incoherent parents — parents that
   are a forced union of disparate scopes)

All three matter for top-down walkability: a classifier asked to
descend the taxonomy reads the parent's definition first, so any child
whose existence isn't reflected in the parent risks being skipped
(coverage / completeness), and a parent definition that spans two
unrelated scopes confuses both the classifier and any LLM judge
scoring the result (cohesion).

Completeness alone is not enough — that was the lesson from the
ed_tracker Learning & School Models Sub-Pillar: a prior revision
satisfied completeness by gluing an out-of-school-time scope
paragraph into a school-models parent, which broke cohesion and
produced 61 of 65 weak/bad Sub-Pillar matches in a 200-entity judge
run. The cohesion check exists to catch that pattern before it
reaches a classifier.

This is also the structural gap that originally surfaced in the One
Earth Regenerative Agriculture pillar: the Food Waste Reduction
sub-pillar existed, but the pillar's definition listed concrete
examples for meat substitution and was silent on food-waste methods,
so the classifier never knew to descend that branch for food-waste
orgs (a completeness gap).

Usage
-----
Library entry point::

    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.taxonomy_mapping.check_taxonomy_coherence import (
        check_taxonomy_coherence,
    )

    with get_session() as session:
        df = check_taxonomy_coherence(
            tables=tables,            # dict[int, pd.DataFrame] keyed by level idx
            levels=ONEEARTH_LEVELS,   # list of level dicts, same shape as the engine uses
            session=session,
            model="gpt-5.4-nano",
            max_workers=16,
            definition_col="Definition",
        )
    df.to_excel("coherence_audit.xlsx", index=False)

All OpenAI calls flow through the SQL prompt/response cache
(``CoherenceAuditCache`` extends ``InstructorPRC``). Cache keys: the
fixed system prompt + the ``AuditResponse`` schema -> ``prompt_id``;
``f"{parent_level}|{parent_name}"`` -> ``given_id``; hash of the user
prompt body (parent definition + children) -> ``text_id``. Re-running
the audit against an unchanged taxonomy is a no-API run.

The output frame has one row per parent node with: parent level/name,
n_children, verdict (ok/minor_gaps/major_gaps), summary, coverage_gaps,
completeness_gaps, suggested_edits. ``write_markdown_report`` formats
the same data as a human-readable Markdown report.
"""
from __future__ import annotations

from typing import Any, Iterable

import pandas as pd
from pydantic import BaseModel

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL
from vdl_tools.shared_tools.openai.openai_api_utils import is_reasoning_model


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are auditing a hierarchical taxonomy for definitional "
    "coherence between a parent node and its direct children. The "
    "taxonomy is meant to be walked top-down by a classifier: it reads "
    "the parent's definition first to decide whether to descend.\n\n"
    "For each parent-children group, identify gaps in three "
    "directions:\n\n"
    "1. COVERAGE: every child's scope should be contained within the "
    "parent's definition. Flag any child whose subject matter is not "
    "implied or covered by the parent's stated scope.\n\n"
    "2. COMPLETENESS / REFLECTION: the parent's definition should "
    "describe categories, methods, or examples that signpost each "
    "child's existence. A parent that mentions a concept but lists no "
    "examples for it leaves the child invisible to a top-down walk. "
    "Flag any child whose existence is not reflected in the parent's "
    "definition — even if the topic is mentioned by name, if the "
    "parent gives concrete examples for some children but not others, "
    "that asymmetry is a reflection gap.\n\n"
    "3. COHESION: the parent definition must describe a SINGLE "
    "coherent category, not a forced union of disparate scopes glued "
    "together to accommodate heterogeneous children. Flag when:\n"
    "   - the parent's body reads like \"this category covers X — and "
    "also covers Y\" with distinct scope paragraphs for what feel "
    "like separate categories;\n"
    "   - siblings at the same level are conceptually different KINDS "
    "of thing (e.g., one child is a 'model' while a sibling is a "
    "'program', or one is an entity type while a sibling is a "
    "method);\n"
    "   - a child sits awkwardly under the parent's NAME — the "
    "parent's name implies one kind of thing but the child is a "
    "different kind of thing that has been forced in for lack of a "
    "better home.\n"
    "Cohesion gaps usually signal a STRUCTURAL fix (split the parent, "
    "rename it, move a child elsewhere) rather than a definition "
    "tweak. Note them as such in the gap issue text; do not silently "
    "imply that a definition rewrite can resolve them.\n\n"
    "Be specific. When citing a gap, quote the relevant phrase from "
    "the parent or name the specific child. Brief, concrete "
    "suggested_edits beat vague \"add more detail\" advice. For "
    "cohesion gaps, suggested_edits should describe the structural "
    "change (or say \"structural — no in-place edit suffices\") rather "
    "than offer a wording patch that would silently re-introduce the "
    "incoherence.\n\n"
    "Return strict JSON with this shape:\n"
    "{\n"
    '  "verdict": "ok" | "minor_gaps" | "major_gaps",\n'
    '  "summary": "one-sentence assessment",\n'
    '  "coverage_gaps": [\n'
    '    {"child": "<child name>", "issue": "<why the parent does not cover it>"}\n'
    "  ],\n"
    '  "completeness_gaps": [\n'
    '    {"child": "<child name>", "issue": "<what the parent fails to reflect>"}\n'
    "  ],\n"
    '  "cohesion_gaps": [\n'
    '    {"child": "<child name or \\"PARENT\\" when the issue is the parent body itself>", "issue": "<why the parent + child set is not a single coherent category>"}\n'
    "  ],\n"
    '  "suggested_edits": "<short, paste-ready edit to the parent definition, or empty string if none needed, or a one-line structural recommendation when cohesion gaps dominate>"\n'
    "}\n\n"
    "Use \"ok\" when the parent definition cleanly summarizes every "
    "child both in scope and in enumerated examples AND the parent + "
    "children form a single coherent category. Use \"minor_gaps\" for "
    "asymmetries in examples or minor wording. Use \"major_gaps\" "
    "when a child is genuinely not implied by the parent or vice "
    "versa, OR when cohesion is broken (the parent definition has "
    "been stretched to cover heterogeneous children)."
)


def _format_children(children: list[dict[str, str]]) -> str:
    """Render the children list as a numbered block for the prompt."""
    lines = []
    for i, c in enumerate(children, start=1):
        lines.append(f"{i}. {c['name']}\n   {c['definition']}")
    return "\n\n".join(lines)


def _build_user_prompt(
    parent_level: str,
    child_level: str,
    parent_name: str,
    parent_definition: str,
    children: list[dict[str, str]],
) -> str:
    return (
        f"Parent level: {parent_level}\n"
        f"Parent name: {parent_name}\n"
        f"Parent definition:\n{parent_definition}\n\n"
        f"Direct children (level: {child_level}):\n\n"
        f"{_format_children(children)}\n\n"
        "Audit this parent-child group for coverage and completeness "
        "as instructed. Return strict JSON."
    )


# ---------------------------------------------------------------------------
# Core checker
# ---------------------------------------------------------------------------

def _children_of(
    child_df: pd.DataFrame,
    parent_filters: Iterable[str],
    parent_row: pd.Series,
    child_key: str,
    definition_col: str,
) -> list[dict[str, str]]:
    """Return ``[{name, definition}, ...]`` for rows in ``child_df`` whose
    parent-filter columns all equal the corresponding values on
    ``parent_row``.
    """
    mask = pd.Series(True, index=child_df.index)
    for filt in parent_filters:
        if filt not in child_df.columns:
            return []
        mask &= child_df[filt] == parent_row[filt]
    sub = child_df[mask]
    out: list[dict[str, str]] = []
    for _, r in sub.iterrows():
        name = str(r[child_key]).strip()
        defn = str(r.get(definition_col, "")).strip()
        if name and defn:
            out.append({"name": name, "definition": defn})
    return out


# ---------------------------------------------------------------------------
# Pydantic response schema + cache class
# ---------------------------------------------------------------------------

class Gap(BaseModel):
    """One coverage / completeness / cohesion finding for a parent-child group."""

    child: str = ""
    issue: str = ""


class AuditResponse(BaseModel):
    """Structured-output schema for one parent's coherence audit."""

    verdict: str = "unknown"  # "ok" | "minor_gaps" | "major_gaps"
    summary: str = ""
    coverage_gaps: list[Gap] = []
    completeness_gaps: list[Gap] = []
    cohesion_gaps: list[Gap] = []
    suggested_edits: str = ""


class CoherenceAuditCache(InstructorPRC):
    """SQL-cached cache class for the coherence audit.

    One instance per (system_prompt, model). Reused across every
    parent-children group in a single ``check_taxonomy_coherence`` call.
    """

    def __init__(
        self,
        session,
        model: str = DEFAULT_MODEL,
        store_results: bool = True,
        filter_by_model: bool = False,
    ):
        super().__init__(
            session=session,
            prompt_str=SYSTEM_PROMPT,
            prompt_name="taxonomy_coherence_audit",
            response_model=AuditResponse,
            model=model,
            filter_by_model=filter_by_model,
            store_results=store_results,
        )


def _gaps_to_text(gaps: Iterable[Gap]) -> str:
    """Flatten a list of ``Gap`` objects into a single cell string."""
    if not gaps:
        return ""
    return "\n".join(
        f"- {(g.child or '?')}: {g.issue}".strip() for g in gaps
    )


def check_taxonomy_coherence(
    *,
    tables: dict[int, pd.DataFrame],
    levels: list[dict[str, Any]],
    session,
    model: str,
    max_workers: int = 16,
    definition_col: str = "Definition",
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    temperature: float | None = 0,
) -> pd.DataFrame:
    """Audit parent-child coherence at every non-leaf level transition.

    Every per-parent OpenAI call flows through the SQL prompt/response
    cache: one ``CoherenceAuditCache.bulk_get_cache_or_run`` for the
    whole audit, parallelism bounded by ``max_workers`` (cache's
    internal pool — the SQLAlchemy session is touched only on the main
    thread).

    Parameters
    ----------
    tables
        ``{level_idx: DataFrame}`` of the taxonomy, as produced by the
        engine's ``load_taxonomy`` / each domain's loader. Each frame
        must carry the level's ``key_col``, all parent ``key_col``s
        listed in ``parent_filters``, and the ``definition_col``.
    levels
        Same shape as ``ONEEARTH_LEVELS``: an ordered list of level
        dicts with ``idx``, ``name``, ``key_col``, ``parent_filters``.
        The last level is treated as a leaf and is never audited as a
        parent.
    session
        SQLAlchemy session for the SQL prompt/response cache. Build via
        ``vdl_tools.shared_tools.database_cache.database_utils.get_session``.
    model
        OpenAI model id.
    max_workers
        Concurrency cap for the cache's API worker pool.
    definition_col
        Column name carrying the definition text on every level frame.
    read_from_cache, write_to_cache
        Forwarded to ``bulk_get_cache_or_run``. Defaults of ``True``
        match legacy behavior; pass ``read_from_cache=False`` to force a
        full re-run, or ``write_to_cache=False`` for a read-only audit.
    temperature
        Sampling temperature for the audit model. Defaults to ``0`` for
        deterministic audits (matching the legacy path). Automatically
        suppressed for reasoning models (``gpt-5*``), which reject it.
        Pass ``None`` to omit entirely.

    Returns
    -------
    A DataFrame with one row per audited parent: ``parent_level``,
    ``parent_name``, ``n_children``, ``verdict``, ``summary``,
    ``coverage_gaps``, ``completeness_gaps``, ``cohesion_gaps``,
    ``suggested_edits``, ``parent_definition``.
    """
    tasks: list[dict[str, Any]] = []

    for i, lvl in enumerate(levels[:-1]):
        child_lvl = levels[i + 1]
        parent_df = tables[lvl["idx"]]
        child_df = tables[child_lvl["idx"]]
        parent_key = lvl["key_col"]

        for _, prow in parent_df.iterrows():
            parent_name = str(prow.get(parent_key, "")).strip()
            parent_def = str(prow.get(definition_col, "")).strip()
            if not parent_name or not parent_def:
                continue
            children = _children_of(
                child_df,
                parent_filters=child_lvl.get("parent_filters", []),
                parent_row=prow,
                child_key=child_lvl["key_col"],
                definition_col=definition_col,
            )
            if not children:
                # Leaf at this point in the tree (e.g. Cross-Cutting pillar);
                # nothing to audit.
                continue
            tasks.append({
                "parent_level": lvl["name"],
                "child_level": child_lvl["name"],
                "parent_name": parent_name,
                "parent_definition": parent_def,
                "children": children,
            })

    print(f"Auditing {len(tasks)} parent-children groups "
          f"with {max_workers} worker(s) using {model}")

    if not tasks:
        return pd.DataFrame()

    cache = CoherenceAuditCache(session=session, model=model)

    # Build (given_id, user_text) pairs. given_id is taxonomy-agnostic —
    # the system prompt + AuditResponse schema (captured in prompt_id)
    # already distinguish this audit from other tools; level + name
    # uniquely identify a parent within one taxonomy.
    requests: list[tuple[str, str]] = []
    for t in tasks:
        given_id = f"{t['parent_level']}|{t['parent_name']}"
        user_text = _build_user_prompt(
            parent_level=t["parent_level"],
            child_level=t["child_level"],
            parent_name=t["parent_name"],
            parent_definition=t["parent_definition"],
            children=t["children"],
        )
        requests.append((given_id, user_text))

    # Restore deterministic audits (temperature=0); reasoning models
    # (gpt-5*) reject the param, so suppress it for them.
    api_kwargs: dict[str, Any] = {}
    if temperature is not None and not is_reasoning_model(model):
        api_kwargs["temperature"] = temperature

    responses = cache.bulk_get_cache_or_run(
        given_ids_texts=requests,
        max_workers=max_workers,
        read_from_cache=read_from_cache,
        write_to_cache=write_to_cache,
        **api_kwargs,
    )

    rows: list[dict[str, Any]] = []
    for t, (given_id, _) in zip(tasks, requests):
        base = {
            "parent_level": t["parent_level"],
            "parent_name": t["parent_name"],
            "n_children": len(t["children"]),
            "parent_definition": t["parent_definition"],
        }
        resp = responses.get(given_id)
        if resp is None:
            rows.append({**base,
                         "verdict": "parse_error",
                         "summary": "API call failed",
                         "coverage_gaps": "",
                         "completeness_gaps": "",
                         "cohesion_gaps": "",
                         "suggested_edits": ""})
            continue
        try:
            audit = AuditResponse.model_validate_json(resp["response_text"])
        except Exception as exc:  # noqa: BLE001
            rows.append({**base,
                         "verdict": "parse_error",
                         "summary": f"parse error: {exc}",
                         "coverage_gaps": "",
                         "completeness_gaps": "",
                         "cohesion_gaps": "",
                         "suggested_edits": ""})
            continue
        rows.append({
            **base,
            "verdict": audit.verdict or "unknown",
            "summary": audit.summary or "",
            "coverage_gaps": _gaps_to_text(audit.coverage_gaps),
            "completeness_gaps": _gaps_to_text(audit.completeness_gaps),
            "cohesion_gaps": _gaps_to_text(audit.cohesion_gaps),
            "suggested_edits": audit.suggested_edits or "",
        })

    session.commit()

    out_df = pd.DataFrame(rows)
    # Stable ordering: level order (per `levels`), then parent name.
    level_order = {lvl["name"]: lvl["idx"] for lvl in levels}
    out_df["_lvl_order"] = out_df["parent_level"].map(level_order)
    out_df = out_df.sort_values(
        ["_lvl_order", "parent_name"]
    ).drop(columns="_lvl_order").reset_index(drop=True)
    return out_df


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------

def write_markdown_report(df: pd.DataFrame, out_path) -> None:
    """Write a human-readable Markdown report of the coherence audit.

    Groups parents by level, sorts within each level by severity
    (major_gaps first), and surfaces verdict / summary / gaps /
    suggested_edits per parent. Skips parents with verdict=ok in the
    detail section but still counts them in the per-level summary.
    """
    sev_order = {
        "major_gaps": 0, "minor_gaps": 1, "ok": 2,
        "parse_error": 3, "unknown": 4,
    }
    df = df.copy()
    df["_sev"] = df["verdict"].map(sev_order).fillna(5)

    lines: list[str] = ["# Taxonomy coherence audit\n"]

    # Overall summary — hand-rolled markdown table so we don't depend
    # on the optional `tabulate` package that `DataFrame.to_markdown()` needs.
    lines.append("## Summary\n")
    summary = (
        df.groupby("parent_level")["verdict"]
        .value_counts().unstack(fill_value=0)
    )
    header = ["parent_level", *summary.columns.tolist()]
    lines.append("| " + " | ".join(str(c) for c in header) + " |")
    lines.append("|" + "|".join(["---"] * len(header)) + "|")
    for lvl, row in summary.iterrows():
        cells = [str(lvl)] + [str(int(row[c])) for c in summary.columns]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    for parent_level, grp in df.groupby("parent_level", sort=False):
        grp = grp.sort_values(["_sev", "parent_name"])
        lines.append(f"\n## Level: {parent_level}\n")
        for _, r in grp.iterrows():
            if r["verdict"] == "ok":
                continue
            lines.append(f"### {r['parent_name']} — {r['verdict']}")
            lines.append(f"_{r['n_children']} children_\n")
            if r["summary"]:
                lines.append(f"**Summary:** {r['summary']}\n")
            if r["coverage_gaps"]:
                lines.append("**Coverage gaps:**")
                lines.append(r["coverage_gaps"])
                lines.append("")
            if r["completeness_gaps"]:
                lines.append("**Completeness gaps:**")
                lines.append(r["completeness_gaps"])
                lines.append("")
            if r.get("cohesion_gaps"):
                lines.append("**Cohesion gaps:**")
                lines.append(r["cohesion_gaps"])
                lines.append("")
            if r["suggested_edits"]:
                lines.append("**Suggested edit:**")
                lines.append(f"> {r['suggested_edits']}")
                lines.append("")
        # Note any "ok" parents at this level so the report is complete.
        ok_names = grp[grp["verdict"] == "ok"]["parent_name"].tolist()
        if ok_names:
            lines.append(f"_OK (no gaps): {', '.join(ok_names)}_\n")

    out_path = str(out_path)
    with open(out_path, "w") as fh:
        fh.write("\n".join(lines))
    print(f"Wrote markdown report to {out_path}")
