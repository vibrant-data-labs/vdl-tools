"""
Hierarchical taxonomy judge — library
=====================================

LLM-as-judge evaluation for any hierarchical taxonomy classification
produced by a sibling ``hierarchical_taxonomy_mapping``-style
classifier. This module is the per-domain-agnostic engine; project
drivers (e.g. ``evaluate_loc_mapping_quality.py``,
``evaluate_drawdown_mapping_quality.py``,
``evaluate_oe_mapping_quality.py``,
``evaluate_mapping_quality.py``) supply a small ``JudgeConfig`` and
call ``run_judge_evaluation``.

For each entity (or research project) the judge sees:
    1. the entity name and description (or title and abstract)
    2. the chosen method's matches per taxonomy level, with each
       node's definition

The judge returns:
    1. a per-match score (good / weak / bad) plus a one-sentence reason
    2. a top-level alignment verdict per entity (correct, wrong,
       ambiguous, mixed, or "no_<X>_expected" where <X> is the top
       level — Pillar / Sector — i.e. the entity does not describe
       work in scope)

Outputs (alongside the per-row file unless ``output_path`` overrides):

    *_quality_scored.{ext}             one row per scored match
    *_quality_<X>_verdicts.{ext}       one row per entity with the
                                       top-level alignment verdict
                                       (filename uses the lowercased
                                       verdict_top_label, e.g.
                                       ``pillar`` or ``sector``)
    *_quality_summary.{ext}            level x score counts and means
    *_quality_per_entity.{ext}         per-entity mean score
    *_quality_report.md                human-readable summary

``.ext`` is ``.xlsx`` or ``.csv`` matched to the per-row input.

Methods
-------
By default the judge evaluates "new" — the hierarchical classifier's
own per-row columns. Set ``old_level_cols`` on the config and pass
``method="old"`` to ``run_judge_evaluation`` to score the prior
embedding-based mapping (only OE currently has this).

Prompt customization
--------------------
The system prompt has five sections, each separately overrideable:

    1. ``taxonomy_intro`` — required; per-project framing paragraphs
    2. ``scoring_rubric`` — defaults to the generic 3-level good/weak/
       bad rubric (works for entity descriptions); override for
       different domains (e.g. research project abstracts)
    3. ``strictness`` — defaults to "name not evidence; qualifier
       lock; placeholder def" rules; override or extend per project
    4. ``verdict_spec`` — auto-generated from
       ``verdict_top_label`` / ``verdict_no_match_value``
    5. ``output_schema`` — auto-generated from ``levels``

Re-runnability
--------------
Pass ``from_scored`` to skip the judge call and regenerate the
summary + report from a previously-saved scored file. Useful when
iterating on report wording / thresholds without re-paying for the
LLM calls.
"""

from __future__ import annotations

import ast
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Mapping

import pandas as pd
from openai import OpenAI


# ---------------------------------------------------------------------------
# Default prompt sections
# ---------------------------------------------------------------------------
# These are the parts that are essentially identical across all judge
# scripts. Per-domain drivers can override any section via JudgeConfig.

DEFAULT_SCORING_RUBRIC = """Scoring rubric:
- "good": The description explicitly names activity, technology, ecosystem, instrument, or practice that fits the taxonomy node's definition. Direct evidence in the description text.
- "weak": The description loosely fits — partial, implicit, or inferential support. Plausible but not strongly grounded.
- "bad": The description does not support this classification. Either the entity does something different, the description is silent on the relevant area, or the connection is too tenuous to call a real match. Generic phrases ("sustainability", "clean energy", "green") on their own are not specific enough to support narrow nodes."""

DEFAULT_STRICTNESS = """Strictness:
- The entity's NAME alone is not evidence. A "Solar Co." description that doesn't mention solar technology does not support a solar match.
- Qualifiers in a node name (Utility-Scale, Offshore, Tropical, etc.) are mandatory: the description must support the qualifier or the match is not "good".
- If the definition is shown as "[definition not found in current taxonomy]", judge based on the node name interpreted plainly and the description."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JudgeConfig:
    """Per-domain configuration for the hierarchical-taxonomy judge.

    Every field has a sensible default for the "OE-style" case
    (organization descriptions + Pillar top-level). Per-project
    drivers override what they need:

    - **All projects** must supply: ``levels``, ``taxonomy_intro``,
      ``new_level_cols``, ``load_taxonomy_tables``,
      ``build_openai_client``.
    - **Sector-based domains** (Drawdown) override
      ``verdict_top_label="Sector"`` (which auto-derives
      ``verdict_key``, ``verdict_no_match_value``, output column).
    - **Research / non-org entity domains** (hopper_dean) override
      ``entity_id_col``, ``entity_name_col``,
      ``entity_description_col``, ``entity_label_in_prompt``,
      ``description_label_in_prompt``, and optionally
      ``scoring_rubric`` and ``strictness`` for the research framing.
    - **Dual-method domains** (OE — old embedding method on the CFT)
      supply ``old_level_cols``; ``run_judge_evaluation`` chooses
      between new and old based on the ``method`` argument.
    """

    # ---- Taxonomy structure ----
    # Sequence of level spec dicts in walk order (top → leaf). Each
    # dict must have ``name`` (used in the prompt + scored level
    # column) and ``output_col`` (matches the per-row file's column
    # for that level). Same shape as the classifier engine's level
    # spec, so the project's existing LEVELS constant can be passed
    # in unchanged.
    levels: tuple[dict, ...]

    # ---- Project naming ----
    taxonomy_label: str                              # e.g. "Drawdown taxonomy"; used in report title
    taxonomy_intro: str                              # opening paragraph(s) of the system prompt

    # ---- Per-row column mappings ----
    # Map of in-prompt level label → per-row column carrying the new
    # method's match at that level. By default uses level["name"] as
    # the label; pass ``level_label_field="output_col"`` to use the
    # output column name (e.g. "Sub-Pillar" instead of "SubPillar").
    new_level_cols: Mapping[str, str] = field(default_factory=dict)
    # Old-method (e.g. OE's embedding-based classification on the
    # CFT) columns. When set, ``method="old"`` becomes usable. Old-
    # method values are expected to be repr-list cells like
    # ``"['Energy Transition', 'Cross-Cutting']"``.
    old_level_cols: Mapping[str, str] | None = None
    level_label_field: Literal["name", "output_col"] = "name"

    # ---- Top-level verdict ----
    verdict_top_label: str = "Pillar"               # "Pillar" / "Sector"; auto-derives the rest

    # ---- Entity column conventions ----
    entity_id_col: str = "uid"
    entity_name_col: str = "Name"
    entity_description_col: str = "Description"
    entity_label_in_prompt: str = "Entity"          # "Entity:" / "Project title:"
    entity_label_plural: str = "entities"           # "entities" / "projects" — used in report prose
    description_label_in_prompt: str = "Description"  # "Description:" / "Abstract:"

    # ---- Taxonomy + OpenAI client builders ----
    load_taxonomy_tables: Callable[[], dict[int, pd.DataFrame]] = None
    taxonomy_label_path_fn: Callable[[], Path] | None = None  # optional; reports the taxonomy file name
    build_openai_client: Callable[[], OpenAI] = None

    # ---- Prompt customization (overrides) ----
    scoring_rubric: str = DEFAULT_SCORING_RUBRIC
    strictness: str = DEFAULT_STRICTNESS

    # ---- Output naming ----
    report_title: str | None = None                 # default derived from taxonomy_label
    failure_sample_n: int = 10

    # ---- Derived properties ----
    @property
    def verdict_key(self) -> str:
        # e.g., "pillar_alignment", "sector_alignment"
        return f"{self.verdict_top_label.lower()}_alignment"

    @property
    def verdict_no_match_value(self) -> str:
        # e.g., "no_pillar_expected", "no_sector_expected"
        return f"no_{self.verdict_top_label.lower()}_expected"

    @property
    def chosen_column_name(self) -> str:
        # e.g., "chosen_pillars", "chosen_sectors"
        return f"chosen_{self.verdict_top_label.lower()}s"

    @property
    def level_labels(self) -> list[str]:
        return [lvl[self.level_label_field] for lvl in self.levels]


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------

def build_judge_system_prompt(config: JudgeConfig) -> str:
    """Assemble the judge system prompt from the config."""
    levels_lines = "\n  ".join(f'"{lbl}":     [...]' for lbl in config.level_labels)

    verdict_spec = (
        f"PART 2 — {config.verdict_top_label} alignment verdict. Given the "
        f"description, decide whether the chosen {config.verdict_top_label}(s) "
        f"is the correct top-level verdict. Values:\n"
        f'- "correct": the chosen {config.verdict_top_label}(s) match what the description says the entity does.\n'
        f'- "wrong": the description supports a DIFFERENT {config.verdict_top_label}; the chosen {config.verdict_top_label} is wrong.\n'
        f'- "ambiguous": more than one {config.verdict_top_label} could be defended; the description is ambiguous on the cross-{config.verdict_top_label.lower()} split.\n'
        f'- "mixed": the description clearly supports MULTIPLE {config.verdict_top_label}s (genuinely distinct, prominent lines of work) but only one was chosen — or the chosen multi-{config.verdict_top_label} set differs from what the description supports.\n'
        f'- "{config.verdict_no_match_value}": the description does not describe work in scope; the entity should have returned no {config.verdict_top_label} match.'
    )

    output_schema = (
        "Output JSON of the form:\n"
        "{\n"
        '  "matches": {\n'
        + "\n".join(f'    "{lbl}":     [{{"match": "<exact name>", "score": "good|weak|bad", "reason": "<one sentence>"}}],'
                    for lbl in config.level_labels)
        + "\n  },\n"
        f'  "{config.verdict_key}": {{\n'
        f'    "verdict": "correct|wrong|ambiguous|mixed|{config.verdict_no_match_value}",\n'
        '    "reason": "<one sentence>"\n'
        "  }\n"
        "}\n\n"
        "The order of matches must match the order shown in the input. "
        "Use the exact match names you were shown. If a level has no matches, "
        'omit it from "matches".'
    )

    return "\n\n".join([
        config.taxonomy_intro,
        "Your job has TWO parts.",
        "PART 1 — Per-match scoring. For every match shown, decide whether the description provides clear and specific evidence supporting the classification, given the taxonomy node's definition.",
        config.scoring_rubric,
        config.strictness,
        verdict_spec,
        output_schema,
    ])


# ---------------------------------------------------------------------------
# Definition lookups
# ---------------------------------------------------------------------------

def build_definition_lookups(
    tables: dict[int, pd.DataFrame],
    config: JudgeConfig,
) -> dict[str, dict[str, tuple[str, str]]]:
    """Return ``{level_label -> {lower_name -> (canonical_name, definition)}}``.

    Keyed by the level's label (from ``config.level_label_field``) so
    callers can look up definitions by the same identifier used in
    the prompt and scored output.
    """
    lookups: dict[str, dict[str, tuple[str, str]]] = {}
    for lvl in config.levels:
        label = lvl[config.level_label_field]
        df = tables[lvl["idx"]]
        key_col = lvl["key_col"]
        d: dict[str, tuple[str, str]] = {}
        for _, row in df.iterrows():
            name = str(row[key_col]).strip()
            defn = str(row.get("Definition", "")).strip() if "Definition" in df.columns else ""
            if name and name.lower() not in d:
                d[name.lower()] = (name, defn)
        lookups[label] = d
    return lookups


def lookup_def(
    level_lookup: dict[str, tuple[str, str]],
    name: str,
) -> tuple[str, str]:
    """Resolve a match name → (canonical_name, definition).

    Returns a "[definition not found...]" placeholder when the name
    doesn't appear in the current taxonomy — so legacy matches still
    get judged (the prompt's strictness section instructs the judge
    to fall back to name + description in that case).
    """
    if not name:
        return name, "[definition not found in current taxonomy]"
    found = level_lookup.get(name.strip().lower())
    if found and found[1]:
        return found
    return name, "[definition not found in current taxonomy]"


# ---------------------------------------------------------------------------
# Match collection
# ---------------------------------------------------------------------------

def _parse_repr_list(cell: Any) -> list[str]:
    """Decode a repr()-encoded list cell to a list of strings.

    Used for old-method columns where matches ship as
    ``"['Energy Transition', 'Cross-Cutting']"``.
    """
    if cell is None:
        return []
    if isinstance(cell, float) and pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s or s == "[]":
        return []
    try:
        v = ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return []
    if not isinstance(v, (list, tuple)):
        return []
    return [str(x).strip() for x in v if str(x).strip()]


def collect_entity_matches(
    per_row_df: pd.DataFrame,
    lookups: dict[str, dict[str, tuple[str, str]]],
    config: JudgeConfig,
    method: str = "new",
) -> dict[str, dict[str, Any]]:
    """Build per-entity match bundles for the judge.

    Returns ``{entity_id: {"name": ..., "description": ...,
                            "matches": {level_label: [(name, defn), ...]}}}``.

    De-duplicates per (entity_id, level, name). Match collection
    depends on ``method``:

    - ``"new"`` (default): reads per-row column values via
      ``config.new_level_cols`` (each row may contribute one value
      per level).
    - ``"old"``: requires ``config.old_level_cols`` set. Reads
      repr-list cells from those columns; the same cell repeats on
      every per-row row for a given entity_id, so reading every row
      is harmless thanks to the set dedupe.
    """
    if method == "old" and not config.old_level_cols:
        raise ValueError(
            "method='old' requires JudgeConfig.old_level_cols to be set"
        )
    level_cols = config.new_level_cols if method == "new" else config.old_level_cols

    from collections import defaultdict
    by_id: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    info_by_id: dict[str, dict[str, str]] = {}

    for _, r in per_row_df.iterrows():
        eid = r[config.entity_id_col]
        info_by_id.setdefault(eid, {
            "name": str(r.get(config.entity_name_col, "")).strip(),
            "description": str(r.get(config.entity_description_col, "")).strip(),
        })
        for label, col in level_cols.items():
            v = r.get(col)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            if method == "old":
                for s in _parse_repr_list(v):
                    by_id[eid][label].add(s)
            else:
                s = str(v).strip()
                if s:
                    by_id[eid][label].add(s)

    out: dict[str, dict[str, Any]] = {}
    for eid, info in info_by_id.items():
        matches = {
            lbl: [
                lookup_def(lookups[lbl], n)
                for n in sorted(by_id[eid].get(lbl, set()))
            ]
            for lbl in level_cols  # preserve iteration order
        }
        out[eid] = {**info, "matches": matches}
    return out


def has_any_matches(matches: dict[str, list[tuple[str, str]]]) -> bool:
    return any(items for items in matches.values())


# ---------------------------------------------------------------------------
# Judge call
# ---------------------------------------------------------------------------

def _format_method_block(matches: dict[str, list[tuple[str, str]]]) -> str:
    lines: list[str] = []
    for label, items in matches.items():
        if not items:
            continue
        lines.append(f"  {label}:")
        for name, defn in items:
            # Cap definition length so we don't blow the prompt budget
            # on very long taxonomy text.
            if len(defn) > 600:
                defn = defn[:600] + "..."
            lines.append(f"    - {name}: {defn}")
    return "\n".join(lines) if lines else "  (no matches)"


def build_user_prompt(
    name: str,
    description: str,
    matches: dict[str, list[tuple[str, str]]],
    config: JudgeConfig,
) -> str:
    return (
        f"{config.entity_label_in_prompt}: {name}\n\n"
        f"{config.description_label_in_prompt}:\n{description}\n\n"
        f"Classifications:\n{_format_method_block(matches)}"
    )


def judge_entity(
    client: OpenAI,
    model: str,
    eid: str,
    name: str,
    description: str,
    matches: dict[str, list[tuple[str, str]]],
    config: JudgeConfig,
    system_prompt: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Score every match for one entity, plus the top-level verdict.

    Returns ``(per-match rows, verdict row)``. Verdict row is ``None``
    when the judge call failed (the per-match rows list is empty in
    that case).
    """
    user_prompt = build_user_prompt(name, description, matches, config)

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
    except Exception as exc:  # noqa: BLE001
        display_name = str(name)[:60]
        print(f"  [error] {display_name}: {exc}")
        return [], None

    raw = resp.choices[0].message.content or "{}"
    try:
        judgment = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  [warn] bad JSON for {str(name)[:60]}: {raw[:200]}")
        return [], None

    rows: list[dict[str, Any]] = []
    matches_block = judgment.get("matches") or {}
    if isinstance(matches_block, dict):
        for level_label, scored in matches_block.items():
            if not isinstance(scored, list):
                continue
            for entry in scored:
                if not isinstance(entry, dict):
                    continue
                score = str(entry.get("score", "")).strip().lower()
                if score not in {"good", "weak", "bad"}:
                    continue
                rows.append({
                    config.entity_id_col: eid,
                    config.entity_name_col: name,
                    "level": level_label,
                    "match": str(entry.get("match", "")).strip(),
                    "score": score,
                    "reason": str(entry.get("reason", "")).strip(),
                })

    pa = judgment.get(config.verdict_key) or {}
    if isinstance(pa, dict):
        verdict = str(pa.get("verdict", "")).strip().lower()
        verdict_reason = str(pa.get("reason", "")).strip()
    else:
        verdict, verdict_reason = "", ""

    # Top-level "chosen" column: matches at the top level only.
    top_label = config.level_labels[0]
    chosen = sorted({m[0] for m in matches.get(top_label, [])})
    verdict_row = {
        config.entity_id_col: eid,
        config.entity_name_col: name,
        config.chosen_column_name: ", ".join(chosen) if chosen else "",
        "verdict": verdict,
        "reason": verdict_reason,
    }
    return rows, verdict_row


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

SCORE_NUMERIC = {"good": 1.0, "weak": 0.5, "bad": 0.0}


def summarize(scored_df: pd.DataFrame, level_order: list[str]) -> pd.DataFrame:
    """Per-level counts, score distribution, and mean."""
    rows: list[dict[str, Any]] = []
    for level in level_order:
        sub = scored_df[scored_df["level"] == level]
        n = len(sub)
        if n == 0:
            continue
        counts = sub["score"].value_counts().to_dict()
        mean = sub["score"].map(SCORE_NUMERIC).mean()
        rows.append({
            "level": level,
            "n_matches": n,
            "good": int(counts.get("good", 0)),
            "weak": int(counts.get("weak", 0)),
            "bad": int(counts.get("bad", 0)),
            "pct_good": round(100 * counts.get("good", 0) / n, 1),
            "pct_bad": round(100 * counts.get("bad", 0) / n, 1),
            "mean_score": round(mean, 3),
        })
    return pd.DataFrame(rows)


def per_entity_means(
    scored_df: pd.DataFrame,
    id_col: str,
    name_col: str,
) -> pd.DataFrame:
    """Per-entity mean score across that entity's scored matches."""
    s = scored_df.copy()
    s["score_num"] = s["score"].map(SCORE_NUMERIC)
    return (
        s.groupby([id_col, name_col])["score_num"]
        .mean()
        .round(3)
        .reset_index(name="mean_score")
        .sort_values("mean_score")
    )


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def failure_samples(
    scored_df: pd.DataFrame,
    name_col: str,
    n: int = 10,
) -> pd.DataFrame:
    sub = scored_df[scored_df["score"] == "bad"]
    return sub[[name_col, "level", "match", "reason"]].head(n).reset_index(drop=True)


def alignment_breakdown(verdicts_df: pd.DataFrame) -> pd.DataFrame:
    if verdicts_df.empty:
        return pd.DataFrame(columns=["verdict", "n", "pct"])
    counts = verdicts_df["verdict"].value_counts(dropna=False).reset_index()
    counts.columns = ["verdict", "n"]
    counts["pct"] = (100 * counts["n"] / counts["n"].sum()).round(1)
    return counts


def _wrong_or_mixed_samples(
    verdicts_df: pd.DataFrame,
    config: JudgeConfig,
    n: int = 10,
) -> pd.DataFrame:
    sub = verdicts_df[verdicts_df["verdict"].isin(
        {"wrong", "mixed", config.verdict_no_match_value}
    )]
    cols = [config.entity_name_col, config.chosen_column_name, "verdict", "reason"]
    return sub[cols].head(n).reset_index(drop=True)


def _format_summary_table(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return "_(no scored matches)_"
    cols = ["level", "n_matches", "good", "weak", "bad",
            "pct_good", "pct_bad", "mean_score"]
    cols = [c for c in cols if c in summary_df.columns]
    header = "| " + " | ".join(cols) + " |"
    sep = "|" + "|".join(["---"] * len(cols)) + "|"
    body = "\n".join(
        "| " + " | ".join(str(r[c]) for c in cols) + " |"
        for _, r in summary_df.iterrows()
    )
    return "\n".join([header, sep, body])


def _format_md_table(df: pd.DataFrame, header_cells: list[str], data_cols: list[str],
                     empty_msg: str = "_(none)_", cell_limit: int = 120) -> str:
    if df.empty:
        return empty_msg
    lines = ["| " + " | ".join(header_cells) + " |",
             "|" + "|".join(["---"] * len(header_cells)) + "|"]
    for _, r in df.iterrows():
        cells = [
            str(r.get(c, "")).replace("|", "\\|").replace("\n", " ")[:cell_limit]
            for c in data_cols
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_report(
    scored_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    verdicts_df: pd.DataFrame,
    total_entities: int,
    per_row_name: str,
    taxonomy_name: str,
    judge_model: str,
    config: JudgeConfig,
    report_path: Path,
    driver_script_name: str,
) -> None:
    """Write the human-readable judge report as markdown."""
    pa = alignment_breakdown(verdicts_df)
    fail = failure_samples(scored_df, config.entity_name_col, config.failure_sample_n)
    wrong = _wrong_or_mixed_samples(verdicts_df, config, config.failure_sample_n)
    n_ids_scored = scored_df[config.entity_id_col].nunique() if not scored_df.empty else 0

    title = config.report_title or f"{config.taxonomy_label}: judge-quality report"
    top = config.verdict_top_label
    entity_word_plural = config.entity_label_plural  # "entities" / "projects"

    parts: list[str] = []
    parts.append(f"# {title}\n")
    parts.append(
        f"Generated by `{driver_script_name}` (via "
        f"`hierarchical_taxonomy_judge.run_judge_evaluation`). Re-run "
        f"to regenerate after the mapping changes.\n"
    )
    parts.append("## Provenance\n")
    parts.append(f"- Per-row classifications: `{per_row_name}`")
    parts.append(f"- Taxonomy used for definition lookups: `{taxonomy_name}`")
    parts.append(f"- Judge model: `{judge_model}`")
    parts.append(
        "- Scoring rubric: good=1.0, weak=0.5, bad=0.0\n"
    )

    parts.append("## Coverage\n")
    parts.append(
        f"- Total {entity_word_plural} in pool: **{total_entities}**\n"
        f"- {entity_word_plural.capitalize()} with at least one scored match: **{n_ids_scored}** "
        f"({n_ids_scored / max(total_entities, 1):.0%})\n"
        f"- {entity_word_plural.capitalize()} the classifier returned no {top} for: "
        f"**{total_entities - n_ids_scored}**\n"
    )

    parts.append("## Quality summary by level\n")
    parts.append(_format_summary_table(summary_df))
    parts.append("")

    parts.append(f"## {top} alignment verdicts\n")
    parts.append(_format_md_table(
        pa, ["verdict", "n", "pct"], ["verdict", "n", "pct"],
        empty_msg="_(no verdicts)_",
    ))
    parts.append("")

    parts.append(f"## {top} alignment — wrong / mixed / {config.verdict_no_match_value} samples\n")
    wrong_headers = [config.entity_name_col, config.chosen_column_name, "verdict", "reason"]
    parts.append(_format_md_table(wrong, wrong_headers, wrong_headers))
    parts.append("")

    parts.append(f"## Bad-scored match samples (up to {config.failure_sample_n})\n")
    fail_headers = [config.entity_name_col, "level", "match", "reason"]
    parts.append(_format_md_table(fail, fail_headers, fail_headers))
    parts.append("")

    # Render pct values in the verdicts table with a "%" suffix:
    # they were stored as floats. Patch in place to avoid restructuring.
    text = "\n".join(parts)
    # Convert numeric pct rows to suffixed strings within the verdicts table.
    # Done with a small regex pass to avoid touching numeric columns elsewhere.
    import re
    def _suffix_pct(match):
        head, body = match.group(1), match.group(2)
        body_lines = body.strip().split("\n")
        out_lines = []
        for line in body_lines:
            # Only touch the third column (pct). Keep the |-cells intact.
            cells = [c.strip() for c in line.strip("|").split("|")]
            if len(cells) == 3:
                v = cells[2]
                try:
                    f = float(v)
                    cells[2] = f"{f}%"
                except ValueError:
                    pass
            out_lines.append("| " + " | ".join(cells) + " |")
        return head + "\n".join(out_lines)
    text = re.sub(
        r"(\| verdict \| n \| pct \|\n\|---\|---\|---\|\n)((?:\|[^\n]*\n?)+)",
        _suffix_pct,
        text,
    )

    report_path.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# I/O helpers — handle CSV / XLSX based on file extension
# ---------------------------------------------------------------------------

def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    return pd.read_excel(path)


def _write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    else:
        df.to_excel(path, index=False)


def _derived_output_paths(
    output_path: Path,
    config: JudgeConfig,
) -> dict[str, Path]:
    """Derive verdicts / summary / per_entity / report paths from the scored path."""
    stem = output_path.stem
    suffix = output_path.suffix
    verdicts_label = config.verdict_top_label.lower()  # pillar / sector

    verdicts_stem = stem.replace("_quality_scored",
                                  f"_quality_{verdicts_label}_verdicts")
    summary_stem = stem.replace("_quality_scored", "_quality_summary")
    pe_stem = stem.replace("_quality_scored", "_quality_per_entity")
    report_stem = stem.replace("_quality_scored", "_quality_report")
    return {
        "verdicts": output_path.with_name(verdicts_stem + suffix),
        "summary": output_path.with_name(summary_stem + suffix),
        "per_entity": output_path.with_name(pe_stem + suffix),
        "report": output_path.with_name(report_stem + ".md"),
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_judge_evaluation(
    config: JudgeConfig,
    per_row_path: Path,
    *,
    output_path: Path | None = None,
    from_scored: Path | None = None,
    method: str = "new",
    judge_model: str = "gpt-4.1",
    workers: int = 32,
    driver_script_name: str = "evaluate_*.py",
) -> None:
    """Run the full judge pipeline: collect bundles, judge, summarize, write outputs.

    Parameters
    ----------
    config
        Per-project ``JudgeConfig``.
    per_row_path
        Path to the per-row xlsx or CSV produced by the classifier.
    output_path
        Override the scored-matches output path. Default: alongside
        the per-row file with ``_quality_scored`` appended to the
        stem. The other outputs (summary, verdicts, per-entity,
        report) are derived from this.
    from_scored
        When set, skip the judge call and regenerate the summary +
        report from a previously-saved scored file.
    method
        ``"new"`` (default) or ``"old"``. ``"old"`` requires
        ``config.old_level_cols`` to be set.
    judge_model
        OpenAI model id for the judge.
    workers
        Thread pool size for parallel judge calls.
    driver_script_name
        Filename of the driver script that called this — used in the
        report's "Generated by …" line for provenance.
    """
    if not per_row_path.exists():
        raise FileNotFoundError(f"Per-row file not found: {per_row_path}")

    print(f"Reading per-row classifications: {per_row_path.name}")
    per_row_df = _read_table(per_row_path)
    n_unique = per_row_df[config.entity_id_col].nunique()
    print(f"  {len(per_row_df)} rows, {n_unique} unique {config.entity_label_plural}")
    print(f"Judging method: {method}")

    # Default scored-output path.
    if output_path is None:
        output_path = per_row_path.with_name(
            f"{per_row_path.stem}_quality_scored{per_row_path.suffix}"
        )
        # If we're judging the old method (rare path), disambiguate
        # the filename so it doesn't clobber the new-method outputs.
        if method == "old":
            output_path = output_path.with_name(
                output_path.stem.replace("_quality_scored", "_quality_scored_old_method")
                + output_path.suffix
            )

    paths = _derived_output_paths(output_path, config)
    verdicts_path = paths["verdicts"]
    summary_path = paths["summary"]
    pe_path = paths["per_entity"]
    report_path = paths["report"]

    # Taxonomy and prompt assembly (needed for both branches so we can
    # report the taxonomy file name in the report).
    print("Loading current taxonomy (for definition lookups + report provenance)...")
    if config.taxonomy_label_path_fn is not None:
        taxonomy_path_for_label = config.taxonomy_label_path_fn()
        taxonomy_name = (
            taxonomy_path_for_label.name
            if isinstance(taxonomy_path_for_label, Path)
            else str(taxonomy_path_for_label)
        )
    else:
        taxonomy_name = "(taxonomy path not reported)"

    if from_scored is not None:
        print(f"Loading existing scored matches from {from_scored.name}")
        scored_df = _read_table(from_scored)
        v_path = paths["verdicts"]
        # When from_scored is used, also try the verdicts file derived
        # from from_scored's location rather than output_path's.
        candidate_v = from_scored.with_name(
            from_scored.stem.replace("_quality_scored",
                                      f"_quality_{config.verdict_top_label.lower()}_verdicts")
            + from_scored.suffix
        )
        if candidate_v.exists():
            v_path = candidate_v
        if v_path.exists():
            verdicts_df = _read_table(v_path)
        else:
            print(f"  [warn] no verdicts file at {v_path}; report will skip it")
            verdicts_df = pd.DataFrame(
                columns=[config.entity_id_col, config.entity_name_col,
                         config.chosen_column_name, "verdict", "reason"]
            )
        print(f"  {len(scored_df)} scored matches loaded")
    else:
        tables = config.load_taxonomy_tables()
        lookups = build_definition_lookups(tables, config)

        print("Collecting per-entity match bundles...")
        bundles = collect_entity_matches(per_row_df, lookups, config, method=method)
        n_to_judge = sum(1 for b in bundles.values() if has_any_matches(b["matches"]))
        print(f"  {len(bundles)} entities total, {n_to_judge} have at least one match to judge")

        system_prompt = build_judge_system_prompt(config)
        print(f"Calling judge ({judge_model}) with {workers} worker(s)...")
        client = config.build_openai_client()

        all_rows: list[dict[str, Any]] = []
        all_verdicts: list[dict[str, Any]] = []

        if workers <= 1:
            for i, (eid, b) in enumerate(bundles.items(), start=1):
                display_name = str(b["name"])[:80]
                print(f"  [{i}/{len(bundles)}] {display_name}")
                rows, verdict = judge_entity(
                    client, judge_model, eid, b["name"], b["description"],
                    b["matches"], config, system_prompt,
                )
                all_rows.extend(rows)
                if verdict is not None:
                    all_verdicts.append(verdict)
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(
                        judge_entity,
                        client, judge_model, eid, b["name"], b["description"],
                        b["matches"], config, system_prompt,
                    ): (eid, b["name"])
                    for eid, b in bundles.items()
                }
                done = 0
                for fut in as_completed(futures):
                    eid, name = futures[fut]
                    done += 1
                    rows, verdict = fut.result()
                    display = str(name)[:60]
                    v_str = verdict.get("verdict") if verdict else "ERR"
                    print(
                        f"  [{done}/{len(futures)}] {display} -> "
                        f"{len(rows)} scored matches, verdict={v_str}"
                    )
                    all_rows.extend(rows)
                    if verdict is not None:
                        all_verdicts.append(verdict)

        scored_df = pd.DataFrame(all_rows)
        _write_table(scored_df, output_path)
        print(f"\nWrote {len(scored_df)} scored matches to {output_path}")

        verdicts_df = pd.DataFrame(all_verdicts)
        _write_table(verdicts_df, verdicts_path)
        print(
            f"Wrote {len(verdicts_df)} {config.verdict_top_label.lower()}-alignment "
            f"verdicts to {verdicts_path}"
        )

    if scored_df.empty:
        print("No matches were scored — nothing to summarize.")
        return

    summary = summarize(scored_df, level_order=config.level_labels)
    _write_table(summary, summary_path)
    print("\n=== Quality summary by level ===")
    print(summary.to_string(index=False))
    print(f"\nWrote summary to {summary_path}")

    pe = per_entity_means(scored_df, config.entity_id_col, config.entity_name_col)
    _write_table(pe, pe_path)
    print(f"Wrote per-entity means to {pe_path}")

    if not verdicts_df.empty:
        pa = alignment_breakdown(verdicts_df)
        print(f"\n=== {config.verdict_top_label} alignment ===")
        print(pa.to_string(index=False))

    write_report(
        scored_df=scored_df,
        summary_df=summary,
        verdicts_df=verdicts_df,
        total_entities=int(n_unique),
        per_row_name=per_row_path.name,
        taxonomy_name=taxonomy_name,
        judge_model=judge_model,
        config=config,
        report_path=report_path,
        driver_script_name=driver_script_name,
    )
    print(f"\nWrote report to {report_path}")


__all__ = [
    "JudgeConfig",
    "DEFAULT_SCORING_RUBRIC",
    "DEFAULT_STRICTNESS",
    "build_judge_system_prompt",
    "build_definition_lookups",
    "lookup_def",
    "collect_entity_matches",
    "has_any_matches",
    "judge_entity",
    "summarize",
    "per_entity_means",
    "failure_samples",
    "alignment_breakdown",
    "write_report",
    "run_judge_evaluation",
]
