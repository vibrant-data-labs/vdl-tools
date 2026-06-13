"""
Hierarchical Taxonomy Mapping — general library
================================================

Reusable utilities for matching a list of entities (companies/organizations)
against a multi-level hierarchical taxonomy via an OpenAI chat-completion
model.

Algorithm
---------
For each entity we walk the taxonomy top-down. At each level we send the
entity description together with the candidate nodes (name + definition)
for that level to the model and ask for the best match(es):

    - if a single best match is returned, we descend to that node's
      children at the next level;
    - if the model returns multiple "best" matches, the entity is treated
      as a generalist at this level and (subject to the descent fan-out
      cap) we descend into each match.

The walk yields one record per leaf in the resulting match tree — a leaf
is a match that was either at the deepest level, had no children matched,
truncated by the descent fan-out cap, or (when the prompt produces a
``mode_of_operation`` field) tagged ``indirect`` alongside other siblings.

This module is taxonomy-agnostic. Callers (driver scripts) supply:

    * a level spec (see below),
    * a system prompt string describing the matching task — assemble one
      with ``build_system_prompt`` (see "System prompt" below),
    * a taxonomy xlsx whose sheet names align with the level spec,
    * an entities DataFrame with id / name / text columns.

See ``drawdown_hierarchical_taxonomy_mapping.py`` for an example driver.

Level Spec
----------
The taxonomy hierarchy is described as a list of dicts (one per level,
ordered shallowest-first). Each dict has these fields:

    {
        "idx":                int,        # 0-based depth
        "name":               str,        # human label used in the prompt and
                                          # as the deepest_match value
        "sheet":              str,        # excel sheet name
        "key_col":            str,        # name column rendered to the LLM
        "output_col":         str,        # column name in the per-row dataframe
        "parent_filters":     list[str],  # cols this level uses to filter by parent path
        "child_filter_col":   str,        # col on a CHILD's row that points back to me
                                          # (defaults to key_col)
        "child_filter_value_col": str,    # col on MY row whose value populates that filter
                                          # (defaults to key_col)
    }

Defaults for ``child_filter_col`` and ``child_filter_value_col`` are
applied by ``normalize_levels``; most levels need not set them.

System prompt
-------------
The ``system_prompt`` passed to ``classify_entities`` can be any string,
but in practice you should build it with ``build_system_prompt``, which
assembles a generic skeleton (task framing, JSON output schema, eight
numbered matching-rule slots, output constraints) and lets you customize
the parts that vary per taxonomy.

The skeleton was extracted from the Drawdown driver, so the rule set
and structure reflect what worked there. The eight rule keys, in order,
are listed in ``PROMPT_RULE_KEYS``:

    1. ``domain_relevance``  — when does the description support a match
    2. ``evidence_only``     — match on description text, not entity name
    3. ``cross_sector``      — keep the entity in its own category
    4. ``specificity``       — broad themes don't justify narrow children
    5. ``multiple_matches``  — when to select more than one sibling
    6. ``qualifier_lock``    — qualifiers in candidate names are mandatory
    7. ``prominence``        — top levels need core-line-of-business
    8. ``advocacy_depth``    — match depth must reflect evidence scope

Each rule has a generic default that contains no domain examples
(except rules 2 and 5, whose examples — name-vs-evidence and the "clean
energy" ambiguity — transfer to any taxonomy). Override any subset by
passing ``rules={"<key>": "<full body without leading number>"}``.

Three layers of customization, from least to most invasive:

    1. ``domain_intro`` (required) — opening paragraph framing what the
       taxonomy classifies. Replaces the climate-mitigation framing
       used by Drawdown.
    2. ``modes`` (optional) — list of ``{"name", "definition"}`` dicts.
       When provided, ``mode_of_operation`` appears in the JSON schema
       and a "Mode of operation" section follows the schema. When
       ``None``, both are omitted entirely. The default rule bodies do
       not reference modes; if your modes belong inside specific rules,
       supply rule overrides for those rules (Drawdown does this for
       ``prominence`` and ``advocacy_depth``).
    3. ``rules`` (optional) — dict mapping any of the eight keys above
       to a full override. Use this when a generic default's wording
       isn't tight enough for your domain — typically because it needs
       domain-specific examples or terminology that improves model
       accuracy.

The Drawdown driver demonstrates all three layers — see
``DRAWDOWN_DOMAIN_INTRO``, ``DRAWDOWN_MODES``, and
``DRAWDOWN_RULE_OVERRIDES`` in ``drawdown_hierarchical_taxonomy_mapping.py``.

For a brand-new taxonomy with no special rules, the minimum is::

    system_prompt = build_system_prompt(
        levels=my_levels,
        domain_intro="You are classifying X against a hierarchical taxonomy of Y...",
    )

Output schema
-------------
``classify_entities`` returns a flat per-row DataFrame. Columns:

    [id_col, name_col, text_col]
    + [other entity columns, in input order]
    + [lvl["output_col"] for lvl in levels]
    + ["deepest_match", "leaf_definition",
       "mode_of_operation", "evidence", "reason"]

One row per (entity, leaf) pair. Entities with no level-0 match still
appear with all level columns null.

``collapse_to_one_row_per_uid`` collapses that frame to one row per
``id_col`` value with ``repr()``-encoded list cells per level.
"""

from __future__ import annotations

import configparser
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from openai import OpenAI


# ---------------------------------------------------------------------------
# OpenAI client
# ---------------------------------------------------------------------------

def build_openai_client(
    config_paths: Path | Iterable[Path],
) -> OpenAI:
    """Read the API key from the first config.ini that exists; return a client.

    ``config_paths`` may be a single Path or an iterable of Paths. The
    function tries each in order, returning a client built from the first
    one that exists and contains an ``[openai]`` section.
    """
    if isinstance(config_paths, Path):
        config_paths = [config_paths]
    cfg = configparser.ConfigParser()
    for candidate in config_paths:
        if candidate.exists():
            cfg.read(candidate)
            if cfg.has_section("openai"):
                return OpenAI(api_key=cfg["openai"]["openai_api_key"])
    raise RuntimeError(
        f"Could not find an [openai] section in any of: "
        f"{[str(p) for p in config_paths]}"
    )


# ---------------------------------------------------------------------------
# Level spec normalization
# ---------------------------------------------------------------------------

def normalize_levels(levels: list[dict]) -> list[dict]:
    """Fill in defaults for ``child_filter_col`` / ``child_filter_value_col``.

    Returns a new list — does not mutate the caller's spec.
    """
    out = []
    for lvl in levels:
        normalized = dict(lvl)
        normalized.setdefault("child_filter_col", normalized["key_col"])
        normalized.setdefault("child_filter_value_col", normalized["key_col"])
        out.append(normalized)
    return out


# ---------------------------------------------------------------------------
# Taxonomy loading
# ---------------------------------------------------------------------------

def load_taxonomy(path: Path, levels: list[dict]) -> dict[int, pd.DataFrame]:
    """Load each level of the taxonomy as a DataFrame, keyed by level index.

    Rows missing the level's key column or ``Definition`` are dropped so
    prompts stay clean.
    """
    tables: dict[int, pd.DataFrame] = {}
    for lvl in levels:
        df = pd.read_excel(path, sheet_name=lvl["sheet"])
        df = df.dropna(subset=[lvl["key_col"], "Definition"]).reset_index(drop=True)
        tables[lvl["idx"]] = df
    return tables


def candidates_for_level(
    tables: dict[int, pd.DataFrame],
    level: int,
    parent_path: dict[str, str],
) -> pd.DataFrame:
    """Return candidate nodes at ``level`` that are children of ``parent_path``.

    ``parent_path`` maps parent column name -> value, e.g.
        {"Sector": "Electricity", "Cluster": "Shift Production"}.
    """
    df = tables[level]
    for col, val in parent_path.items():
        df = df[df[col] == val]
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Prompting
# ---------------------------------------------------------------------------

# Ordered list of rule keys understood by ``build_system_prompt``. The
# numbered rules in the assembled prompt appear in this order.
PROMPT_RULE_KEYS: tuple[str, ...] = (
    "domain_relevance",
    "evidence_only",
    "cross_sector",
    "specificity",
    "multiple_matches",
    "qualifier_lock",
    "prominence",
    "advocacy_depth",
)


def _default_rule_bodies(levels: list[dict]) -> dict[str, str]:
    """Generic rule defaults used by ``build_system_prompt``.

    No domain examples appear in any rule body except rules 2 and 5,
    whose name-vs-evidence and ambiguous-phrase examples transfer to
    any taxonomy. Rule 7 references the first two level names from the
    ``levels`` spec so the wording reads naturally for any hierarchy.
    Rule bodies do NOT reference mode-of-operation; callers whose
    domain uses modes should override ``prominence`` and
    ``advocacy_depth`` to bring back mode-aware language.
    """
    top_name = levels[0]["name"] if len(levels) >= 1 else "top-level"
    mid_name = levels[1]["name"] if len(levels) >= 2 else "second-level"
    return {
        "domain_relevance": (
            "Domain relevance. Match a candidate when the description "
            "names activities aligned with the candidate's definition. "
            "The taxonomy already establishes the relevance mechanism "
            "for each candidate — your job is to decide whether the "
            "description names the activity, not to re-derive whether "
            "it qualifies. When the description is too thin to commit "
            "even at the top level, return `matches: []`."
        ),
        "evidence_only": (
            "Evidence from the description only. Base every match on "
            "specific language from the provided description — not on "
            "the entity's name, reputation, or outside knowledge. Quote "
            "or closely paraphrase that language in the `evidence` "
            "field. The entity's NAME is not evidence: 'Prairie "
            "Preservation Society' does not by itself support a "
            "'Protect Grasslands' candidate unless the description "
            "names grasslands, prairies, or that practice; 'Solar Co.' "
            "does not support a solar candidate unless the description "
            "names solar. If the only thing pointing at a candidate is "
            "the name, return `matches: []`."
        ),
        "cross_sector": (
            "No cross-category inference. Assign a node only if the "
            "entity itself performs the activity at that node. An "
            "entity that supplies an input or service used by another "
            "category belongs to its own category, unless the "
            "description says the entity also operates in the "
            "destination category."
        ),
        "specificity": (
            "Specificity must match the level. A candidate is "
            "selectable only when the description names activity "
            "specific enough to support it. Broad themes can support "
            "top-level categories but are not sufficient for a narrower "
            "child unless the description also names the specific "
            "technology, process, material, or practice the child "
            "covers. When in doubt, return `matches: []` and let the "
            "walk stop one level higher."
        ),
        "multiple_matches": (
            "Selecting multiple matches. Look at the specific phrase "
            "you would cite as evidence — apply selection per phrase, "
            "not per entity. Select every candidate whose technology "
            "or practice is named by its own distinct phrase. Entities "
            "doing several things may match several siblings. Do not "
            "select a candidate when the phrase that would support it "
            "is generic enough to fit multiple siblings (e.g. 'clean "
            "energy' could be wind, solar, or nuclear). Return an "
            "empty list when no candidate has specific supporting "
            "evidence."
        ),
        "qualifier_lock": (
            "Qualifier lock. Qualifiers in a candidate's name are "
            "mandatory constraints, not flavor. If the description "
            "identifies a different qualifier, do NOT select that "
            "candidate. If the description is silent on the qualifier, "
            "do NOT select — the candidate's qualifier must be "
            "supported by the description. When no sibling's qualifier "
            "matches the description, return an empty list at this "
            "level."
        ),
        "prominence": (
            f"Prominence at {top_name} and {mid_name} levels. At these "
            "two levels, select only activities that are a core line "
            "of business — a distinct area with multiple sentences, "
            "listed among the entity's main offerings, or described as "
            "a primary focus. A single incidental phrase ('also "
            "supports X', 'including X', 'in addition to Y') about an "
            "activity otherwise absent from the description is not "
            "enough at these levels. At deeper levels, this threshold "
            "does not apply for entities directly performing or "
            "enabling the technology — a specifically named technology "
            "or practice is sufficient even if briefly mentioned."
        ),
        "advocacy_depth": (
            "Match depth must reflect evidence scope. A match must sit "
            "at the level whose scope is supported by the description, "
            "not at deeper levels the entity does not itself address. "
            "The narrower the candidate, the more explicit the "
            "supporting evidence must be."
        ),
    }


_OUTPUT_CONSTRAINTS = (
    "Output constraints:\n"
    "- `index` must be the integer shown before a candidate in the "
    "list. Indices are 1-based and must match a number actually shown "
    "in the candidate list. Do not invent indices and do not return "
    "names. To indicate no match, return `matches: []` — never return "
    "index 0 or any index outside the listed range."
)

_TASK_FRAMING = (
    "You will be given an entity description and a numbered list of "
    "candidate nodes (name + definition) at a single level of the "
    "hierarchy. Identify which node(s) the description shows the "
    "entity is actually working on."
)


def build_system_prompt(
    *,
    levels: list[dict],
    domain_intro: str,
    modes: list[dict] | None = None,
    rules: dict[str, str] | None = None,
    include_confidence: bool = False,
) -> str:
    """Assemble a hierarchical-taxonomy classification system prompt.

    Composes a generic skeleton — ``domain_intro``, task framing, JSON
    output schema, an optional Mode-of-operation section, eight
    matching-rule slots, and output constraints — with any caller-
    supplied overrides. Each rule has a generic default that contains
    no domain examples (except rules 2 and 5, whose examples transfer
    to any taxonomy). Pass ``rules={"<key>": "<override text>"}`` to
    replace any rule with domain-specific wording; missing keys keep
    their generic default.

    Parameters
    ----------
    levels
        Same level-spec list passed to ``classify_entities``. The
        ``name`` field on each spec is used in rule 7's "Prominence
        at <top> and <mid> levels" wording.
    domain_intro
        Opening paragraph framing what the taxonomy classifies.
    modes
        Optional list of mode dicts, each with ``name`` and
        ``definition``. When provided, ``mode_of_operation`` appears
        in the JSON schema and a "Mode of operation" section follows
        the schema. When ``None``, both are omitted entirely. The
        default rule bodies do not reference modes; supply rule
        overrides if mode-aware language belongs in your rules.
    rules
        Optional dict mapping any of the keys in ``PROMPT_RULE_KEYS``
        to a full override body (without leading number). Missing
        keys fall back to ``_default_rule_bodies(levels)``.
    """
    overrides = rules or {}

    # Optional confidence field — when present, each match carries a
    # 0-1 confidence and downstream code can threshold on it (see
    # ``confidence_threshold``). Enabling it also changes the matching
    # disposition: the model surfaces plausible-but-weak candidates with
    # low confidence instead of self-filtering, so the threshold becomes a
    # genuine bidirectional precision/recall knob.
    conf_field = '"confidence": <number 0-1>, ' if include_confidence else ''

    # JSON output schema — mode_of_operation included only when modes given.
    if modes:
        mode_union = " | ".join(f'"{m["name"]}"' for m in modes)
        schema = (
            '  {"matches": [{"index": <int>, '
            f'"mode_of_operation": {mode_union}, '
            + conf_field +
            '"evidence": "<phrase(s) from the description that '
            'support the match>", '
            '"reason": "<how the evidence maps to the candidate '
            'definition>"}]}'
        )
    else:
        schema = (
            '  {"matches": [{"index": <int>, '
            + conf_field +
            '"evidence": "<phrase(s) from the description that '
            'support the match>", '
            '"reason": "<how the evidence maps to the candidate '
            'definition>"}]}'
        )

    # Optional mode-of-operation section.
    if modes:
        mode_lines = [
            "Mode of operation — for each selected candidate, classify "
            "HOW the entity relates to the matched candidate:"
        ]
        for m in modes:
            mode_lines.append(f"- '{m['name']}': {m['definition']}")
        mode_lines.append(
            "Pick the mode that best fits the entity's primary "
            "relationship to the matched candidate."
        )
        modes_section: str | None = "\n".join(mode_lines)
    else:
        modes_section = None

    defaults = _default_rule_bodies(levels)
    numbered_rules: list[str] = []
    for i, key in enumerate(PROMPT_RULE_KEYS, start=1):
        body = overrides.get(key, defaults[key])
        numbered_rules.append(f"{i}. {body}")

    parts: list[str] = [
        domain_intro,
        _TASK_FRAMING,
        f"Return JSON of the form:\n{schema}",
    ]
    if modes_section is not None:
        parts.append(modes_section)
    parts.append("Matching rules:\n" + "\n".join(numbered_rules))
    if include_confidence:
        parts.append(
            "Confidence. For each match, include a `confidence` in [0,1] "
            "reflecting how strongly the description supports it (1 = the "
            "description names the activity explicitly; lower = weaker or "
            "more inferential support). Surface every candidate that "
            "plausibly fits, including weak or uncertain ones with low "
            "confidence — do not omit a plausible candidate just because "
            "it is uncertain. A downstream threshold decides which to keep."
        )
    parts.append(_OUTPUT_CONSTRAINTS)

    return "\n\n".join(parts)


def format_candidates(candidates: pd.DataFrame, key_col: str) -> str:
    """Render candidates as a numbered list of name + definition."""
    lines = []
    for i, row in candidates.iterrows():
        name = str(row[key_col]).strip()
        definition = str(row["Definition"]).strip()
        lines.append(f"{i + 1}. {name}\n   Definition: {definition}")
    return "\n\n".join(lines)


def call_openai_match(
    client: OpenAI,
    system_prompt: str,
    model: str,
    entity_name: str,
    entity_description: str,
    level_name: str,
    candidates: pd.DataFrame,
    key_col: str,
    confidence_threshold: float | None = None,
) -> list[dict[str, str]]:
    """Ask the model for the best candidate match(es) at this level.

    When ``confidence_threshold`` is set, the system prompt is expected to
    request a per-match ``confidence`` (build it with
    ``build_system_prompt(include_confidence=True)``); matches whose
    confidence is below the threshold are dropped. The threshold is the
    precision/recall knob: lower keeps more (weak) matches, higher keeps
    only strong ones. Missing/unparseable confidence defaults to 1.0 so a
    non-confidence prompt behaves exactly as before.
    """
    user_prompt = (
        f"Entity name: {entity_name}\n\n"
        f"Entity description:\n{entity_description}\n\n"
        f"Taxonomy level: {level_name}\n"
        f"Candidate nodes at this level:\n\n"
        f"{format_candidates(candidates, key_col)}"
    )

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    raw = resp.choices[0].message.content or "{}"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  [warn] bad JSON at level {level_name}: {raw[:200]}")
        return []

    matches = data.get("matches", []) or []
    n = len(candidates)
    cleaned: list[dict[str, str]] = []
    seen: set[int] = set()
    for m in matches:
        raw_idx = m.get("index")
        try:
            idx = int(raw_idx)
        except (TypeError, ValueError):
            print(f"  [warn] non-integer index at {level_name}: {raw_idx!r}")
            continue
        # Candidate list is rendered 1-based; convert to a 0-based row index.
        # The prompt says to return `matches: []` for "no match", but some
        # models still emit index 0 as a no-match signal — accept it silently.
        if idx == 0:
            continue
        if not (1 <= idx <= n):
            print(f"  [warn] out-of-range index at {level_name}: {idx} (n={n})")
            continue
        if idx in seen:
            continue
        seen.add(idx)
        name = str(candidates.iloc[idx - 1][key_col])
        mode = str(m.get("mode_of_operation", "")).strip().lower()
        # Empty / unrecognized modes are normalized to "" so downstream
        # rules (e.g. the indirect-fanout stop) can compare safely.
        if mode and mode not in {"direct", "enabling tech", "indirect"}:
            print(f"  [warn] unexpected mode_of_operation at {level_name}: {mode!r}")
            mode = ""
        # Confidence: parse to float in [0,1] when present, else None (so a
        # non-confidence prompt leaves the field blank rather than implying
        # a score). A missing/unparseable confidence never filters — only an
        # explicit below-threshold value drops the match.
        raw_conf = m.get("confidence")
        if raw_conf is None:
            confidence = None
        else:
            try:
                confidence = max(0.0, min(1.0, float(raw_conf)))
            except (TypeError, ValueError):
                confidence = None
        if (confidence_threshold is not None and confidence is not None
                and confidence < confidence_threshold):
            continue
        cleaned.append({
            "name": name,
            "mode_of_operation": mode,
            "evidence": str(m.get("evidence", "")).strip(),
            "reason": str(m.get("reason", "")).strip(),
            "confidence": confidence,
        })
    return cleaned


# ---------------------------------------------------------------------------
# Hierarchical walk
# ---------------------------------------------------------------------------

def classify_entity(
    client: OpenAI,
    tables: dict[int, pd.DataFrame],
    levels: list[dict],
    system_prompt: str,
    entity_name: str,
    entity_description: str,
    model: str,
    descent_fanout_cap: int,
    confidence_threshold: float | None = None,
    emit_per_level: bool = False,
    seed_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Walk the taxonomy top-down for a single entity.

    ``seed_names`` fixes a contiguous top prefix of the levels (mapping each
    seeded level's ``output_col`` to its value, e.g. ``{"Pillar": "Energy
    Transition"}``) and the walk descends only the remaining levels from
    that seeded parent — used to continue into a subtree after a top-level
    node has been assigned elsewhere (e.g. by the empties adjudicator). The
    seeded levels carry no evidence/reason of their own.

    Returns one record per LEAF in the match tree — i.e. per root-to-tip
    path through the accepted matches. A leaf is a match that was either
    (a) at the deepest level, (b) had no children matched, (c) truncated
    by the descent fan-out cap, or (d) tagged ``indirect`` by the prompt
    when there were multiple sibling matches at the same step (the
    "indirect-fanout stop"; a no-op when the prompt does not return a
    ``mode_of_operation`` field).

    Each record has one column per level (named by ``output_col``, empty
    where the branch stopped early) plus the leaf's level name in
    ``deepest_match``, plus its definition, mode, evidence, and reason.
    """
    empty_leaf = {
        "deepest_match": None,
        "leaf_definition": None,
        "mode_of_operation": None,
        "evidence": None,
        "reason": None,
        "confidence": None,
    }
    last_idx = levels[-1]["idx"]

    # path_meta: out_col -> {evidence, reason, confidence, mode} for every
    # level matched along this path, for the per-level output (emit_per_level).
    if seed_names:
        # Reconstruct the seeded prefix's names + parent_path so the walk can
        # descend its children. Stops seeding at the first level whose value
        # is missing or not found (then walks normally from there).
        names: dict[str, Any] = {}
        parent_path: dict[str, Any] = {}
        seed_leaf = empty_leaf
        start_idx = 0
        for lvl in levels:
            oc = lvl["output_col"]
            if oc not in seed_names:
                break
            cands = candidates_for_level(tables, lvl["idx"], parent_path)
            match = cands[cands[lvl["key_col"]] == seed_names[oc]]
            if match.empty:
                break
            row = match.iloc[0]
            names[oc] = seed_names[oc]
            if lvl["idx"] != last_idx:
                parent_path[lvl["child_filter_col"]] = row[lvl["child_filter_value_col"]]
            seed_leaf = {
                "deepest_match": lvl["name"],
                "leaf_definition": str(row["Definition"]).strip(),
                "mode_of_operation": None,
                "evidence": None,
                "reason": None,
                "confidence": None,
            }
            start_idx = lvl["idx"] + 1
        initial_branch = {"names": names, "parent_path": parent_path,
                          "leaf": seed_leaf, "path_meta": {}}
        levels_to_walk = [lvl for lvl in levels if lvl["idx"] >= start_idx]
    else:
        initial_branch = {
            "names": {}, "parent_path": {}, "leaf": empty_leaf, "path_meta": {},
        }
        levels_to_walk = levels

    active: list[dict[str, Any]] = [initial_branch]
    leaves: list[dict[str, Any]] = []

    for lvl in levels_to_walk:
        level_idx = lvl["idx"]
        key_col = lvl["key_col"]
        out_col = lvl["output_col"]
        is_last_level = level_idx == last_idx
        if not is_last_level:
            child_filter_col = lvl["child_filter_col"]
            child_filter_value_col = lvl["child_filter_value_col"]

        next_active: list[dict[str, Any]] = []

        for branch in active:
            candidates = candidates_for_level(tables, level_idx, branch["parent_path"])
            if candidates.empty:
                if branch["leaf"] is not empty_leaf:
                    leaves.append(branch)
                continue

            matches = call_openai_match(
                client=client,
                system_prompt=system_prompt,
                model=model,
                entity_name=entity_name,
                entity_description=entity_description,
                level_name=lvl["name"],
                candidates=candidates,
                key_col=key_col,
                confidence_threshold=confidence_threshold,
            )

            if not matches:
                if branch["leaf"] is not empty_leaf:
                    leaves.append(branch)
                continue

            for i, m in enumerate(matches):
                cand_row = candidates[candidates[key_col] == m["name"]].iloc[0]
                new_names = dict(branch["names"])
                new_names[out_col] = m["name"]
                new_parent_path = dict(branch["parent_path"])
                if not is_last_level:
                    new_parent_path[child_filter_col] = cand_row[child_filter_value_col]
                new_leaf = {
                    "deepest_match": lvl["name"],
                    "leaf_definition": str(cand_row["Definition"]).strip(),
                    "mode_of_operation": m["mode_of_operation"],
                    "evidence": m["evidence"],
                    "reason": m["reason"],
                    "confidence": m.get("confidence"),
                }
                new_path_meta = dict(branch["path_meta"])
                new_path_meta[out_col] = {
                    "evidence": m["evidence"],
                    "reason": m["reason"],
                    "confidence": m.get("confidence"),
                    "mode_of_operation": m["mode_of_operation"],
                }
                new_branch = {
                    "names": new_names,
                    "parent_path": new_parent_path,
                    "leaf": new_leaf,
                    "path_meta": new_path_meta,
                }
                # Indirect-fanout stop: an `indirect` (advocacy / policy /
                # education) match descends only when it is the SOLE match
                # at this step. Multiple indirect siblings indicate the
                # entity advocates broadly across the level and naming any
                # specific child would be guesswork; record them as final
                # leaves at the current level instead. Direct / enabling-
                # tech matches still descend normally (subject to the
                # fan-out cap).
                indirect_fanout = (
                    m["mode_of_operation"] == "indirect"
                    and len(matches) > 1
                )
                if is_last_level or i >= descent_fanout_cap or indirect_fanout:
                    leaves.append(new_branch)
                else:
                    next_active.append(new_branch)

        active = next_active
        if not active:
            break

    leaves.extend(active)

    out_cols = [lvl["output_col"] for lvl in levels]

    def _record(b: dict[str, Any]) -> dict[str, Any]:
        rec = {c: b["names"].get(c) for c in out_cols}
        rec.update(b["leaf"])
        if emit_per_level:
            # Per-level evidence / reason / confidence for every level matched
            # on this path (None where the branch stopped short). Columns are
            # named "<output_col> evidence" etc.
            for oc in out_cols:
                pm = b["path_meta"].get(oc, {})
                rec[f"{oc} evidence"] = pm.get("evidence")
                rec[f"{oc} reason"] = pm.get("reason")
                rec[f"{oc} confidence"] = pm.get("confidence")
        return rec

    return [_record(b) for b in leaves]


# ---------------------------------------------------------------------------
# Per-entity worker
# ---------------------------------------------------------------------------

def _classify_one(
    client: OpenAI,
    tables: dict[int, pd.DataFrame],
    levels: list[dict],
    system_prompt: str,
    row: pd.Series,
    id_col: str,
    name_col: str,
    text_col: str,
    model: str,
    descent_fanout_cap: int,
    confidence_threshold: float | None = None,
    emit_per_level: bool = False,
    seed_col: str | None = None,
) -> list[dict[str, Any]]:
    """Classify a single entity and return flat output rows.

    All columns of ``row`` are carried through into ``base`` so the
    per-row output keeps any extra attributes (Funding, x/y, etc.).
    Emits one null-taxonomy row when there is no level-0 match so the
    entity still appears in the output. When ``seed_col`` is set and the
    row has a non-empty value there, the walk is seeded at the top level
    with that value (see ``classify_entity``'s ``seed_names``).
    """
    name = str(row[name_col])
    desc = str(row[text_col])
    base = {col: row[col] for col in row.index}
    base[name_col] = name
    base[text_col] = desc

    seed_names = None
    if seed_col is not None:
        seed_val = row.get(seed_col)
        if seed_val is not None and str(seed_val).strip() not in ("", "nan", "None"):
            seed_names = {levels[0]["output_col"]: str(seed_val)}

    try:
        records = classify_entity(
            client=client,
            tables=tables,
            levels=levels,
            system_prompt=system_prompt,
            entity_name=name,
            entity_description=desc,
            model=model,
            descent_fanout_cap=descent_fanout_cap,
            confidence_threshold=confidence_threshold,
            emit_per_level=emit_per_level,
            seed_names=seed_names,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  [error] {name}: {exc}")
        return []

    leaf_keys = ["deepest_match", "leaf_definition",
                 "mode_of_operation", "evidence", "reason", "confidence"]
    if not records:
        empty = {lvl["output_col"]: None for lvl in levels}
        empty.update({k: None for k in leaf_keys})
        if emit_per_level:
            for lvl in levels:
                oc = lvl["output_col"]
                empty[f"{oc} evidence"] = None
                empty[f"{oc} reason"] = None
                empty[f"{oc} confidence"] = None
        return [{**base, **empty}]
    return [{**base, **r} for r in records]


# ---------------------------------------------------------------------------
# Batch entry point
# ---------------------------------------------------------------------------

def classify_entities(
    *,
    client: OpenAI,
    tables: dict[int, pd.DataFrame],
    levels: list[dict],
    system_prompt: str,
    entities: pd.DataFrame,
    id_col: str,
    name_col: str,
    text_col: str,
    model: str = "gpt-4.1",
    descent_fanout_cap: int = 3,
    max_workers: int = 8,
    confidence_threshold: float | None = None,
    emit_per_level: bool = False,
    seed_col: str | None = None,
) -> pd.DataFrame:
    """Classify many entities in parallel; return one flat DataFrame.

    When ``seed_col`` is set, each entity whose ``seed_col`` value is
    non-empty has its walk seeded at the top level with that value (the
    walk descends that node's subtree); entities with an empty seed walk
    from the root as usual. Useful for descending into a subtree after a
    top-level assignment from elsewhere (e.g. the empties adjudicator).

    ``entities`` must have at least ``id_col``, ``name_col``, ``text_col``.
    The output has those columns followed by every other entity column
    (in input order), then one column per level (``output_col``), then
    five leaf columns: ``deepest_match``, ``leaf_definition``,
    ``mode_of_operation``, ``evidence``, ``reason``.

    Uses a thread pool because each entity's hierarchical walk is
    independent and the work is I/O-bound on the OpenAI API.
    """
    levels = normalize_levels(levels)

    print(
        f"Classifying {len(entities)} entities "
        f"with {max_workers} worker(s)"
    )

    all_records: list[dict[str, Any]] = []
    t0 = time.time()

    if max_workers <= 1:
        # Single-threaded path — kept unthreaded to make debugging easier.
        for i, row in entities.iterrows():
            print(f"[{i + 1}/{len(entities)}] {row[name_col]}")
            all_records.extend(_classify_one(
                client, tables, levels, system_prompt, row,
                id_col, name_col, text_col, model, descent_fanout_cap,
                confidence_threshold, emit_per_level, seed_col,
            ))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _classify_one,
                    client, tables, levels, system_prompt, row,
                    id_col, name_col, text_col, model, descent_fanout_cap,
                    confidence_threshold, emit_per_level, seed_col,
                ): row
                for _, row in entities.iterrows()
            }
            done = 0
            for fut in as_completed(futures):
                row = futures[fut]
                done += 1
                recs = fut.result()
                print(f"[{done}/{len(entities)}] {row[name_col]} -> {len(recs)} rows")
                all_records.extend(recs)

    print(f"\nTotal elapsed: {time.time() - t0:.1f}s")

    front = [id_col, name_col, text_col]
    extras = [c for c in entities.columns if c not in front]
    classification_cols = (
        [lvl["output_col"] for lvl in levels]
        + ["deepest_match", "leaf_definition",
           "mode_of_operation", "evidence", "reason", "confidence"]
    )
    if emit_per_level:
        for lvl in levels:
            oc = lvl["output_col"]
            classification_cols += [f"{oc} evidence", f"{oc} reason",
                                    f"{oc} confidence"]
    out_cols = front + extras + classification_cols
    return pd.DataFrame(all_records, columns=out_cols)


# ---------------------------------------------------------------------------
# Collapsed (one-row-per-id) output
# ---------------------------------------------------------------------------

def _dedup_preserve(values: list) -> list:
    """Keep the first occurrence of each non-null/non-empty value."""
    seen: set = set()
    out: list = []
    for v in values:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        s = str(v).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def collapse_to_one_row_per_uid(
    df: pd.DataFrame,
    levels: list[dict],
    id_col: str = "uid",
) -> pd.DataFrame:
    """Collapse a per-row classification DataFrame to one row per id_col.

    Each level's per-row column (``output_col``) becomes a list of the
    unique non-empty values seen for that id, rendered with ``repr()`` so
    cells round-trip via ``ast.literal_eval``. The collapsed column uses
    the level's ``name`` as its header (so e.g. Drawdown's per-row
    ``Cluster`` column becomes the collapsed ``SectorCluster`` column).
    The deepest non-empty level's ``name`` is reported in
    ``deepest_match``; ids with all level lists empty get
    ``deepest_match == "NoMatch"``.

    Non-classification columns (anything that isn't ``id_col`` or one of
    the level / leaf columns) carry through using each id's first
    non-null value.
    """
    levels = normalize_levels(levels)

    classification_set = (
        {lvl["output_col"] for lvl in levels}
        | {"deepest_match", "leaf_definition",
           "mode_of_operation", "evidence", "reason"}
    )
    carry_cols = [c for c in df.columns if c != id_col and c not in classification_set]

    rows: list[dict[str, Any]] = []
    for uid, group in df.groupby(id_col, sort=False):
        row: dict[str, Any] = {id_col: uid}
        for col in carry_cols:
            non_null = group[col].dropna()
            row[col] = non_null.iloc[0] if len(non_null) else None

        deepest_name: str | None = None
        for lvl in levels:
            src = lvl["output_col"]
            collapsed_col = lvl["name"]
            values = _dedup_preserve(group[src].tolist()) if src in group else []
            row[collapsed_col] = repr(values)
            if values:
                deepest_name = lvl["name"]
        row["deepest_match"] = deepest_name if deepest_name else "NoMatch"

        modes = (
            _dedup_preserve(group["mode_of_operation"].tolist())
            if "mode_of_operation" in group else []
        )
        row["mode_of_operation"] = repr(modes)
        rows.append(row)

    out_cols = (
        [id_col]
        + carry_cols
        + [lvl["name"] for lvl in levels]
        + ["deepest_match", "mode_of_operation"]
    )
    return pd.DataFrame(rows, columns=out_cols)


# ---------------------------------------------------------------------------
# Adjudication of unmatched entities (second-stage scope check)
# ---------------------------------------------------------------------------

def build_default_scope_prompt(
    tables: dict[int, pd.DataFrame],
    levels: list[dict],
) -> str:
    """Generic default scope prompt for ``adjudicate_unmatched``.

    Renders the top-level node names + definitions from the taxonomy and a
    generic in-scope/category/reason instruction. This is the runnable
    baseline — like the engine's default rule bodies, it works for any
    taxonomy with no authoring, but a caller-supplied ``scope_prompt`` with
    domain-specific in/out guidance and routing will classify more
    accurately.
    """
    levels = normalize_levels(levels)
    top = levels[0]
    t = tables[top["idx"]]
    block = "\n".join(
        f"- {row[top['key_col']]}: {str(row['Definition']).strip()}"
        for _, row in t.iterrows()
        if pd.notna(row.get(top["key_col"])) and pd.notna(row.get("Definition"))
    )
    return (
        "You decide whether an entity's own work is in scope for the taxonomy "
        "below. Top-level categories and their scope:\n"
        f"{block}\n\n"
        "Judge by whether the entity's own activity is itself one of these "
        "categories (an implicit mechanism is acceptable), not an incidental "
        "side-effect of otherwise out-of-scope work. Return JSON: "
        "{\"in_scope\": true|false, \"category\": \"<exact category name or "
        "null>\", \"reason\": \"<one sentence>\"}."
    )


def adjudicate_unmatched(
    df: pd.DataFrame,
    *,
    client: OpenAI,
    model: str,
    id_col: str,
    name_col: str,
    text_col: str,
    levels: list[dict] | None = None,
    tables: dict[int, pd.DataFrame] | None = None,
    top_level_col: str | None = None,
    category_choices: Iterable[str] | None = None,
    scope_prompt: str | None = None,
    max_workers: int = 8,
) -> pd.DataFrame:
    """Second-stage scope check on entities the walk left unmatched.

    The top-down walk is precision-tuned and refuses entities whose
    in-scope activity is described obliquely (civic / service framing) —
    but those refusals are a mix of genuine out-of-scope and missed
    in-scope. The walk's own (single, refusing) judgment cannot tell them
    apart. This runs an INDEPENDENT scope judgment with ``model`` (use a
    stronger model than the classifier) over only the entities with no
    top-level match, returning whether each is in scope and which
    top-level category fits.

    Adds three columns, filled only for unmatched entities (matched
    entities and their rows keep ``None``):
        - ``adjudicator_in_scope``      bool | None
        - ``adjudicated_<top_level_col>`` str | None  (validated category)
        - ``adjudicator_reason``        str | None

    ``top_level_col``, ``category_choices``, and ``scope_prompt`` each
    default from ``levels`` + ``tables`` when omitted (top-level output
    column; top-level node names; ``build_default_scope_prompt``), so a
    generic caller can pass just ``levels`` + ``tables``. Supply any of them
    explicitly to override — a domain-specific ``scope_prompt`` in
    particular classifies more accurately than the default. The scope prompt
    must instruct the model to return JSON ``{"in_scope": bool, "<category
    key>": "<name or null>", "reason": "..."}``; category values are
    validated (case-insensitively) against ``category_choices``.
    """
    if top_level_col is None or category_choices is None or scope_prompt is None:
        if levels is None or tables is None:
            raise ValueError(
                "adjudicate_unmatched: supply levels + tables to default "
                "top_level_col / category_choices / scope_prompt, or pass all "
                "three explicitly."
            )
        nlevels = normalize_levels(levels)
        top = nlevels[0]
        if top_level_col is None:
            top_level_col = top["output_col"]
        if category_choices is None:
            category_choices = list(tables[top["idx"]][top["key_col"]].dropna())
        if scope_prompt is None:
            scope_prompt = build_default_scope_prompt(tables, nlevels)

    cats = {str(c).strip().lower(): str(c) for c in category_choices}

    def _empty(s: pd.Series) -> pd.Series:
        return s.isna() | s.astype(str).str.strip().isin(["", "None", "nan"])

    unmatched_ids = [
        uid for uid, g in df.groupby(id_col, sort=False)
        if bool(_empty(g[top_level_col]).all())
    ]
    rep = (df[df[id_col].isin(unmatched_ids)]
           .drop_duplicates(id_col).set_index(id_col))

    cat_key = top_level_col.strip().lower()

    def _adjudicate(uid: Any) -> tuple[Any, tuple]:
        name = str(rep.loc[uid, name_col])
        text = str(rep.loc[uid, text_col])
        user_prompt = f"Organization: {name}\n\nDescription:\n{text}"
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": scope_prompt},
                          {"role": "user", "content": user_prompt}],
                response_format={"type": "json_object"}, temperature=0,
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            in_scope = bool(data.get("in_scope"))
            raw_cat = data.get(cat_key) or data.get("category") or data.get("pillar")
            cat = cats.get(str(raw_cat).strip().lower()) if raw_cat else None
            reason = str(data.get("reason", "")).strip()
            return uid, (in_scope, cat if in_scope else None, reason)
        except Exception as exc:  # noqa: BLE001
            return uid, (None, None, f"adjudicator error: {exc}")

    results: dict[Any, tuple] = {}
    if unmatched_ids:
        print(f"Adjudicating {len(unmatched_ids)} unmatched entities with {model}")
        if max_workers <= 1:
            for uid in unmatched_ids:
                _, res = _adjudicate(uid)
                results[uid] = res
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for uid, res in pool.map(_adjudicate, unmatched_ids):
                    results[uid] = res

    df = df.copy()
    none3 = (None, None, None)
    df["adjudicator_in_scope"] = df[id_col].map(lambda u: results.get(u, none3)[0])
    df[f"adjudicated_{top_level_col}"] = df[id_col].map(lambda u: results.get(u, none3)[1])
    df["adjudicator_reason"] = df[id_col].map(lambda u: results.get(u, none3)[2])
    return df
