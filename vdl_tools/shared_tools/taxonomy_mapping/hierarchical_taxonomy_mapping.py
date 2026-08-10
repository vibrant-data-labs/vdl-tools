"""
Hierarchical Taxonomy Mapping — general library
================================================

Reusable utilities for matching a list of entities (companies/organizations)
against a multi-level hierarchical taxonomy via an OpenAI model, with all
calls routed through the project's SQL prompt/response cache.

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
truncated by the descent fan-out cap, or (when the ``match_schema`` has a
``mode_of_operation`` field) tagged ``indirect`` alongside other siblings.

This module is taxonomy-agnostic. Callers (driver scripts) supply:

    * a level spec (see below),
    * a system prompt string describing the matching task — assemble the
      prose with ``build_system_prompt`` (see "System prompt" below),
    * a Pydantic ``match_schema`` describing the per-match response shape
      and carrying the per-field / selection guidance (see "Response
      schema" below),
    * a taxonomy xlsx whose sheet names align with the level spec,
    * an entities DataFrame with id / name / text columns,
    * a SQLAlchemy session for the prompt/response cache (see "Caching").

See ``oe_hierarchical_taxonomy_mapping.py`` for an example driver.

Caching
-------
Every OpenAI call goes through ``TaxonomyMatchCache`` /
``ScopeRecoveryCache`` (in ``taxonomy_mapping_cache.py``), which extend
``PromptResponseCacheSQL``. Cache rows are keyed by
``(prompt_id, given_id, text_id)``:

    * ``prompt_id`` — hash of the system prompt + the Pydantic response
      schema, so changing either invalidates all rows for the next run.
    * ``given_id`` — built per-call from
      ``f"{entity_id}|{level_name}|{sorted parent_path}"``; stable across
      re-runs.
    * ``text_id`` — hash of the user-message body (entity name +
      description + level name + rendered candidate list); changing a
      candidate's definition invalidates only the branches that saw it.

The engine batches one ``bulk_get_cache_or_run`` per level — concurrency
comes from the cache's worker pool (bounded by ``max_workers``), and the
session is touched only on the main thread between batches.

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
but in practice you should build the prose with ``build_system_prompt``,
which assembles a generic skeleton (``domain_intro``, task framing, and
eight numbered matching-rule slots) and lets you customize the parts
that vary per taxonomy. It deliberately does NOT describe the JSON
output shape — that lives on the Pydantic ``match_schema`` (see
"Response schema" below), which ``InstructorPRC`` appends to the prompt
automatically.

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

Two layers of prose customization:

    1. ``domain_intro`` (required) — opening paragraph framing what the
       taxonomy classifies.
    2. ``rules`` (optional) — dict mapping any of the eight keys above
       to a full override. Use this when a generic default's wording
       isn't tight enough for your domain — typically because it needs
       domain-specific examples or terminology that improves model
       accuracy. Domain-specific selection rules (e.g. One Earth's
       cross-pillar routing) also go here, or are appended by the driver
       after ``build_system_prompt`` returns.

The Drawdown and One Earth drivers demonstrate both layers — see
``DRAWDOWN_DOMAIN_INTRO`` / ``DRAWDOWN_RULE_OVERRIDES`` in
``drawdown_hierarchical_taxonomy_mapping.py`` and ``ONEEARTH_DOMAIN_INTRO``
/ ``ONEEARTH_RULE_OVERRIDES`` in ``oe_hierarchical_taxonomy_mapping.py``.

For a brand-new taxonomy with no special rules, the minimum is::

    system_prompt = build_system_prompt(
        levels=my_levels,
        domain_intro="You are classifying X against a hierarchical taxonomy of Y...",
    )

Response schema
---------------
``classify_entities`` takes a Pydantic ``match_schema`` (defaulting to
``MatchesResponse`` from ``taxonomy_mapping_cache``) that defines the
per-match response shape AND carries all schema-level instruction:

    * **Per-field guidance** — ``Field(description=...)`` on each field:
      a ``mode_of_operation`` field's allowed values and their
      definitions, the "1-based index" note, evidence / reason guidance.
    * **Selection-level guidance** — the response class's docstring
      (e.g. "self-filter weak candidates" vs "surface every plausible
      candidate with low confidence").

``InstructorPRC`` appends ``match_schema.schema_json()`` to the system
prompt, so the model sees the field descriptions and the response class
docstring there. Because the structural constraint (the schema) and its
human-readable guidance are the same object, the prompt and the API's
strict-mode enforcement cannot drift apart. The driver is responsible
for pairing the right ``match_schema`` with the prose it built — see
``oneearth_match_schema()`` next to ``build_oneearth_system_prompt()``
for the pattern.

One consequence worth knowing: a field present in the schema is
REQUIRED under strict mode (only ``null`` allowed if typed
``T | None``). Don't include ``mode_of_operation`` / ``confidence`` in a
schema for a taxonomy that doesn't use them, or the model is forced to
emit them anyway.

Output schema
-------------
``classify_entities`` returns a flat per-row DataFrame. Columns:

    [id_col, name_col, text_col]
    + [other entity columns, in input order]
    + [lvl["output_col"] for lvl in levels]
    + ["deepest_match", "leaf_definition",
       "mode_of_operation", "evidence", "reason", "confidence"]

One row per (entity, leaf) pair. Entities the walk never produced a leaf
for still appear with all level columns null; when the model refused at
the entity's first level and the response schema carries a
``no_match_reason`` (the default does), that row's ``reason`` holds the
model's own explanation of why nothing fit.

``collapse_to_one_row_per_uid`` collapses that frame to one row per
``id_col`` value with ``repr()``-encoded list cells per level.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from pydantic import BaseModel

from vdl_tools.shared_tools.openai.openai_api_utils import is_reasoning_model
from vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping_cache import (
    MatchesResponse,
    ScopeDecision,
    ScopeRecoveryCache,
    TaxonomyMatchCache,
)
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping import (
    redistribute_funding_fracs,
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


_TASK_FRAMING = (
    "You will be given an entity description and a numbered list of "
    "candidate nodes (name + definition) at a single level of the "
    "hierarchy. Identify which node(s) the description shows the "
    "entity is actually working on. Output strict JSON matching the "
    "response schema you are given — per-field guidance lives in the "
    "schema's field descriptions and selection-level guidance lives "
    "on the response class's docstring."
)


def build_system_prompt(
    *,
    levels: list[dict],
    domain_intro: str,
    rules: dict[str, str] | None = None,
) -> str:
    """Assemble a hierarchical-taxonomy classification system prompt.

    Composes a generic prose skeleton: ``domain_intro``, task framing,
    eight matching-rule slots (with overrides). The JSON output shape,
    per-field guidance (mode definitions, confidence semantics), and
    selection-level guidance (when to surface weak candidates) ALL live
    on the Pydantic response class the driver passes to
    ``classify_entities(match_schema=...)``. ``InstructorPRC`` appends
    that class's JSON schema to the prompt, so the model sees field
    descriptions and the response class's docstring there. Keeping those
    on the schema is what makes prompt and structural enforcement
    impossible to drift apart.

    Parameters
    ----------
    levels
        Same level-spec list passed to ``classify_entities``. The
        ``name`` field on each spec is used in rule 7's "Prominence
        at <top> and <mid> levels" wording.
    domain_intro
        Opening paragraph framing what the taxonomy classifies.
    rules
        Optional dict mapping any of the keys in ``PROMPT_RULE_KEYS``
        to a full override body (without leading number). Missing
        keys fall back to ``_default_rule_bodies(levels)``. Domain-
        specific selection rules (e.g. cross-pillar routing for OE)
        also go here, either as overrides of a default rule or
        appended by the driver after ``build_system_prompt`` returns.
    """
    overrides = rules or {}

    defaults = _default_rule_bodies(levels)
    numbered_rules: list[str] = []
    for i, key in enumerate(PROMPT_RULE_KEYS, start=1):
        body = overrides.get(key, defaults[key])
        numbered_rules.append(f"{i}. {body}")

    parts: list[str] = [
        domain_intro,
        _TASK_FRAMING,
        "Matching rules:\n" + "\n".join(numbered_rules),
    ]

    return "\n\n".join(parts)


def format_candidates(candidates: pd.DataFrame, key_col: str) -> str:
    """Render candidates as a numbered list of name + definition."""
    lines = []
    for i, row in candidates.iterrows():
        name = str(row[key_col]).strip()
        definition = str(row["Definition"]).strip()
        lines.append(f"{i + 1}. {name}\n   Definition: {definition}")
    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# Per-level request building + match parsing
# ---------------------------------------------------------------------------

def _build_user_text(
    entity_name: str,
    entity_description: str,
    level_name: str,
    candidates: pd.DataFrame,
    key_col: str,
) -> str:
    """User-message body for a single per-level match call.

    The body is the cache's ``text`` input — its hash becomes the cache
    row's ``text_id``. Changing any candidate's name or definition
    therefore invalidates only the rows that saw that candidate list.
    """
    return (
        f"Entity name: {entity_name}\n\n"
        f"Entity description:\n{entity_description}\n\n"
        f"Taxonomy level: {level_name}\n"
        f"Candidate nodes at this level:\n\n"
        f"{format_candidates(candidates, key_col)}"
    )


def _build_given_id(
    entity_id: Any,
    parent_path: dict[str, Any],
    level_name: str,
) -> str:
    """Stable cache ``given_id`` for one (entity, parent_path, level) call.

    Parent-path entries are alphabetized so dict iteration order can't
    perturb the hash across runs. ``|`` and ``>`` are safe separators —
    taxonomy level / column names use spaces, hyphens, and parentheses
    but neither of these characters.
    """
    if parent_path:
        path = ">".join(f"{k}={v}" for k, v in sorted(parent_path.items()))
    else:
        path = ""
    return f"{entity_id}|{level_name}|{path}"


def _clean_matches(
    matches: list[Any],
    candidates: pd.DataFrame,
    key_col: str,
    level_name: str,
    confidence_threshold: float | None,
) -> list[dict[str, Any]]:
    """Validate parsed ``Match`` objects against the candidate list.

    Drops idx==0 (no-match sentinel some models still emit), out-of-range
    indices, duplicates, and (when set) matches below
    ``confidence_threshold``. Returns dicts shaped like the legacy
    ``call_openai_match`` output so the rest of the walk is unchanged.

    Uses ``getattr`` for ``mode_of_operation`` / ``confidence`` so this
    works whether the driver's ``match_schema`` includes those fields
    or not. A schema without ``mode_of_operation`` yields ``mode=""``.
    """
    n = len(candidates)
    cleaned: list[dict[str, Any]] = []
    seen: set[int] = set()
    for m in matches:
        idx = m.index
        if idx == 0:
            continue
        if not (1 <= idx <= n):
            logger.warning(f"  [warn] out-of-range index at {level_name}: {idx} (n={n})")
            continue
        if idx in seen:
            continue
        seen.add(idx)
        name = str(candidates.iloc[idx - 1][key_col])
        mode = (getattr(m, "mode_of_operation", None) or "").strip().lower()
        if mode and mode not in {"direct", "enabling tech", "indirect"}:
            logger.warning(f"  [warn] unexpected mode_of_operation at {level_name}: {mode!r}")
            mode = ""
        confidence = getattr(m, "confidence", None)
        if confidence is not None:
            try:
                confidence = max(0.0, min(1.0, float(confidence)))
            except (TypeError, ValueError):
                confidence = None
        if (confidence_threshold is not None and confidence is not None
                and confidence < confidence_threshold):
            continue
        cleaned.append({
            "name": name,
            "mode_of_operation": mode,
            "evidence": (m.evidence or "").strip(),
            "reason": (m.reason or "").strip(),
            "confidence": confidence,
        })
    return cleaned


def _seed_branch(
    row: pd.Series,
    levels: list[dict],
    tables: dict[int, pd.DataFrame],
    seed_col: str | None,
    empty_leaf: dict[str, Any],
    last_idx: int,
) -> tuple[dict[str, Any], dict[str, Any], int, dict[str, Any]]:
    """Compute (names, parent_path, start_idx, seed_leaf) for an entity.

    With no seeding (no ``seed_col`` or a blank value), returns
    ``({}, {}, 0, empty_leaf)`` — equivalent to walking from the root.
    Otherwise reconstructs the seeded prefix's ``names`` and
    ``parent_path`` so the level loop can descend the seeded subtree
    starting at ``start_idx``.
    """
    if seed_col is None:
        return {}, {}, 0, empty_leaf
    seed_val = row.get(seed_col)
    if seed_val is None or str(seed_val).strip() in ("", "nan", "None"):
        return {}, {}, 0, empty_leaf

    seed_names = {levels[0]["output_col"]: str(seed_val)}
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
        cand_row = match.iloc[0]
        names[oc] = seed_names[oc]
        if lvl["idx"] != last_idx:
            parent_path[lvl["child_filter_col"]] = cand_row[lvl["child_filter_value_col"]]
        seed_leaf = {
            "deepest_match": lvl["name"],
            "leaf_definition": str(cand_row["Definition"]).strip(),
            "mode_of_operation": None,
            "evidence": None,
            "reason": None,
            "confidence": None,
        }
        start_idx = lvl["idx"] + 1
    return names, parent_path, start_idx, seed_leaf


# ---------------------------------------------------------------------------
# Batch entry point — bulk-per-level walk against the SQL cache
# ---------------------------------------------------------------------------

def _build_llm_api_kwargs(
    model: str,
    temperature: float | None,
    llm_api_kwargs: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge the ``temperature`` parameter into a copy of ``llm_api_kwargs``.

    One policy for every cached call site:

    - A ``temperature`` inside ``llm_api_kwargs`` wins over the parameter,
      whose default of 0 would otherwise silently clobber an explicitly
      requested value.
    - Reasoning models reject ``temperature``; it is stripped whichever
      channel it came from (with a warning when it was passed explicitly
      via ``llm_api_kwargs``).
    - A reasoning model with no explicit ``reasoning`` kwarg gets a
      warning: the model's own default effort applies and the cache key
      records only the kwarg's absence.
    """
    api_kwargs: dict[str, Any] = dict(llm_api_kwargs or {})
    if is_reasoning_model(model):
        if "temperature" in api_kwargs:
            logger.warning(
                f"{model} is a reasoning model and rejects `temperature`; "
                f"dropping temperature={api_kwargs['temperature']!r} from "
                f"llm_api_kwargs."
            )
            api_kwargs.pop("temperature")
        if "reasoning" not in api_kwargs:
            logger.warning(
                f"No reasoning effort set for {model} — its own default applies "
                f"and the cache key records only the absence of the kwarg; pass "
                f"reasoning={{'effort': ...}} to pin it."
            )
    else:
        # Non-reasoning models reject `reasoning`; strip it whichever channel
        # it came from (mirrors the temperature handling above) so a shared
        # default like {"reasoning": {"effort": "medium"}} does not fail on
        # gpt-4.1-class models.
        if "reasoning" in api_kwargs:
            logger.warning(
                f"{model} is not a reasoning model and rejects `reasoning`; "
                f"dropping reasoning={api_kwargs['reasoning']!r} from "
                f"llm_api_kwargs."
            )
            api_kwargs.pop("reasoning")
        if "temperature" not in api_kwargs and temperature is not None:
            api_kwargs["temperature"] = temperature
    return api_kwargs


def classify_entities(
    *,
    session,
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
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    match_schema: type[BaseModel] | None = None,
    temperature: float | None = 0,
    filter_by_model: bool = False,
    llm_api_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Classify many entities against the taxonomy via a SQL-cached walk.

    Walks the taxonomy level-by-level. At each level, every active
    branch's OpenAI call is batched into a single
    ``bulk_get_cache_or_run`` so cache hits skip the API entirely and
    fresh calls are parallelized inside the cache. The SQLAlchemy
    ``session`` is touched only on the main thread between batches —
    workers do API I/O only.

    Cache keys:
        prompt_id = hash(system_prompt + match_schema JSON schema)
        given_id  = "<entity_id>|<level_name>|<sorted parent_path>"
        text_id   = hash(entity_name + description + level_name + candidates)

    Re-running the same pipeline against the same taxonomy + system
    prompt is a no-API run after the first successful pass; changing
    the taxonomy xlsx invalidates only the affected branches; changing
    the system prompt invalidates everything.

    Parameters
    ----------
    session
        SQLAlchemy session used by the cache. Build via
        ``vdl_tools.shared_tools.database_cache.database_utils.get_session``;
        the caller's ``with get_session() as session:`` block scopes the
        transaction. The cache commits per-chunk; this function also
        commits once at the end so a caller using a short-lived session
        does not lose the last batch.
    tables
        Per-level taxonomy DataFrames keyed by level index.
    levels
        Level spec — see module docstring.
    system_prompt
        System prompt for every match call. Build via
        ``build_system_prompt`` (or a thin wrapper).
    entities
        Entities to classify. Must contain ``id_col``, ``name_col``,
        ``text_col``; any other columns are carried through to the
        per-row output untouched.
    id_col, name_col, text_col
        Column names on ``entities``.
    model
        OpenAI model id (e.g. ``"gpt-4.1"``, ``"gpt-5-nano"``). Routed
        through ``InstructorPRC.get_completion`` -> Responses API; the
        cache scopes by model name only when constructed with
        ``filter_by_model=True`` (not the default — share cache rows
        across models for the same prompt + text).
    descent_fanout_cap
        Maximum number of children to descend into when a level returns
        multiple matches.
    max_workers
        Concurrency cap for the cache's API worker pool, applied per
        level batch.
    confidence_threshold
        Drop matches whose confidence is below this. Requires
        ``match_schema`` to include a ``confidence`` field (e.g. OE's
        ``OneEarthMatchesWithConfidenceResponse``); matches with no
        confidence value are never dropped.
    emit_per_level
        When True, output gains ``<level> evidence / reason / confidence``
        columns for every level matched along the path.
    seed_col
        Optional input column whose value pre-seeds the walk at the
        top level (the walk descends that node's subtree). Used by the
        recovery flow to second-walk entities the first pass left empty.
    read_from_cache
        When False, bypass cached rows and re-issue every API call
        (force-refresh). Default True.
    write_to_cache
        When False, leave the cache untouched (read-only / passthrough).
        Also gated by the cache's constructor-level ``store_results``.
        Default True.
    match_schema
        Optional Pydantic class for the per-match response shape. Drives
        the cache's structured-output constraint (via
        ``TaxonomyMatchCache(response_model=...)``), the response parsing
        here, AND — because ``InstructorPRC`` appends its
        ``schema_json()`` to the system prompt — the model-facing
        per-field / selection guidance (carried on the class's
        ``Field(description=...)`` and its docstring; see "Response
        schema" in the module docstring). The driver is responsible for
        pairing this with the prose it passed to ``build_system_prompt``;
        the two are not auto-validated against each other. When ``None``,
        the default ``MatchesResponse`` is used — permissive (mode +
        confidence nullable).
    temperature
        Sampling temperature for the model. Defaults to ``0`` to match
        the legacy ``call_openai_match`` path (deterministic outputs).
        Automatically suppressed for reasoning models (``gpt-5*``),
        which reject the parameter — those models use their own
        sampling internally. Pass ``None`` to omit entirely.
    filter_by_model
        When True, cache reads are scoped to the model identity (name +
        output-affecting kwargs; see "Kwargs-aware cache keys" in the
        ``PromptResponseCacheSQL`` docstring). **Set this True whenever
        you run more than one model identity against the same taxonomy**,
        otherwise an identity switch silently reuses the prior identity's
        cached results.
    llm_api_kwargs
        OpenAI API kwargs for the model call, forwarded to the cache and
        the API — e.g. ``{"reasoning": {"effort": "low"}}`` for reasoning
        models. An explicit dict (not ``**kwargs``) so that a typo'd
        function kwarg fails loudly at this call instead of riding into
        the API workers and surfacing as cache error rows. A
        ``temperature`` inside this dict takes precedence over the
        ``temperature`` parameter, and is dropped (with a warning) for
        reasoning models, which reject it; see ``_build_llm_api_kwargs``.
        How these kwargs key the cache is described in "Kwargs-aware cache
        keys" in the ``PromptResponseCacheSQL`` docstring. Note: when a
        reasoning model runs with no explicit ``reasoning``, the model's
        own default effort applies (it differs by model and is OpenAI's to
        change) and the key records only "no reasoning kwarg" — pass
        ``reasoning`` explicitly to pin it.
    """
    levels = normalize_levels(levels)
    last_idx = levels[-1]["idx"]
    out_cols = [lvl["output_col"] for lvl in levels]
    leaf_keys = ["deepest_match", "leaf_definition", "mode_of_operation",
                 "evidence", "reason", "confidence"]
    empty_leaf = {k: None for k in leaf_keys}
    response_model: type[BaseModel] = match_schema or MatchesResponse

    api_kwargs = _build_llm_api_kwargs(model, temperature, llm_api_kwargs)

    logger.info(f"Classifying {len(entities)} entities (cached, level-batched)")
    t0 = time.time()

    cache = TaxonomyMatchCache(
        session=session,
        system_prompt=system_prompt,
        model=model,
        response_model=response_model,
        filter_by_model=filter_by_model,
    )

    # Initialize one branch per entity. Carry each input row's columns
    # into base_by_entity_id so the per-row output keeps all input
    # attributes (Funding, x/y, etc.).
    initial_branches: list[dict[str, Any]] = []
    base_by_entity_id: dict[Any, dict[str, Any]] = {}
    for _, row in entities.iterrows():
        entity_id = row[id_col]
        name = str(row[name_col])
        desc = str(row[text_col])
        base = {col: row[col] for col in row.index}
        base[name_col] = name
        base[text_col] = desc
        base_by_entity_id[entity_id] = base

        names, parent_path, start_idx, seed_leaf = _seed_branch(
            row, levels, tables, seed_col, empty_leaf, last_idx,
        )
        initial_branches.append({
            "entity_id": entity_id,
            "entity_name": name,
            "entity_description": desc,
            "names": names,
            "parent_path": parent_path,
            "leaf": seed_leaf,
            "path_meta": {},
            "start_idx": start_idx,
        })

    active = initial_branches
    leaves: list[dict[str, Any]] = []
    # Model refusal rationale per entity, captured when the walk dies at
    # its first level (the branch still carries empty_leaf) and the
    # response schema has a non-empty ``no_match_reason``. Persisted on
    # the entity's all-null fallback row below.
    no_match_reason_by_entity: dict[Any, str] = {}

    for lvl in levels:
        level_idx = lvl["idx"]
        key_col = lvl["key_col"]
        out_col = lvl["output_col"]
        is_last_level = level_idx == last_idx
        if not is_last_level:
            child_filter_col = lvl["child_filter_col"]
            child_filter_value_col = lvl["child_filter_value_col"]

        # Phase A — assemble requests; carry seeded branches forward
        # until they reach their start_idx.
        requests: list[tuple[str, str, dict[str, Any], pd.DataFrame]] = []
        next_active: list[dict[str, Any]] = []
        for branch in active:
            if level_idx < branch["start_idx"]:
                next_active.append(branch)
                continue
            candidates = candidates_for_level(tables, level_idx, branch["parent_path"])
            if candidates.empty:
                if branch["leaf"] is not empty_leaf:
                    leaves.append(branch)
                continue
            user_text = _build_user_text(
                branch["entity_name"], branch["entity_description"],
                lvl["name"], candidates, key_col,
            )
            given_id = _build_given_id(
                branch["entity_id"], branch["parent_path"], lvl["name"],
            )
            requests.append((given_id, user_text, branch, candidates))

        # Phase B — one bulk call for this level.
        if requests:
            logger.info(f"  [{lvl['name']}] {len(requests)} request(s)")
            responses = cache.bulk_get_cache_or_run(
                given_ids_texts=[(gid, txt) for gid, txt, _, _ in requests],
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
                max_workers=max_workers,
                **api_kwargs,
            )

            # Phase C — parse responses; spawn child branches with the
            # same descent / fan-out / indirect-stop rules as the legacy
            # walk.
            for given_id, _, branch, candidates in requests:
                resp = responses.get(given_id)
                if resp is None:
                    # API failed (recorded as an error row); treat as
                    # no-match for this branch.
                    if branch["leaf"] is not empty_leaf:
                        leaves.append(branch)
                    continue
                try:
                    parsed = response_model.model_validate_json(resp["response_text"])
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"  [warn] bad response at {lvl['name']} "
                          f"for {branch['entity_id']}: {exc}")
                    if branch["leaf"] is not empty_leaf:
                        leaves.append(branch)
                    continue

                # Descent / fan-out. Any unexpected failure here (dtype
                # mismatch on the candidate filter, empty `.iloc[0]`, a
                # malformed candidate table, etc.) must NOT abort the
                # whole batch after the paid bulk call already ran.
                # Isolate it per branch: log and degrade to no-match,
                # matching the legacy `_classify_one` null-row behavior.
                # Child branches are staged locally and only committed
                # once the whole branch succeeds, so a mid-fan-out
                # exception cannot leave partial descendants behind.
                try:
                    matches = _clean_matches(
                        parsed.matches, candidates, key_col, lvl["name"],
                        confidence_threshold,
                    )
                    if not matches:
                        if branch["leaf"] is not empty_leaf:
                            leaves.append(branch)
                        elif not parsed.matches:
                            # Refusal at the entity's first level: no leaf
                            # exists to carry the model's explanation, so
                            # stash it for the fallback row. Gated on the
                            # RAW matches being empty — when _clean_matches
                            # filtered real matches away (bad indices,
                            # confidence threshold) the model did not
                            # refuse and its no_match_reason is not the
                            # story of this row.
                            refusal = (getattr(parsed, "no_match_reason", "")
                                       or "").strip()
                            if refusal:
                                no_match_reason_by_entity[
                                    branch["entity_id"]] = refusal
                        continue

                    branch_leaves: list[dict[str, Any]] = []
                    branch_active: list[dict[str, Any]] = []
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
                            "entity_id": branch["entity_id"],
                            "entity_name": branch["entity_name"],
                            "entity_description": branch["entity_description"],
                            "names": new_names,
                            "parent_path": new_parent_path,
                            "leaf": new_leaf,
                            "path_meta": new_path_meta,
                            "start_idx": branch["start_idx"],
                        }
                        if is_last_level or i >= descent_fanout_cap:
                            branch_leaves.append(new_branch)
                        else:
                            branch_active.append(new_branch)
                    leaves.extend(branch_leaves)
                    next_active.extend(branch_active)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(f"  [warn] descent failed at {lvl['name']} "
                          f"for {branch['entity_id']}: {exc}")
                    if branch["leaf"] is not empty_leaf:
                        leaves.append(branch)
                    continue

        active = next_active
        if not active:
            break

    leaves.extend(active)

    # Final commit so a caller using a short-lived session does not
    # lose the trailing bulk-upsert chunk.
    session.commit()

    # Assemble flat per-row output.
    def _record(b: dict[str, Any]) -> dict[str, Any]:
        rec = {c: b["names"].get(c) for c in out_cols}
        rec.update(b["leaf"])
        if emit_per_level:
            for oc in out_cols:
                pm = b["path_meta"].get(oc, {})
                rec[f"{oc} evidence"] = pm.get("evidence")
                rec[f"{oc} reason"] = pm.get("reason")
                rec[f"{oc} confidence"] = pm.get("confidence")
        return rec

    all_records: list[dict[str, Any]] = []
    seen_entity_ids: set[Any] = set()
    for b in leaves:
        base = base_by_entity_id[b["entity_id"]]
        all_records.append({**base, **_record(b)})
        seen_entity_ids.add(b["entity_id"])

    # Entities the walk never produced a leaf for still get a null
    # taxonomy row so the caller can join on id. ``reason`` carries the
    # model's own refusal rationale when it returned an empty match list
    # at the entity's first level — the one piece of walk output a
    # NoMatch row has.
    for entity_id, base in base_by_entity_id.items():
        if entity_id in seen_entity_ids:
            continue
        empty = {c: None for c in out_cols}
        empty.update(empty_leaf)
        empty["reason"] = no_match_reason_by_entity.get(entity_id)
        if emit_per_level:
            for lvl in levels:
                oc = lvl["output_col"]
                empty[f"{oc} evidence"] = None
                empty[f"{oc} reason"] = None
                empty[f"{oc} confidence"] = None
        all_records.append({**base, **empty})

    logger.info(f"\nTotal elapsed: {time.time() - t0:.1f}s")

    front = [id_col, name_col, text_col]
    extras = [c for c in entities.columns if c not in front]
    classification_cols = out_cols + leaf_keys
    if emit_per_level:
        for lvl in levels:
            oc = lvl["output_col"]
            classification_cols += [f"{oc} evidence", f"{oc} reason",
                                    f"{oc} confidence"]
    out_cols_final = front + extras + classification_cols
    return pd.DataFrame(all_records, columns=out_cols_final)


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
# Post-classification mapping columns + funding distribution
# ---------------------------------------------------------------------------
# Generic successors of the embedding pipeline's post-processing
# (``taxonomy_mapping.add_mapping_to_orgs`` + the ``distr_df`` half of
# ``add_taxonomy_mapping``), operating on ``classify_entities``' per-row
# output instead of embedding matches. Everything here is expressible in
# terms of (per_row_mapping_df, levels, id_col, name_col, mapping_name) —
# no domain knowledge — so project drivers stay thin.
#
# ``per_row_mapping_df`` throughout is the DataFrame returned by
# ``classify_entities``: one row per (entity, matched leaf path), plus one
# all-null row per entity the walk left unmatched.

def _clean_str(v):
    """Trimmed level value, or ``None`` when blank / non-string.

    Single source of truth for "is this level cell a real match?" — shared by
    ``_path_depth``, the primary-path columns, and the funding frame so all
    three agree on both emptiness and whitespace normalization.
    """
    return v.strip() if isinstance(v, str) and v.strip() else None


def _path_depth(row, output_cols: list[str]) -> int:
    """0-based index of the deepest non-empty level value in a per-row record.

    Returns -1 when the row has no match at any level (the null taxonomy
    row ``classify_entities`` emits for unmatched entities).
    """
    depth = -1
    for i, col in enumerate(output_cols):
        if _clean_str(row.get(col)) is not None:
            depth = i
    return depth


def select_primary_paths(
    per_row_mapping_df: pd.DataFrame,
    levels: list[dict],
    id_col: str,
    strategy="deepest",
) -> pd.DataFrame:
    """Pick one "primary" match row per id from the per-row mapping frame.

    ``strategy="deepest"`` keeps the row whose deepest non-empty level is
    deepest (first row on ties — per-row order is walk order, so this is
    deterministic). A callable can be passed instead — it receives
    ``(per_row_mapping_df, levels, id_col)`` and must return the same shape
    — which is the hook where an LLM primary-picker (the hierarchical
    successor of ``run_primary_category_selection``) can slot in later.

    Returns one row per id with columns ``[id_col, *output_cols,
    "cat_level"]`` where ``cat_level`` is the primary path's 0-based depth
    (pd.NA for ids with no match; their level values are all null).
    """
    if callable(strategy):
        return strategy(per_row_mapping_df, levels, id_col)
    if strategy != "deepest":
        raise ValueError(f"Unknown primary strategy {strategy!r}; expected 'deepest' or a callable")

    levels = normalize_levels(levels)
    output_cols = [lvl["output_col"] for lvl in levels]

    present = [c for c in output_cols if c in per_row_mapping_df.columns]
    # reset_index so idxmax labels + .loc select exactly one row per id even
    # when a precomputed per_row_mapping_df (the escape hatch) carries a
    # non-unique index (e.g. a pd.concat'd recovery frame).
    work = per_row_mapping_df[[id_col] + present].reset_index(drop=True)
    for c in output_cols:
        if c not in work.columns:
            work[c] = None
    work["_depth"] = work.apply(lambda r: _path_depth(r, output_cols), axis=1)

    # idxmax keeps the first occurrence of the max depth per id.
    idx = work.groupby(id_col, sort=False)["_depth"].idxmax()
    primary = work.loc[idx, [id_col] + output_cols + ["_depth"]].reset_index(drop=True)
    # Nullable Int64 (not object) so `cat_level` stays a real integer column —
    # pd.NA for no-match ids — and legacy `> max_level` / `!= 0` comparisons
    # don't hit object-dtype surprises.
    primary["cat_level"] = pd.array(
        [pd.NA if d < 0 else int(d) for d in primary["_depth"]], dtype="Int64"
    )
    return primary.drop(columns=["_depth"])


def attach_mapping_columns(
    df: pd.DataFrame,
    per_row_mapping_df: pd.DataFrame,
    levels: list[dict],
    id_col: str,
    mapping_name: str | None = None,
    primary="deepest",
    pad_to_n_levels: int | None = None,
) -> pd.DataFrame:
    """Attach the legacy mapping-column schema to ``df`` (in place).

    Hierarchical successor of ``taxonomy_mapping.add_mapping_to_orgs`` —
    same suffix convention (``mapping_name=None`` -> unsuffixed ``level0``
    / ``all_level0`` / ``mapped_category``; ``"ed_category"`` ->
    ``level0_ed_category`` / ... / ``ed_category``):

      - ``all_level{i}<suffix>`` — native Python list of ordered-unique
        non-empty values per id (``[]`` when no match). Downstream code
        commonly checks ``isinstance(x, list)``, so these are real lists,
        not repr strings.
      - ``level{i}<suffix>`` — the primary path's value at each level
        (see ``select_primary_paths``); null for no-match ids.
      - ``<mapping_name>`` (or ``mapped_category``) — the deepest
        non-empty value on the primary path; null for no-match ids.
      - ``cat_level<suffix>`` — the primary path's 0-based depth.
      - ``pad_to_n_levels`` — when consumers expect a deeper legacy schema
        than the taxonomy has, emit blank singular (null) and list (``[]``)
        columns for the missing levels.
    """
    levels = normalize_levels(levels)
    output_cols = [lvl["output_col"] for lvl in levels]
    suffix = f"_{mapping_name}" if mapping_name else ""
    category_col = mapping_name if mapping_name else "mapped_category"

    # The mapping columns are joined onto df by id *value* (ids.map below), so a
    # dtype skew (e.g. str "123" vs int64 123) silently yields all-empty
    # columns. Warn rather than fail so a genuine no-match run still passes.
    if df[id_col].dtype != per_row_mapping_df[id_col].dtype:
        logger.warning(
            "attach_mapping_columns: %r dtype differs between df (%s) and "
            "per_row_mapping_df (%s); the value join will silently produce "
            "empty mapping columns on mismatch.",
            id_col, df[id_col].dtype, per_row_mapping_df[id_col].dtype,
        )

    grouped = per_row_mapping_df.groupby(id_col, sort=False)
    all_maps: dict[int, dict] = {}
    for i, col in enumerate(output_cols):
        if col in per_row_mapping_df.columns:
            all_maps[i] = grouped[col].apply(lambda s: _dedup_preserve(s.tolist())).to_dict()
        else:
            all_maps[i] = {}

    primary_df = select_primary_paths(per_row_mapping_df, levels, id_col, strategy=primary)
    primary_df = primary_df.set_index(id_col)

    def _value_at_depth(r):
        d = r["cat_level"]
        if pd.isna(d):
            return None
        return _clean_str(r[output_cols[int(d)]])

    primary_category = primary_df.apply(_value_at_depth, axis=1)

    ids = df[id_col]
    for i, col in enumerate(output_cols):
        # `.get(x, ())` + list() yields a fresh list per row — no aliasing.
        df[f"all_level{i}{suffix}"] = ids.map(lambda x, i=i: list(all_maps[i].get(x, ())))
        # _clean_str so level{i} matches the stripped all_level{i} / funding
        # frame values (avoids "Solar " vs "Solar" join misses downstream).
        df[f"level{i}{suffix}"] = ids.map(primary_df[col]).map(_clean_str)
    df[category_col] = ids.map(primary_category)
    df[f"cat_level{suffix}"] = ids.map(primary_df["cat_level"])

    if pad_to_n_levels is not None:
        for i in range(len(output_cols), pad_to_n_levels):
            df[f"level{i}{suffix}"] = None
            df[f"all_level{i}{suffix}"] = [[] for _ in range(len(df))]
    return df


def distribute_funding_from_matches(
    per_row_mapping_df: pd.DataFrame,
    levels: list[dict],
    id_col: str,
    name_col: str,
    max_level: int = 2,
) -> pd.DataFrame:
    """Build the per-(org, path) ``FundingFrac`` frame from the mapping frame.

    Exact-contract adapter onto ``redistribute_funding_fracs`` — the same
    single call that produced ``distr_df`` inside the embedding pipeline's
    ``add_taxonomy_mapping``, so equal-split (1/n), ``No_Level_n`` backfill
    for shallow matches, and per-org normalization to 1.0 are guaranteed
    identical. ``FundingFrac`` never depended on embedding scores — only on
    the set of matched paths — which is what makes this a pure reuse.

    Rows with no level-0 match are dropped (consumers emit their own
    "No Match" rows). Level columns are renamed to the generic
    ``level{i}`` names ``redistribute_funding_fracs`` expects, so this
    works for any taxonomy's ``output_col`` naming.

    Returns columns ``[id_col, cat_level, level0..level{max}, name_col,
    FundingFrac]`` with ``FundingFrac`` summing to 1.0 per id.
    """
    levels = normalize_levels(levels)
    output_cols = [lvl["output_col"] for lvl in levels]

    # redistribute_funding_fracs returns None (not a frame) when the taxonomy
    # is shallower than max_level; clamp to the deepest level the taxonomy
    # actually has so a 1-/2-level taxonomy still gets a funding frame instead
    # of a silent None masquerading as "funding disabled".
    max_level = min(max_level, len(output_cols) - 1)

    # reset_index so the boolean masks / positional column assembly below align
    # by position even when a precomputed per_row_mapping_df carries a
    # non-unique index.
    per_row_mapping_df = per_row_mapping_df.reset_index(drop=True)
    depths = per_row_mapping_df.apply(lambda r: _path_depth(r, output_cols), axis=1)
    matched = per_row_mapping_df[depths >= 0]
    paths_df = pd.DataFrame({
        id_col: matched[id_col],
        name_col: matched[name_col],
        "cat_level": depths[depths >= 0].astype(int),
    })
    for i, col in enumerate(output_cols):
        paths_df[f"level{i}"] = matched[col].map(_clean_str)

    # Duplicate ids in the input frame are walked once per row (emitting
    # identical match rows), and concatenated precomputed frames can repeat
    # rows outright. redistribute_funding_fracs' shallow-duplicate check
    # zeroes every copy of a duplicated shallow path (each copy "matches
    # more than itself"), which would erase the org's funding.
    paths_df = paths_df.drop_duplicates()

    if paths_df.empty:
        logger.warning("distribute_funding_from_matches: no matched paths")
        paths_df["FundingFrac"] = pd.Series(dtype=float)
        return paths_df

    # When name_col *is* the id column, don't duplicate it into keepcols —
    # redistribute_funding_fracs does df.set_index(id_attr)[keepcols], which
    # would KeyError on the now-consumed id column.
    keepcols = [] if name_col == id_col else [name_col]
    return redistribute_funding_fracs(
        paths_df,
        id_attr=id_col,
        keepcols=keepcols,
        max_level=max_level,
    )


def add_hierarchical_taxonomy_mapping(
    *,
    df: pd.DataFrame,
    levels: list[dict],
    id_col: str,
    name_col: str,
    mapping_name: str | None = None,
    primary="deepest",
    distribute_funding: bool = True,
    max_distr_funding_level: int = 2,
    funding_name_col: str | None = None,
    pad_to_n_levels: int | None = None,
    # --- classification inputs -------------------------------------------
    # Either pass a precomputed ``per_row_mapping_df`` (skip classification
    # entirely — the escape hatch for drivers with a custom classify wrapper,
    # e.g. drawdown's scope-recovery ``classify_entities_extended``), or
    # supply the inputs below and this orchestrates the classify call.
    # ``classify_fn`` is called with the standard ``classify_entities``
    # kwargs, so an injected wrapper must accept that signature (or bake its
    # extras in via ``functools.partial``).
    per_row_mapping_df: pd.DataFrame | None = None,
    classify_fn=classify_entities,
    session=None,
    tables: dict[int, pd.DataFrame] | None = None,
    system_prompt: str | None = None,
    text_col: str | None = None,
    match_schema: type[BaseModel] | None = None,
    model: str = "gpt-4.1",
    descent_fanout_cap: int = 3,
    max_workers: int = 8,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    temperature: float | None = 0,
    filter_by_model: bool = False,
    llm_api_kwargs: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None, pd.DataFrame]:
    """Classify entities and attach the full legacy mapping outputs.

    Orchestrates the common cross-project composition — classify ->
    attach mapping columns -> select primary -> distribute funding —
    mirroring the embedding pipeline's ``add_taxonomy_mapping`` return
    contract, with the hierarchical inputs (levels / prompt /
    match_schema) in place of embeddings.

    Classification is injectable so callers with a custom classify stage
    aren't excluded from the post-processing: either pass a precomputed
    ``per_row_mapping_df`` (this skips classification), or pass a
    ``classify_fn`` (defaults to ``classify_entities``) plus the standard
    classify inputs (``session``, ``tables``, ``system_prompt``,
    ``text_col``, ...). Drivers like drawdown that add a scope-recovery
    re-walk pass their recovered frame in as ``per_row_mapping_df`` and keep
    that stage.

    ``funding_name_col`` lets the funding frame carry a different name
    column than the one shown to the model in prompts (defaults to
    ``name_col``). Note the prompt name feeds the cache's ``text_id``, so
    keep ``name_col`` stable across runs to preserve cache hits.

    ``filter_by_model`` is forwarded to ``classify_entities`` — set it True
    whenever you run more than one model identity (name OR hyperparameters)
    against the same taxonomy + prompt, otherwise an identity switch
    silently reuses the prior identity's cached matches.

    ``llm_api_kwargs`` (e.g. ``{"reasoning": {"effort": "low"}}``) is
    forwarded to ``classify_fn`` and from there to the OpenAI API and the
    cache key — see ``classify_entities``. An explicit dict (not
    ``**kwargs``) so a typo'd function kwarg fails loudly at this call
    instead of riding into the API workers. Only forwarded when supplied,
    so an injected ``classify_fn`` written before this parameter existed
    keeps working — it must accept the ``llm_api_kwargs`` keyword only if
    you pass one.

    Performs no file I/O — callers decide where the frames land, so data
    lineage stays with the project.

    Returns ``(df_with_columns, distr_df | None, per_row_mapping_df)``; the
    mapping frame is returned for lineage / audit.
    """
    if per_row_mapping_df is None:
        missing = [
            n for n, v in (
                ("session", session), ("tables", tables),
                ("system_prompt", system_prompt), ("text_col", text_col),
            ) if v is None
        ]
        if missing:
            raise ValueError(
                "add_hierarchical_taxonomy_mapping needs "
                f"{missing} to classify — supply them, or pass a precomputed "
                "per_row_mapping_df."
            )
        # Forward llm_api_kwargs only when supplied: an injected classify_fn
        # written before the parameter existed keeps working until a caller
        # actually uses the feature.
        classify_fn_kwargs: dict[str, Any] = {}
        if llm_api_kwargs is not None:
            classify_fn_kwargs["llm_api_kwargs"] = llm_api_kwargs
        per_row_mapping_df = classify_fn(
            session=session,
            tables=tables,
            levels=levels,
            system_prompt=system_prompt,
            entities=df,
            id_col=id_col,
            name_col=name_col,
            text_col=text_col,
            model=model,
            descent_fanout_cap=descent_fanout_cap,
            max_workers=max_workers,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            match_schema=match_schema,
            temperature=temperature,
            filter_by_model=filter_by_model,
            **classify_fn_kwargs,
        )

    df = attach_mapping_columns(
        df, per_row_mapping_df, levels, id_col,
        mapping_name=mapping_name,
        primary=primary,
        pad_to_n_levels=pad_to_n_levels,
    )

    distr = None
    if distribute_funding:
        distr = distribute_funding_from_matches(
            per_row_mapping_df, levels, id_col,
            name_col=funding_name_col or name_col,
            max_level=max_distr_funding_level,
        )
    return df, distr, per_row_mapping_df


# ---------------------------------------------------------------------------
# Recovery of unmatched entities (second-stage scope check)
# ---------------------------------------------------------------------------

def build_default_scope_prompt(
    tables: dict[int, pd.DataFrame],
    levels: list[dict],
) -> str:
    """Generic default scope prompt for ``recover_unmatched``.

    Renders the top-level node names + definitions from the taxonomy and a
    generic in-scope/category/reason instruction. This is the runnable
    baseline — like the engine's default rule bodies, it works for any
    taxonomy with no authoring, but a caller-supplied ``scope_prompt`` with
    domain-specific in/out guidance and routing will classify more
    accurately.

    The recovered category is returned in the ``category`` field of the
    ``ScopeDecision`` structured-output schema; the prompt text below
    matches that field name.
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


def recover_unmatched(
    df: pd.DataFrame,
    *,
    session,
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
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    temperature: float | None = 0,
    filter_by_model: bool = False,
    llm_api_kwargs: dict[str, Any] | None = None,
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

    Backed by ``ScopeRecoveryCache``: the call is keyed by the scope
    prompt + the entity's id + a hash of the entity name+description, so
    re-runs hit the cache for unchanged inputs.

    Adds three columns, filled only for unmatched entities (matched
    entities and their rows keep ``None``):
        - ``recovered_in_scope``      bool | None
        - ``recovered_<top_level_col>`` str | None  (validated category)
        - ``recovered_reason``        str | None

    ``top_level_col``, ``category_choices``, and ``scope_prompt`` each
    default from ``levels`` + ``tables`` when omitted (top-level output
    column; top-level node names; ``build_default_scope_prompt``), so a
    generic caller can pass just ``levels`` + ``tables``. Supply any of
    them explicitly to override — a domain-specific ``scope_prompt`` in
    particular classifies more accurately than the default. The scope
    prompt must instruct the model to populate the ``category`` field of
    the structured-output schema with the exact name of an in-scope
    top-level node (or null); ``ScopeDecision`` is the source of truth.

    ``filter_by_model`` (default False, matching the engine default)
    scopes ``ScopeRecoveryCache`` reads to the model identity (name +
    output-affecting kwargs; see "Kwargs-aware cache keys" in the
    ``PromptResponseCacheSQL`` docstring). Pass True when A/B-comparing
    recovery models or hyperparameters against the same scope prompt;
    ``map_to_oneearth`` forwards its own ``filter_by_model`` here.

    ``llm_api_kwargs`` (e.g. ``{"reasoning": {"effort": "low"}}``) is
    forwarded to the OpenAI API and the cache key — see
    ``classify_entities`` for the semantics (temperature precedence and
    reasoning-model stripping included).
    """
    if top_level_col is None or category_choices is None or scope_prompt is None:
        if levels is None or tables is None:
            raise ValueError(
                "recover_unmatched: supply levels + tables to default "
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

    results: dict[Any, tuple] = {}
    if unmatched_ids:
        logger.info(f"Recovering {len(unmatched_ids)} unmatched entities with {model}")
        rep = (df[df[id_col].isin(unmatched_ids)]
               .drop_duplicates(id_col).set_index(id_col))

        cache = ScopeRecoveryCache(
            session=session,
            scope_prompt=scope_prompt,
            model=model,
            filter_by_model=filter_by_model,
        )

        requests: list[tuple[str, str]] = []
        uid_by_str: dict[str, Any] = {}
        for uid in unmatched_ids:
            name = str(rep.loc[uid, name_col])
            text = str(rep.loc[uid, text_col])
            user_text = f"Organization: {name}\n\nDescription:\n{text}"
            requests.append((str(uid), user_text))
            uid_by_str[str(uid)] = uid

        api_kwargs = _build_llm_api_kwargs(model, temperature, llm_api_kwargs)

        responses = cache.bulk_get_cache_or_run(
            given_ids_texts=requests,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            max_workers=max_workers,
            **api_kwargs,
        )

        for str_uid, user_text in requests:
            uid = uid_by_str[str_uid]
            resp = responses.get(str_uid)
            if resp is None:
                results[uid] = (None, None, "recovery error: no response")
                continue
            try:
                decision = ScopeDecision.model_validate_json(resp["response_text"])
            except Exception as exc:  # noqa: BLE001
                results[uid] = (None, None, f"recovery parse error: {exc}")
                continue
            cat = (cats.get(decision.category.strip().lower())
                   if decision.category else None)
            results[uid] = (
                decision.in_scope,
                cat if decision.in_scope else None,
                decision.reason.strip(),
            )

        session.commit()

    df = df.copy()
    none3 = (None, None, None)
    df["recovered_in_scope"] = df[id_col].map(lambda u: results.get(u, none3)[0])
    df[f"recovered_{top_level_col}"] = df[id_col].map(lambda u: results.get(u, none3)[1])
    df["recovered_reason"] = df[id_col].map(lambda u: results.get(u, none3)[2])
    return df
