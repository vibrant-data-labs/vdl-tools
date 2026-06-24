"""SQL-backed prompt/response cache classes for hierarchical taxonomy mapping.

Two thin ``InstructorPRC`` subclasses + their Pydantic response schemas:

- ``TaxonomyMatchCache`` — per-level match call used by ``classify_entities``.
  Asks the model which candidate node(s) at a given taxonomy level the
  entity's description supports. Response: ``MatchesResponse``.

- ``ScopeRecoveryCache`` — second-stage scope check used by
  ``recover_unmatched`` for entities the top-down walk left empty.
  Response: ``ScopeDecision``.

Both classes inherit ``bulk_get_cache_or_run`` from ``InstructorPRC``; the
hierarchical mapping engine drives them one bulk call per level.

The default ``MatchesResponse`` is permissive on optional fields
(``mode_of_operation``, ``confidence``) so it accepts responses from any
prompt. **But** under OpenAI strict-mode structured output, those fields
become "nullable required" — the model is forced to emit *something* for
each, even when the prompt didn't ask. For prompts without modes (e.g.
ED-tracker), that can spuriously trigger the engine's indirect-fanout-stop
when the model guesses ``"indirect"``. Drivers that want tight control
should define their own minimal Pydantic class and pass it as
``response_model`` to ``TaxonomyMatchCache`` (and as ``match_schema`` to
``build_system_prompt`` / ``classify_entities``).
"""

from __future__ import annotations

from pydantic import BaseModel

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL


class Match(BaseModel):
    """One candidate match at a single taxonomy level.

    ``index`` is 1-based and refers to the candidate's position in the
    numbered list rendered in the user prompt — the engine validates it
    against the candidate count in Python (out-of-range / index=0 / duplicate
    indices are dropped with a warning, matching the pre-cache behavior).

    ``mode_of_operation`` and ``confidence`` stay loosely-typed strings /
    floats so the schema accepts responses from prompts built without modes
    or without ``include_confidence=True``. The engine normalizes unknown
    modes to ``""`` and clamps confidence into [0, 1].
    """

    index: int
    mode_of_operation: str | None = None
    evidence: str = ""
    reason: str = ""
    confidence: float | None = None


class MatchesResponse(BaseModel):
    """Top-level response shape returned by the per-level match call."""

    matches: list[Match] = []


class ScopeDecision(BaseModel):
    """Second-stage scope-recovery response.

    ``category`` is the universal field name for the recovered top-level
    node (e.g. a Pillar). Older free-form prompts that emitted ``pillar``
    must be rewritten to emit ``category`` — the structured-output schema
    is the source of truth.
    """

    in_scope: bool
    category: str | None = None
    reason: str = ""


class TaxonomyMatchCache(InstructorPRC):
    """Per-level taxonomy match cache.

    Construct once per (system_prompt, model); reuse across every level of
    the walk. Cache rows are keyed by (prompt_id, given_id, text_id):
    ``prompt_id`` is derived from the system prompt + the response schema,
    ``given_id`` is built by the engine from
    ``(entity_id, parent_path, level_name)``, ``text_id`` hashes the
    user-message body (entity name/description + candidate list).

    ``response_model`` defaults to ``MatchesResponse``. Drivers that want
    tighter control over what the model emits (e.g. a schema without
    ``mode_of_operation`` for projects that don't use modes) should pass
    their own Pydantic class — the same one they handed to
    ``build_system_prompt(match_schema=...)`` so prompt and structural
    enforcement stay in sync.
    """

    def __init__(
        self,
        session,
        system_prompt: str,
        model: str = DEFAULT_MODEL,
        store_results: bool = True,
        filter_by_model: bool = False,
        response_model: type[BaseModel] = MatchesResponse,
    ):
        super().__init__(
            session=session,
            prompt_str=system_prompt,
            prompt_name="hierarchical_taxonomy_match",
            response_model=response_model,
            model=model,
            filter_by_model=filter_by_model,
            store_results=store_results,
        )


class ScopeRecoveryCache(InstructorPRC):
    """Second-stage scope-recovery cache for ``recover_unmatched``.

    One call per unmatched entity; ``given_id`` is the entity's id, ``text``
    is the entity name + description. The scope prompt must instruct the
    model to populate ``category`` with the exact name of an in-scope
    top-level node (or null), matching the ``ScopeDecision`` schema.
    """

    def __init__(
        self,
        session,
        scope_prompt: str,
        model: str = DEFAULT_MODEL,
        store_results: bool = True,
        filter_by_model: bool = False,
    ):
        super().__init__(
            session=session,
            prompt_str=scope_prompt,
            prompt_name="hierarchical_taxonomy_scope_recovery",
            response_model=ScopeDecision,
            model=model,
            filter_by_model=filter_by_model,
            store_results=store_results,
        )
