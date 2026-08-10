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
each, even when the prompt didn't ask, so a modeless prompt (e.g.
ED-tracker) still gets a guessed ``mode_of_operation`` on every match.
Drivers that want tight control should define their own Pydantic class
(embedding per-field guidance via
``Field(description=...)`` and selection-level guidance on the response
class's docstring) and pass it as ``response_model`` to
``TaxonomyMatchCache`` / as ``match_schema`` to ``classify_entities``.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL


class Match(BaseModel):
    """One candidate match at a single taxonomy level.

    The engine validates ``index`` against the candidate count in Python
    (out-of-range / index=0 / duplicate indices are dropped with a warning,
    matching the pre-cache behavior).

    ``mode_of_operation`` and ``confidence`` stay loosely-typed strings /
    floats so the default schema accepts responses from prompts built
    without modes or confidence. The engine normalizes unknown modes to
    ``""`` and clamps confidence into [0, 1].
    """

    index: int = Field(
        description=(
            "The 1-based position of the matched candidate in the numbered "
            "candidate list shown in the user prompt. Never return 0 or an "
            "out-of-range index — to indicate no match, return `matches: []`."
        ),
    )
    mode_of_operation: str | None = Field(
        default=None,
        description=(
            "How the entity relates to the matched candidate. Use the "
            "values defined by the project's mode-of-operation field on "
            "its own Match subclass. Leave null when the prompt does not "
            "ask for a mode."
        ),
    )
    evidence: str = Field(
        default="",
        description=(
            "Phrase(s) from the description that support the match. Quote "
            "or closely paraphrase the supporting language. If no language "
            "in the description supports the candidate, omit the match — "
            "do not invent evidence."
        ),
    )
    reason: str = Field(
        default="",
        description=(
            "One sentence explaining how the evidence maps to the "
            "candidate's definition."
        ),
    )
    confidence: float | None = Field(
        default=None,
        description=(
            "Confidence in [0, 1] that the description supports this match. "
            "Leave null when the prompt does not ask for a confidence."
        ),
    )


# Shared Field for the refusal-rationale slot on match-set response
# classes. Kept as a module-level constant so project-specific response
# classes (e.g. the One Earth variants) attach the identical contract.
# Under strict-mode structured output the field is always emitted; the
# description confines real content to the empty-matches case.
NO_MATCH_REASON_FIELD = Field(
    default="",
    description=(
        "Only when `matches` is empty: one or two sentences explaining "
        "why none of the candidates fit — what the entity actually does "
        "and why that falls outside every candidate's definition. When "
        "`matches` is non-empty, return an empty string."
    ),
)


class MatchesResponse(BaseModel):
    """Default match-set response.

    Selection behavior: emit only the matches that the description clearly
    supports — self-filter weak candidates. Drivers whose schema enables a
    ``confidence`` field should subclass this and override the docstring to
    say *emit every plausible candidate with low confidence; let downstream
    threshold filter* — that selection rule is what tells the model to
    score weak matches instead of dropping them.

    ``no_match_reason`` captures the model's own refusal rationale when it
    returns ``matches: []``; the walk persists it on the entity's NoMatch
    row when the refusal happens at the top level (see
    ``classify_entities``).
    """

    matches: list[Match] = []
    no_match_reason: str = NO_MATCH_REASON_FIELD


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
    their own Pydantic class. The model sees the class's Field
    descriptions and class docstring via the schema JSON that
    ``InstructorPRC`` appends to the prompt, so per-field guidance and
    selection-level guidance both live on the schema itself.
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
