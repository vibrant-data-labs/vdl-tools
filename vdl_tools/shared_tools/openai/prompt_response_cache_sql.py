"""SQL-backed cache for OpenAI prompt/response pairs.

This module provides a cache that stores prompt inputs and API responses in a
database so repeated calls with the same (prompt, text) can return cached
results instead of calling the API again. Cache keys are (prompt_id, given_id,
text_id) where text_id is a hash of the input text, so changing the text
invalidates the cache for that given_id.

The main entry point is `PromptResponseCacheSQL`: construct it with a session,
prompt (or prompt_str / prompt_id), and optional model; then use
`get_cache_or_run` or `bulk_get_cache_or_run` to get completions with caching.
All OpenAI API options are passed through via ``**kwargs``—see the class
docstring and method Notes for model-specific parameters.
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor as ThreadPool
import datetime as dt
import json
import warnings

from sqlalchemy import select, func, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from more_itertools import chunked

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.openai_api_utils import get_completion, get_context_window, get_num_tokens
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.unique_ids import create_deterministic_md5

import logging
logger.setLevel(logging.DEBUG)
from typing import Any, Optional


# Why `object()` and not `True`/`False`/`None`?
#
# We need to distinguish THREE caller states, not two:
#   1. didn't pass `use_cached_result` at all   → no warning, use new flags
#   2. passed `use_cached_result=True`          → warn + map to read_from_cache=True
#   3. passed `use_cached_result=False`         → warn + map to read_from_cache=False
#
# Any bool default collapses one of those states into another:
#   - default=False would silently force-refresh every legacy call site that
#     omitted the kwarg (~151 of them); historical default was `True`.
#   - default=True can't distinguish "explicitly passed True" from "defaulted
#     True," so the warning either fires for everyone (spam) or no one
#     (legacy `False` callers get silently dropped).
#   - default=None is workable but `None` can leak from upstream code that's
#     translating other state, producing a false-positive warning.
#
# `object()` produces a brand-new instance whose identity is unique — no
# caller can accidentally produce the same value, so `is _UNSET` is a
# bulletproof "did the caller touch this kwarg?" check. Standard Python
# pattern; same idea as `dataclasses.MISSING` and `dict.pop(key, _missing)`.
_UNSET = object()


# API kwargs that change what the model produces, and therefore belong in the
# cache key. Anything NOT on this list is treated as cosmetic and ignored when
# hashing — notably `text_format`: we store `response_full` raw and parse it at
# retrieval time, so the same row can be re-parsed against a different Pydantic
# model without a cache miss. New output-affecting kwargs must be added here
# explicitly (via PR) — opt-in keeps cache invalidation predictable.
KWARG_KEYS_THAT_AFFECT_OUTPUT = frozenset({
    "reasoning",
    "tools",
    "tool_choice",
    "temperature",
    "top_p",
    "max_output_tokens",
    "seed",
    "service_tier",
})


def _normalize_request_kwargs(kwargs: dict) -> dict:
    """Filter kwargs to the output-affecting allowlist and normalize values.

    Values are round-tripped through JSON with sorted keys so structural
    equality maps to the same normalized form (e.g. dict key order or tool
    list contents don't produce spurious cache misses).
    """
    norm = {}
    for key in sorted(kwargs):
        if key not in KWARG_KEYS_THAT_AFFECT_OUTPUT:
            continue
        norm[key] = json.loads(json.dumps(kwargs[key], sort_keys=True, default=str))
    return norm


def _request_hash(norm_kwargs: dict) -> str:
    """Hash normalized kwargs into the cache-key component.

    Empty kwargs hash to '' so calls with no output-affecting kwargs keep
    matching pre-hash rows (which were backfilled with '').
    """
    if not norm_kwargs:
        return ""
    return create_deterministic_md5(json.dumps(norm_kwargs, sort_keys=True))


def _resolve_cache_flags(
    read_from_cache: bool,
    write_to_cache: bool,
    use_cached_result,
) -> tuple[bool, bool]:
    """Translate the legacy ``use_cached_result`` kwarg into ``(read, write)``.

    ``use_cached_result`` historically gated only the READ path; writes
    always happened (subject to the constructor-level ``store_results``).
    This helper preserves that behavior for callers who still pass the
    legacy kwarg: it overrides ``read_from_cache`` and leaves
    ``write_to_cache`` alone.

    Parameters
    ----------
    read_from_cache : bool
        New flag — whether to consult the cache before calling the API.
    write_to_cache : bool
        New flag — whether to persist API results back into the cache.
    use_cached_result : bool or sentinel
        Legacy flag. If ``_UNSET`` (default), ignored. Otherwise emits a
        ``DeprecationWarning`` and overrides ``read_from_cache``.

    Returns
    -------
    tuple[bool, bool]
        Resolved ``(read_from_cache, write_to_cache)``.
    """
    if use_cached_result is _UNSET:
        return read_from_cache, write_to_cache
    warnings.warn(
        "`use_cached_result` is deprecated; use `read_from_cache` and "
        "`write_to_cache` instead. The legacy flag only controlled the "
        "read side, so it now sets `read_from_cache=<value>` and leaves "
        "`write_to_cache` unchanged.",
        DeprecationWarning,
        stacklevel=3,
    )
    return bool(use_cached_result), write_to_cache


def get_prompt_by_id(prompt_id: str, session):
    """Load a single prompt from the database by ID.

    Parameters
    ----------
    prompt_id : str
        The prompt's unique identifier.
    session
        SQLAlchemy session for the query.

    Returns
    -------
    Prompt or None
        The prompt row if found, otherwise None.
    """
    prompt_by_id_stmnt = select(Prompt).where(Prompt.id == prompt_id)
    return session.scalars(prompt_by_id_stmnt).first()


def load_prompts(session):
    """Load all prompts from the database as a dict keyed by prompt id.

    Parameters
    ----------
    session
        SQLAlchemy session for the query.

    Returns
    -------
    dict[str, Prompt]
        Mapping from prompt id to Prompt instance.
    """
    return {x.id: x for x in session.query(Prompt).all()}


def register_prompt(prompt: Prompt, session):
    """Ensure a prompt exists in the database; insert if missing.

    Parameters
    ----------
    prompt : Prompt
        The prompt to register.
    session
        SQLAlchemy session for the query/insert.

    Returns
    -------
    Prompt
        The prompt row (existing or newly added).
    """
    # Add the session to the prompt object
    prompt_obj = get_prompt_by_id(prompt.id, session)
    print(prompt_obj)
    if not prompt_obj:
        session.add(prompt)
        prompt_obj = get_prompt_by_id(prompt.id, session)
    return prompt_obj


DEFAULT_MODEL = "gpt-4.1-mini"


class PromptResponseCacheSQL():
    """SQL-backed cache for OpenAI Responses API completions.

    Looks up cached responses by (prompt_id, given_id, text_id). If
    ``filter_by_model`` is True, cache entries are also keyed by model name,
    so different models get separate cache entries for the same prompt/text.

    When ``store_results`` is True (default), successful responses and errors
    are persisted to the database so future lookups can return cached data.
    When ``store_results`` is False, the API is still called and results are
    returned, but nothing is written to the DB (no cache fill, no error
    recording). Useful for one-off runs or testing without polluting the cache.

    Notes
    -----
    **OpenAI API kwargs and model-specific parameters**

    Methods that call the API (`get_cache_or_run`, `bulk_get_cache_or_run`,
    and their internal helpers) accept ``**kwargs`` and forward them to
    `openai_api_utils.get_completion`, which passes them to the OpenAI
    Responses API. **Which kwargs are valid depends on the model** you set in
    the constructor:

    - **All models** (via Responses API): ``response_format`` (or
      ``text_format`` for structured output with a Pydantic model), and other
      common options supported by the API.
    - **Reasoning models** (e.g. ``gpt-5-nano``, names starting with
      ``gpt-5``): support extra parameters such as ``reasoning``, e.g.
      ``reasoning={"effort": "high", "summary": "auto"}``. Use these to
      control reasoning effort and whether a summary is returned. Non-reasoning
      models ignore or reject these.
    - **Chat / non-reasoning models** (e.g. ``gpt-4.1-mini``): support
      parameters like ``temperature``, ``max_tokens``, ``stop``, etc. as
      allowed by the API for that model.

    Always pass kwargs that match the **model** you configured (e.g. do not
    pass ``reasoning`` when using a non-reasoning model). The cache stores
    responses per (prompt, text, model) when ``filter_by_model`` is True, so
    switching model or kwargs can yield different cached or fresh results.

    **Kwargs-aware cache keys (request_hash)**

    Kwargs that change what the model produces are part of the cache key: a
    ``request_hash`` is computed per call from the allowlist
    ``KWARG_KEYS_THAT_AFFECT_OUTPUT`` (``reasoning``, ``tools``,
    ``tool_choice``, ``temperature``, ``top_p``, ``max_output_tokens``,
    ``seed``, ``service_tier``) and stored on the row, so e.g.
    ``reasoning={"effort": "low"}`` vs ``{"effort": "high"}`` occupy separate
    rows instead of colliding. Kwargs NOT on the allowlist are treated as
    cosmetic and reuse the cache — notably ``text_format``: ``response_full``
    is stored raw and parsed at retrieval, so changing the Pydantic response
    model does not bust the cache. If you pass a new kwarg that changes model
    output, it must be added to the allowlist explicitly (via PR); until then
    it will silently share rows. Calls with no allowlisted kwargs hash to
    ``''``, which also matches rows written before the hash existed. The
    normalized kwargs are stored alongside the hash in ``request_kwargs``
    for auditability.
    """

    def __init__(
        self,
        session,
        prompt: Prompt = None,
        prompt_str: str = None,
        prompt_id: str = None,
        prompt_name: str = "",
        prompt_description: str = "",
        filter_by_model: bool = False,
        model=DEFAULT_MODEL,
        store_results = True,
    ):
        """Initialize the cache for a given prompt and model.

        Parameters
        ----------
        session
            SQLAlchemy session for database access.
        prompt : Prompt, optional
            Prompt instance to use. If provided with prompt_str or prompt_id,
            this takes precedence.
        prompt_str : str, optional
            Raw prompt string; used to create or look up a prompt if prompt
            and prompt_id are not provided.
        prompt_id : str, optional
            Id of an existing prompt to load from the database.
        prompt_name : str, optional
            Name for the prompt when creating from prompt_str. Default is "".
        prompt_description : str, optional
            Description for the prompt when creating from prompt_str. Default is "".
        filter_by_model : bool, optional
            If True, cache lookups and stores are scoped by model name so
            different models have separate cache entries. Default is False.
        model : str, optional
            OpenAI model name (e.g. gpt-4.1-mini, gpt-5-nano). Used for API
            calls and, when filter_by_model is True, for cache keying. Default
            is DEFAULT_MODEL.
        store_results : bool, optional
            If True, persist successful responses and errors to the database
            so they can be returned from cache on future requests. If False,
            results are returned but not stored (no cache fill). Default is
            True.

        Raises
        ------
        Exception
            If none of prompt, prompt_str, or prompt_id are provided.
        """
        if not any([prompt is not None, prompt_str, prompt_id]):
            raise Exception("Need to give at least one of prompt, prompt_str, prompt_id")

        self.session = session
        self.prompt = self._set_prompt_obj(
            prompt=prompt,
            prompt_str=prompt_str,
            prompt_id=prompt_id,
            prompt_name=prompt_name,
            prompt_description=prompt_description,
        )
        self.filter_by_model = filter_by_model
        self.model = model
        self.store_results = store_results

        context_window, max_output_tokens = get_context_window(self.model)
        if context_window:
            prompt_tokens = get_num_tokens(self.prompt.prompt_str, self.model)
            self.max_text_tokens = context_window - prompt_tokens - max_output_tokens
        else:
            self.max_text_tokens = None

    def _set_prompt_obj(
        self, prompt, prompt_str, prompt_id, prompt_name, prompt_description):
        """Resolve and set the prompt for this cache instance.

        Resolution order when multiple are given: prompt object > prompt_str
        (creates/uses ad-hoc prompt) > prompt_id (load from DB). Registers
        the prompt in the database if it is not already present.

        Parameters
        ----------
        prompt : Prompt or dict or None
            Prompt instance or dict to build one from.
        prompt_str : str or None
            Raw prompt text; used if prompt is not an existing Prompt.
        prompt_id : str or None
            Id of a registered prompt to load.
        prompt_name : str
            Name when creating a prompt from prompt_str.
        prompt_description : str
            Description when creating a prompt from prompt_str.

        Returns
        -------
        Prompt
            The prompt object for this cache.

        Raises
        ------
        Exception
            If prompt_id is given but no prompt exists in the registry.
        """

        if prompt and isinstance(prompt, dict):
            prompt = Prompt(**prompt)

        all_prompts = load_prompts(self.session)
        if prompt and isinstance(prompt, Prompt):
            logger.info("Prompt object passed, using that")
            logger.info(str(prompt))
            temp_prompt = prompt

            if prompt_str and prompt_str != prompt.prompt_str:
                logger.warning("Given prompt_str and prompt.prompt_str are different")

            if prompt_id and prompt_id != prompt.id:
                logger.warning("Given prompt_id and prompt.prompt_id are different")
        elif prompt_id:
            logger.info("Retrieving by prompt_id")
            temp_prompt = all_prompts.get(prompt_id)
            if not temp_prompt:
                raise Exception(f"No prompt for {prompt_id} found in registry")

        elif prompt_str:
            logger.warning("prompt_str passed, using that to create a prompt")
            temp_prompt = Prompt(
                name=prompt_name,
                prompt_str=prompt_str,
                description=prompt_description,
            )

        else:
            logger.info("Retrieving by prompt_id")
            temp_prompt = all_prompts.get(prompt_id)
            if not temp_prompt:
                raise Exception(f"No prompt for {prompt_id} found in registry")

        if temp_prompt.id not in all_prompts:
            logger.info("Registering prompt %s", temp_prompt.id)
            register_prompt(temp_prompt, self.session)
        else:
            temp_prompt = all_prompts[temp_prompt.id]
        return temp_prompt

    def get_prompt_response_obj(self, given_id: str, text: str, request_hash: str = ""):
        """Fetch a single cached response for (given_id, text).

        Parameters
        ----------
        given_id : str
            User-defined identifier for the request (e.g. URL, row id).
        text : str
            Input text that was sent to the model (used to compute text_id).
        request_hash : str, optional
            Hash of the output-affecting API kwargs (see
            ``KWARG_KEYS_THAT_AFFECT_OUTPUT``). Filtered unconditionally —
            unlike ``filter_by_model``, kwargs differences are exactly what
            the hash keys against. Default '' matches rows written with no
            output-affecting kwargs (including pre-hash legacy rows).

        Returns
        -------
        PromptResponse or None
            Cached row if found and not error-stored, else None.
        """
        text_id = PromptResponse.create_text_id(text)

        filters = [
                PromptResponse.prompt_id == self.prompt.id,
                PromptResponse.text_id == text_id,
                PromptResponse.given_id == given_id,
                PromptResponse.request_hash == request_hash,
        ]
        if self.filter_by_model:
            filters.append(PromptResponse.model_name == self.model)

        prompt_response_obj = (
            self.session
            .query(PromptResponse)
            .filter(*filters)
        )
        return prompt_response_obj.first()

    def get_prompt_response_obj_bulk(
        self,
        given_ids_texts: list[tuple[str, str]],
        request_hash: str = "",
    ):
        """Fetch cached responses for many (given_id, text) pairs.

        Excludes rows that have num_errors set (treated as failed runs).

        Parameters
        ----------
        given_ids_texts : list of tuple[str, str]
            Pairs of (given_id, text) to look up.
        request_hash : str, optional
            Hash of the output-affecting API kwargs; filtered
            unconditionally. See ``get_prompt_response_obj``.

        Returns
        -------
        tuple[list[PromptResponse], dict]
            (list of found cache rows, dict mapping (given_id, text_id) to
            num_errors for missing or error rows).
        """
        logger.info(
            "Starting to pull %s previous results for prompt: %s",
            len(given_ids_texts),
            self.prompt.name,
        )
        given_ids, _ = zip(*given_ids_texts)
        text_ids = [PromptResponse.create_text_id(x[1]) for x in given_ids_texts]
        given_ids_text_ids = list(zip(given_ids, text_ids))

        filters = [
            PromptResponse.prompt_id == self.prompt.id,
            PromptResponse.text_id.in_(text_ids),
            PromptResponse.request_hash == request_hash,
        ]

        if self.filter_by_model:
            filters.append(PromptResponse.model_name == self.model)

        found_rows = (
            self.session
            .query(PromptResponse)
            .filter(*filters)
        ).all()

        logger.info("Found %s total rows", len(found_rows))

        found_rows_to_errors = {(x.given_id, x.text_id): x.num_errors for x in found_rows}
        found_rows = [x for x in found_rows if not found_rows_to_errors.get((x.given_id, x.text_id))]

        found_rows_keys = found_rows_to_errors.keys()
        unfound_ids_or_errors = {
            x: found_rows_to_errors.get(x, 0) for x in given_ids_text_ids
            if x not in found_rows_keys or found_rows_to_errors.get(x)
        }
        logger.info("%s previous found, %s unfound", len(found_rows), len(unfound_ids_or_errors))
        return found_rows, unfound_ids_or_errors

    def _build_success_row(
        self,
        given_id: str,
        text,
        response,
        request_hash: str = "",
        request_kwargs: dict | None = None,
    ) -> dict[str, Any]:
        """Build a row dict for a successful API response.

        Subclasses override this to customize serialization (e.g. JSON-
        encoded dict text, or a different response payload shape) without
        touching the bulk-upsert plumbing.

        Returns
        -------
        dict
            Kwargs suitable for `PromptResponse(**row)` or for use as a
            VALUES row in `INSERT ... ON CONFLICT DO UPDATE`. Includes
            `text_id` so the INSERT path can populate the indexed column.
        """
        text_id = PromptResponse.create_text_id(text)
        return {
            "prompt_id": self.prompt.id,
            "given_id": str(given_id),
            "model_name": self.model,
            "request_hash": request_hash,
            "request_kwargs": request_kwargs or None,
            "text_id": text_id,
            "input_text": text,
            "response_full": response.model_dump_json(),
            "response_text": response.output_text,
            "num_errors": None,
        }

    def _build_error_row(
        self,
        given_id: str,
        text,
        response_full,
        request_hash: str = "",
        request_kwargs: dict | None = None,
    ) -> dict[str, Any]:
        """Build a row dict for a failed API call (single-error row).

        The `num_errors` field is set to 1 for the INSERT case; the
        bulk-upsert helper handles the COALESCE-and-increment for the
        UPDATE case via the `set_` clause.
        """
        text_id = PromptResponse.create_text_id(text)
        return {
            "prompt_id": self.prompt.id,
            "given_id": str(given_id),
            "model_name": self.model,
            "request_hash": request_hash,
            "request_kwargs": request_kwargs or None,
            "text_id": text_id,
            "input_text": text,
            "response_full": response_full,
            "num_errors": 1,
        }

    def _upsert_success_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert successful response rows.

        Uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` on the
        composite PK (prompt_id, given_id, model_name). On conflict,
        overwrites response columns, clears `num_errors`, and bumps
        `date_updated` (Core's `ON CONFLICT DO UPDATE` doesn't fire the
        column's ORM `onupdate` hook, so we set it explicitly).

        Parameters
        ----------
        rows : list[dict]
            Row dicts as built by `_build_success_row`. May be empty
            (no-op).

        Notes
        -----
        Skipped when `self.store_results` is False.
        """
        if not rows or not self.store_results:
            return
        # Dedupe by PK so a single batch never violates ON CONFLICT semantics.
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            key = (row["prompt_id"], row["given_id"], row["model_name"], row["request_hash"])
            deduped[key] = row
        rows = list(deduped.values())

        # Stamp timestamps explicitly so a fresh insert via Core gets both
        # date_added and date_updated populated even though the model's
        # Python-side `default=` / `onupdate=` hooks don't fire reliably
        # through `pg_insert().values([...])`.
        # `dt.datetime.utcnow()` is deprecated in 3.12+. Columns are naive
        # `DateTime` (see `BaseMixin`), so strip tz to keep round-trip identical.
        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(PromptResponse).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["prompt_id", "given_id", "model_name", "request_hash"],
            set_={
                "text_id": stmt.excluded.text_id,
                "input_text": stmt.excluded.input_text,
                "response_full": stmt.excluded.response_full,
                "response_text": stmt.excluded.response_text,
                "request_kwargs": stmt.excluded.request_kwargs,
                "num_errors": None,
                # date_added intentionally NOT in the SET clause — preserve
                # the original "first cached at" timestamp on refresh.
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    def _upsert_error_rows(self, rows: list[dict[str, Any]]):
        """Bulk upsert error rows, incrementing `num_errors` on conflict.

        Uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` on the
        composite PK. On conflict, overwrites `response_full`, increments
        `num_errors` via `COALESCE(num_errors, 0) + 1`, and bumps
        `date_updated`.

        Parameters
        ----------
        rows : list[dict]
            Row dicts as built by `_build_error_row`. May be empty (no-op).

        Notes
        -----
        Skipped when `self.store_results` is False.
        """
        if not rows or not self.store_results:
            return
        deduped: dict[tuple, dict[str, Any]] = {}
        for row in rows:
            key = (row["prompt_id"], row["given_id"], row["model_name"], row["request_hash"])
            deduped[key] = row
        rows = list(deduped.values())

        # `dt.datetime.utcnow()` is deprecated in 3.12+. Columns are naive
        # `DateTime` (see `BaseMixin`), so strip tz to keep round-trip identical.
        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        for row in rows:
            row.setdefault("date_added", now)
            row.setdefault("date_updated", now)

        stmt = pg_insert(PromptResponse).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["prompt_id", "given_id", "model_name", "request_hash"],
            set_={
                "response_full": stmt.excluded.response_full,
                "num_errors": func.coalesce(PromptResponse.num_errors, 0) + 1,
                "date_updated": now,
            },
        )
        self.session.execute(stmt)

    def store_error(
        self,
        given_id: str,
        text,
        response_full,
        request_hash: str = "",
        request_kwargs: dict | None = None,
    ):
        """Store or update a cache row for a failed API call (increment num_errors).

        Single-row path. Used by `get_cache_or_run` (the non-bulk entry
        point). The bulk path uses `_upsert_error_rows` instead.

        Parameters
        ----------
        given_id : str
            User-defined identifier for the request.
        text
            Input text (used to compute text_id).
        response_full
            Raw error/response payload to store (e.g. dict or JSON string).
        request_hash : str, optional
            Cache-key component hashed from the output-affecting API kwargs.
        request_kwargs : dict, optional
            The normalized kwargs the hash was computed from (lineage).

        Returns
        -------
        PromptResponse
            The cache row (persisted only if store_results is True).

        Notes
        -----
        When ``store_results`` is False, the merge to the database is skipped;
        the in-memory row is still returned.
        """
        logger.info("Storing error for %s, %s", self.prompt.name, given_id)
        text_id = PromptResponse.create_text_id(text)
        previous_response = (
            self.session
            .query(PromptResponse)
            .filter(
                PromptResponse.prompt_id == self.prompt.id,
                PromptResponse.text_id == text_id,
                PromptResponse.request_hash == request_hash,
            )
            .first()
        )
        if previous_response:
            previous_response.response_full = response_full
            if previous_response.num_errors:
                previous_response.num_errors += 1
            else:
                previous_response.num_errors = 1
            if self.store_results:
                self.session.merge(previous_response)
            prompt_response_obj = previous_response
        else:
            row = self._build_error_row(
                given_id, text, response_full,
                request_hash=request_hash, request_kwargs=request_kwargs,
            )
            prompt_response_obj = PromptResponse(**row)
            if self.store_results:
                self.session.merge(prompt_response_obj)
        return prompt_response_obj

    def store_item(
        self,
        given_id: str,
        text,
        response,
        request_hash: str = "",
        request_kwargs: dict | None = None,
    ):
        """Store a successful API response in the cache.

        Single-row path. Used by `get_cache_or_run` (the non-bulk entry
        point). The bulk path uses `_upsert_success_rows` instead.

        When ``store_results`` is False (set in the constructor), the merge
        to the database is skipped; the in-memory PromptResponse is still
        returned.

        Parameters
        ----------
        given_id : str
            User-defined identifier for the request.
        text
            Input text that was sent to the model.
        response
            Full API response object (must have model_dump_json, output_text).

        Returns
        -------
        PromptResponse
            The cache row (persisted only if store_results is True).
        """
        row = self._build_success_row(
            given_id, text, response,
            request_hash=request_hash, request_kwargs=request_kwargs,
        )
        prompt_response_obj = PromptResponse(**row)

        logger.debug("Storing response for %s, %s", self.prompt.name, given_id)
        if self.store_results:
            self.session.merge(prompt_response_obj)
        return prompt_response_obj

    def get_completion_catch_error(self, prompt_str, text, **kwargs):
        """Call the API and return (result, error_flag); on exception return (error_info, True).

        Parameters
        ----------
        prompt_str : str
            System prompt / instructions.
        text : str
            User message content.
        **kwargs
            Passed to `get_completion` (and thus to the OpenAI API). Use
            model-appropriate kwargs; see class Notes.

        Returns
        -------
        tuple
            (completion or error dict, bool): second element True if an error occurred.
        """
        try:
            completion = self.get_completion(prompt_str, text, **kwargs)
            return completion, False
        except Exception as ex:
            logger.error("Error getting completion: %s", ex)
            response = {
                "message": str(ex),
            }
            return response, True

    def get_completion(self, prompt_str, text, **kwargs):
        """Call the OpenAI Responses API for this cache's model.

        Override in subclasses to customize how completions are obtained.
        All kwargs are forwarded to `openai_api_utils.get_completion`;
        use model-appropriate kwargs (e.g. reasoning for gpt-5, response_format
        for structured output). See class docstring Notes.

        Parameters
        ----------
        prompt_str : str
            System prompt / instructions.
        text : str
            User message content.
        **kwargs
            Passed to the OpenAI Responses API (e.g. text_format, reasoning).

        Returns
        -------
        Parsed completion from the API.
        """
        return get_completion(
            prompt=prompt_str,
            text=text,
            model=self.model,
            max_text_tokens=self.max_text_tokens,
            **kwargs,
        )

    def _get_cache_or_run(
        self,
        given_id: str,
        text,
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        use_cached_result=_UNSET,
        **kwargs
    ) -> str:
        """Look up cache or call API; store result and return as dict or None on error.

        Parameters
        ----------
        given_id : str
            User-defined identifier for this request.
        text
            Input text to send to the model (and to key the cache).
        read_from_cache : bool, optional
            If True, consult the cache before calling the API. If False,
            always call the API even if a cached row exists (force-refresh).
            Default is True.
        write_to_cache : bool, optional
            If True, persist API results (success or error) back into the
            cache. If False, the call is a passthrough — the cache is not
            updated. Default is True. Also gated by the constructor's
            ``store_results``.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``
            instead. When supplied, overrides ``read_from_cache``; preserves
            historical behavior where the legacy flag only controlled the
            read path (writes still happened on misses).
        **kwargs
            Passed to the OpenAI API. Use model-appropriate options; see class Notes.

        Returns
        -------
        dict or None
            Cached or new response as dict (e.g. with 'response_text', 'response_full'),
            or None if the call failed and an error was stored.
        """
        read_from_cache, write_to_cache = _resolve_cache_flags(
            read_from_cache, write_to_cache, use_cached_result,
        )

        # One hash per call — kwargs are shared by the lookup and the write.
        request_kwargs = _normalize_request_kwargs(kwargs)
        request_hash = _request_hash(request_kwargs)

        if read_from_cache:
            data = self.get_prompt_response_obj(
                given_id=given_id,
                text=text,
                request_hash=request_hash,
            )
            if data:
                logger.info("Found cached response for %s", given_id)
                return data.to_dict()

        response, error = self.get_completion_catch_error(
            prompt_str=self.prompt.prompt_str,
            text=text,
            return_all=True,
            **kwargs
        )
        if not error:
            if write_to_cache:
                data = self.store_item(
                    given_id=given_id,
                    text=text,
                    response=response,
                    request_hash=request_hash,
                    request_kwargs=request_kwargs,
                )
                return data.to_dict()
            # Read-only / passthrough: build the response shape without persisting.
            row = self._build_success_row(
                given_id, text, response,
                request_hash=request_hash, request_kwargs=request_kwargs,
            )
            return self._row_to_response_dict(row)
        else:
            logger.warning("No response text for %s", given_id)
            if write_to_cache:
                self.store_error(
                    given_id=given_id,
                    text=text,
                    response_full=response,
                    request_hash=request_hash,
                    request_kwargs=request_kwargs,
                )
            return None

    def get_cache_or_run(
        self,
        given_id: str,
        text,
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        use_cached_result=_UNSET,
        **kwargs,
    ):
        """Return cached response for (given_id, text) or run the API and cache the result.

        This is the main entry point for a single completion. Pass any
        model-specific OpenAI API options via kwargs (e.g. ``text_format`` for
        structured output, ``reasoning={"effort": "high"}`` for reasoning
        models). See the class docstring Notes for which kwargs apply to which
        model types.

        Parameters
        ----------
        given_id : str
            User-defined identifier for this request (e.g. URL, row id).
        text
            Input text to send to the model. Cache is keyed by hash of this.
        read_from_cache : bool, optional
            If True, return a cached response when available. If False,
            always hit the API (force-refresh). Default is True.
        write_to_cache : bool, optional
            If True, persist new API results to the cache. If False, the
            cache is left untouched (read-only / passthrough). Default is
            True. Also gated by the constructor's ``store_results``.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``
            instead. When supplied, overrides ``read_from_cache``.
        **kwargs
            Forwarded to the OpenAI Responses API. Valid options depend on
            the model (see class Notes): e.g. ``text_format``, ``reasoning``.

        Returns
        -------
        dict or None
            Cached or newly fetched response as a dict, or None if the API
            call failed and an error was stored. When ``store_results`` is
            False (constructor) or ``write_to_cache=False``, new results
            are not persisted to the cache.
        """
        return self._get_cache_or_run(
            given_id=given_id,
            text=text,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            use_cached_result=use_cached_result,
            **kwargs,
        )

    def _bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        n_per_commit: int = 200,
        max_workers=20,
        max_errors=1,
        use_cached_result=_UNSET,
        **kwargs
    ) -> dict[str, dict]:
        """Bulk lookup/call: for each (given_id, text), return cache or run API and store.

        Lookup is by (prompt_id, given_id, text_id) where text_id is the hash
        of the input text. If the text changes, the cache miss is re-run and
        the new result overwrites the previous one for that (prompt_id, given_id).

        Parameters
        ----------
        given_ids_texts : list[tuple[str, str]]
            List of (given_id, text) pairs. given_id is a human-readable
            identifier (e.g. URL for a summarization task).
        read_from_cache : bool, optional
            If True, consult the cache before calling the API. If False,
            always call the API for every item (force-refresh). Default is
            True.
        write_to_cache : bool, optional
            If True, persist API results (success and error) back to the
            cache. If False, the cache is left untouched (read-only /
            passthrough). Default is True. Also gated by the constructor's
            ``store_results``.
        n_per_commit : int, optional
            Number of completions to run before bulk-upserting and committing
            to the DB. Default is 200. Each chunk issues one bulk
            `INSERT ... ON CONFLICT DO UPDATE` per kind (success/error), so
            DB cost scales with chunk count, not item count.
        max_workers : int, optional
            Number of parallel API workers. Default is 20. Workers do not
            touch the DB session — concurrency is bounded only by OpenAI
            rate limits. Tune up (e.g. 50-100) for nano/mini models with
            generous quotas; down (e.g. 5-10) for tier-1 mainline models.
        max_errors : int, optional
            Maximum number of errors to allow for a (given_id, text) before
            excluding from retries. Default is 1.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``
            instead. When supplied, overrides ``read_from_cache``.
        **kwargs
            Passed to the OpenAI API for each call. Use model-appropriate
            kwargs (e.g. text_format, reasoning for gpt-5). See class docstring Notes.

        Returns
        -------
        dict[str, dict]
            Map from given_id to response dict (e.g. response_text, response_full).
            Only includes given_ids that succeeded or were found in cache.
        """
        read_from_cache, write_to_cache = _resolve_cache_flags(
            read_from_cache, write_to_cache, use_cached_result,
        )

        if not given_ids_texts:
            logger.warning("No given_ids_texts passed")
            return {}

        # One hash per bulk call — every item shares the same kwargs.
        request_kwargs = _normalize_request_kwargs(kwargs)
        request_hash = _request_hash(request_kwargs)

        # Convert the given_ids to strings in case they are integers
        given_ids_texts = [(str(x[0]), x[1]) for x in given_ids_texts]

        # Remove duplicates when the text is a string
        # Sometimes it's a dict like with taxonmy mapping
        if isinstance(given_ids_texts[0][1], str):
            given_ids_texts = list(set([(x[0], x[1]) for x in given_ids_texts]))
        else:
            given_ids_texts = given_ids_texts

        requested_given_ids = {given_id for given_id, _ in given_ids_texts}
        if read_from_cache:
            found_rows, unfound_ids_errors = self.get_prompt_response_obj_bulk(
                given_ids_texts,
                request_hash=request_hash,
            )
            unfound_rows = []
            for given_id, text in given_ids_texts:
                text_id = PromptResponse.create_text_id(text)
                errors_for_id = unfound_ids_errors.get((given_id, text_id), 0)
                if (
                    (given_id, text_id) in unfound_ids_errors and
                    (errors_for_id == 0 or errors_for_id < max_errors)
                ):
                    unfound_rows.append((given_id, text))

        else:
            unfound_rows = given_ids_texts
            found_rows = []

        res = {x.given_id: x.to_dict() for x in found_rows}

        len_unfound = len(unfound_rows)
        logger.info("Found %s cached responses", len(res))
        logger.info("Need to run %s responses", len_unfound)

        def _api_only(given_id_text):
            """Worker: API call only, no session access. Returns (given_id, text, response_or_err, error_flag)."""
            given_id, text = given_id_text
            response, error = self.get_completion_catch_error(
                prompt_str=self.prompt.prompt_str,
                text=text,
                return_all=True,
                **kwargs,
            )
            return given_id, text, response, error

        if len(unfound_rows) == 0:
            res = {x: res[x] for x in requested_given_ids if x in res}
            return res
        n_run = 0

        with ThreadPool(max_workers=max_workers) as executor:
            for chunk in chunked(unfound_rows, n_per_commit):
                try:
                    logger.info("Running GPT %s on chunk of %s", self.prompt.name, len(chunk))
                    # Workers do not touch self.session — concurrency is bounded
                    # only by the OpenAI API.
                    results = list(executor.map(_api_only, chunk))

                    success_rows: list[dict[str, Any]] = []
                    error_rows: list[dict[str, Any]] = []
                    response_dicts_by_given_id: dict[str, dict] = {}

                    for given_id, text, response, error in results:
                        if error:
                            logger.warning("No response text for %s", given_id)
                            error_rows.append(
                                self._build_error_row(
                                    given_id, text, response,
                                    request_hash=request_hash,
                                    request_kwargs=request_kwargs,
                                )
                            )
                        else:
                            row = self._build_success_row(
                                given_id, text, response,
                                request_hash=request_hash,
                                request_kwargs=request_kwargs,
                            )
                            success_rows.append(row)
                            # Build the to_dict() shape the legacy path returns,
                            # without needing to round-trip through the DB.
                            response_dicts_by_given_id[str(given_id)] = (
                                self._row_to_response_dict(row)
                            )

                    # Single bulk upsert per kind, then one commit per chunk
                    # (matches the prior commit cadence). DB writes happen on
                    # the main thread, so no Session thread-safety hazard.
                    # When `write_to_cache` is False this chunk is a
                    # passthrough — results are still returned in-memory.
                    if write_to_cache:
                        self._upsert_success_rows(success_rows)
                        self._upsert_error_rows(error_rows)
                        self.session.commit()

                    res.update(response_dicts_by_given_id)
                    n_run += len(results)
                except KeyboardInterrupt:
                    logger.warning("Received KeyboardInterrupt, returning the currently scraped data...")
                    break

                if n_run % (10 * n_per_commit) == 0:
                    logger.info(
                        "Completed %s of %s (%s / %s total completed)",
                        n_run,
                        len_unfound,
                        len(res),
                        len(requested_given_ids),
                    )

        # filter res to only the requested given_ids
        res = {x: res[x] for x in requested_given_ids if x in res}
        return res

    def _row_to_response_dict(self, row: dict[str, Any]) -> dict[str, Any]:
        """Convert a built-row dict into the `PromptResponse.to_dict()`-shaped
        dict the bulk path returns to callers.

        Mirrors the legacy ``data.to_dict()`` shape so callers don't see a
        behavior change. Date columns aren't populated client-side — they
        default to None in the returned dict, matching how a freshly
        constructed (un-committed) PromptResponse would render.
        """
        return {
            "prompt_id": row.get("prompt_id"),
            "given_id": row.get("given_id"),
            "model_name": row.get("model_name"),
            "request_hash": row.get("request_hash", ""),
            "request_kwargs": row.get("request_kwargs"),
            "text_id": row.get("text_id"),
            "input_text": row.get("input_text"),
            "response_full": row.get("response_full"),
            "response_text": row.get("response_text"),
            "num_errors": row.get("num_errors"),
            "date_added": None,
            "date_updated": None,
        }

    def bulk_get_cache_or_run(
        self,
        given_ids_texts: list[tuple[str, str]],
        read_from_cache: bool = True,
        write_to_cache: bool = True,
        n_per_commit: int = 200,
        max_workers=20,
        max_errors=1,
        use_cached_result=_UNSET,
        **kwargs
    ):
        """Bulk get cached or fresh completions for many (given_id, text) pairs.

        Forwards all kwargs to the OpenAI API; use model-appropriate options
        (e.g. ``text_format``, ``reasoning``). See class docstring Notes.

        Parameters
        ----------
        given_ids_texts : list[tuple[str, str]]
            (given_id, text) pairs to process.
        read_from_cache : bool, optional
            If True, consult the cache before calling the API. If False,
            always call the API (force-refresh). Default is True.
        write_to_cache : bool, optional
            If True, persist results back to the cache. If False, the cache
            is left untouched (read-only / passthrough). Default is True.
            Also gated by the constructor's ``store_results``.
        n_per_commit : int, optional
            Chunk size for DB commits (one bulk upsert per kind per chunk).
            Default is 200.
        max_workers : int, optional
            Number of parallel API workers. Default is 20. Tune up for
            nano/mini models with generous quotas; down for tier-1 mainline.
        max_errors : int, optional
            Max errors per (given_id, text) before skipping. Default is 1.
        use_cached_result : bool, optional
            **Deprecated.** Use ``read_from_cache`` and ``write_to_cache``
            instead. When supplied, overrides ``read_from_cache``.
        **kwargs
            Passed to the OpenAI API for each call (model-dependent; see class Notes).

        Returns
        -------
        dict[str, dict]
            Map from given_id to response dict.
        """
        return self._bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
            use_cached_result=use_cached_result,
            **kwargs,
        )


if __name__ == "__main__":
    from pydantic import BaseModel
    import json

    with get_session() as session:
        prc = PromptResponseCacheSQL(
            session=session,
            model="gpt-5-nano",
            prompt_str="Give the world capital of the country."
        )
        response_gpt_5_nano = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
        )
        print("GPT-5-Nano response:", response_gpt_5_nano['response_text'])

    with get_session() as session:
        prc = PromptResponseCacheSQL(
            session=session,
            model="gpt-4.1-mini",
            prompt_str="Give the world capital of the country."
        )
        response_gpt_4_1_mini = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
        )
        print("GPT-4.1-Mini response:", response_gpt_4_1_mini['response_text'])

    class Response(BaseModel):
        capital: str

    with get_session() as session:
        prc = PromptResponseCacheSQL(
            session=session,
            model="gpt-4.1-mini",
            prompt_str="Give the world capital of the country."
        )
        response_gpt_4_1_mini_with_response_model = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
            text_format=Response,
        )
        print("GPT-4.1-Mini response with response model:", response_gpt_4_1_mini_with_response_model['response_text'])

    with get_session() as session:
        prc = PromptResponseCacheSQL(
            session=session,
            model="gpt-5-nano",
            prompt_str="Give the world capital of the country."
        )
        response_gpt_5_nano_with_response_model_advanced_reasoning = prc.get_cache_or_run(
            given_id="test",
            text="France",
            use_cached_result=False,
            text_format=Response,
            reasoning={"effort": "high", "summary": "auto"},
        )
        print("GPT-5-Nano response with response model:", response_gpt_5_nano_with_response_model_advanced_reasoning['response_text'])
        response_from_api_as_dict = json.loads(response_gpt_5_nano_with_response_model_advanced_reasoning['response_full'])

        print("GPT-5-Nano response with response model reasoning effort:", response_from_api_as_dict['reasoning']['effort'])
        reasoning_list = [x for x in response_from_api_as_dict['output'] if x['type'] == 'reasoning'][0]['summary']
        reasoning_summary_full = ""
        for i, reasoning in enumerate(reasoning_list):
            reasoning_summary_full += f"Step {i+1}: {reasoning['text']}\n\n"
        print("GPT-5-Nano response with response model reasoning summary:", reasoning_summary_full)


    with get_session() as session:
        prc = PromptResponseCacheSQL(
            session=session,
            model="gpt-4.1-mini",
            prompt_str="Give the world capital of the country.",
            store_results=False,
        )
        response_gpt_4_1_mini_without_storing = prc.get_cache_or_run(
            given_id="test_cambodia_no_storage",
            text="Cambodia",
            use_cached_result=True,
        )
        print("GPT-4.1-Mini response without storing:", response_gpt_4_1_mini_without_storing['response_text'])