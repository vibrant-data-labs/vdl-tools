"""
Thin structured-output wrapper over the shared psql prompt cache.
=================================================================

Every LLM call in this pipeline returns a Pydantic model and is cached in
``prompt_response`` keyed by (prompt, given_id, text hash, model, request
kwargs). ``reasoning`` is inside the request hash, so two runs of the same
prompt at different reasoning efforts do not collide.

The bulk cache path already calls the API with ``return_all=True`` and stores
``response.output_text`` / ``response.model_dump_json()``. Passing
``text_format`` alone therefore gives structured output through the stock cache
with no subclassing: the cached ``response_text`` is the JSON the schema
produced. Do not also pass ``return_all`` — the cache layer supplies it.
"""

from __future__ import annotations

import hashlib
import json
import warnings
from typing import TypeVar

from pydantic import BaseModel, ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vdl_tools.shared_tools.tools.logger import logger

T = TypeVar("T", bound=BaseModel)


# Control characters postgres rejects in a string literal (NUL) or that
# openpyxl refuses to write to a cell. Newline, tab and carriage return are
# kept; everything else in these ranges is dropped.
_ILLEGAL_CHARS = dict.fromkeys(list(range(0x00, 0x09)) + [0x0B, 0x0C] + list(range(0x0E, 0x20)))


def scrub(value: str) -> str:
    """Strip control characters that break the psql cache and Excel writers."""
    return value.translate(_ILLEGAL_CHARS)


def scrub_model(model: T) -> T:
    """Return a copy of a parsed model with every string field scrubbed.

    A model can emit a NUL inside a string field. ``json.dumps`` stores it
    escaped, so the cache write succeeds — but ``json.loads`` turns it back into
    a real NUL, which then lands in the next prompt's ``input_text`` and makes
    postgres reject the insert. Scrubbing on the way out of the cache stops it
    propagating to any downstream stage.
    """

    def _walk(value):
        if isinstance(value, str):
            return scrub(value)
        if isinstance(value, list):
            return [_walk(v) for v in value]
        if isinstance(value, dict):
            return {k: _walk(v) for k, v in value.items()}
        return value

    return type(model).model_validate(_walk(model.model_dump()))


# The shared engine is built with plain create_engine(), so a pooled connection
# that the server has since closed is handed out and fails on write. A slow
# model makes this routine: the session sits idle for minutes between commits
# while API calls run, and the write then dies with "server closed the
# connection unexpectedly", losing every response already paid for. pre_ping
# checks the connection before use and transparently replaces a dead one.
_SESSION_FACTORY = None


def _resilient_session():
    """A session whose pooled connections are checked before use.

    Built once and reused. ``pool_pre_ping`` issues a cheap liveness check
    before handing out a connection, and ``pool_recycle`` retires ones older
    than half an hour.
    """
    from vdl_tools.shared_tools.database_cache.database_utils import get_configuration, get_url

    global _SESSION_FACTORY
    if _SESSION_FACTORY is None:
        cfg = get_configuration()
        url = get_url(
            host=cfg["postgres"]["host"],
            port=cfg["postgres"]["port"],
            user=cfg["postgres"]["user"],
            password=cfg["postgres"]["password"],
            database=cfg["postgres"]["database"],
        )
        _SESSION_FACTORY = sessionmaker(
            bind=create_engine(url, pool_pre_ping=True, pool_recycle=1800)
        )
    return _SESSION_FACTORY()


def run_structured(
    given_ids_texts: list[tuple[str, str]],
    prompt_str: str,
    prompt_name: str,
    response_model: type[T],
    model: str,
    reasoning: dict | None = None,
    max_workers: int = 16,
    read_from_cache: bool = True,
    write_to_cache: bool = True,
    # Judge responses carry tens of KB of response JSON each; a 50-row insert
    # runs to megabytes and has dropped the postgres connection mid-write.
    n_per_commit: int = 20,
    max_errors: int = 1,
    session=None,
    **api_kwargs,
) -> dict[str, T]:
    """Run one structured prompt over many inputs, returning parsed models.

    Parameters
    ----------
    given_ids_texts : list[tuple[str, str]]
        (cache key, user text) pairs. The key should be stable across runs so
        an unchanged input hits the cache.
    response_model : type[BaseModel]
        Schema forced on the model via ``text_format``.
    reasoning : dict, optional
        e.g. ``{"effort": "low"}``. ``None`` leaves the model's own default.
    **api_kwargs
        Extra Responses API options, e.g. ``tools=[{"type": "web_search"}]``.
        Options in ``KWARG_KEYS_THAT_AFFECT_OUTPUT`` (``tools`` among them)
        enter the cache key, so a cached row always describes the call that
        produced it. Do not pass ``return_all`` — the cache layer supplies it.

    Returns
    -------
    dict[str, BaseModel]
        Cache key -> parsed model, omitting inputs that errored or whose
        cached JSON no longer validates against the schema.
    """
    from vdl_tools.shared_tools.database_cache.database_utils import get_session
    from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

    if not given_ids_texts:
        return {}

    # The cached prompt id hashes prompt_str only, so a schema change alone
    # would leave rows whose JSON no longer validates — they'd be dropped one
    # by one at parse time and the entities would silently vanish from the
    # results. The schema shapes the output as surely as the prompt does, so
    # fold its fingerprint into the prompt identity so those runs miss the
    # cache and re-run.
    # Scrub inputs too: a text assembled from an earlier stage's output, or
    # from scraped page text, can carry the same illegal characters.
    given_ids_texts = [(gid, scrub(text)) for gid, text in given_ids_texts]

    schema_json = json.dumps(response_model.model_json_schema(), sort_keys=True)
    fingerprint = hashlib.md5(schema_json.encode("utf-8")).hexdigest()[:12]
    prompt_str = f"{prompt_str}\n\n[response schema {response_model.__name__} #{fingerprint}]"

    api_kwargs = {"text_format": response_model, **api_kwargs}
    if reasoning:
        api_kwargs["reasoning"] = reasoning

    # The OpenAI SDK emits a UserWarning per web-search call when the cache
    # serializes the full response (its union of tool-call types doesn't cover
    # web_search_call). The stored JSON is correct; only the warning is noise.
    session = session or _resilient_session()
    with warnings.catch_warnings(), get_session(session=session) as session:
        warnings.filterwarnings("ignore", message=".*serialized value may not be as expected.*")
        cache = PromptResponseCacheSQL(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            model=model,
            # The shared cache defaults to filter_by_model=False, which makes
            # reads match a row written by ANY model. Changing --judge-model or
            # --model then replays the previous model's answers with no API call
            # and no warning — a cross-family comparison silently becomes a
            # comparison of a run with itself. Model identity is part of the key
            # here.
            filter_by_model=True,
        )
        raw = cache.bulk_get_cache_or_run(
            given_ids_texts,
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,
            **api_kwargs,
        )

    parsed: dict[str, T] = {}
    for given_id, row in (raw or {}).items():
        text = (row or {}).get("response_text")
        if not text:
            continue
        try:
            parsed[given_id] = scrub_model(response_model.model_validate(json.loads(text)))
        except (json.JSONDecodeError, ValidationError) as ex:
            # A schema change invalidates old cached rows; surface it rather
            # than silently dropping the entity from the results.
            logger.warning("Discarding unparseable cached response for %s: %s", given_id, ex)
    if len(parsed) < len(given_ids_texts):
        logger.warning(
            "%s of %s inputs returned a usable response for prompt '%s'",
            len(parsed),
            len(given_ids_texts),
            prompt_name,
        )
    return parsed
