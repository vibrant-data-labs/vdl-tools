"""
Structured-output probe: can an open-weight model return a valid activity profile?
==================================================================================

Before benchmarking open-weight models against ``gpt-5.4-mini`` on activity
extraction, check that each one returns the profile schema at all. Through the
Vercel gateway, the providers serving open models have been seen to ignore a
requested JSON schema and return well-formed JSON with invented keys (2026-08,
``openai/gpt-oss-120b``), a silent failure a parser with optional fields would
turn into empty rows. Baseten documents enforced structured output for its Model
APIs (2026-11), but not how it enforces it.

Each route (a model plus the way it is reached) is probed in three modes:

``schema``
    The production call shape: the extraction prompt, with the schema sent as a
    strict ``json_schema`` text format, exactly as the baseline was sent it.
``prompt_only``
    No text format; the schema JSON is appended to the prompt, the way
    ``InstructorPRC`` builds its prompt. The fallback if ``schema`` fails.
``canary``
    A two-field schema with invented key names and a prompt that never mentions
    them. Only a provider that constrains decoding to the schema can return those
    keys, so this separates enforcement from a model that merely follows a schema
    it was shown.

Frozen inputs
-------------
Organizations are drawn from the 153-organization seed-42 cohort that the
``activity_list_alignment_v2`` judge scored (``agree_v1|<org id>`` rows). Each is
given the exact text its ``gpt-5.4-mini`` medium-effort baseline row received
(``prompt_response.input_text``) and that row's prompt (``PROMPT_ID``), so a later
benchmark compares models on identical input. The baseline's text format is the
strict schema of ``GeneralActivityProfile`` (verified identical, 2026-11-05).

Cache isolation
---------------
Every call goes through ``PromptResponseCacheSQL`` with ``filter_by_model=True``
and never touches an existing row:

- ``given_id`` is ``probe_v1|<mode>|<org id>``; no row outside this probe uses that
  prefix, so the primary key (prompt, given_id, model, request hash) is new.
- The route's backend, provider pinning and text format are in the request hash
  (``api_backend`` and ``extra_body``), so each route and mode has its own rows
  even where two routes share a model name.
- ``read_from_cache`` stays on: a rerun reads its own rows back instead of
  calling again, and a failed call is retried only up to ``--max-errors``. To
  re-probe from scratch, bump ``PROBE_VERSION``.
- ``schema`` reuses the baseline's prompt row; ``prompt_only`` and ``canary``
  register new prompt rows. Prompt rows are keyed by text, so this only inserts.

Usage
-----
    python -m vdl_tools.shared_tools.activity_extraction.model_probe --dry-run
    python -m vdl_tools.shared_tools.activity_extraction.model_probe --n-orgs 10
    python -m vdl_tools.shared_tools.activity_extraction.model_probe \\
        --routes baseten:deepseek-v4-flash --modes canary

Results go to ``data_out/activity_extraction_probe/`` (gitignored): one CSV row per
call plus a markdown summary per route and mode.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Literal

import pandas as pd
from openai.lib._parsing._responses import type_to_text_format_param
from pydantic import BaseModel, ValidationError
from sqlalchemy import select, text

from vdl_tools.shared_tools.activity_extraction.general import GeneralActivityProfile
from vdl_tools.shared_tools.database_cache.database_models.prompt import PromptResponse
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger

# Bump to re-probe from scratch: it is part of every given_id, so new rows result.
PROBE_VERSION = "probe_v1"

# The prompt and request the seed-42 baseline ran with (oneearth, 2026-10-09):
# gpt-5.4-mini at medium effort. Two prompts carry the name
# activity_extraction_general, so these are selected by id, never by name.
PROMPT_ID = "5415e5d5-d67b-54a0-bc47-be25b5513595"
BASELINE_MODEL = "gpt-5.4-mini"
BASELINE_MEDIUM_HASH = "c05a0fb2880d1921cd250a1ba47489aa"
BASELINE_ROUTE = f"baseline:{BASELINE_MODEL}-medium"
# Direct-OpenAI list price, USD per token, from MODEL_DATA.
BASELINE_PRICE = (
    MODEL_DATA[BASELINE_MODEL]["input_cost_per_token"],
    MODEL_DATA[BASELINE_MODEL]["output_cost_per_token"],
)
# The run-to-run alignment judge; its agree_v1 rows name the seed-42 cohort.
ALIGNMENT_PROMPT_ID = "14e4c0dd-ff95-5003-a092-2e966933a74e"
COHORT_FAMILY = "agree_v1"

# The baseline set no output cap. Providers' defaults can be a few thousand
# tokens, which would cut a 20-activity profile short, and on Baseten the cap
# also covers reasoning tokens. Raised from 16k on 2026-11-12: DeepSeek V4.1 Flash
# stops reasoning at 75% of this cap (16k gave 12,014 reasoning tokens, 24k gave
# 18,014), and GLM 5.3 Flash keeps no room for the answer. The 16k rows stay
# stored under their own request hash.
MAX_OUTPUT_TOKENS = 24_000

# Wall-clock limit per call. The SDK's 600 s timeout is per read and retried four
# times, so a gateway that holds a connection open never trips it: on 2026-11-05
# four vercel-baseten:deepseek-v4-pro calls hung for 9 hours. The slowest calls
# that did finish (deepseek-v4-pro, the longest inputs) took a few minutes.
CALL_DEADLINE_S = 900

# Where DeadlineCache stamps each call's wall-clock seconds in the stored response.
LATENCY_KEY = "vdl_probe_latency_s"

# Pause before retrying failed calls within a run.
RETRY_WAIT_S = 30

# The canary needs only enough text for the model to have something to say.
CANARY_CHARS = 4_000
CANARY_PROMPT = (
    "Read the organization's website text below and describe the organization in " "one sentence."
)

Mode = Literal["schema", "prompt_only", "canary"]
MODES: tuple[Mode, ...] = ("schema", "prompt_only", "canary")


class CanaryAnswer(BaseModel):
    """Two invented keys the canary prompt never names."""

    zq_animal: Literal["otter", "heron"]
    zq_count: int


@dataclass(frozen=True)
class Route:
    """A model and the way it is reached.

    Attributes
    ----------
    label : str
        Short name used on the command line and in reports.
    backend : str
        ``api_backend`` for the cache: "baseten" or "vercel".
    model : str
        The model ID in that backend's catalog (pin dated versions; slugs move).
    provider : str, optional
        Vercel provider slug to pin with ``gateway.only`` (no fallback).
    reasoning : dict, optional
        The Responses ``reasoning`` argument. Unset, the model runs at its
        provider default, and the request (so the cache key) is as before.
    """

    label: str
    backend: str
    model: str
    provider: str | None = None
    reasoning: dict | None = None

    def extra_body(self, text_format: dict | None) -> dict:
        """Request body additions: provider pinning, plus the text format if any.

        The text format travels in ``extra_body`` rather than as the SDK's ``text``
        argument because ``get_completion`` already uses ``text`` for the input.
        Being in ``extra_body`` also puts it in the cache's request hash.
        """
        body = {}
        if self.provider:
            body["providerOptions"] = {"gateway": {"only": [self.provider]}}
        if text_format is not None:
            body["text"] = {"format": text_format}
        return body


# Candidates chosen 2026-11-05 from the live catalogs. The baseten: and vercel-baseten:
# pairs are the same weights, so a mismatch between them isolates the gateway.
ROUTES: dict[str, Route] = {
    r.label: r
    for r in [
        Route("baseten:deepseek-v4-flash", "baseten", "deepseek-ai/DeepSeek-V4-Flash-0731"),
        Route("baseten:deepseek-v4.1-flash", "baseten", "deepseek-ai/DeepSeek-V4.1-Flash-1017"),
        Route("baseten:deepseek-v4-pro", "baseten", "deepseek-ai/DeepSeek-V4-Pro-0813"),
        Route("baseten:glm-5.3-flash", "baseten", "zai-org/GLM-5.3-Flash"),
        # Added 2026-11-06 as candidate quality ceilings that, unlike DeepSeek V4 Pro,
        # might honor the schema. Baseten lists no dated snapshot for either.
        Route("baseten:glm-5.3", "baseten", "zai-org/GLM-5.3"),
        Route(
            "baseten:nemotron-3.5-super",
            "baseten",
            "nvidia/NVIDIA-Nemotron-3.5-Super-VL-120B-A12B",
        ),
        Route(
            "vercel-baseten:deepseek-v4-flash",
            "vercel",
            "deepseek/deepseek-v4-flash-0731",
            "baseten",
        ),
        Route(
            "vercel-baseten:deepseek-v4.1-flash",
            "vercel",
            "deepseek/deepseek-v4.1-flash-1017",
            "baseten",
        ),
        Route(
            "vercel-baseten:deepseek-v4-pro", "vercel", "deepseek/deepseek-v4-pro-0813", "baseten"
        ),
        Route("vercel-baseten:glm-5.3-flash", "vercel", "zai/glm-5.3-flash", "baseten"),
        # Reasoning levels, added 2026-11-11; the label suffix is the effort sent.
        # Baseten's levels and defaults (docs.baseten.co, model-apis/reasoning):
        # DeepSeek V4 Flash 0731 none..max, default high, and it stops at 4,096
        # reasoning tokens unless reasoning.max_tokens is set, which the Responses
        # endpoint ignores (tested); DeepSeek V4.1 Flash none/low/high/max, default
        # high; GLM 5.3 Flash none/low/high/max, default high, none runs at low;
        # Nemotron 3.5 Super VL is off by default and every other level runs at high.
        Route(
            "baseten:deepseek-v4-flash@max",
            "baseten",
            "deepseek-ai/DeepSeek-V4-Flash-0731",
            reasoning={"effort": "max", "max_tokens": 12_000},
        ),
        Route(
            "baseten:deepseek-v4.1-flash@low",
            "baseten",
            "deepseek-ai/DeepSeek-V4.1-Flash-1017",
            reasoning={"effort": "low"},
        ),
        Route(
            "baseten:deepseek-v4.1-flash@max",
            "baseten",
            "deepseek-ai/DeepSeek-V4.1-Flash-1017",
            reasoning={"effort": "max"},
        ),
        Route(
            "baseten:glm-5.3-flash@high",
            "baseten",
            "zai-org/GLM-5.3-Flash",
            reasoning={"effort": "high"},
        ),
        Route(
            "baseten:glm-5.3-flash@max",
            "baseten",
            "zai-org/GLM-5.3-Flash",
            reasoning={"effort": "max"},
        ),
        Route(
            "baseten:nemotron-3.5-super@high",
            "baseten",
            "nvidia/NVIDIA-Nemotron-3.5-Super-VL-120B-A12B",
            reasoning={"effort": "high"},
        ),
        # The baseline re-run through the probe, added 2026-11-13, so its latency is
        # timed the same way as the candidates' (stored baseline rows carry only
        # OpenAI's server timestamps). Same model and effort as the baseline.
        Route(
            "openai:gpt-5.4-mini@medium",
            "openai",
            "gpt-5.4-mini",
            reasoning={"effort": "medium"},
        ),
        # Not on Baseten. DeepInfra documents strict json_schema, and serves hy3
        # without hidden reasoning (summarization benchmark, 2026-08).
        Route("vercel-deepinfra:hy3", "vercel", "tencent/hy3", "deepinfra"),
    ]
}


@dataclass(frozen=True)
class OrgInput:
    """One cohort organization's frozen baseline input and output."""

    org_id: str
    input_text: str
    baseline_text: str
    baseline_full: dict


def _as_dict(value) -> dict:
    """A stored ``response_full``, which the cache keeps as a JSON string."""
    return json.loads(value) if isinstance(value, str) else (value or {})


def _usage(full: dict) -> dict:
    """Token counts from a Responses payload; output tokens include reasoning tokens."""
    usage = full.get("usage") or {}
    return {
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "reasoning_tokens": (usage.get("output_tokens_details") or {}).get("reasoning_tokens"),
    }


def baseten_prices() -> dict[str, tuple[float, float]]:
    """USD per input and output token for each Baseten model, from its live catalog.

    Read live rather than hardcoded: a hardcoded judge price in the summarization
    benchmark was 5x off. Models without a listed price are left out.
    """
    from vdl_tools.shared_tools.openai.openai_api_utils import get_baseten_client

    prices = {}
    for model in get_baseten_client().models.list():
        pricing = getattr(model, "pricing", None) or {}
        if pricing.get("prompt") is not None and pricing.get("completion") is not None:
            prices[model.id] = (float(pricing["prompt"]), float(pricing["completion"]))
    return prices


def call_cost(row: dict, price: tuple[float, float] | None) -> float | None:
    """USD for one call at list price, or None without tokens or a price."""
    if price is None or row.get("input_tokens") is None or row.get("output_tokens") is None:
        return None
    return row["input_tokens"] * price[0] + row["output_tokens"] * price[1]


def profile_text_format() -> dict:
    """The strict json_schema text format the SDK builds for the profile model."""
    return type_to_text_format_param(GeneralActivityProfile)


def given_id(mode: Mode, org_id: str) -> str:
    """The probe's cache key for one organization in one mode."""
    return f"{PROBE_VERSION}|{mode}|{org_id}"


def load_cohort(session, n_orgs: int) -> list[OrgInput]:
    """Load ``n_orgs`` seed-42 organizations spread evenly across input length.

    The spread puts the longest inputs in the probe, where context limits and
    truncation would show, without random sampling.

    Parameters
    ----------
    session
        SQLAlchemy session.
    n_orgs : int
        Number of organizations, at most the cohort size.

    Returns
    -------
    list[OrgInput]
        Organizations ordered by input length.
    """
    rows = session.execute(
        text("""
            select r.given_id, r.input_text, r.response_text, r.response_full
            from prompt_response r
            where r.prompt_id = :prompt_id
              and r.model_name = :model
              and r.request_hash = :hash
              and r.num_errors is null
              and r.given_id in (
                  select split_part(a.given_id, '|', 2) from prompt_response a
                  where a.prompt_id = :alignment_id and a.given_id like :family
              )
            """),
        {
            "prompt_id": PROMPT_ID,
            "model": BASELINE_MODEL,
            "hash": BASELINE_MEDIUM_HASH,
            "alignment_id": ALIGNMENT_PROMPT_ID,
            "family": f"{COHORT_FAMILY}|%",
        },
    ).all()
    cohort = sorted(
        (
            OrgInput(org_id=g, input_text=t, baseline_text=b, baseline_full=_as_dict(f))
            for g, t, b, f in rows
        ),
        key=lambda o: (len(o.input_text), o.org_id),
    )
    if not cohort:
        raise SystemExit("No seed-42 cohort rows found; check PROMPT_ID and the alignment prompt")
    n_orgs = min(n_orgs, len(cohort))
    if n_orgs == 1:
        return [cohort[-1]]
    picks = sorted({round(i * (len(cohort) - 1) / (n_orgs - 1)) for i in range(n_orgs)})
    return [cohort[i] for i in picks]


def load_orgs(session, org_ids: list[str]) -> list[OrgInput]:
    """Load named organizations from their stored baseline extraction, ordered by input length.

    Any organization the baseline extracted (prompt, model and effort as the cohort)
    can be probed: its frozen input and its baseline output come from that row.
    """
    rows = session.execute(
        text("""
            select given_id, input_text, response_text, response_full from prompt_response
            where prompt_id = :prompt_id and model_name = :model and request_hash = :hash
              and num_errors is null and given_id = any(:ids)
            """),
        {
            "prompt_id": PROMPT_ID,
            "model": BASELINE_MODEL,
            "hash": BASELINE_MEDIUM_HASH,
            "ids": list(org_ids),
        },
    ).all()
    missing = set(org_ids) - {g for g, *_ in rows}
    if missing:
        raise SystemExit(
            f"No stored baseline extraction for {len(missing)} orgs: {sorted(missing)[:5]}"
        )
    return sorted(
        (
            OrgInput(org_id=g, input_text=t, baseline_text=b, baseline_full=_as_dict(f))
            for g, t, b, f in rows
        ),
        key=lambda o: (len(o.input_text), o.org_id),
    )


def sample_org_ids(path: str | Path) -> list[str]:
    """The organization ids in a sample file: ``{"orgs": [{"org_id": ...}, ...]}``."""
    return [o["org_id"] for o in json.loads(Path(path).read_text())["orgs"]]


def build_requests(mode: Mode, orgs: list[OrgInput], baseline_prompt: str):
    """Return the prompt text, (given_id, input) pairs and text format for a mode."""
    if mode == "schema":
        return (
            baseline_prompt,
            [(given_id(mode, o.org_id), o.input_text) for o in orgs],
            profile_text_format(),
        )
    if mode == "prompt_only":
        schema = json.dumps(GeneralActivityProfile.model_json_schema(), indent=2)
        prompt = f"{baseline_prompt}\n{schema}"
        return prompt, [(given_id(mode, o.org_id), o.input_text) for o in orgs], None
    pairs = [(given_id(mode, o.org_id), o.input_text[:CANARY_CHARS]) for o in orgs]
    return CANARY_PROMPT, pairs, type_to_text_format_param(CanaryAnswer)


class DeadlineCache(PromptResponseCacheSQL):
    """The shared cache, with a wall-clock deadline and a latency stamp on each call.

    A call past ``deadline_s`` raises ``TimeoutError``, which the bulk path stores
    as a failed row like any other API error, so the run moves on and the report
    shows the route as hanging. The abandoned request runs on in a daemon thread
    until its connection closes or the process exits.

    A call that returns has its wall-clock seconds written into the response's
    ``metadata`` under ``LATENCY_KEY`` before the cache stores it, so the latency
    lives in the row and a rerun that reads the row back still reports it. The
    providers' own ``created_at``/``completed_at`` are equal on every probe row
    (2026-11-06), so they cannot stand in. The time includes SDK retries and is
    measured with ``--workers`` calls in flight.
    """

    deadline_s: float = CALL_DEADLINE_S

    def get_completion(self, prompt_str, text, **kwargs):
        outcome = {}
        started = perf_counter()

        def call():
            try:
                outcome["response"] = super(DeadlineCache, self).get_completion(
                    prompt_str, text, **kwargs
                )
            except BaseException as err:  # re-raised on the calling thread below
                outcome["error"] = err

        worker = threading.Thread(target=call, daemon=True)
        worker.start()
        worker.join(self.deadline_s)
        if worker.is_alive():
            raise TimeoutError(f"no response within {self.deadline_s:g} s")
        if "error" in outcome:
            raise outcome["error"]
        response = outcome["response"]
        if hasattr(response, "metadata"):
            response.metadata = {
                **(response.metadata or {}),
                LATENCY_KEY: f"{perf_counter() - started:.2f}",
            }
        return response


def make_cache(session, route: Route, mode: Mode, prompt_str: str) -> PromptResponseCacheSQL:
    """A cache scoped to one route and mode; ``schema`` reuses the baseline prompt row."""
    prompt_kwargs = (
        {"prompt_id": PROMPT_ID}
        if mode == "schema"
        else {
            "prompt_str": prompt_str,
            "prompt_name": f"activity_extraction_{PROBE_VERSION}_{mode}",
        }
    )
    return DeadlineCache(
        session=session,
        model=route.model,
        api_backend=route.backend,
        filter_by_model=True,
        **prompt_kwargs,
    )


def request_kwargs(route: Route, text_format: dict | None) -> dict:
    """The per-call kwargs, identical for the request and the cache key."""
    kwargs = {"max_output_tokens": MAX_OUTPUT_TOKENS, "extra_body": route.extra_body(text_format)}
    if route.reasoning:
        kwargs["reasoning"] = route.reasoning
    return kwargs


def cached_rows(session, cache: PromptResponseCacheSQL, given_ids: list[str], kwargs: dict):
    """Every stored row for these keys, failures included (the bulk call drops them)."""
    request_hash = PromptResponse.create_request_hash(cache._request_kwargs(kwargs))
    stmt = select(PromptResponse).where(
        PromptResponse.prompt_id == cache.prompt.id,
        PromptResponse.given_id.in_(given_ids),
        PromptResponse.model_name == cache.model,
        PromptResponse.request_hash == request_hash,
    )
    return {row.given_id: row for row in session.execute(stmt).scalars()}


_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.S)


def _load_json(raw: str | None):
    """Parse output text as JSON, tolerating one Markdown code fence."""
    if not raw:
        return None, False
    match = _FENCE.match(raw)
    body = match.group(1) if match else raw
    try:
        return json.loads(body), bool(match)
    except json.JSONDecodeError:
        return None, bool(match)


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[\"'“”‘’]", "", value)).strip().lower()


def grounded_share(profile: GeneralActivityProfile, source: str) -> float | None:
    """Share of activities whose evidence appears verbatim in the source (rough check).

    Quote characters and whitespace are normalized; nothing else is. This is a
    screen for invented evidence, not the oneearth grounding check.
    """
    if not profile.activities:
        return None
    haystack = _norm(source)
    return sum(_norm(a.evidence) in haystack for a in profile.activities) / len(profile.activities)


def score_row(mode: Mode, row, source: str) -> dict:
    """Judge one stored response without calling a model.

    Parameters
    ----------
    mode : Mode
        Probe mode, which selects the schema the output must satisfy.
    row : PromptResponse or None
        The stored row; None if the call never produced one.
    source : str
        The input text the model saw, for the evidence check.

    Returns
    -------
    dict
        One report row: call status, schema validity and output statistics.
    """
    result = {
        "stored": row is not None,
        "error": None,
        "status": None,
        "json_ok": False,
        "fenced": False,
        "valid": False,
        "keys": None,
        "n_activities": None,
        "grounded": None,
        "site_match": None,
        "coverage": None,
        "input_tokens": None,
        "output_tokens": None,
        "reasoning_tokens": None,
        "latency_s": None,
        "validation_error": None,
    }
    if row is None:
        return result
    full = row.response_full
    full = json.loads(full) if isinstance(full, str) else (full or {})
    if row.num_errors:
        result["error"] = str(full.get("message", full))[:300]
        return result
    latency = (full.get("metadata") or {}).get(LATENCY_KEY)
    result.update(
        latency_s=float(latency) if latency is not None else None,
        status=full.get("status"),
        **_usage(full),
    )
    if full.get("incomplete_details"):
        result["status"] = f"incomplete:{full['incomplete_details'].get('reason')}"
    parsed, fenced = _load_json(row.response_text)
    result.update(json_ok=parsed is not None, fenced=fenced)
    if isinstance(parsed, dict):
        result["keys"] = ",".join(sorted(parsed))
    model = CanaryAnswer if mode == "canary" else GeneralActivityProfile
    try:
        validated = model.model_validate(parsed)
    except ValidationError as err:
        result["validation_error"] = "; ".join(
            f"{'.'.join(map(str, e['loc']))}: {e['type']}" for e in err.errors()[:5]
        )
        return result
    result["valid"] = True
    if mode != "canary":
        result.update(_profile_stats(validated, source))
    return result


def _profile_stats(profile: GeneralActivityProfile, source: str) -> dict:
    return {
        "n_activities": len(profile.activities),
        "grounded": grounded_share(profile, source),
        "site_match": profile.site_matches_organization,
        "coverage": profile.coverage,
    }


def baseline_rows(orgs: list[OrgInput]) -> list[dict]:
    """Report rows for the cached gpt-5.4-mini baseline, as a reference."""
    out = []
    for org in orgs:
        profile = GeneralActivityProfile.model_validate_json(org.baseline_text)
        out.append(
            {
                "route": BASELINE_ROUTE,
                "mode": "schema",
                "org_id": org.org_id,
                "input_chars": len(org.input_text),
                "stored": True,
                "valid": True,
                "json_ok": True,
                **_usage(org.baseline_full),
                **_profile_stats(profile, org.input_text),
            }
        )
    return out


def add_comparisons(frame: pd.DataFrame, prices: dict[str, tuple[float, float]]) -> pd.DataFrame:
    """Add per-call cost and agreement with the baseline on the org-level verdicts.

    ``site_match`` (does the site belong to the named organization) and ``coverage``
    are the profile's two categorical judgments about the input as a whole, so
    matching the baseline on them is a cheap first read on whether a model
    reads the same sites the same way. Activity-level agreement needs the judge.
    """
    frame = frame.copy()
    models = {label: route.model for label, route in ROUTES.items()}
    models[BASELINE_ROUTE] = BASELINE_MODEL
    all_prices = {**prices, BASELINE_MODEL: BASELINE_PRICE}
    frame["cost_usd"] = [
        call_cost(row, all_prices.get(models.get(row["route"]))) for row in frame.to_dict("records")
    ]
    base = frame[frame["route"] == BASELINE_ROUTE].set_index("org_id")
    for column in ("site_match", "coverage"):
        reference = frame["org_id"].map(base[column]) if column in base else None
        if reference is None or column not in frame:
            continue
        agrees = frame[column] == reference
        frame[f"{column}_agrees"] = agrees.where(frame[column].notna())
    return frame


def probe_route(
    session,
    route: Route,
    mode: Mode,
    orgs: list[OrgInput],
    baseline_prompt: str,
    *,
    dry_run: bool,
    max_workers: int,
    max_errors: int,
) -> list[dict]:
    """Run (or, with ``dry_run``, only count) one route in one mode and score the rows."""
    prompt_str, pairs, text_format = build_requests(mode, orgs, baseline_prompt)
    cache = make_cache(session, route, mode, prompt_str)
    kwargs = request_kwargs(route, text_format)
    ids = [g for g, _ in pairs]
    # The baseline re-run shares the baseline's model and prompt row. Its keys carry
    # the probe prefix, and its request (output cap, effort) must hash differently
    # from the stored baseline's, so the cache can neither read nor replace those rows.
    if route.model == BASELINE_MODEL and (
        PromptResponse.create_request_hash(cache._request_kwargs(kwargs)) == BASELINE_MEDIUM_HASH
    ):
        raise ValueError("This request would share the stored baseline's request hash")
    if not all(g.startswith(f"{PROBE_VERSION}|") for g in ids):
        raise ValueError("Every probe given_id must carry the probe prefix")
    before = cached_rows(session, cache, ids, kwargs)
    if dry_run:
        print(f"  {route.label:38s} {mode:12s} {len(before)}/{len(ids)} already stored")
        return []
    # The cache retries a failed key only while it has failed fewer than
    # max_errors times, so raising the limit one step per pass retries each
    # failure once per pass. The wait lets a 429 rate limit (seen on Baseten
    # with 4 workers, 2026-11-06) clear before the next pass.
    for attempt in range(1, max_errors + 1):
        if attempt > 1:
            failed = [
                g for g, r in cached_rows(session, cache, ids, kwargs).items() if r.num_errors
            ]
            if not failed:
                break
            logger.info("Retrying %d failed calls in %d s", len(failed), RETRY_WAIT_S)
            time.sleep(RETRY_WAIT_S)
        cache.bulk_get_cache_or_run(
            pairs,
            read_from_cache=True,
            write_to_cache=True,
            max_workers=max_workers,
            max_errors=attempt,
            **kwargs,
        )
    after = cached_rows(session, cache, ids, kwargs)
    sources = dict(pairs)
    report = []
    for org in orgs:
        gid = given_id(mode, org.org_id)
        row = after.get(gid)
        report.append(
            {
                "route": route.label,
                "mode": mode,
                "org_id": org.org_id,
                "input_chars": len(sources[gid]),
                "new_call": gid not in before,
                **score_row(mode, row, sources[gid]),
            }
        )
    return report


def summarize(report: pd.DataFrame) -> str:
    """Markdown table: one line per route and mode."""

    def _median(values):
        values = [v for v in values if pd.notna(v)]
        return round(statistics.median(values), 1) if values else None

    def _p90(values):
        values = sorted(v for v in values if pd.notna(v))
        return round(values[min(len(values) - 1, int(0.9 * len(values)))], 1) if values else None

    def _share(values):
        values = values.dropna() if values is not None else []
        return round(float(values.astype(float).mean()), 2) if len(values) else None

    lines = [
        "| route | mode | n | errors | json | fenced | valid | incomplete | "
        "activities (med) | evidence verbatim (mean) | in tok (med) | out tok (med) | "
        "reasoning tok (med) | latency s (med) | latency s (p90) | $ per 1k orgs | "
        "site match = baseline | coverage = baseline |",
        "|" + "---|" * 18,
    ]
    for (route, mode), g in report.groupby(["route", "mode"], sort=False):
        status = g["status"] if "status" in g else pd.Series(dtype=str)
        grounded = g["grounded"].dropna()
        cost = g["cost_usd"].dropna() if "cost_usd" in g else pd.Series(dtype=float)
        per_1k = round(cost.mean() * 1000, 2) if len(cost) else None
        extra = (
            f"| {per_1k} | {_share(g.get('site_match_agrees'))} "
            f"| {_share(g.get('coverage_agrees'))} |"
        )
        lines.append(
            f"| {route} | {mode} | {len(g)} | {int(g['error'].notna().sum()) if 'error' in g else 0} "
            f"| {int(g['json_ok'].sum())} | {int(g['fenced'].fillna(False).sum()) if 'fenced' in g else 0} "
            f"| {int(g['valid'].sum())} "
            f"| {int(status.fillna('').str.startswith('incomplete').sum())} "
            f"| {_median(g['n_activities'])} "
            f"| {round(grounded.mean(), 2) if len(grounded) else None} "
            f"| {_median(g.get('input_tokens', []))} | {_median(g.get('output_tokens', []))} "
            f"| {_median(g.get('reasoning_tokens', []))} "
            f"| {_median(g.get('latency_s', []))} | {_p90(g.get('latency_s', []))} " + extra
        )
    return "\n".join(lines)


def run(args: argparse.Namespace) -> None:
    from vdl_tools.shared_tools.database_cache.database_utils import get_session

    unknown = set(args.routes) - set(ROUTES)
    if unknown:
        raise SystemExit(f"Unknown routes {sorted(unknown)}; choose from {sorted(ROUTES)}")
    routes = [ROUTES[label] for label in args.routes]
    with get_session() as session:
        if args.org_file:
            orgs = load_orgs(session, sample_org_ids(args.org_file))
        else:
            orgs = load_cohort(session, args.n_orgs)
        baseline_prompt = _prompt_text(session)
        logger.info(
            "Probing %d orgs (input %d-%d chars) on %d routes x %s",
            len(orgs),
            len(orgs[0].input_text),
            len(orgs[-1].input_text),
            len(routes),
            ",".join(args.modes),
        )
        report = baseline_rows(orgs) if not args.dry_run else []
        for route in routes:
            for mode in args.modes:
                mode_orgs = orgs[: args.canary_orgs] if mode == "canary" else orgs
                report += probe_route(
                    session,
                    route,
                    mode,
                    mode_orgs,
                    baseline_prompt,
                    dry_run=args.dry_run,
                    max_workers=args.workers,
                    max_errors=args.max_errors,
                )
        if args.dry_run:
            # Building a cache registers its prompt row in the session, and
            # get_session commits on exit; a dry run must leave the DB as it was.
            session.rollback()
    if args.dry_run:
        return
    prices = baseten_prices() if any(r.backend == "baseten" for r in routes) else {}
    frame = add_comparisons(pd.DataFrame(report), prices)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_dir / "probe_calls.csv", index=False)
    table = summarize(frame[~frame["route"].str.startswith("baseline:")])
    reference = summarize(frame[frame["route"].str.startswith("baseline:")])
    (out_dir / "probe_summary.md").write_text(
        f"# Structured-output probe ({PROBE_VERSION})\n\n{table}\n\n## Baseline (cached)\n\n{reference}\n"
    )
    print(table)
    print(f"\nWrote {out_dir / 'probe_calls.csv'} and {out_dir / 'probe_summary.md'}")


def _prompt_text(session) -> str:
    """The baseline's prompt text, read from its prompt row."""
    prompt_str = session.execute(
        text("select prompt_str from prompt where id = :id"), {"id": PROMPT_ID}
    ).scalar()
    if not prompt_str:
        raise SystemExit(f"Prompt {PROMPT_ID} not found")
    return prompt_str


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--routes", nargs="+", default=list(ROUTES), help="Route labels (default: all)"
    )
    parser.add_argument(
        "--modes", nargs="+", default=list(MODES), choices=MODES, help="Modes (default: all)"
    )
    parser.add_argument(
        "--n-orgs", type=int, default=10, help="Cohort organizations per route (default: 10)"
    )
    parser.add_argument(
        "--org-file",
        default=None,
        help="Probe the organizations in this sample file instead of the cohort",
    )
    parser.add_argument(
        "--canary-orgs", type=int, default=3, help="Organizations for the canary (default: 3)"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Parallel calls per route (default: 4)"
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=2,
        help="Attempts per call, within this run and across reruns (default: 2)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Count stored rows per route and mode; call nothing"
    )
    parser.add_argument(
        "--out-dir",
        default="data_out/activity_extraction_probe",
        help="Report directory (default: data_out/activity_extraction_probe, gitignored)",
    )
    run(parser.parse_args())


if __name__ == "__main__":
    main()
