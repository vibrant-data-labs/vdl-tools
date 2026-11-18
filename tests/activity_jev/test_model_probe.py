"""Offline checks of the structured-output probe: cache isolation and scoring.

No model is called and no database is touched; the cache is built with its prompt
lookup patched out, as in the gateway contract tests.
"""

import itertools
import threading
import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest
from openai import OpenAI

from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.general import GeneralActivityProfile
from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.openai import openai_api_utils as api
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

SOURCE = "ORGANIZATION: River Friends\n\nURL: https://example.org\nTEXT: We run monthly river cleanups with volunteers."
ACTIVITY = {
    "activity": "River cleanups",
    "what_happens": "Volunteers remove trash",
    "who_carries_it_out": "Volunteers",
    "aim": "Cleaner rivers",
    "sector": "Environment",
    "methods": "Cleanups",
    "geography": "Not stated",
    "evidence": "We run monthly river cleanups with volunteers.",
    "certainty": "high",
    "prominence": "core",
    "recurrence": "recurring",
}
PROFILE = {
    "summary": "River Friends runs cleanups.",
    "activities": [ACTIVITY],
    "geographic_orientation_evidence": "Not stated",
    "site_matches_organization": "yes",
    "site_match_evidence": "Name matches",
    "coverage": "thin",
}


@pytest.fixture
def no_prompt_lookup(monkeypatch):
    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **k: Prompt(prompt_str=k.get("prompt_str") or "baseline prompt"),
    )


def _hash(route, mode):
    _, _, text_format = probe.build_requests(mode, [], "baseline prompt")
    kwargs = api.completion_request_kwargs(
        route.backend, None, probe.request_kwargs(route, text_format)
    )
    return PromptResponse.create_request_hash(kwargs)


def test_every_route_and_mode_has_its_own_cache_key():
    # A row's key is (prompt, given_id, model_name, request_hash). The hash leaves
    # the model out because model_name is its own column, so check the pair.
    keys = list(itertools.product(probe.ROUTES.values(), probe.MODES))
    assert len({(route.model, _hash(route, mode)) for route, mode in keys}) == len(keys)
    # The same weights reached directly and through the gateway still get two rows,
    # and each mode changes the hash even for one route.
    direct, gateway = (
        probe.ROUTES["baseten:glm-5.3-flash"],
        probe.ROUTES["vercel-baseten:glm-5.3-flash"],
    )
    assert _hash(direct, "schema") != _hash(gateway, "schema")
    assert len({_hash(direct, mode) for mode in probe.MODES}) == len(probe.MODES)
    # The baseline's hash belongs to gpt-5.4-mini at medium effort only.
    assert probe.BASELINE_MEDIUM_HASH not in {_hash(route, mode) for route, mode in keys}


def test_probe_keys_cannot_meet_existing_rows():
    gid = probe.given_id("schema", "22-3488225")
    assert gid == f"{probe.PROBE_VERSION}|schema|22-3488225"
    # Baseline rows are keyed by the bare organization ID; alignment rows by agree_v1|...
    assert gid != "22-3488225" and not gid.startswith(probe.COHORT_FAMILY)
    # The baseline re-run uses the baseline's model; the probe prefix keeps its keys
    # apart from the stored baseline rows, and its request hash differs as well.
    rerun = probe.ROUTES["openai:gpt-5.4-mini@medium"]
    assert rerun.model == probe.BASELINE_MODEL
    assert _hash(rerun, "schema") != probe.BASELINE_MEDIUM_HASH


def test_schema_mode_sends_the_baseline_text_format():
    route = probe.ROUTES["vercel-baseten:deepseek-v4-flash"]
    prompt, _, text_format = probe.build_requests("schema", [], "baseline prompt")
    body = probe.request_kwargs(route, text_format)["extra_body"]
    assert prompt == "baseline prompt"
    assert body["text"]["format"]["strict"] is True
    assert body["text"]["format"]["name"] == "GeneralActivityProfile"
    assert body["providerOptions"] == {"gateway": {"only": ["baseten"]}}


def test_prompt_only_mode_appends_schema_and_sends_no_format():
    prompt, _, text_format = probe.build_requests("prompt_only", [], "baseline prompt")
    assert text_format is None
    assert prompt.startswith("baseline prompt\n") and '"activities"' in prompt


def test_make_cache_reuses_baseline_prompt_only_in_schema_mode(no_prompt_lookup, monkeypatch):
    seen = []
    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **k: seen.append(k) or Prompt(prompt_str="x"),
    )
    route = probe.ROUTES["baseten:deepseek-v4-flash"]
    probe.make_cache(MagicMock(), route, "schema", "ignored")
    probe.make_cache(MagicMock(), route, "canary", probe.CANARY_PROMPT)
    assert seen[0]["prompt_id"] == probe.PROMPT_ID and not seen[0]["prompt_str"]
    assert seen[1]["prompt_str"] == probe.CANARY_PROMPT and seen[1]["prompt_id"] is None


def test_probe_refuses_a_baseline_rerun_with_the_stored_hash(no_prompt_lookup, monkeypatch):
    route = probe.ROUTES["openai:gpt-5.4-mini@medium"]
    monkeypatch.setattr(probe, "BASELINE_MEDIUM_HASH", _hash(route, "schema"))
    with pytest.raises(ValueError, match="stored baseline"):
        probe.probe_route(
            MagicMock(), route, "schema", [], "p", dry_run=True, max_workers=1, max_errors=1
        )


def test_wire_request_carries_format_cap_and_pinning(no_prompt_lookup, monkeypatch):
    seen = []

    def respond(request):
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "r",
                "object": "response",
                "created_at": 0,
                "model": "m",
                "status": "completed",
                "output": [],
                "parallel_tool_calls": False,
                "tool_choice": "auto",
                "tools": [],
            },
        )

    route = probe.ROUTES["vercel-baseten:deepseek-v4-flash"]
    with ExitStack() as stack:
        client = stack.enter_context(
            OpenAI(
                api_key="k",
                base_url=api.VERCEL_BASE_URL,
                max_retries=0,
                http_client=httpx.Client(transport=httpx.MockTransport(respond)),
            )
        )
        monkeypatch.setattr(api, "get_vercel_client", lambda: client)
        prompt, _, text_format = probe.build_requests("schema", [], "baseline prompt")
        cache = probe.make_cache(MagicMock(), route, "schema", prompt)
        cache.get_completion(
            prompt, SOURCE, return_all=True, **probe.request_kwargs(route, text_format)
        )
    (body,) = seen
    assert body["model"] == route.model and body["max_output_tokens"] == probe.MAX_OUTPUT_TOKENS
    assert body["text"]["format"]["name"] == "GeneralActivityProfile"
    assert body["providerOptions"] == {"gateway": {"only": ["baseten"]}}
    assert [m["role"] for m in body["input"]] == ["system", "user"]


def _row(text, *, status="completed", num_errors=None, full=None):
    full = full or {
        "status": status,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 0},
        },
    }
    return SimpleNamespace(
        response_text=text, response_full=json.dumps(full), num_errors=num_errors
    )


def test_score_valid_profile_counts_grounded_evidence():
    result = probe.score_row("schema", _row(json.dumps(PROFILE)), SOURCE)
    assert result["valid"] and result["n_activities"] == 1 and result["grounded"] == 1.0
    assert result["input_tokens"] == 10 and result["status"] == "completed"


def test_score_fenced_json_is_parsed_but_flagged():
    result = probe.score_row("prompt_only", _row(f"```json\n{json.dumps(PROFILE)}\n```"), SOURCE)
    assert result["valid"] and result["fenced"]


def test_score_invented_keys_is_json_but_not_valid():
    # The silent gateway failure: well-formed JSON under a key nobody asked for.
    result = probe.score_row("schema", _row('{"answer": "River Friends runs cleanups"}'), SOURCE)
    assert result["json_ok"] and not result["valid"]
    assert result["keys"] == "answer" and "summary" in result["validation_error"]


def test_score_reports_truncation_and_errors():
    cut = probe.score_row(
        "schema",
        _row(
            '{"summary": "Riv',
            full={"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}},
        ),
        SOURCE,
    )
    assert cut["status"] == "incomplete:max_output_tokens" and not cut["json_ok"]
    failed = probe.score_row("schema", _row(None, num_errors=1, full={"message": "429"}), SOURCE)
    assert failed["error"] == "429" and not failed["valid"]
    assert not probe.score_row("schema", None, SOURCE)["stored"]


def test_score_canary_requires_the_invented_keys():
    assert probe.score_row("canary", _row('{"zq_animal": "otter", "zq_count": 2}'), SOURCE)["valid"]
    assert not probe.score_row("canary", _row('{"summary": "A river group"}'), SOURCE)["valid"]


def test_grounding_ignores_quotes_and_whitespace():
    profile = GeneralActivityProfile.model_validate(
        {**PROFILE, "activities": [{**ACTIVITY, "evidence": "“monthly   river cleanups”"}]}
    )
    assert probe.grounded_share(profile, SOURCE) == 1.0


def test_cohort_spread_keeps_shortest_and_longest():
    rows = [(f"org{i}", "x" * (i + 1), "{}", '{"usage": {}}') for i in range(20)]
    session = MagicMock()
    session.execute.return_value.all.return_value = rows
    picked = probe.load_cohort(session, 5)
    assert [o.org_id for o in picked][0] == "org0" and picked[-1].org_id == "org19"
    assert len(picked) == 5
    assert picked[0].baseline_full == {"usage": {}}


def _report_row(route, org, **values):
    # Every column score_row produces, so summarize sees a real report's shape.
    base = probe.score_row("schema", None, SOURCE)
    return {"route": route, "mode": "schema", "org_id": org, **base, **values}


def test_comparisons_price_each_call_and_compare_verdicts_to_baseline():
    route = "baseten:glm-5.3-flash"
    frame = pd.DataFrame(
        [
            _report_row(
                probe.BASELINE_ROUTE,
                "a",
                input_tokens=1000,
                output_tokens=100,
                site_match="yes",
                coverage="rich",
            ),
            _report_row(
                probe.BASELINE_ROUTE,
                "b",
                input_tokens=1000,
                output_tokens=100,
                site_match="no",
                coverage="thin",
            ),
            _report_row(
                route, "a", input_tokens=1000, output_tokens=100, site_match="yes", coverage="thin"
            ),
            _report_row(
                route, "b", input_tokens=None, output_tokens=None, site_match=None, coverage=None
            ),
        ]
    )
    out = probe.add_comparisons(frame, {"zai-org/GLM-5.3-Flash": (1e-6, 2e-6)})
    glm = out[out["route"] == route].set_index("org_id")
    assert glm.loc["a", "cost_usd"] == pytest.approx(1000 * 1e-6 + 100 * 2e-6)
    assert pd.isna(glm.loc["b", "cost_usd"])  # no tokens, so no cost
    assert glm.loc["a", "site_match_agrees"] and not glm.loc["a", "coverage_agrees"]
    assert pd.isna(glm.loc["b", "site_match_agrees"])  # failed call: not a disagreement
    base_cost = out[out["route"] == probe.BASELINE_ROUTE]["cost_usd"].iloc[0]
    assert base_cost == pytest.approx(
        1000 * probe.BASELINE_PRICE[0] + 100 * probe.BASELINE_PRICE[1]
    )
    table = probe.summarize(out[out["route"] == route])
    # $1.20 per 1k orgs from the one priced call; site match 1/1 and coverage 0/1,
    # with the failed call counted as neither agreeing nor disagreeing.
    assert "$ per 1k orgs" in table and table.endswith("| 1.2 | 1.0 | 0.0 |")


def test_failed_calls_are_retried_once_per_pass(monkeypatch):
    passes, failing = [], {"probe_v1|schema|a"}

    class FakeCache:
        def bulk_get_cache_or_run(self, pairs, **kw):
            passes.append(kw["max_errors"])
            if len(passes) > 1:
                failing.clear()

    def fake_rows(session, cache, ids, kwargs):
        return {
            g: SimpleNamespace(
                num_errors=1 if g in failing else None, response_full="{}", response_text=None
            )
            for g in ids
        }

    monkeypatch.setattr(probe, "make_cache", lambda *a: FakeCache())
    monkeypatch.setattr(probe, "cached_rows", fake_rows)
    monkeypatch.setattr(probe, "RETRY_WAIT_S", 0)
    org = probe.OrgInput("a", SOURCE, json.dumps(PROFILE), {})
    route = probe.ROUTES["baseten:glm-5.3-flash"]
    probe.probe_route(
        MagicMock(), route, "schema", [org], "p", dry_run=False, max_workers=1, max_errors=3
    )
    # First pass runs everything; the second retries the one failure; no third pass.
    assert passes == [1, 2]


def test_deadline_turns_a_hung_call_into_a_stored_failure(no_prompt_lookup, monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(
        PromptResponseCacheSQL, "get_completion", lambda self, p, t, **k: release.wait(10)
    )
    cache = probe.make_cache(MagicMock(), probe.ROUTES["baseten:glm-5.3-flash"], "schema", "p")
    monkeypatch.setattr(cache, "deadline_s", 0.05)
    try:
        with pytest.raises(TimeoutError, match="no response within 0.05 s"):
            cache.get_completion("p", SOURCE)
        # The bulk path's error handler records the timeout like any API error.
        message, failed = cache.get_completion_catch_error("p", SOURCE)
        assert failed and "no response within" in message["message"]
    finally:
        release.set()


def test_deadline_passes_responses_and_errors_through(no_prompt_lookup, monkeypatch):
    cache = probe.make_cache(MagicMock(), probe.ROUTES["baseten:glm-5.3-flash"], "schema", "p")
    monkeypatch.setattr(PromptResponseCacheSQL, "get_completion", lambda self, p, t, **k: (p, t, k))
    assert cache.get_completion("p", "t", max_output_tokens=5) == (
        "p",
        "t",
        {"max_output_tokens": 5},
    )

    def fail(self, p, t, **k):
        raise RuntimeError("429")

    monkeypatch.setattr(PromptResponseCacheSQL, "get_completion", fail)
    with pytest.raises(RuntimeError, match="429"):
        cache.get_completion("p", "t")


def test_latency_is_stamped_into_the_stored_response(no_prompt_lookup, monkeypatch):
    from openai.types.responses import Response

    response = Response.model_validate(
        {
            "id": "r",
            "object": "response",
            "created_at": 0,
            "model": "m",
            "status": "completed",
            "output": [],
            "parallel_tool_calls": False,
            "tool_choice": "auto",
            "tools": [],
            "metadata": {"kept": "yes"},
        }
    )
    monkeypatch.setattr(PromptResponseCacheSQL, "get_completion", lambda self, p, t, **k: response)
    cache = probe.make_cache(MagicMock(), probe.ROUTES["baseten:glm-5.3-flash"], "schema", "p")
    stored = json.loads(cache.get_completion("p", "t").model_dump_json())
    assert stored["metadata"]["kept"] == "yes"
    assert float(stored["metadata"][probe.LATENCY_KEY]) >= 0
    # The report reads it back from the stored row.
    row = _row(json.dumps(PROFILE), full={**stored, "status": "completed"})
    assert probe.score_row("schema", row, SOURCE)["latency_s"] == float(
        stored["metadata"][probe.LATENCY_KEY]
    )


def test_rows_stored_before_latency_report_none():
    assert probe.score_row("schema", _row(json.dumps(PROFILE)), SOURCE)["latency_s"] is None


def test_sample_file_lists_org_ids(tmp_path):
    path = tmp_path / "sample.json"
    path.write_text('{"source_schema": "s", "orgs": [{"org_id": "a"}, {"org_id": "b"}]}')
    assert probe.sample_org_ids(path) == ["a", "b"]


def test_baseline_rerun_route_asks_for_the_baseline_effort():
    route = probe.ROUTES["openai:gpt-5.4-mini@medium"]
    kwargs = probe.request_kwargs(route, None)
    assert route.model == probe.BASELINE_MODEL and kwargs["reasoning"] == {"effort": "medium"}
