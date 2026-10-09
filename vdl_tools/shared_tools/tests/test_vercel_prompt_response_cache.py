"""Offline gateway contract tests using the real OpenAI SDK and a mock transport."""
import json
from contextlib import ExitStack
from threading import get_ident
from unittest.mock import MagicMock

import httpx
import pytest
from openai import OpenAI
from pydantic import BaseModel
from sqlalchemy.dialects import postgresql

from vdl_tools.shared_tools.openai import openai_api_utils as api
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse


class Answer(BaseModel):
    answer: str


def successful_response():
    return httpx.Response(200, json={
        "id": "resp_test", "object": "response", "created_at": 0,
        "model": "anthropic/claude-sonnet-4.6", "status": "completed",
        "output": [{"id": "msg_test", "type": "message", "role": "assistant",
                    "status": "completed", "content": [{"type": "output_text",
                    "text": '{"answer":"yes"}', "annotations": []}]}],
        "parallel_tool_calls": False, "tool_choice": "auto", "tools": [],
    })


@pytest.fixture
def gateway_transport(monkeypatch):
    """Exercise SDK HTTP/error handling without network access or real credentials."""
    with ExitStack() as stack:
        def install(handler):
            client = stack.enter_context(OpenAI(
                api_key="test-gateway-key", base_url=api.VERCEL_BASE_URL,
                max_retries=0,
                http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            ))
            monkeypatch.setattr(api, "get_vercel_client", lambda: client)
        yield install


@pytest.fixture
def requests(gateway_transport):
    seen = []

    def respond(request):
        seen.append(request)
        return successful_response()

    gateway_transport(respond)
    return seen


def make_cache(monkeypatch, **kwargs):
    monkeypatch.setattr(PromptResponseCacheSQL, "_set_prompt_obj",
                        lambda *a, **k: Prompt(prompt_str="Summarize"))
    return PromptResponseCacheSQL(
        session=MagicMock(), prompt_str="Summarize", filter_by_model=True,
        model="anthropic/claude-sonnet-4.6", api_backend="vercel", **kwargs,
    )


def test_gateway_wire_contract_and_structured_output(monkeypatch, requests):
    cache = make_cache(monkeypatch, model_provider="bedrock")
    assert cache.max_text_tokens is None
    response = cache.get_completion("Summarize", "hello", return_all=True,
                                    text_format=Answer, reasoning={"effort": "high"},
                                    temperature=0.2, max_output_tokens=123)
    assert response.output_parsed == Answer(answer="yes")
    request = requests[0]
    assert str(request.url) == api.VERCEL_BASE_URL + "/responses"
    assert request.headers["authorization"] == "Bearer test-gateway-key"
    body = json.loads(request.content)
    assert body["providerOptions"]["gateway"] == {"order": ["bedrock"]}
    assert body["reasoning"] == {"effort": "high"}
    assert body["temperature"] == 0.2
    assert body["max_output_tokens"] == 123
    assert body["text"]["format"]["type"] == "json_schema"
    assert "api_backend" not in body


def test_default_routing_single_and_bulk(monkeypatch, requests):
    cache = make_cache(monkeypatch)
    single = cache.get_cache_or_run("1", "hello", read_from_cache=False, write_to_cache=False)
    bulk = cache.bulk_get_cache_or_run([("2", "world")], read_from_cache=False, write_to_cache=False)
    assert single["response_text"] == bulk["2"]["response_text"] == '{"answer":"yes"}'
    assert single["request_kwargs"] == {"api_backend": "vercel"}
    assert all("providerOptions" not in json.loads(r.content) for r in requests)
    cache.session.commit.assert_not_called()


def test_provider_options_merge_and_cache_identity(monkeypatch, requests):
    cache = make_cache(monkeypatch, model_provider="bedrock")
    kwargs = {"extra_body": {"providerOptions": {
        "gateway": {"order": ["anthropic"], "only": ["anthropic"]},
        "anthropic": {"thinking": {"type": "enabled", "budgetTokens": 1024}},
    }}}
    original = json.dumps(kwargs)
    response = cache.get_completion("Summarize", "hello", return_all=True, **kwargs)
    row = cache._build_success_row("1", "hello", response, kwargs)
    error = cache._build_error_row("1", "hello", "error", kwargs)
    assert row["request_hash"] == error["request_hash"]
    assert json.loads(requests[0].content)["providerOptions"] == kwargs["extra_body"]["providerOptions"]
    assert json.dumps(kwargs) == original
    default_hash = cache._build_success_row("1", "hello", response, {})["request_hash"]
    assert row["request_hash"] != default_hash
    cache.model_provider = None
    assert cache._build_success_row("1", "hello", response, {})["request_hash"] != default_hash
    cache.get_prompt_response_obj("1", "hello", request_kwargs=kwargs)
    filters = cache.session.query.return_value.filter.call_args.args
    assert row["request_hash"] in [f.right.value for f in filters]
    assert PromptResponse.create_request_hash({}) == ""
    assert PromptResponse.create_request_hash({"timeout": 10}) == ""


def test_direct_openai_unchanged(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(api, "CLIENT", client)
    api.get_completion("Summarize", "gpt-5-mini", "hello", reasoning={"effort": "low"})
    client.responses.parse.assert_called_once_with(
        model="gpt-5-mini", instructions="Summarize", input="hello", reasoning={"effort": "low"})


def test_gateway_key_is_independent(monkeypatch):
    api.get_vercel_client.cache_clear()
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(api, "get_configuration", lambda: {})
    monkeypatch.delenv("AI_GATEWAY_API_KEY", raising=False)
    with pytest.raises(ValueError, match="AI_GATEWAY_API_KEY"):
        api.get_vercel_client()
    monkeypatch.setattr(api, "get_configuration", lambda: pytest.fail("Config should not be read when env key is set"))
    monkeypatch.setenv("AI_GATEWAY_API_KEY", "test-key")
    client = api.get_vercel_client()
    assert client.api_key == "test-key"
    assert str(client.base_url) == api.VERCEL_BASE_URL + "/"
    client.close()
    api.get_vercel_client.cache_clear()


@pytest.mark.parametrize("backend,provider", [("unknown", None), ("openai", "bedrock"), ("vercel", "")])
def test_invalid_routing(backend, provider):
    with pytest.raises(ValueError):
        api.completion_request_kwargs(backend, provider, {})


def test_cached_hit_avoids_gateway(monkeypatch, requests):
    cache = make_cache(monkeypatch, model_provider="bedrock")
    response = cache.get_completion("Summarize", "hello", return_all=True)
    row = cache._build_success_row("1", "hello", response, {"reasoning": {"effort": "high"}})
    cached = PromptResponse(**row)
    cache.session.query.return_value.filter.return_value.order_by.return_value.first.return_value = cached
    result = cache.get_cache_or_run("1", "hello", reasoning={"effort": "high"})
    assert result["request_hash"] == row["request_hash"]
    assert len(requests) == 1
    cache.get_prompt_response_obj_bulk([("1", "hello")], {"reasoning": {"effort": "high"}})
    filters = cache.session.query.return_value.filter.call_args.args
    assert row["request_hash"] in [getattr(f.right, "value", None) for f in filters]


def test_direct_non_reasoning_and_lazy_clients(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    lazy = api._LazyOpenAIClient()
    assert "_client" not in vars(lazy)
    with lazy._client as client:
        assert client.api_key == "test-openai-key"
        assert lazy.responses is client.responses
    client = MagicMock()
    monkeypatch.setattr(api, "CLIENT", client)
    api.get_completion("Summarize", "gpt-4.1-mini", "hello", temperature=0.1)
    client.responses.parse.assert_called_once_with(
        model="gpt-4.1-mini", input=[{"role": "system", "content": "Summarize"},
                                   {"role": "user", "content": "hello"}], temperature=0.1)


def test_invalid_model_and_backend_override(monkeypatch):
    with pytest.raises(ValueError, match="creator/model"):
        api.get_completion("Summarize", "claude", "hello", api_backend="vercel")
    with pytest.raises(ValueError, match="constructor"):
        api.completion_request_kwargs("openai", None, {"api_backend": "vercel"})


@pytest.mark.parametrize("store_results", [True, False])
def test_single_success_persistence(monkeypatch, requests, store_results):
    cache = make_cache(monkeypatch, model_provider="bedrock", store_results=store_results)
    result = cache.get_cache_or_run(
        "1", "hello", read_from_cache=False, reasoning={"effort": "high"})
    assert result["response_text"] == '{"answer":"yes"}'
    assert json.loads(result["response_full"])["id"] == "resp_test"
    assert len(requests) == 1
    if store_results:
        cache.session.merge.assert_called_once()
        stored = cache.session.merge.call_args.args[0]
        assert stored.to_dict() == result
        assert stored.request_kwargs == {
            "api_backend": "vercel", "reasoning": {"effort": "high"},
            "extra_body": {"providerOptions": {"gateway": {"order": ["bedrock"]}}},
        }
    else:
        cache.session.merge.assert_not_called()


@pytest.mark.parametrize("status", [400, 401, 429, 500])
@pytest.mark.parametrize("write_to_cache", [True, False])
def test_gateway_http_errors(monkeypatch, gateway_transport, status, write_to_cache):
    gateway_transport(lambda request: httpx.Response(
        status, json={"error": {"message": "Gateway rejected request", "type": "api_error"}}))
    cache = make_cache(monkeypatch, model_provider="bedrock")
    cache.session.query.return_value.filter.return_value.first.return_value = None
    result = cache.get_cache_or_run(
        "failed", "hello", read_from_cache=False, write_to_cache=write_to_cache,
        reasoning={"effort": "high"})
    assert result is None
    if write_to_cache:
        cache.session.merge.assert_called_once()
        stored = cache.session.merge.call_args.args[0]
        assert stored.given_id == "failed"
        assert stored.num_errors == 1
        assert "Gateway rejected request" in stored.response_full["message"]
        assert stored.request_kwargs["reasoning"] == {"effort": "high"}
        assert stored.request_kwargs["extra_body"]["providerOptions"]["gateway"]["order"] == ["bedrock"]
    else:
        cache.session.merge.assert_not_called()


def test_bulk_mixed_results_write_on_main_thread(monkeypatch, gateway_transport):
    worker_threads = []
    bodies = []

    def respond(request):
        worker_threads.append(get_ident())
        body = json.loads(request.content)
        bodies.append(body)
        if body["input"][-1]["content"] == "bad":
            return httpx.Response(400, json={"error": {"message": "Invalid input"}})
        return successful_response()

    gateway_transport(respond)
    cache = make_cache(monkeypatch, model_provider="bedrock")
    main_thread = get_ident()
    statements = []

    def execute(statement):
        assert get_ident() == main_thread
        statements.append(statement.compile(dialect=postgresql.dialect()))

    cache.session.execute.side_effect = execute
    result = cache.bulk_get_cache_or_run(
        [("good", "hello"), ("bad", "bad")], read_from_cache=False,
        max_workers=2, reasoning={"effort": "high"}, max_output_tokens=123)
    assert set(result) == {"good"}
    assert len(bodies) == 2
    assert all(t != main_thread for t in worker_threads)
    assert all(b["reasoning"] == {"effort": "high"} for b in bodies)
    assert all(b["providerOptions"]["gateway"]["order"] == ["bedrock"] for b in bodies)
    assert len(statements) == 2
    success, error = statements
    assert success.params["given_id_m0"] == "good"
    assert error.params["given_id_m0"] == "bad"
    assert error.params["num_errors_m0"] == 1
    assert success.params["request_hash_m0"] == error.params["request_hash_m0"] == result["good"]["request_hash"]
    assert success.params["request_kwargs_m0"] == error.params["request_kwargs_m0"]
    assert all("ON CONFLICT (prompt_id, given_id, model_name, request_hash)" in str(s) for s in statements)
    cache.session.commit.assert_called_once()


def test_bulk_fetches_only_missing_and_retryable_items(monkeypatch, requests):
    cache = make_cache(monkeypatch, model_provider="bedrock")
    response = cache.get_completion("Summarize", "cached text", return_all=True)
    cached = PromptResponse(**cache._build_success_row("cached", "cached text", response, {}))
    exhausted = PromptResponse(**cache._build_error_row("exhausted", "bad", "error", {}))
    retryable = PromptResponse(**cache._build_error_row("retryable", "retry", "error", {}))
    exhausted.num_errors = 2
    cache.session.query.return_value.filter.return_value.all.return_value = [cached, exhausted, retryable]
    requests.clear()
    result = cache.bulk_get_cache_or_run(
        [("cached", "cached text"), ("exhausted", "bad"),
         ("retryable", "retry"), ("new", "new text")], max_errors=2, write_to_cache=False)
    assert set(result) == {"cached", "retryable", "new"}
    assert {json.loads(r.content)["input"][-1]["content"] for r in requests} == {"retry", "new text"}
    cache.session.execute.assert_not_called()
    cache.session.commit.assert_not_called()


def test_single_cache_retries_a_stored_error(monkeypatch, requests):
    cache = make_cache(monkeypatch)
    failed = PromptResponse(**cache._build_error_row("failed", "retry", "error", {}))
    cache.session.query.return_value.filter.return_value.order_by.return_value.first.return_value = failed

    result = cache.get_cache_or_run("failed", "retry", write_to_cache=False)

    assert result["response_text"] == '{"answer":"yes"}'
    assert len(requests) == 1


def test_bulk_retries_a_stored_error_by_default(monkeypatch, requests):
    cache = make_cache(monkeypatch)
    failed = PromptResponse(**cache._build_error_row("failed", "retry", "error", {}))
    cache.session.query.return_value.filter.return_value.all.return_value = [failed]

    result = cache.bulk_get_cache_or_run([("failed", "retry")], write_to_cache=False)

    assert result["failed"]["response_text"] == '{"answer":"yes"}'
    assert len(requests) == 1

    requests.clear()
    failed.num_errors = 2
    assert "failed" in cache.bulk_get_cache_or_run(
        [("failed", "retry")], write_to_cache=False
    )
    assert len(requests) == 1

    requests.clear()
    failed.num_errors = 3
    assert cache.bulk_get_cache_or_run([("failed", "retry")], write_to_cache=False) == {}
    assert requests == []

    failed.num_errors = 1
    assert cache.bulk_get_cache_or_run(
        [("failed", "retry")], max_errors=1, write_to_cache=False
    ) == {}
    assert requests == []


@pytest.mark.parametrize("kwargs", [
    {},
    {"timeout": 30, "text_format": Answer},
    {"reasoning": {"effort": "high"}, "temperature": 0.2, "max_output_tokens": 1000},
    {"tools": [{"type": "web_search_preview"}], "tool_choice": "auto", "seed": 42},
])
def test_existing_openai_cache_hashes_are_unchanged(kwargs):
    # Freeze the pre-gateway allowlist and algorithm independently of the
    # production allowlist so future additions cannot silently weaken this test.
    import hashlib

    previous_keys = {
        "reasoning", "tools", "tool_choice", "temperature", "top_p",
        "max_output_tokens", "seed", "service_tier",
    }
    previous_kwargs = {k: v for k, v in kwargs.items() if k in previous_keys}
    expected = (
        hashlib.md5(json.dumps(previous_kwargs, sort_keys=True).encode()).hexdigest()
        if previous_kwargs else ""
    )
    actual_kwargs = api.completion_request_kwargs("openai", None, kwargs)
    assert "api_backend" not in actual_kwargs
    assert PromptResponse.create_request_hash(actual_kwargs) == expected


def test_existing_extra_body_calls_intentionally_get_new_hash():
    assert PromptResponse.create_request_hash({"extra_body": {"custom_parameter": 1}}) != ""


@pytest.mark.parametrize("use_config_path", [False, True])
def test_gateway_key_from_config_file(monkeypatch, tmp_path, use_config_path):
    from vdl_tools.shared_tools.tools.config_utils import get_configuration

    config_path = tmp_path / ("custom.ini" if use_config_path else "config.ini")
    config_path.write_text("[vercel]\nvercel_api_key = test-config-key\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("AI_GATEWAY_API_KEY", raising=False)
    monkeypatch.delenv("VDL_GLOBAL_CONFIG_PATH", raising=False)
    if use_config_path:
        monkeypatch.setenv("VDL_GLOBAL_CONFIG_PATH", str(config_path))
    monkeypatch.setattr(api, "get_configuration", get_configuration)
    api.get_vercel_client.cache_clear()
    try:
        with api.get_vercel_client() as client:
            assert client.api_key == "test-config-key"
            assert str(client.base_url) == api.VERCEL_BASE_URL + "/"
    finally:
        api.get_vercel_client.cache_clear()


@pytest.mark.parametrize("config", [{}, {"vercel": {}}, {"vercel": {"vercel_api_key": "  "}}])
def test_gateway_missing_config_key_has_actionable_error(monkeypatch, config):
    monkeypatch.delenv("AI_GATEWAY_API_KEY", raising=False)
    monkeypatch.setattr(api, "get_configuration", lambda: config)
    api.get_vercel_client.cache_clear()
    with pytest.raises(ValueError, match=r"AI_GATEWAY_API_KEY or \[vercel\] vercel_api_key"):
        api.get_vercel_client()
