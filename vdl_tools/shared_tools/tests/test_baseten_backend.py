"""Offline contract tests for the Baseten Model APIs backend, mirroring the Vercel ones."""

import json
from contextlib import ExitStack
from unittest.mock import MagicMock

import httpx
import pytest
from openai import OpenAI

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.openai import openai_api_utils as api
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

MODEL = "deepseek-ai/DeepSeek-V4-Flash-0731"


def successful_response():
    return httpx.Response(
        200,
        json={
            "id": "resp_test",
            "object": "response",
            "created_at": 0,
            "model": MODEL,
            "status": "completed",
            "output": [
                {
                    "id": "msg_test",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {"type": "output_text", "text": '{"answer":"yes"}', "annotations": []}
                    ],
                }
            ],
            "parallel_tool_calls": False,
            "tool_choice": "auto",
            "tools": [],
        },
    )


@pytest.fixture
def baseten_requests(monkeypatch):
    """Capture requests sent to a Baseten client, without network or credentials."""
    seen = []

    def respond(request):
        seen.append(request)
        return successful_response()

    with ExitStack() as stack:
        client = stack.enter_context(
            OpenAI(
                api_key="test-baseten-key",
                base_url=api.BASETEN_BASE_URL,
                max_retries=0,
                http_client=httpx.Client(transport=httpx.MockTransport(respond)),
            )
        )
        monkeypatch.setattr(api, "get_baseten_client", lambda: client)
        monkeypatch.setattr(api, "CLIENT", MagicMock())
        yield seen


def make_cache(monkeypatch, **kwargs):
    monkeypatch.setattr(
        PromptResponseCacheSQL, "_set_prompt_obj", lambda *a, **k: Prompt(prompt_str="Summarize")
    )
    return PromptResponseCacheSQL(
        session=MagicMock(),
        prompt_str="Summarize",
        filter_by_model=True,
        model=MODEL,
        api_backend="baseten",
        **kwargs,
    )


def test_baseten_calls_go_to_baseten_not_openai(monkeypatch, baseten_requests):
    cache = make_cache(monkeypatch)
    response = cache.get_completion("Summarize", "text", return_all=True, max_output_tokens=50)
    (request,) = baseten_requests
    body = json.loads(request.content)
    assert str(request.url) == api.BASETEN_BASE_URL + "/responses"
    assert request.headers["authorization"] == "Bearer test-baseten-key"
    assert body["model"] == MODEL and body["max_output_tokens"] == 50
    assert "api_backend" not in body
    assert response.output_text == '{"answer":"yes"}'
    api.CLIENT.responses.parse.assert_not_called()


def test_baseten_skips_openai_context_window(monkeypatch):
    # tiktoken counts and MODEL_DATA limits are OpenAI's; Baseten enforces its own.
    assert make_cache(monkeypatch).max_text_tokens is None


def test_baseten_rows_never_share_a_hash_with_openai_or_vercel():
    kwargs = {"max_output_tokens": 100}
    hashes = {
        backend: PromptResponse.create_request_hash(
            api.completion_request_kwargs(backend, None, kwargs)
        )
        for backend in api.API_BACKENDS
    }
    assert len(set(hashes.values())) == len(api.API_BACKENDS)
    # Direct OpenAI keeps its legacy hash; the others carry their backend.
    assert "api_backend" not in api.completion_request_kwargs("openai", None, kwargs)
    assert api.completion_request_kwargs("baseten", None, kwargs)["api_backend"] == "baseten"


def test_model_provider_is_vercel_only():
    with pytest.raises(ValueError, match="model_provider requires api_backend='vercel'"):
        api.completion_request_kwargs("baseten", "deepinfra", {})


def test_unknown_backend_is_rejected():
    with pytest.raises(ValueError, match="api_backend must be one of"):
        api.get_completion("p", "m", "t", api_backend="together")


def test_baseten_key_from_config(monkeypatch):
    monkeypatch.delenv("BASETEN_API_KEY", raising=False)
    monkeypatch.setattr(api, "get_configuration", lambda: {"baseten": {"api_key": " test-key "}})
    api.get_baseten_client.cache_clear()
    try:
        with api.get_baseten_client() as client:
            assert client.api_key == "test-key"
            assert str(client.base_url) == api.BASETEN_BASE_URL + "/"
    finally:
        api.get_baseten_client.cache_clear()


@pytest.mark.parametrize("config", [{}, {"baseten": {}}, {"baseten": {"api_key": "  "}}])
def test_baseten_missing_key_has_actionable_error(monkeypatch, config):
    monkeypatch.delenv("BASETEN_API_KEY", raising=False)
    monkeypatch.setattr(api, "get_configuration", lambda: config)
    api.get_baseten_client.cache_clear()
    try:
        with pytest.raises(ValueError, match=r"BASETEN_API_KEY or \[baseten\] api_key"):
            api.get_baseten_client()
    finally:
        api.get_baseten_client.cache_clear()
