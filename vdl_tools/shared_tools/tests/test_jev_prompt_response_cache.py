"""Offline contract and cache tests for Jev through Vercel AI Gateway."""

import json
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy.dialects import postgresql

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt, PromptResponse
from vdl_tools.shared_tools.openai import prompt_response_cache_jev as jev
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL


QUESTIONS = {
    "group": {
        "type": "choice",
        "instructions": "Which industry group describes the company?",
        "criteria": {"01": "Agriculture", "60": "Banking"},
    }
}
ANSWER = {
    "model": "typesafe-ai/jev",
    "answers": {
        "group": {
            "type": "choice",
            "choice": "60",
            "confidence": 0.94,
            "probabilities": {"01": 0.06, "60": 0.94},
        }
    },
    "usage": {"input_tokens": 100, "output_tokens": 0},
}


@pytest.fixture
def cache(monkeypatch):
    monkeypatch.setattr(
        PromptResponseCacheSQL,
        "_set_prompt_obj",
        lambda self, **kwargs: Prompt(prompt_str=kwargs["prompt_str"]),
    )
    return jev.JevPromptResponseCacheSQL(session=MagicMock(), questions=QUESTIONS)


@pytest.fixture
def gateway(monkeypatch):
    calls = []
    monkeypatch.setattr(jev, "get_vercel_client", lambda: MagicMock(api_key="test-key"))

    def post(url, **kwargs):
        calls.append((url, kwargs))
        response = MagicMock()
        response.json.return_value = ANSWER
        return response

    monkeypatch.setattr(jev.requests, "post", post)
    return calls


def test_jev_wire_format_and_single_cache_write(cache, gateway):
    row = cache.get_cache_or_run("filing-1", "Company operates a bank.", read_from_cache=False)

    assert len(gateway) == 1
    url, request = gateway[0]
    assert url == "https://ai-gateway.vercel.sh/typesafe/v1/systemone"
    assert request == {
        "headers": {"Authorization": "Bearer test-key"},
        "json": {
            "model": "typesafe-ai/jev",
            "state": "Company operates a bank.",
            "questions": QUESTIONS,
        },
        "timeout": 120.0,
    }
    assert json.loads(row["response_text"]) == ANSWER["answers"]
    assert json.loads(row["response_full"]) == ANSWER
    assert row["request_kwargs"] == {"api_backend": "vercel"}
    cache.session.merge.assert_called_once()


def test_question_definition_and_state_are_cache_identity(cache):
    changed = json.loads(json.dumps(QUESTIONS))
    changed["group"]["criteria"]["60"] = "Depository institutions"
    other = jev.JevPromptResponseCacheSQL(session=MagicMock(), questions=changed)

    assert cache.prompt.id != other.prompt.id
    assert PromptResponse.create_text_id("state A") != PromptResponse.create_text_id("state B")
    assert cache.filter_by_model is True
    assert cache.model == "typesafe-ai/jev"
    assert cache.max_text_tokens is None


def test_cache_hit_reuses_typed_answers_without_gateway(cache, monkeypatch):
    response = jev._JevResponse(ANSWER)
    cached = PromptResponse(**cache._build_success_row("filing-1", "state", response, {}))
    cache.session.query.return_value.filter.return_value.order_by.return_value.first.return_value = cached
    monkeypatch.setattr(jev.requests, "post", lambda *a, **k: pytest.fail("Gateway called"))

    row = cache.get_cache_or_run("filing-1", "state")

    assert json.loads(row["response_text"])["group"]["confidence"] == 0.94
    cache.session.merge.assert_not_called()


def test_single_error_is_recorded_then_retried(cache, monkeypatch, gateway):
    error_response = MagicMock()
    error_response.raise_for_status.side_effect = requests.HTTPError("503 Service Unavailable")
    monkeypatch.setattr(jev.requests, "post", lambda *a, **k: error_response)
    cache.session.query.return_value.filter.return_value.first.return_value = None

    assert cache.get_cache_or_run("filing-1", "state", read_from_cache=False) is None
    failed = cache.session.merge.call_args.args[0]
    assert failed.num_errors == 1
    assert "503 Service Unavailable" in failed.response_full["message"]

    cache.session.query.return_value.filter.return_value.order_by.return_value.first.return_value = failed
    assert cache.get_prompt_response_obj("filing-1", "state") is None


def test_bulk_keeps_successes_and_records_http_errors(cache, gateway, monkeypatch):
    def post(url, **kwargs):
        if kwargs["json"]["state"] == "bad":
            response = MagicMock()
            response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")
            return response
        response = MagicMock()
        response.json.return_value = ANSWER
        return response

    monkeypatch.setattr(jev.requests, "post", post)
    statements = []
    cache.session.execute.side_effect = lambda stmt: statements.append(
        stmt.compile(dialect=postgresql.dialect())
    )

    result = cache.bulk_get_cache_or_run(
        [("good", "banking"), ("bad", "bad")],
        read_from_cache=False,
        max_workers=2,
    )

    assert set(result) == {"good"}
    assert json.loads(result["good"]["response_text"]) == ANSWER["answers"]
    assert len(statements) == 2
    assert {stmt.params["given_id_m0"] for stmt in statements} == {"good", "bad"}
    cache.session.commit.assert_called_once()


def test_rejects_invalid_configuration_and_unhandled_generation_options(cache):
    with pytest.raises(ValueError, match="at least one question"):
        jev.JevPromptResponseCacheSQL(session=MagicMock(), questions={})
    with pytest.raises(ValueError, match="timeout must be positive"):
        jev.JevPromptResponseCacheSQL(session=MagicMock(), questions=QUESTIONS, timeout=0)
    with pytest.raises(TypeError, match="Responses API options"):
        cache.get_completion("", "state", max_output_tokens=100)
