"""Circuit breaker tests, executed using mocked HTTP and a mocked SQL session."""

import json
from threading import Lock, get_ident
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy.dialects import postgresql

from vdl_tools.shared_tools.database_cache.database_models.prompt import Prompt
from vdl_tools.shared_tools.openai import prompt_response_cache_jev as jev
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    BulkRequestAborted,
    PromptResponseCacheSQL,
)

ANSWER = {"answers": {"node": {"noul": 0.8}}}


@pytest.fixture
def cache(monkeypatch):
    monkeypatch.setattr(PromptResponseCacheSQL, "_set_prompt_obj", lambda self, **kw: Prompt(prompt_str=kw["prompt_str"]))
    monkeypatch.setattr(jev, "get_vercel_client", lambda: MagicMock(api_key="test"))
    monkeypatch.setattr(jev.time, "sleep", lambda seconds: None)
    return jev.JevPromptResponseCacheSQL(
        session=MagicMock(), questions={"node": {"type": "noul", "instructions": "Solar?"}},
        max_server_errors=3,
    )


def response(status):
    result = requests.Response()
    result.status_code = status
    result.url = jev.JEV_GATEWAY_URL
    result._content = json.dumps(ANSWER).encode()
    return result


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_circuit_stops_retries_and_subsequent_calls(cache, monkeypatch, status):
    calls = []
    monkeypatch.setattr(jev.requests, "post", lambda *a, **kw: calls.append(kw) or response(status))
    with pytest.raises(BulkRequestAborted, match="3 HTTP 5xx"):
        cache.get_completion("", "solar")
    assert len(calls) == 3
    with pytest.raises(BulkRequestAborted):
        cache.get_completion("", "another activity")
    assert len(calls) == 3


def test_bulk_commits_successes_and_does_not_cache_unissued_inputs(cache, monkeypatch):
    cache.max_retries = 0
    cache.max_server_errors = 2
    calls = []
    statements = []
    main_thread = get_ident()

    def post(*args, **kwargs):
        calls.append(kwargs["json"]["state"])
        return response(200 if len(calls) == 1 else 503)

    def execute(statement):
        assert get_ident() == main_thread
        statements.append(statement.compile(dialect=postgresql.dialect()))

    monkeypatch.setattr(jev.requests, "post", post)
    cache.session.execute.side_effect = execute
    with pytest.raises(BulkRequestAborted, match="2 HTTP 5xx"):
        cache.bulk_get_cache_or_run(
            [(str(i), str(i)) for i in range(8)], read_from_cache=False,
            max_workers=1, n_per_commit=4,
        )
    assert len(calls) == 3  # One success, two 503s; queued inputs and later chunks stop.
    cache.session.commit.assert_called_once()
    assert len(statements) == 2
    assert {stmt.params["given_id_m0"] for stmt in statements} == set(calls[:2])
    assert statements[0].params["response_text_m0"] == json.dumps(ANSWER["answers"])


def test_success_already_in_flight_is_committed_after_circuit_opens(cache, monkeypatch):
    cache.max_retries = 0
    cache.max_server_errors = 1
    lock, calls = Lock(), []

    def post(*args, **kwargs):
        with lock:
            calls.append(kwargs["json"]["state"])
            index = len(calls)
        if index == 1:
            assert cache._circuit_open.wait(5)
            return response(200)
        return response(503)

    monkeypatch.setattr(jev.requests, "post", post)
    statements = []
    cache.session.execute.side_effect = lambda statement: statements.append(statement.compile(dialect=postgresql.dialect()))
    with pytest.raises(BulkRequestAborted):
        cache.bulk_get_cache_or_run(
            [(str(i), str(i)) for i in range(10)], read_from_cache=False, max_workers=2,
        )
    assert len(calls) == 2
    cache.session.commit.assert_called_once()
    assert len(statements) == 1
    assert statements[0].params["given_id_m0"] == calls[0]
    assert statements[0].params["response_text_m0"] == json.dumps(ANSWER["answers"])


@pytest.mark.parametrize("status", [400, 429])
def test_client_and_rate_limit_errors_do_not_trip_5xx_circuit(cache, monkeypatch, status):
    cache.max_retries = 0
    cache.max_server_errors = 1
    monkeypatch.setattr(jev.requests, "post", lambda *a, **kw: response(status))
    with pytest.raises(requests.HTTPError):
        cache.get_completion("", "solar")
    assert not cache._circuit_open.is_set()


def test_triggering_5xx_logs_gateway_details_before_abort(cache, monkeypatch, caplog):
    monkeypatch.setattr(jev.logger, "handlers", [caplog.handler])
    cache.max_server_errors = 1
    failed = response(503)
    failed.headers["x-vercel-id"] = "vercel-test-id"
    failed.headers["x-request-id"] = "request-test-id"
    failed._content = b'{"message":"provider unavailable","error_type":"upstream_error"}'
    monkeypatch.setattr(jev.requests, "post", lambda *a, **kw: failed)
    with pytest.raises(BulkRequestAborted):
        cache.get_completion("", "private activity text")
    assert "HTTP 503 (attempt 1/3" in caplog.text
    assert "vercel-test-id" in caplog.text
    assert "request-test-id" in caplog.text
    assert "provider unavailable" in caplog.text
    assert "private activity text" not in caplog.text
