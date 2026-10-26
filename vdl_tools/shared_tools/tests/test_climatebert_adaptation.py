"""Tests for climatebert_adaptation.ensure_deployment_active and
AdaptationMitigationCache.bulk_predict's deployment check and parallel chunks.

Baseten's management API is faked: each GET of the production deployment
returns the next scripted status, and activation POSTs are recorded.
"""

import threading

import pytest
import requests

from vdl_tools.shared_tools import climatebert_adaptation as adp


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


@pytest.fixture()
def baseten(monkeypatch):
    """Returns (statuses, calls): append statuses to script the GETs; the
    last one repeats once the script runs out."""
    statuses = []
    calls = {"get": 0, "activate": []}

    def fake_get(url, headers, timeout):
        assert url == "https://api.baseten.co/v1/models/m1/deployments/production"
        status = statuses[min(calls["get"], len(statuses) - 1)]
        calls["get"] += 1
        return _Resp({"id": "dep1", "status": status})

    def fake_post(url, headers, json, timeout):
        calls["activate"].append(url)
        return _Resp({"success": True, "no_op": False})

    monkeypatch.setattr(adp.requests, "get", fake_get)
    monkeypatch.setattr(adp.requests, "post", fake_post)
    monkeypatch.setattr(adp.time, "sleep", lambda seconds: None)
    return statuses, calls


@pytest.mark.parametrize("status", ["ACTIVE", "SCALED_TO_ZERO", "WAKING_UP"])
def test_live_deployment_is_left_alone(baseten, status):
    statuses, calls = baseten
    statuses.append(status)
    assert adp.ensure_deployment_active("m1", "key")["status"] == status
    assert calls["activate"] == []
    assert calls["get"] == 1


def test_inactive_deployment_is_activated_once_and_awaited(baseten):
    statuses, calls = baseten
    statuses.extend(["INACTIVE", "INACTIVE", "LOADING_MODEL", "ACTIVE"])
    assert adp.ensure_deployment_active("m1", "key")["status"] == "ACTIVE"
    assert calls["activate"] == ["https://api.baseten.co/v1/models/m1/deployments/dep1/activate"]
    assert calls["get"] == 4


def test_deactivating_deployment_is_activated_once_inactive(baseten):
    statuses, calls = baseten
    statuses.extend(["DEACTIVATING", "INACTIVE", "DEPLOYING", "SCALED_TO_ZERO"])
    assert adp.ensure_deployment_active("m1", "key")["status"] == "SCALED_TO_ZERO"
    assert len(calls["activate"]) == 1


@pytest.mark.parametrize("status", ["FAILED", "DEPLOY_FAILED", "UNHEALTHY"])
def test_broken_deployment_raises(baseten, status):
    statuses, calls = baseten
    statuses.append(status)
    with pytest.raises(RuntimeError, match=status):
        adp.ensure_deployment_active("m1", "key")
    assert calls["activate"] == []


def test_deployment_that_never_comes_up_times_out(baseten):
    statuses, calls = baseten
    statuses.append("INACTIVE")
    with pytest.raises(TimeoutError, match="still INACTIVE"):
        adp.ensure_deployment_active("m1", "key", timeout=0)
    assert len(calls["activate"]) == 1


def _bare_cache():
    """An AdaptationMitigationCache without a database session; only safe
    with read_from_cache=False."""
    cache = adp.AdaptationMitigationCache.__new__(adp.AdaptationMitigationCache)
    cache.model = "m1"
    cache.api_key = "key"
    return cache


def test_dead_deployment_stops_bulk_predict_before_any_error_rows(monkeypatch):
    cache = _bare_cache()

    def dead(model_id, api_key):
        raise RuntimeError("deployment is FAILED")

    def must_not_run(*args, **kwargs):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(adp, "ensure_deployment_active", dead)
    monkeypatch.setattr(cache, "_post_batch", must_not_run, raising=False)
    monkeypatch.setattr(cache, "_upsert_error_rows", must_not_run, raising=False)

    with pytest.raises(RuntimeError, match="FAILED"):
        cache.bulk_predict([("org1", "some text")], read_from_cache=False)


class _Session:
    def commit(self):
        _assert_main_thread()


def _assert_main_thread():
    assert threading.current_thread() is threading.main_thread()


@pytest.fixture()
def parallel_cache(monkeypatch):
    """A bare cache against a fake 3-replica deployment. Saved and errored
    rows are recorded and must be written from the main thread."""
    cache = _bare_cache()
    cache.session = _Session()
    saved, errored = [], []

    def build_success_row(given_id, text, completion, request_kwargs):
        return given_id, completion.prediction

    def build_error_row(given_id, text, response_full, request_kwargs):
        return given_id

    def upsert_success_rows(rows):
        _assert_main_thread()
        saved.extend(rows)

    def upsert_error_rows(rows):
        _assert_main_thread()
        errored.extend(rows)

    monkeypatch.setattr(
        adp, "ensure_deployment_active",
        lambda model_id, api_key: {"status": "ACTIVE", "autoscaling_settings": {"max_replica": 3}},
    )
    monkeypatch.setattr(cache, "_build_success_row", build_success_row, raising=False)
    monkeypatch.setattr(cache, "_build_error_row", build_error_row, raising=False)
    monkeypatch.setattr(cache, "_upsert_success_rows", upsert_success_rows, raising=False)
    monkeypatch.setattr(cache, "_upsert_error_rows", upsert_error_rows, raising=False)
    return cache, saved, errored


def test_chunks_run_up_to_max_replica_at_a_time(monkeypatch, parallel_cache):
    cache, saved, errored = parallel_cache
    # Each chunk waits until two others are in flight with it, so this only
    # finishes if three chunks really run at once.
    barrier = threading.Barrier(3, timeout=5)
    lock = threading.Lock()
    in_flight = {"now": 0, "max": 0}

    def post_batch(chunk):
        with lock:
            in_flight["now"] += 1
            in_flight["max"] = max(in_flight["max"], in_flight["now"])
        barrier.wait()
        with lock:
            in_flight["now"] -= 1
        return [f"label-{given_id}" for given_id, _ in chunk]

    monkeypatch.setattr(cache, "_post_batch", post_batch, raising=False)
    orgs = [(f"org{i}", f"text {i}") for i in range(12)]

    res = cache.bulk_predict(orgs, chunk_size=2, read_from_cache=False)

    assert res == {f"org{i}": f"label-org{i}" for i in range(12)}
    assert in_flight["max"] == 3
    assert sorted(saved) == sorted(res.items())
    assert errored == []


def test_one_failed_chunk_does_not_lose_the_others(monkeypatch, parallel_cache):
    cache, saved, errored = parallel_cache

    def post_batch(chunk):
        if any(given_id == "org0" for given_id, _ in chunk):
            raise Exception("Request failed with status code 500")
        return ["mitigation"] * len(chunk)

    monkeypatch.setattr(cache, "_post_batch", post_batch, raising=False)
    orgs = [(f"org{i}", f"text {i}") for i in range(6)]

    res = cache.bulk_predict(orgs, chunk_size=1, read_from_cache=False)

    assert errored == ["org0"]
    assert set(res) == {f"org{i}" for i in range(1, 6)}
    assert len(saved) == 5


def test_nothing_to_run_skips_the_check(monkeypatch):
    cache = _bare_cache()

    def must_not_run(*args, **kwargs):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(adp, "ensure_deployment_active", must_not_run)
    assert cache.bulk_predict([], read_from_cache=False) == {}
