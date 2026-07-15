import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
import requests

from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI
from vdl_tools.scrape_enrich.netzero_insights.filters import MainFilter, Sorting
from vdl_tools.shared_tools.database_cache.database_models import Startup


@pytest.fixture
def mock_session():
    with patch('requests.Session') as mock:
        session = Mock()
        mock.return_value = session
        yield session


@pytest.fixture
def netzero_api(mock_session):
    return NetZeroAPI(
        username="test_user",
        password="test_pass",
        use_sandbox=True
    )


def _response(payload=None, status=200):
    resp = Mock()
    resp.status_code = status
    resp.json.return_value = payload
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _search_responder(total, key="clientID", dataset=None):
    """Fake `session.request` that serves offset/limit slices of a dataset."""
    dataset = dataset if dataset is not None else [{key: i} for i in range(total)]

    def responder(method, url, **kwargs):
        payload = kwargs.get("json") or {}
        offset = payload.get("offset", 0)
        limit = payload.get("limit", 100)
        return _response({"count": total, "results": dataset[offset:offset + limit]})

    return responder


def test_initialization(netzero_api, mock_session):
    assert netzero_api.username == "test_user"
    assert netzero_api.password == "test_pass"
    assert netzero_api.base_url == "https://20.108.20.67"
    assert netzero_api.session == mock_session


def test_authentication_success(netzero_api, mock_session):
    mock_session.post.reset_mock()
    mock_session.post.return_value = _response()

    netzero_api._authenticate()

    mock_session.post.assert_called_once_with(
        "https://20.108.20.67/security/formLogin",
        data={"username": "test_user", "password": "test_pass"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
    )


def test_authentication_failure(netzero_api, mock_session):
    mock_session.post.return_value = _response(status=401)

    with pytest.raises(requests.exceptions.RequestException):
        netzero_api._authenticate()


def test_logout_success(netzero_api, mock_session):
    mock_session.get.return_value = _response()

    netzero_api.logout()

    mock_session.get.assert_called_once_with(
        "https://20.108.20.67/security/logout",
        verify=False,
    )


def test_search_entities_single_request(netzero_api, mock_session):
    mock_session.request.side_effect = _search_responder(total=2)

    result = netzero_api._search_entities(
        endpoint="companies",
        filter=MainFilter(),
        limit=2,
    )

    assert result["total_count"] == 2
    assert result["count"] == 2
    assert len(result["results"]) == 2
    # limit <= page_size goes out as one request
    assert mock_session.request.call_count == 1


def test_search_entities_no_filter(netzero_api, mock_session):
    mock_session.request.side_effect = _search_responder(total=1)

    result = netzero_api._search_entities(endpoint="companies", limit=1)

    assert result["count"] == 1


def test_search_entities_limit_not_truncated_to_page_multiple(netzero_api, mock_session):
    # limit=250 previously returned only 200 results (limit // page_size pages)
    mock_session.request.side_effect = _search_responder(total=400)

    result = netzero_api._search_entities(
        endpoint="companies",
        filter=MainFilter(),
        limit=250,
        page_size=100,
    )

    assert result["total_count"] == 400
    assert result["count"] == 250
    assert [r["clientID"] for r in result["results"]] == list(range(250))
    assert mock_session.request.call_count == 3


def test_search_entities_fetches_all_pages_concurrently(netzero_api, mock_session):
    mock_session.request.side_effect = _search_responder(total=250)

    result = netzero_api._search_entities(
        endpoint="companies",
        filter=MainFilter(),
        limit=None,
        page_size=100,
    )

    assert result["count"] == 250
    assert [r["clientID"] for r in result["results"]] == list(range(250))
    assert mock_session.request.call_count == 3


def test_search_entities_dedupes_by_unique_key(netzero_api, mock_session):
    # Page boundary shift: id 99 appears on both page 1 and page 2
    dataset = [{"clientID": i} for i in range(100)] + [{"clientID": 99}] + [
        {"clientID": i} for i in range(100, 149)
    ]
    mock_session.request.side_effect = _search_responder(total=150, dataset=dataset)

    result = netzero_api._search_entities(
        endpoint="companies",
        filter=MainFilter(),
        page_size=100,
        unique_key="clientID",
    )

    client_ids = [r["clientID"] for r in result["results"]]
    assert len(client_ids) == len(set(client_ids))
    assert result["count"] == 149


def test_retry_on_500(netzero_api, mock_session):
    mock_session.request.side_effect = [
        _response(status=500),
        _response({"count": 1, "results": [{"clientID": 1}]}),
    ]

    with patch("vdl_tools.scrape_enrich.netzero_insights.netzero_api.time.sleep"):
        result = netzero_api._search_entities(endpoint="companies", limit=1)

    assert result["count"] == 1
    assert mock_session.request.call_count == 2


def test_reauth_on_401(netzero_api, mock_session):
    mock_session.post.reset_mock()
    mock_session.post.return_value = _response()  # the re-login call
    mock_session.request.side_effect = [
        _response(status=401),
        _response({"count": 1, "results": [{"clientID": 1}]}),
    ]

    result = netzero_api._search_entities(endpoint="companies", limit=1)

    assert result["count"] == 1
    # 401 triggered exactly one re-login before the retry succeeded
    mock_session.post.assert_called_once()
    assert netzero_api._auth_generation == 1


def test_persistent_401_raises(netzero_api, mock_session):
    mock_session.post.return_value = _response()
    mock_session.request.side_effect = [_response(status=401)] * 5

    with pytest.raises(requests.exceptions.HTTPError):
        netzero_api._search_entities(endpoint="companies", limit=1)


class TestSearchCheckpoint:

    def _run_search(self, netzero_api, tmp_path, **kwargs):
        return netzero_api._search_entities(
            endpoint="companies",
            filter=MainFilter(),
            sorting=Sorting(field="clientID"),
            page_size=100,
            checkpoint_dir=str(tmp_path),
            **kwargs,
        )

    def test_checkpoint_writes_pages_and_meta(self, netzero_api, mock_session, tmp_path):
        mock_session.request.side_effect = _search_responder(total=250)

        result = self._run_search(netzero_api, tmp_path)

        assert result["count"] == 250
        search_dirs = list(tmp_path.iterdir())
        assert len(search_dirs) == 1
        files = {f.name for f in search_dirs[0].iterdir()}
        assert "meta.json" in files
        assert len([f for f in files if f.startswith("page_")]) == 3

    def test_full_resume_makes_no_requests(self, netzero_api, mock_session, tmp_path):
        mock_session.request.side_effect = _search_responder(total=250)
        first = self._run_search(netzero_api, tmp_path)

        mock_session.request.reset_mock()
        mock_session.request.side_effect = AssertionError("should not hit the API")
        resumed = self._run_search(netzero_api, tmp_path)

        assert resumed["results"] == first["results"]
        assert mock_session.request.call_count == 0

    def test_partial_resume_fetches_only_missing_pages(self, netzero_api, mock_session, tmp_path):
        mock_session.request.side_effect = _search_responder(total=250)
        first = self._run_search(netzero_api, tmp_path)

        # Simulate dying before the middle page was persisted
        search_dir = next(tmp_path.iterdir())
        (search_dir / "page_000000100.json.gz").unlink()

        mock_session.request.reset_mock()
        mock_session.request.side_effect = _search_responder(total=250)
        resumed = self._run_search(netzero_api, tmp_path)

        assert resumed["results"] == first["results"]
        assert mock_session.request.call_count == 1

    def test_stale_checkpoint_is_refetched(self, netzero_api, mock_session, tmp_path):
        mock_session.request.side_effect = _search_responder(total=250)
        self._run_search(netzero_api, tmp_path)

        meta_path = next(tmp_path.iterdir()) / "meta.json"
        meta = json.loads(meta_path.read_text())
        meta["created_at"] = (
            datetime.now(timezone.utc) - timedelta(days=10)
        ).isoformat()
        meta_path.write_text(json.dumps(meta))

        mock_session.request.reset_mock()
        mock_session.request.side_effect = _search_responder(total=250)
        result = self._run_search(netzero_api, tmp_path)

        assert result["count"] == 250
        assert mock_session.request.call_count == 3

    def test_different_query_uses_different_checkpoint(self, netzero_api, mock_session, tmp_path):
        mock_session.request.side_effect = _search_responder(total=250)
        self._run_search(netzero_api, tmp_path)

        mock_session.request.reset_mock()
        mock_session.request.side_effect = _search_responder(total=250)
        netzero_api._search_entities(
            endpoint="companies",
            filter=MainFilter(include={"name": "different"}),
            sorting=Sorting(field="clientID"),
            page_size=100,
            checkpoint_dir=str(tmp_path),
        )

        # New query hash -> fresh fetch, second checkpoint dir
        assert mock_session.request.call_count == 3
        assert len(list(tmp_path.iterdir())) == 2


class _FakeAiohttpResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload

    async def text(self):
        return json.dumps(self._payload)


class _FakeCM:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *args):
        return False


def test_get_details_batch(netzero_api, mock_session):
    mock_session.cookies.get_dict.return_value = {"JSESSIONID": "abc"}

    fake_http_session = Mock()
    fake_http_session.get = Mock(
        side_effect=lambda url, **kwargs: _FakeCM(
            _FakeAiohttpResponse({"clientID": int(url.rsplit("/", 1)[1]), "name": "Test"})
        )
    )

    with patch("aiohttp.ClientSession", return_value=_FakeCM(fake_http_session)):
        results = asyncio.run(
            netzero_api._get_details_batch(
                ids=[1, 2],
                endpoint="getStartup",
                model_class=Startup,
                read_from_cache=False,
                write_to_cache=False,
            )
        )

    assert len(results) == 2
    assert {r["clientID"] for r in results} == {1, 2}
    assert all(r["fullData"]["name"] == "Test" for r in results)


def test_get_details_batch_reauths_on_401(netzero_api, mock_session):
    mock_session.cookies.get_dict.return_value = {"JSESSIONID": "abc"}
    mock_session.post.reset_mock()
    mock_session.post.return_value = _response()  # re-login

    calls = {"n": 0}

    def get_side_effect(url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeCM(_FakeAiohttpResponse({}, status=401))
        return _FakeCM(_FakeAiohttpResponse({"clientID": 1, "name": "Test"}))

    fake_http_session = Mock()
    fake_http_session.get = Mock(side_effect=get_side_effect)

    with patch("aiohttp.ClientSession", return_value=_FakeCM(fake_http_session)):
        results = asyncio.run(
            netzero_api._get_details_batch(
                ids=[1],
                endpoint="getStartup",
                model_class=Startup,
                read_from_cache=False,
                write_to_cache=False,
            )
        )

    assert len(results) == 1
    mock_session.post.assert_called_once()
