import pytest
from unittest.mock import Mock, patch, AsyncMock
import aiohttp
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

def test_initialization(netzero_api, mock_session):
    assert netzero_api.username == "test_user"
    assert netzero_api.password == "test_pass"
    assert netzero_api.base_url == "https://20.108.20.67"
    assert netzero_api.session == mock_session

def test_authentication_success(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_session.post.return_value = mock_response

    netzero_api._authenticate()

    mock_session.post.assert_called_once_with(
        "https://20.108.20.67/security/formLogin",
        data={"username": "test_user", "password": "test_pass"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=True
    )

def test_authentication_failure(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Auth failed")
    mock_session.post.return_value = mock_response

    with pytest.raises(requests.exceptions.RequestException):
        netzero_api._authenticate()

def test_logout_success(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value = mock_response

    netzero_api.logout()

    mock_session.get.assert_called_once_with("https://20.108.20.67/security/logout")

def test_pagination(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.json.return_value = {
        "count": 3,
        "results": [{"id": 1}, {"id": 2}, {"id": 3}]
    }
    mock_response.raise_for_status.return_value = None
    mock_session.post.return_value = mock_response

    results = list(netzero_api._paginate(
        endpoint="test",
        payload={},
        page_size=100
    ))

    assert len(results) == 1
    assert results[0]["count"] == 3
    assert len(results[0]["results"]) == 3

def test_search_entities(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.json.return_value = {
        "count": 2,
        "results": [{"id": 1}, {"id": 2}]
    }
    mock_response.raise_for_status.return_value = None
    mock_session.post.return_value = mock_response

    result = netzero_api._search_entities(
        endpoint="companies",
        filter=MainFilter(),
        limit=2
    )

    assert result["total_count"] == 2
    assert result["count"] == 2
    assert len(result["results"]) == 2

@pytest.mark.asyncio
async def test_get_details_batch(netzero_api, mock_session):
    # Mock database session
    with patch('vdl_tools.shared_tools.database_cache.database_utils.get_session') as mock_get_session:
        mock_db_session = Mock()
        mock_get_session.return_value.__enter__.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.all.return_value = []

        # Mock aiohttp session
        mock_aiohttp_session = AsyncMock()
        mock_aiohttp_session.get.return_value.__aenter__.return_value.json = AsyncMock(
            return_value={"id": 1, "name": "Test Startup"}
        )
        mock_aiohttp_session.get.return_value.__aenter__.return_value.raise_for_status = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_aiohttp_session):
            results = await netzero_api._get_details_batch(
                ids=[1, 2],
                endpoint="getStartup",
                model_class=Startup
            )

            assert len(results) == 2
            assert results[0]["id"] == 1
            assert results[0]["name"] == "Test Startup"

def test_get_startup_detail(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.json.return_value = {"id": 1, "name": "Test Startup"}
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value = mock_response

    with patch('vdl_tools.shared_tools.database_cache.database_utils.get_session') as mock_get_session:
        mock_db_session = Mock()
        mock_get_session.return_value.__enter__.return_value = mock_db_session
        mock_db_session.query.return_value.filter.return_value.first.return_value = None

        result = netzero_api.get_startup_detail(startup_id=1)

        assert result["id"] == 1
        assert result["name"] == "Test Startup"

def test_cache_handling(netzero_api, mock_session):
    # Mock database session with cached result
    with patch('vdl_tools.shared_tools.database_cache.database_utils.get_session') as mock_get_session:
        mock_db_session = Mock()
        mock_get_session.return_value.__enter__.return_value = mock_db_session
        
        # Create a mock cached startup
        mock_cached_startup = Mock()
        mock_cached_startup.to_dict.return_value = {"id": 1, "name": "Cached Startup"}
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_cached_startup

        result = netzero_api.get_startup_detail(startup_id=1, read_from_cache=True)

        assert result["id"] == 1
        assert result["name"] == "Cached Startup"
        # Verify API was not called
        mock_session.get.assert_not_called()

def test_error_handling(netzero_api, mock_session):
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("API Error")
    mock_session.get.return_value = mock_response

    with pytest.raises(requests.exceptions.RequestException):
        netzero_api.get_startup_detail(startup_id=1) 