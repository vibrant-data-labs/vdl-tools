"""Tests for the NZI v2 API path: auth, pagination, filters and field aliasing."""

from unittest.mock import Mock, patch

import pytest
import requests

from vdl_tools.scrape_enrich.netzero_insights import api_v2
from vdl_tools.scrape_enrich.netzero_insights.filters import (
    InvestorFilter,
    MainFilter,
    Sorting,
    StartupFilter,
)
from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI


TOKEN = "eyJhbGciOi.TESTTOKEN.signature"


def _response(payload=None, status=200, headers=None):
    resp = Mock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.json.return_value = payload
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


@pytest.fixture
def mock_session():
    with patch('requests.Session') as mock:
        session = Mock()
        session.cookies.get_dict.return_value = {}
        session.post.return_value = _response({"access_token": TOKEN})
        mock.return_value = session
        yield session


@pytest.fixture
def api_v2_client(mock_session):
    return NetZeroAPI(
        username="user@example.com",
        password="test_pass",
        api_version="v2",
    )


# --------------------------------------------------------------------------
# Client configuration
# --------------------------------------------------------------------------

def test_v2_uses_new_base_url_and_verifies_ssl(api_v2_client):
    assert api_v2_client.base_url == "https://api-new.netzeroinsights.com"
    # The v1 sandbox was a bare IP with a dead cert; both v2 hosts are real.
    assert api_v2_client.verify_ssl is True


def test_v2_sandbox_uses_stage_host(mock_session):
    client = NetZeroAPI("user@example.com", "pw", use_sandbox=True, api_version="v2")
    assert client.base_url == "https://api-stage.netzeroinsights.com"
    assert client.verify_ssl is True


def test_v2_only_reauthenticates_on_401(api_v2_client):
    # v2 documents 403 as "insufficient access level" — re-login cannot fix it.
    assert api_v2_client.auth_status_codes == frozenset({401})


def test_invalid_api_version_rejected(mock_session):
    with pytest.raises(ValueError, match="api_version"):
        NetZeroAPI("u", "p", api_version="v3")


# --------------------------------------------------------------------------
# Authentication
# --------------------------------------------------------------------------

def test_v2_login_posts_credentials_as_query_params(api_v2_client, mock_session):
    mock_session.post.assert_called_once_with(
        "https://api-new.netzeroinsights.com/auth/login",
        params={"email": "user@example.com", "password": "test_pass"},
        verify=True,
    )
    assert api_v2_client._access_token == TOKEN


def test_v2_token_read_from_response_header(mock_session):
    mock_session.post.return_value = _response(
        payload=None, headers={"access_token": f"Bearer {TOKEN}"},
    )
    client = NetZeroAPI("user@example.com", "pw", api_version="v2")
    assert client._access_token == TOKEN


def test_v2_login_without_token_raises(mock_session):
    mock_session.post.return_value = _response(payload={"status": "ok"})
    with pytest.raises(RuntimeError, match="no access_token"):
        NetZeroAPI("user@example.com", "pw", api_version="v2")


def test_v2_requests_carry_bearer_header(api_v2_client, mock_session):
    mock_session.request.return_value = _response({"content": [], "totalElements": 0})

    api_v2_client._search_entities(operation="search_companies", limit=1)

    _, kwargs = mock_session.request.call_args
    assert kwargs["headers"]["Authorization"] == f"Bearer {TOKEN}"


def test_v2_logout_posts_and_clears_token(api_v2_client, mock_session):
    mock_session.post.reset_mock()
    mock_session.post.return_value = _response()

    api_v2_client.logout()

    mock_session.post.assert_called_once_with(
        "https://api-new.netzeroinsights.com/auth/logout",
        headers={"Authorization": f"Bearer {TOKEN}"},
        verify=True,
    )
    assert api_v2_client._access_token is None


# --------------------------------------------------------------------------
# Search: pagination and response envelope
# --------------------------------------------------------------------------

def _page_responder(total, page_key="content"):
    """Fake `session.request` serving pageNumber/pageSize slices."""
    dataset = [{"id": i, "name": f"co-{i}"} for i in range(total)]

    def responder(method, url, **kwargs):
        params = kwargs.get("params") or {}
        page_number = params.get("pageNumber", 0)
        page_size = params.get("pageSize", 15)
        start = page_number * page_size
        return _response({
            page_key: dataset[start:start + page_size],
            "totalElements": total,
            "pageNumber": page_number,
            "pageSize": page_size,
        })

    return responder


def test_v2_search_uses_page_params_and_content_envelope(api_v2_client, mock_session):
    mock_session.request.side_effect = _page_responder(total=5)

    result = api_v2_client._search_entities(
        operation="search_companies", limit=3, page_size=10,
    )

    assert result["total_count"] == 5
    assert result["count"] == 3
    _, kwargs = mock_session.request.call_args
    assert kwargs["params"]["pageNumber"] == 0
    assert kwargs["params"]["pageSize"] == 3


def test_v2_search_paginates_across_pages(api_v2_client, mock_session):
    mock_session.request.side_effect = _page_responder(total=25)

    result = api_v2_client._search_entities(
        operation="search_companies", page_size=10, checkpoint_dir=None,
    )

    assert result["total_count"] == 25
    assert result["count"] == 25
    assert [r["id"] for r in result["results"]] == list(range(25))


def test_v2_search_hits_advanced_filters_path(api_v2_client, mock_session):
    mock_session.request.side_effect = _page_responder(total=1)

    api_v2_client._search_entities(operation="search_companies", limit=1)

    args, _ = mock_session.request.call_args
    assert args[1] == "https://api-new.netzeroinsights.com/advanced-filters/companies"


def test_v2_search_results_get_legacy_clientid_alias(api_v2_client, mock_session):
    mock_session.request.side_effect = _page_responder(total=1)

    result = api_v2_client._search_entities(operation="search_companies", limit=1)

    # Downstream (process_nzi, the Postgres cache) keys on clientID.
    assert result["results"][0]["clientID"] == 0
    assert result["results"][0]["id"] == 0


def test_v2_rejects_offset_that_is_not_a_whole_page(api_v2_client, mock_session):
    mock_session.request.side_effect = _page_responder(total=100)

    with pytest.raises(ValueError, match="multiple of page_size"):
        api_v2_client._search_entities(
            operation="search_companies", offset=5, page_size=10, limit=50,
        )


def test_v1_and_v2_checkpoints_do_not_collide():
    from vdl_tools.scrape_enrich.netzero_insights.netzero_api import SearchCheckpoint

    v1 = SearchCheckpoint("/tmp/cp", "v1:companies", {}, 100)
    v2 = SearchCheckpoint("/tmp/cp", "v2:advanced-filters/companies", {}, 100)
    assert v1.search_key != v2.search_key


# --------------------------------------------------------------------------
# Filter translation
# --------------------------------------------------------------------------

def test_company_filter_is_renamed_and_renested():
    main_filter = MainFilter(
        include=StartupFilter(
            searchableLocations=[226],
            fundingsFrom=1_000_000,
            numberOfRoundFrom=2,
            wildcards=['"ocean"'],
            wildcardsFields=["pitchLine"],
        ),
    )

    body = api_v2.to_v2_payload("search_companies", main_filter)

    assert body["companyInclude"] == {
        "searchableLocationIDs": [226],
        "totalFundingAmountFrom": 1_000_000,
        "numberOfDealsFrom": 2,
        "wildcards": ['"ocean"'],
        "wildcardsFields": ["pitchLine"],
    }
    assert body["companyExclude"] == {}


def test_company_ids_translate_to_companyids():
    # Absent from the published docs, present in NZI's MCP filter schema, and
    # confirmed live: companyIDs=[668] returns Sunfire.
    main_filter = MainFilter(include=StartupFilter(ids=[1, 2, 3]))
    body = api_v2.to_v2_payload("search_companies", main_filter)
    assert body["companyInclude"] == {"companyIDs": [1, 2, 3]}


def test_taxonomy_items_filter_raises_with_tagid_guidance():
    main_filter = MainFilter(include=StartupFilter(taxonomyItems=[1, 2]))
    with pytest.raises(ValueError, match="tagID"):
        api_v2.to_v2_payload("search_companies", main_filter)


def test_tags_filter_translates_to_tag_ids():
    main_filter = MainFilter(include=StartupFilter(tags=[995], tagsMode="OR"))
    body = api_v2.to_v2_payload("search_companies", main_filter)
    assert body["companyInclude"] == {"tagIDs": [995], "tagsConceptsMode": "OR"}


def test_investor_ids_stay_on_the_investor_filter():
    # v2's composite filter accepts investorInclude on a company search;
    # confirmed live (investorIDs=[716] -> EIB's 183 portfolio companies).
    main_filter = MainFilter(investorInclude=InvestorFilter(investorIDs=[9156]))

    body = api_v2.to_v2_payload("search_companies", main_filter)

    assert body["investorInclude"] == {"investorIDs": [9156]}
    assert body["companyInclude"] == {}
    assert "dealInclude" not in body


def test_investor_search_filter_is_translated():
    main_filter = MainFilter(
        investorInclude=InvestorFilter(
            investorTypeIDs=[3], investorSearchableLocations=[226],
        ),
    )
    body = api_v2.to_v2_payload("search_investors", main_filter)
    assert body["investorInclude"] == {
        "typeIDs": [3], "searchableLocationIDs": [226],
    }


def test_sorting_moves_to_query_params():
    params = api_v2.to_v2_query_params(
        page_number=2, page_size=50, sorting=Sorting(field="fundingAmount", order="desc"),
    )
    assert params == {
        "pageNumber": 2,
        "pageSize": 50,
        "sortField": "TOTAL_FUNDING_AMOUNT",
        "sortDirection": "DESC",
    }


# --------------------------------------------------------------------------
# Response normalisation
# --------------------------------------------------------------------------

def test_normalize_company_adds_legacy_aliases_without_dropping_v2_fields():
    company = {
        "id": 2657,
        "fundingAmountEUR": 39_334_712,
        "lastDealDate": "2026-01-01",
        "dealCount": 4,
        "logoUrl": "https://example.com/logo.png",
        "isActive": True,
        "searchableLocation": {
            "cityName": "White Rock",
            "country": {"name": "United States", "id": 226},
            "continent": {"name": "North America", "id": 4},
        },
        "sizeRange": {"rangeTextFormat": "11 - 50", "id": 3},
    }

    out = api_v2.normalize_company(company)

    assert out["clientID"] == 2657
    assert out["fundingAmount"] == 39_334_712
    assert out["lastRoundDate"] == "2026-01-01"
    assert out["roundCount"] == 4
    assert out["logo"] == "https://example.com/logo.png"
    assert out["active"] is True
    assert out["city"] == "White Rock"
    assert out["country"] == "United States"
    assert out["countryID"] == 226
    assert out["continent"] == "North America"
    assert out["size"] == "11 - 50"
    # v2 fields survive: the record is a superset, nothing is lost for audit.
    assert out["id"] == 2657
    assert out["searchableLocation"]["cityName"] == "White Rock"


def test_normalize_company_does_not_clobber_a_real_v2_field():
    out = api_v2.normalize_company({"id": 1, "logo": "already-set", "logoUrl": "new"})
    assert out["logo"] == "already-set"


def test_normalize_deal_derives_round_investor_ids():
    # v1 returned a flat ID list; v2 embeds investor objects, and both
    # search_netzero_api and process_nzi.funding_round read the ID list.
    deal = {
        "id": 728142,
        "dealDate": "2026-06-01",
        "amountUSD": 500_000,
        "company": {"id": 16441},
        # REST docs spell the embedded investor id `id`; NZI's MCP view spells
        # it `investorID`. Both must yield an ID.
        "investors": [{"id": 11, "name": "A"}, {"investorID": 22, "name": "B"}],
        "type": {"label": "Series A", "filterable": False, "id": 91},
        "fundingType": {"label": "Equity", "id": 1},
        "equityStage": {"label": "Early stage", "id": 3},
        "exitStage": {"label": "Venture", "id": 1},
        "connectedToInfrastructure": "NO",
    }

    out = api_v2.normalize_deal(deal)

    assert out["roundInvestorIDs"] == [11, 22]
    assert out["coFundingRoundID"] == 728142
    assert out["clientId"] == 16441
    assert out["roundDate"] == "2026-06-01"
    assert out["roundAmountUSD"] == 500_000
    # v1 shipped bare strings here and process_nzi string-compares them
    # (`round_type_nzi.isin(DISCLOSED_STAGES_ORDERED)`, `== "Equity"`).
    assert out["roundType"] == "Series A"
    assert out["financingType"] == "Equity"
    assert out["equityStageID"] == 3
    assert out["exitStageID"] == 1
    # "NO" is a truthy string; the legacy alias must be a real bool.
    assert out["connectedToInfrastructureDeal"] is False
    # The v2 objects are kept intact for audit.
    assert out["type"] == {"label": "Series A", "filterable": False, "id": 91}


def test_normalize_investor_aliases_id():
    out = api_v2.normalize_investor({"id": 9156, "primaryType": "Venture Capital"})
    assert out["investorID"] == 9156
    assert out["investorType"] == "Venture Capital"


# --------------------------------------------------------------------------
# Endpoints with no v2 equivalent
# --------------------------------------------------------------------------

def test_deal_details_by_id_raises_on_v2(api_v2_client):
    import asyncio

    with pytest.raises(NotImplementedError, match="no deal-details-by-ID"):
        asyncio.run(api_v2_client.get_funding_round_details(425794))


def test_v2_detail_endpoints_are_remapped(api_v2_client):
    assert api_v2_client.endpoints["company_details"] == "companies"
    assert api_v2_client.endpoints["investor_details"] == "investors"
    assert api_v2_client.endpoints["company_deals"] == "deals/company"


def test_deal_filter_is_translated_on_a_deal_search():
    from vdl_tools.scrape_enrich.netzero_insights.filters import DealFilter

    main_filter = MainFilter(
        fundingRoundInclude=DealFilter(amount_from=1_000_000, dates_from="2024-01-01"),
    )
    body = api_v2.to_v2_payload("search_deals", main_filter)
    assert body["dealInclude"] == {
        "amountFrom": 1_000_000, "datesFrom": "2024-01-01",
    }


def test_deal_filter_still_applies_on_a_company_search():
    from vdl_tools.scrape_enrich.netzero_insights.filters import DealFilter

    main_filter = MainFilter(
        include=StartupFilter(name="Solar"),
        fundingRoundInclude=DealFilter(amount_from=500_000),
        investorInclude=InvestorFilter(investorIDs=[9156]),
    )
    body = api_v2.to_v2_payload("search_companies", main_filter)
    assert body["companyInclude"] == {"name": "Solar"}
    assert body["dealInclude"] == {"amountFrom": 500_000}
    assert body["investorInclude"] == {"investorIDs": [9156]}


def test_normalize_investor_collapses_type_objects_to_labels():
    # v1: primaryType "Venture Capital", secondaryTypes ["..."]. v2 wraps both
    # in {label, id}. process_nzi.investor does `primaryType in source_types`
    # and `set(secondaryTypes) & source_types` — objects would silently miss
    # the first and raise (unhashable dict) on the second.
    out = api_v2.normalize_investor({
        "id": 10393,
        "primaryType": {"label": "Venture Capital", "id": 10},
        "secondaryTypes": [{"label": "Academic/Research Institutions", "id": 75}],
        "isStrategic": True,
        "isGrowthInvestor": False,
        "growthInvestmentCount": 4,
        "dealsCount": 18,
        "linkedinUrl": "https://linkedin.com/company/x",
    })
    assert out["primaryType"] == "Venture Capital"
    assert out["primaryTypeID"] == 10
    assert out["secondaryTypes"] == ["Academic/Research Institutions"]
    assert out["secondaryTypeIDs"] == [75]
    assert out["investorType"] == "Venture Capital"
    # v1's bare names for the flags/counts process_nzi.investor lists.
    assert out["strategic"] is True
    assert out["growthInvestor"] is False
    assert out["growthDealsCount"] == 4
    assert out["numberOfDeals"] == 18
    assert out["linkedInURL"] == "https://linkedin.com/company/x"


def test_normalize_company_collapses_type_objects_to_labels():
    out = api_v2.normalize_company({
        "id": 668,
        "lastDealType": {"label": "Grant", "id": 79},
        "fundingTypes": [{"label": "Equity", "id": 1}, {"label": "Grant", "id": 3}],
        "foundedYear": 2010,
    })
    assert out["lastRoundType"] == "Grant"
    assert out["fundingTypes"] == ["Equity", "Grant"]
    assert out["fundingTypeObjects"] == [{"label": "Equity", "id": 1}, {"label": "Grant", "id": 3}]
    # v1 `foundedDate` was already the integer year, so this is exact.
    assert out["foundedDate"] == 2010


def test_v2_token_read_from_any_token_like_header(mock_session):
    # The docs never name the header, only that the token is "in the headers".
    mock_session.post.return_value = _response(payload=None, headers={"X-Access-Token": TOKEN})
    client = NetZeroAPI("user@example.com", "pw", api_version="v2")
    assert client._access_token == TOKEN


def test_v2_short_page_with_rows_remaining_raises(api_v2_client, mock_session):
    # The server clamped pageSize=100 down to 10 while 100 rows exist. Paging
    # on with page_number*100 offsets would silently skip rows 10-99, 110-199…
    mock_session.request.return_value = _response({
        "content": [{"id": i} for i in range(10)],
        "totalElements": 100, "pageSize": 10, "pageNumber": 0,
    })
    with pytest.raises(ValueError, match="clamps page size"):
        api_v2_client._search_entities(
            operation="search_companies", page_size=100, checkpoint_dir=None,
        )


def test_v2_final_short_page_is_not_an_error(api_v2_client, mock_session):
    # A short LAST page is normal: 25 rows, page_size 10 -> pages of 10/10/5.
    mock_session.request.side_effect = _page_responder(total=25)
    result = api_v2_client._search_entities(
        operation="search_companies", page_size=10, checkpoint_dir=None,
    )
    assert result["count"] == 25
