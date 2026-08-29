"""Net Zero Insights API v2 support: endpoints, filters, and field aliasing.

NZI rewrote their API. The legacy API (``api.netzeroinsights.com``, session
cookie auth, ``POST /companies``) is documented as "[OLD]" and is supported
until **2027-02-28**; the current API is ``api-new.netzeroinsights.com`` with
JWT bearer auth and ``POST /advanced-filters/companies``.

Three things changed and each is handled here:

1. **Endpoint paths** — see :data:`ENDPOINTS_V2`.
2. **Request shape** — search filters were renamed and re-nested
   (``include`` -> ``companyInclude``), and limit/offset pagination became
   ``pageNumber``/``pageSize`` query params with sorting moved out of the body
   into ``sortField``/``sortDirection``. See :func:`to_v2_payload`.
3. **Response shape** — the entity JSON was renamed throughout
   (``clientID`` -> ``id``, ``lastRoundDate`` -> ``lastDealDate``, flat
   ``city``/``country`` -> nested ``searchableLocation``). See
   :func:`normalize_company`.

The normalisers **add** legacy aliases rather than replacing the v2 fields, so
a normalised record is a superset of what the API returned: nothing is dropped,
`process_nzi` keeps finding the names it expects, and the raw v2 payload stays
available for audit. Aliases are only written when the legacy key is absent.

Mappings marked ``# UNVERIFIED`` below are inferred from the published docs and
have not been checked against a live v2 response — see README for the
verification checklist.
"""

from typing import Any, Dict, List, Optional

from vdl_tools.scrape_enrich.netzero_insights.filters import MainFilter, Sorting
from vdl_tools.shared_tools.tools.logger import logger


PROD_BASE_URL_V2 = "https://api-new.netzeroinsights.com"
STAGE_BASE_URL_V2 = "https://api-stage.netzeroinsights.com"

# Legacy paths -> v2 paths. Keyed by the logical operation so the client can
# resolve an endpoint by name instead of branching on version at each call.
ENDPOINTS_V2 = {
    "login": "auth/login",
    "logout": "auth/logout",
    "search_companies": "advanced-filters/companies",
    "search_deals": "advanced-filters/deals",
    "search_investors": "advanced-filters/investors",
    "company_details": "companies",
    "investor_details": "investors",
    "company_deals": "deals/company",
    "company_commercial_deals": "commercial-deals/connected-entities/company",
}

ENDPOINTS_V1 = {
    "login": "security/formLogin",
    "logout": "security/logout",
    "search_companies": "companies",
    "search_deals": "fundingRounds",
    "search_investors": "investors",
    "company_details": "getStartup",
    "investor_details": "getInvestor",
    "company_deals": "fundingRound/prints",
    "company_commercial_deals": "commercial-deals/connected-entities/company",
}

# The v2 search body nests filters under an entity-scoped key rather than the
# legacy generic ``include``/``exclude``.
FILTER_KEYS_V2 = {
    "search_companies": ("companyInclude", "companyExclude"),
    "search_deals": ("dealInclude", "dealExclude"),
    "search_investors": ("investorInclude", "investorExclude"),
}

# Legacy ``Sorting.field`` values -> v2 ``sortField`` enum. v2 rejects
# unknown sort fields, so anything not listed here is passed through with a
# warning rather than silently dropped.
SORT_FIELDS_V2 = {
    "name": "NAME",
    "website": "WEBSITE",
    "country": "COUNTRY",
    "city": "CITY",
    "foundedDate": "FOUNDED_YEAR",
    "foundedYear": "FOUNDED_YEAR",
    "acquisitionDate": "ACQUISITION_DATE",
    "updatedDate": "UPDATED_DATE",
    "size": "SIZE",
    "stage": "GROWTH_STAGE",
    "lastRoundDate": "LAST_DEAL_DATE",
    "lastRoundType": "LAST_DEAL_TYPE",
    "lastRoundAmount": "LAST_DEAL_AMOUNT",
    "fundingAmount": "TOTAL_FUNDING_AMOUNT",
    "trl": "TRL",
    "platformOrder": "PLATFORM_ORDER",
}

# Legacy StartupFilter field -> v2 Company Filter field.
COMPANY_FILTER_MAP_V2 = {
    "name": "name",
    "searchableLocations": "searchableLocationIDs",
    "financialStageIDs": "financialStageIDs",
    "trls": "trlIDs",
    "fundingsFrom": "totalFundingAmountFrom",
    "fundingsTo": "totalFundingAmountTo",
    "numberOfRoundFrom": "numberOfDealsFrom",
    "numberOfRoundTo": "numberOfDealsTo",
    "employeesFrom": "employeesCountFrom",
    "employeesTo": "employeesCountTo",
    "acquisitionDateFrom": "acquisitionDateFrom",
    "acquisitionDateTo": "acquisitionDateTo",
    "foundedDatesFrom": "foundedYearFrom",
    "foundedDatesTo": "foundedYearTo",
    "commercialAgreementCountFrom": "commercialAgreementCountFrom",
    "wildcards": "wildcards",
    "wildcardsFields": "wildcardsFields",
    "tags": "tagIDs",
    "tagsMode": "tagsConceptsMode",
}

# Legacy DealFilter field -> v2 Deal Filter field.
#
# NOTE: the legacy `DealFilter` model declares snake_case fields
# (`amount_from`, `dates_from`, ...) but the v1 API documents camelCase
# (`amountFrom`, `datesFrom`, ...) — so that model never matched v1 either. It
# is unused by `create_search_filter`, which is why the mismatch went
# unnoticed. Both spellings are accepted here so a caller who built one by hand
# still gets a correctly translated v2 filter.
DEAL_FILTER_MAP_V2 = {
    "acquisition_date_from": "acquisitionDateFrom",
    "acquisitionDateFrom": "acquisitionDateFrom",
    "acquisition_date_to": "acquisitionDateTo",
    "acquisitionDateTo": "acquisitionDateTo",
    "dates_from": "datesFrom",
    "datesFrom": "datesFrom",
    "dates_to": "datesTo",
    "datesTo": "datesTo",
    "last_round_days": "lastRoundDays",
    "lastRoundDays": "lastRoundDays",
    "amount_from": "amountFrom",
    "amountFrom": "amountFrom",
    "amount_to": "amountTo",
    "amountTo": "amountTo",
    "types": "typeIDs",
    "typeIDs": "typeIDs",
    "allow_null_amounts": "allowNullAmounts",
    "allowNullAmounts": "allowNullAmounts",
    "number_from": "numberFrom",
    "numberFrom": "numberFrom",
    "number_to": "numberTo",
    "numberTo": "numberTo",
    "investors": "investorIDs",
    "investorIDs": "investorIDs",
    "total_funding_from": "totalFundingFrom",
    "totalFundingFrom": "totalFundingFrom",
    "total_funding_to": "totalFundingTo",
    "totalFundingTo": "totalFundingTo",
    "equity_stages": "equityStageIDs",
    "equityStageIDs": "equityStageIDs",
    "exit_stages": "dealCategoryIDs",
    "financing_instruments": "fundingTypeIDs",
}

# Legacy InvestorFilter field -> v2 Investor Filter field.
INVESTOR_FILTER_MAP_V2 = {
    "investorTypeIDs": "typeIDs",
    "includeOtherInvestorTypes": "includeOtherInvestorTypes",
    "investorDealsFrom": "numberOfDealsFrom",
    "investorDealsTo": "numberOfDealsTo",
    "investorSearchableLocations": "searchableLocationIDs",
    "investorRegions": "regionIDs",
    "coInvestors": "coInvestorIDs",
    "investments": "investmentIDs",
    "investorFoundedDatesFrom": "foundedYearFrom",
    "investorFoundedDatesTo": "foundedYearTo",
}

# Legacy StartupFilter fields with no v2 Company Filter equivalent. Dropping
# these silently would widen a search without the caller noticing, so
# `to_v2_payload` raises instead.
UNSUPPORTED_COMPANY_FILTERS_V2 = {
    # v2's Company Filter has no company-ID predicate at all.
    "ids": (
        "v2 has no company-ID filter. Fetch the companies directly via "
        "get_startup_details(ids) instead of filtering a search by ID."
    ),
    # v2 folded taxonomy items into tags, but the IDs are NOT interchangeable:
    # /taxonomy/itemDtos returns both an item `id` and a separate `tagID`.
    "taxonomyItems": (
        "v2 replaced `taxonomyItems` with `tagIDs`, which take the taxonomy "
        "item's `tagID` (not its `id`). Translate via /taxonomy/itemDtos, "
        "then pass tags=[...] instead of taxonomy_items=[...]."
    ),
    "taxonomyItemsMode": (
        "v2 replaced `taxonomyItemsMode` with `tagsConceptsMode`; pass "
        "tags_mode=... instead."
    ),
    "sdgs": "v2's Company Filter has no SDG predicate.",
    "sustainabilities": "v2's Company Filter has no sustainability predicate.",
    "patentSearch": "v2's Company Filter has no patent predicates.",
    "investors": (
        "v2 filters companies by investor through `dealInclude.investorIDs`; "
        "pass include_investors=... which is routed to the deal filter."
    ),
}


def _prune(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Drop None values — v2 rejects some explicit nulls that v1 tolerated."""
    return {k: v for k, v in payload.items() if v is not None}


def _as_dict(filter_obj) -> Dict[str, Any]:
    if filter_obj is None:
        return {}
    if isinstance(filter_obj, dict):
        return dict(filter_obj)
    return filter_obj.model_dump()


def translate_company_filter(startup_filter) -> Dict[str, Any]:
    """Translate a legacy :class:`StartupFilter` into a v2 Company Filter.

    Raises ``ValueError`` for legacy predicates that v2 dropped, rather than
    quietly returning a broader result set than the caller asked for.
    """
    source = _prune(_as_dict(startup_filter))
    translated = {}
    for key, value in source.items():
        if key in UNSUPPORTED_COMPANY_FILTERS_V2:
            raise ValueError(
                f"Filter `{key}` is not supported by the NZI v2 API. "
                f"{UNSUPPORTED_COMPANY_FILTERS_V2[key]}"
            )
        if key in COMPANY_FILTER_MAP_V2:
            translated[COMPANY_FILTER_MAP_V2[key]] = value
        else:
            logger.warning(
                "NZI v2: dropping unmapped company filter `%s` — it has no "
                "documented v2 equivalent", key,
            )
    return translated


def translate_investor_filter(investor_filter) -> Dict[str, Any]:
    """Translate a legacy :class:`InvestorFilter` into a v2 Investor Filter."""
    source = _prune(_as_dict(investor_filter))
    translated = {}
    for key, value in source.items():
        if key == "investorIDs":
            # v2's Investor Filter has no investorIDs; the caller-facing
            # meaning ("companies backed by these investors") lives on the
            # deal filter in v2. Handled by to_v2_payload, not here.
            continue
        if key in INVESTOR_FILTER_MAP_V2:
            translated[INVESTOR_FILTER_MAP_V2[key]] = value
        else:
            logger.warning(
                "NZI v2: dropping unmapped investor filter `%s`", key,
            )
    return translated


def translate_deal_filter(deal_filter) -> Dict[str, Any]:
    """Translate a legacy :class:`DealFilter` into a v2 Deal Filter."""
    source = _prune(_as_dict(deal_filter))
    translated = {}
    for key, value in source.items():
        if key in DEAL_FILTER_MAP_V2:
            translated[DEAL_FILTER_MAP_V2[key]] = value
        else:
            logger.warning("NZI v2: dropping unmapped deal filter `%s`", key)
    return translated


def to_v2_payload(operation: str, main_filter: Optional[MainFilter]) -> Dict[str, Any]:
    """Build the v2 request body for a search operation.

    ``investorIDs`` is rerouted onto ``dealInclude``/``dealExclude``: in v1 an
    investor-ID predicate on a company search lived under ``investorInclude``,
    but v2's Investor Filter has no ID predicate and the equivalent is
    ``dealInclude.investorIDs``.
    """
    include_key, exclude_key = FILTER_KEYS_V2[operation]
    main_filter = main_filter or MainFilter()
    source = _as_dict(main_filter)

    body: Dict[str, Any] = {include_key: {}, exclude_key: {}}

    if operation == "search_companies":
        body[include_key] = translate_company_filter(source.get("include"))
        body[exclude_key] = translate_company_filter(source.get("exclude"))
    elif operation == "search_investors":
        body[include_key] = translate_investor_filter(source.get("investorInclude"))
        body[exclude_key] = translate_investor_filter(source.get("investorExclude"))
    elif operation == "search_deals":
        body[include_key] = translate_deal_filter(source.get("fundingRoundInclude"))
        body[exclude_key] = translate_deal_filter(source.get("fundingRoundExclude"))

    # A deal-scoped filter still applies when searching companies: v2 keeps
    # `dealInclude`/`dealExclude` alongside `companyInclude`/`companyExclude`.
    if operation != "search_deals":
        for legacy_key, v2_key in (("fundingRoundInclude", "dealInclude"),
                                   ("fundingRoundExclude", "dealExclude")):
            translated = translate_deal_filter(source.get(legacy_key))
            if translated:
                body.setdefault(v2_key, {}).update(translated)

    # Investor-ID predicates ride on the deal filter in v2.
    for legacy_key, v2_key in (("investorInclude", "dealInclude"),
                               ("investorExclude", "dealExclude")):
        investor_ids = _prune(_as_dict(source.get(legacy_key))).get("investorIDs")
        if investor_ids:
            body.setdefault(v2_key, {})["investorIDs"] = investor_ids

    return body


def to_v2_query_params(
    page_number: int,
    page_size: int,
    sorting: Optional[Sorting] = None,
) -> Dict[str, Any]:
    """Build the v2 pagination/sorting query params.

    v1 carried limit/offset/sorting in the request body; v2 takes
    ``pageNumber``/``pageSize``/``sortField``/``sortDirection`` in the query
    string.
    """
    params: Dict[str, Any] = {"pageNumber": page_number, "pageSize": page_size}
    if sorting is None:
        return params

    sort_field = SORT_FIELDS_V2.get(sorting.field)
    if sort_field is None:
        logger.warning(
            "NZI v2: no documented sortField for `%s` — passing it through "
            "unchanged; v2 may reject it", sorting.field,
        )
        sort_field = sorting.field
    params["sortField"] = sort_field
    params["sortDirection"] = "DESC" if str(sorting.order).lower().startswith("desc") else "ASC"
    return params


def _alias(record: Dict[str, Any], legacy_key: str, value: Any) -> None:
    """Add a legacy alias without clobbering a real v2 field of the same name."""
    if value is not None and legacy_key not in record:
        record[legacy_key] = value


def _dig(record: Dict[str, Any], *path: str) -> Any:
    current: Any = record
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def normalize_company(company: Dict[str, Any]) -> Dict[str, Any]:
    """Add legacy field aliases to a v2 company record.

    The v2 payload is preserved as-is; legacy names used by `process_nzi` and
    the Postgres cache are layered on top.
    """
    if not isinstance(company, dict):
        return company
    out = dict(company)

    _alias(out, "clientID", out.get("id"))
    # v2 splits every monetary field into explicit EUR/USD variants; v1's
    # unsuffixed field was the EUR one.
    _alias(out, "fundingAmount", out.get("fundingAmountEUR"))
    _alias(out, "fundingString", out.get("fundingStringEUR"))
    _alias(out, "fundingRange", out.get("fundingRangeEUR"))
    _alias(out, "lastRoundAmount", out.get("lastDealAmountEUR"))
    _alias(out, "lastRoundAmountUSD", out.get("lastDealAmountUSD"))
    _alias(out, "lastRoundAmountString", out.get("lastDealAmountStringEUR"))
    _alias(out, "lastRoundAmountStringUSD", out.get("lastDealAmountStringUSD"))
    _alias(out, "lastRoundDate", out.get("lastDealDate"))
    _alias(out, "lastRoundType", out.get("lastDealType"))
    _alias(out, "roundCount", out.get("dealCount"))
    _alias(out, "numberOfEquityRounds", out.get("numberOfEquityDeals"))
    _alias(out, "numberOfGrants", out.get("numberOfGrantDeals"))
    _alias(out, "numberOfDebtRounds", out.get("numberOfDebtDeals"))
    _alias(out, "logo", out.get("logoUrl"))
    _alias(out, "linkedinURL", out.get("linkedinUrl"))
    _alias(out, "twitterURL", out.get("twitterUrl"))
    _alias(out, "facebookURL", out.get("facebookUrl"))
    _alias(out, "active", out.get("isActive"))
    _alias(out, "acquired", out.get("isAcquired"))
    _alias(out, "champion", out.get("isChampion"))
    _alias(out, "emerging", out.get("isEmerging"))
    _alias(out, "newEntrant", out.get("isNewEntrant"))
    # NOTE: type change — v1 `foundedDate` was a date string, v2 `foundedYear`
    # is an integer year. Downstream code that parses this as a date must be
    # updated; we alias it so the column exists, not because it is equivalent.
    _alias(out, "foundedDate", out.get("foundedYear"))

    # v2 nests what v1 kept flat.
    _alias(out, "city", _dig(out, "searchableLocation", "cityName"))
    _alias(out, "admin4", _dig(out, "searchableLocation", "adminName4"))
    _alias(out, "country", _dig(out, "searchableLocation", "country", "name"))
    _alias(out, "countryID", _dig(out, "searchableLocation", "country", "id"))
    _alias(out, "continent", _dig(out, "searchableLocation", "continent", "name"))
    _alias(out, "size", _dig(out, "sizeRange", "rangeTextFormat"))
    _alias(out, "sizeID", _dig(out, "sizeRange", "id"))
    _alias(out, "stage", _dig(out, "growthStage", "label"))
    _alias(out, "stageID", _dig(out, "growthStage", "id"))

    return out


def normalize_deal(deal: Dict[str, Any], company_id: Optional[int] = None) -> Dict[str, Any]:
    """Add legacy funding-round aliases to a v2 deal record.

    ``roundInvestorIDs`` is the important one: v1 returned a flat ID list,
    v2 returns embedded investor objects, and both `search_netzero_api` and
    `process_nzi.funding_round` read the ID list.
    """
    if not isinstance(deal, dict):
        return deal
    out = dict(deal)

    _alias(out, "coFundingRoundID", out.get("id"))
    _alias(out, "clientId", company_id if company_id is not None else _dig(out, "company", "id"))
    _alias(out, "roundDate", out.get("dealDate"))
    _alias(out, "roundType", out.get("type"))
    _alias(out, "financingType", out.get("fundingType"))  # UNVERIFIED
    _alias(out, "roundAmount", out.get("amountEUR"))
    _alias(out, "roundAmountUSD", out.get("amountUSD"))
    _alias(out, "roundInvestors", out.get("investors"))
    _alias(out, "roundNews", out.get("news"))
    _alias(out, "connectedToInfrastructureDeal", out.get("connectedToInfrastructure"))

    investors = out.get("investors")
    if isinstance(investors, list) and "roundInvestorIDs" not in out:
        out["roundInvestorIDs"] = [
            investor.get("id")
            for investor in investors
            if isinstance(investor, dict) and investor.get("id") is not None
        ]

    return out


def normalize_investor(investor: Dict[str, Any]) -> Dict[str, Any]:
    """Add legacy investor aliases to a v2 investor record."""
    if not isinstance(investor, dict):
        return investor
    out = dict(investor)

    _alias(out, "investorID", out.get("id"))
    _alias(out, "logoURL", out.get("logoUrl"))
    # v1 exposed both `investorType` and `primaryType`; v2 keeps only the latter.
    _alias(out, "investorType", out.get("primaryType"))
    return out


def normalize_search_results(operation: str, results: List[Dict]) -> List[Dict]:
    normalizer = {
        "search_companies": normalize_company,
        "search_deals": normalize_deal,
        "search_investors": normalize_investor,
    }[operation]
    return [normalizer(record) for record in results]
