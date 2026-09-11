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

A v2 subtlety that bites downstream: fields v1 exposed as bare strings
(``roundType: "Late VC"``, ``primaryType: "Venture Capital"``) are
``{label, id}`` objects in v2. `process_nzi` compares these as strings, so the
legacy aliases hold the ``label`` and the object is kept under its v2 name (or
an ``…ID`` alias where v1 also had one).

Every mapping here was verified against the live v2 REST endpoint on
2026-09-11 (auth, envelope, each filter field's effect on ``totalElements``,
entity shapes, vocabularies), and the client was run end to end. Where the
published docs or NZI's MCP server disagree with the endpoint, the endpoint
wins and the discrepancy is noted inline. See the module README.
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

# Legacy ``Sorting.field`` values -> v2 ``sortField`` enum. The endpoint
# answers HTTP 400 for anything outside its enum, so `to_v2_query_params`
# raises on a field it cannot map (a v2 enum value passes through).
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
    # Translated per operation in `to_v2_payload`: the REST endpoint silently
    # ignores `investorIDs` under investorInclude and honours it only under
    # dealInclude (confirmed live 2026-09-11).
    "investorIDs": "investorIDs",
    "investorTypeIDs": "typeIDs",
    # The docs call this `includeOtherInvestorTypes`, which the endpoint
    # silently ignores; `includeSecondaryTypes` (the spelling in NZI's MCP
    # schema) is the one that changes the count — typeIDs=[1] gives 222
    # without it and 252 with it. Confirmed live.
    "includeOtherInvestorTypes": "includeSecondaryTypes",
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
    # The REST endpoint has no company-ID predicate. `ids`, `companyIDs`,
    # `companyIds` and `id` all return the full universe (confirmed live).
    # NZI's own MCP server *appears* to support `companyIDs`, but it emulates
    # it client-side; the endpoint itself silently drops unknown fields.
    "ids": (
        "v2 has no company-ID search filter — the endpoint silently ignores "
        "every spelling. Fetch the companies directly via "
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
        "pass include_investors=... which is routed there."
    ),
}


# Value coercions applied after a key is mapped. v1 documented `wildcards` and
# `regexps` as lists and accepted them; v2's docs still say "List of string"
# but the endpoint deserialises a single String — a list is an HTTP 500
# ("Cannot deserialize value of type `java.lang.String` from Array value").
# Confirmed against the live REST endpoint 2026-09-11. `create_search_filter`
# builds a one-element list whose element is already the space-joined query,
# so joining is lossless for every caller in this repo.
V2_VALUE_COERCIONS = {
    "wildcards": lambda v: " ".join(v) if isinstance(v, list) else v,
    "regexps": lambda v: " ".join(v) if isinstance(v, list) else v,
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
            v2_key = COMPANY_FILTER_MAP_V2[key]
            translated[v2_key] = V2_VALUE_COERCIONS.get(v2_key, lambda v: v)(value)
        else:
            logger.warning(
                "NZI v2: dropping unmapped company filter `%s` — it has no "
                "documented v2 equivalent", key,
            )
    if "wildcards" in translated and not translated.get("wildcardsFields"):
        # The endpoint answers HTTP 500 ("wildcardFields is null") rather than
        # defaulting the fields, so fail here with a message that says why.
        raise ValueError(
            "NZI v2 requires `wildcardsFields` alongside `wildcards`. Valid "
            "fields: pitchLine, description, websiteContent."
        )
    return translated


def translate_investor_filter(investor_filter) -> Dict[str, Any]:
    """Translate a legacy :class:`InvestorFilter` into a v2 Investor Filter."""
    source = _prune(_as_dict(investor_filter))
    translated = {}
    for key, value in source.items():
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

    The v2 filter is composite: every search accepts all six of
    ``companyInclude/Exclude``, ``investorInclude/Exclude`` and
    ``dealInclude/Exclude`` (confirmed against NZI's MCP schema), so each
    legacy section is translated to its v2 counterpart. The operation's own
    pair is always present, even when empty; the others only when non-empty.
    """
    include_key, exclude_key = FILTER_KEYS_V2[operation]
    source = _as_dict(main_filter or MainFilter())

    sections = (
        ("include", "companyInclude", translate_company_filter),
        ("exclude", "companyExclude", translate_company_filter),
        ("investorInclude", "investorInclude", translate_investor_filter),
        ("investorExclude", "investorExclude", translate_investor_filter),
        ("fundingRoundInclude", "dealInclude", translate_deal_filter),
        ("fundingRoundExclude", "dealExclude", translate_deal_filter),
    )
    body: Dict[str, Any] = {}
    for legacy_key, v2_key, translate in sections:
        translated = translate(source.get(legacy_key))
        if translated or v2_key in (include_key, exclude_key):
            body[v2_key] = translated

    # `investorIDs` has exactly one working home on the REST endpoint:
    # `dealInclude`/`dealExclude`. Confirmed live: investorInclude.investorIDs
    # is silently ignored on every search, while dealInclude.investorIDs=[716]
    # narrows a company search to EIB's 183 portfolio companies — the number
    # NZI's MCP reports, because the MCP performs this same translation. On
    # an investor search there is no working predicate at all, so that raises
    # rather than returning the whole universe.
    for investor_key, deal_key in (("investorInclude", "dealInclude"),
                                   ("investorExclude", "dealExclude")):
        investor_ids = body.get(investor_key, {}).pop("investorIDs", None)
        if not investor_ids:
            continue
        if operation == "search_investors":
            raise ValueError(
                "Filter `investorIDs` is not supported by the NZI v2 investor "
                "search — the endpoint ignores it. Fetch investors by ID with "
                "get_investor_details() instead."
            )
        body.setdefault(deal_key, {})["investorIDs"] = investor_ids
        if not body[investor_key] and investor_key not in (include_key, exclude_key):
            del body[investor_key]
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
        if sorting.field in SORT_FIELDS_V2.values():
            sort_field = sorting.field  # already a v2 enum value
        else:
            # The endpoint answers HTTP 400 for anything outside its enum.
            raise ValueError(
                f"Unknown NZI v2 sort field {sorting.field!r}. Use a legacy name "
                f"{sorted(SORT_FIELDS_V2)} or a v2 value "
                f"{sorted(set(SORT_FIELDS_V2.values()))}."
            )
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


def _label(value: Any) -> Any:
    """Collapse a v2 ``{label, id}`` object to its label; pass strings through.

    v1 exposed these as bare strings and downstream compares them as strings
    (``round_type_nzi.isin([...])``, ``primaryType in source_types``), so the
    legacy alias must carry the label, never the object.
    """
    if isinstance(value, dict):
        return value.get("label")
    return value


def _id_of(value: Any) -> Any:
    return value.get("id") if isinstance(value, dict) else None


def _labels(values: Any) -> Any:
    if isinstance(values, list):
        return [_label(v) for v in values]
    return values


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
    # v1: "Grant"; v2: {label: "Grant", id: 79}
    _alias(out, "lastRoundType", _label(out.get("lastDealType")))
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
    # Despite the name, v1 `foundedDate` was already the integer year
    # (e.g. 2010), so this alias is exact.
    _alias(out, "foundedDate", out.get("foundedYear"))
    # v1: ["Grant"]; v2: [{label: "Grant", id: 79}]. Same key in both
    # versions, so the labels overwrite in place and the objects are kept.
    if isinstance(out.get("fundingTypes"), list) and any(
        isinstance(v, dict) for v in out["fundingTypes"]
    ):
        out["fundingTypeObjects"] = out["fundingTypes"]
        out["fundingTypes"] = _labels(out["fundingTypes"])

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

    # Tags: v1 nested the type name under the same key as the object —
    # tagType: {tagType: "technology", tagFamily: {tagFamily: "Solutions"}} —
    # and `process_nzi.company.parse_company_tags` reads exactly that path.
    # v2 uses `label` at both levels. Alias inside each tag so the parser
    # works unchanged; the tag objects themselves are otherwise kept as-is.
    tags = out.get("tags")
    if isinstance(tags, list):
        out["tags"] = [_normalize_tag(tag) for tag in tags]

    return out


def _normalize_tag(tag: Any) -> Any:
    if not isinstance(tag, dict):
        return tag
    tag = dict(tag)
    tag_type = tag.get("tagType")
    if isinstance(tag_type, dict):
        tag_type = dict(tag_type)
        _alias(tag_type, "tagType", tag_type.get("label"))
        family = tag_type.get("tagFamily")
        if isinstance(family, dict):
            family = dict(family)
            _alias(family, "tagFamily", family.get("label"))
            tag_type["tagFamily"] = family
        tag["tagType"] = tag_type
        _alias(tag, "tagTypeId", tag_type.get("id"))
    _alias(tag, "umbrella", tag.get("isUmbrella"))
    return tag


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
    # v1: roundType "Late VC", financingType "Equity" — bare strings that
    # `process_nzi` matches against DISCLOSED_STAGES_ORDERED / "Equity".
    # v2: type {label: "Late VC", id: 83}, fundingType {label: "Equity", id: 1}.
    # The label vocabulary is unchanged (checked against NZI's DEAL_TYPE and
    # FUNDING_TYPE lookups), so the alias carries the label.
    _alias(out, "roundType", _label(out.get("type")))
    _alias(out, "financingType", _label(out.get("fundingType")))
    # v1 exposed the stage IDs; v2 nests them.
    _alias(out, "equityStageID", _id_of(out.get("equityStage")))
    _alias(out, "exitStageID", _id_of(out.get("exitStage")))
    _alias(out, "roundAmount", out.get("amountEUR"))
    _alias(out, "roundAmountUSD", out.get("amountUSD"))
    _alias(out, "roundInvestors", out.get("investors"))
    _alias(out, "roundNews", out.get("news"))
    # Both versions ship this as a "YES"/"NO" string (v1 records in the
    # Postgres cache carry "NO" verbatim), so it passes through unchanged —
    # coercing it to a bool here would make v2 records differ from v1.
    _alias(out, "connectedToInfrastructureDeal", out.get("connectedToInfrastructure"))

    investors = out.get("investors")
    if isinstance(investors, list) and "roundInvestorIDs" not in out:
        # Embedded investor objects carry `id` per the REST docs; NZI's MCP
        # view spells it `investorID`. Accept either.
        ids = [
            investor.get("id", investor.get("investorID"))
            for investor in investors
            if isinstance(investor, dict)
        ]
        out["roundInvestorIDs"] = [i for i in ids if i is not None]

    return out


def normalize_investor(investor: Dict[str, Any]) -> Dict[str, Any]:
    """Add legacy investor aliases to a v2 investor record."""
    if not isinstance(investor, dict):
        return investor
    out = dict(investor)

    _alias(out, "investorID", out.get("id"))
    _alias(out, "logoURL", out.get("logoUrl"))
    _alias(out, "linkedInURL", out.get("linkedinUrl"))  # v1 capitalised the I
    _alias(out, "numberOfDeals", out.get("dealsCount"))
    _alias(out, "isLP", out.get("isLimitedPartner"))
    # Present on investor list rows and on investors embedded in deals; the
    # details endpoint may omit it, in which case these stay absent.
    _alias(out, "city", _dig(out, "searchableLocation", "cityName"))
    _alias(out, "country", _dig(out, "searchableLocation", "country", "name"))
    _alias(out, "continent", _dig(out, "searchableLocation", "continent", "name"))

    # v1: primaryType "Venture Capital", primaryTypeID 10, secondaryTypes [...].
    # v2: primaryType {label, id}, secondaryTypes [{label, id}]. Same keys,
    # different types — and `process_nzi.investor` does
    # `primaryType in source_types` and `set(secondaryTypes) & source_types`,
    # which would silently miss (or raise on unhashable dicts). The labels go
    # back under the v1 names; the IDs under the v1 `…ID` names.
    primary = out.get("primaryType")
    if isinstance(primary, dict):
        out["primaryType"] = _label(primary)
        _alias(out, "primaryTypeID", _id_of(primary))
    secondary = out.get("secondaryTypes")
    if isinstance(secondary, list) and any(isinstance(v, dict) for v in secondary):
        out["secondaryTypeIDs"] = [_id_of(v) for v in secondary]
        out["secondaryTypes"] = _labels(secondary)
    # v1 exposed both `investorType` and `primaryType`; v2 keeps only the latter.
    _alias(out, "investorType", out.get("primaryType"))

    # v1's bare boolean/count names -> v2's `is…` / `…InvestmentCount` names.
    for legacy, v2 in (
        ("acquirer", "isAcquirer"),
        ("strategic", "isStrategic"),
        ("buyoutInvestor", "isBuyoutInvestor"),
        ("equityInvestor", "isEquityInvestor"),
        ("growthInvestor", "isGrowthInvestor"),
        ("ventureInvestor", "isVentureInvestor"),
        ("financialInvestor", "isFinancialInvestor"),
        ("infrastructureInvestor", "isInfrastructureInvestor"),
        ("commercialBuyer", "isCommercialBuyer"),
        ("commercialPartner", "isCommercialPartner"),
        ("limitedPartner", "isLimitedPartner"),
        ("growthDealsCount", "growthInvestmentCount"),
        ("ventureDealsCount", "ventureInvestmentCount"),
        ("infrastructureDealsCount", "infrastructureInvestmentCount"),
    ):
        _alias(out, legacy, out.get(v2))
    return out


def normalize_search_results(operation: str, results: List[Dict]) -> List[Dict]:
    normalizer = {
        "search_companies": normalize_company,
        "search_deals": normalize_deal,
        "search_investors": normalize_investor,
    }[operation]
    return [normalizer(record) for record in results]
