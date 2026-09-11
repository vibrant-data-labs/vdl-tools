# NetZero Insights API Client

A Python client library for interacting with the NetZero Insights API.

## API versions (read this first)

NZI rewrote their API. Both versions are live:

| | v1 (legacy) | v2 (current) |
|---|---|---|
| Host | `api.netzeroinsights.com` | `api-new.netzeroinsights.com` |
| Auth | `POST /security/formLogin`, `JSESSIONID` cookie, 30-min idle expiry | `POST /auth/login?email=&password=`, JWT bearer, 30-day expiry |
| Company search | `POST /companies` | `POST /advanced-filters/companies` |
| Deal search | `POST /fundingRounds` | `POST /advanced-filters/deals` |
| Investor search | `POST /investors` | `POST /advanced-filters/investors` |
| Company details | `GET /getStartup/{id}` | `GET /companies/{id}` |
| Investor details | `GET /getInvestor/{id}` | `GET /investors/{id}` |
| Company's deals | `GET /fundingRoundsPrints/{id}` | `GET /deals/company/{id}` |
| Pagination | `limit`/`offset` in the body | `pageNumber`/`pageSize` query params |
| Sorting | `sorting` object in the body | `sortField`/`sortDirection` query params |
| Search envelope | `{count, results}` | `{content, totalElements, totalPages, …}` |
| Entity ID field | `clientID` / `investorID` | `id` |

**NZI supports v1 until 2027-02-28.** This client still defaults to v1 because
the v2 field mapping has not been checked against live credentials yet.

```python
# Opt in per client…
client = NetZeroAPI(username=..., password=..., api_version="v2")

# …or for a whole run
NZI_API_VERSION=v2 python your_script.py
```

`config.ini` may also set `api_version` under `[netzero_insights]`.

### What v2 changes for callers

* **Response fields are renamed** (`clientID`→`id`, `lastRoundDate`→
  `lastDealDate`, flat `city`/`country`→nested `searchableLocation`, and so
  on). `api_v2.normalize_*` re-adds the legacy names on top of the v2 payload,
  so `process_nzi` and the Postgres cache keep working and nothing is dropped.
  Records come back as a **superset** of what the API returned.
* **Strings became objects.** v1 shipped `roundType: "Late VC"`,
  `primaryType: "Venture Capital"`, `lastRoundType: "Grant"`; v2 ships
  `type: {label, id}`, `primaryType: {label, id}`, `lastDealType: {label, id}`.
  `process_nzi` string-compares these, so the legacy aliases carry the
  **label**; where v1 and v2 share the key name (`primaryType`,
  `secondaryTypes`, `fundingTypes`) the label overwrites in place and the
  objects/IDs are kept under `primaryTypeID`, `secondaryTypeIDs`,
  `fundingTypeObjects`. The label vocabularies are unchanged — checked against
  NZI's `DEAL_TYPE` and `INVESTOR_TYPE` lookups (see below).
* **`taxonomyItems` became `tagIDs`, keyed by `tagID`, not by the taxonomy
  item's `id`.** Passing the old IDs through raises a `ValueError` naming the
  replacement rather than silently filtering on the wrong concepts. Translate
  with `NetZeroAPI.get_taxonomy_item_tag_ids()`.
* **There is no company-ID search filter.** `ids`, `companyIDs`, `companyIds`
  and `id` all silently return the full universe — the endpoint drops unknown
  fields without error (NZI's own MCP server *emulates* `companyIDs`, which is
  misleading). `ids` raises; fetch by ID with `get_startup_details()` instead.
* **`investorIDs` only works under `dealInclude`.** `investorInclude.investorIDs`
  is silently ignored on every search; `dealInclude.investorIDs` narrows a
  company search correctly. `create_search_filter(include_investors=...)` routes
  it there. On an *investor* search no spelling works, so that raises.
* **`wildcards` is a single string and needs `wildcardsFields`.** The docs say
  *List of string*; a list is an HTTP 500, and so is omitting the fields.
  Space-joined phrases match any (`"a" "b"` widens), `-"c"` negates, and the
  literal word `OR` must not be used (it matches almost everything).
* **`searchableLocationIDs` takes searchable-location entity IDs**, from
  `lookup_searchable_locations("Germany")` → 817600 — *not* the `country.id`
  (80) nested in company records, which is silently ignored.
* **The docs' `includeOtherInvestorTypes` is a no-op**; `includeSecondaryTypes`
  is the field that works. Mapped accordingly.
* **`get_funding_round_details()` raises on v2** — NZI documents no
  deal-by-ID endpoint. Use `get_company_funding_rounds()` or `search_deals()`.
* **Taxonomy is a different graph on the v2 host.** `POST /taxonomy/graph/{id}`
  works there (POST, despite the docs' GET), but legacy node IDs such as 660
  return nothing and the root (`POST /taxonomy/graph`) is a different tree
  ("Verticals map" / "Horizontals map"). Children keep the legacy
  `{id, label, description, hasChildren}` keys — recursion via `child["id"]`
  works — and add `tag`, `companyCount`, `dealCount` and funding totals.
  `/taxonomy/itemDtos` and `/taxonomy/item/*` do not exist on v2 at all. Use
  `search_tags(name)` (`GET /tags`) to resolve tag IDs on v2;
  `get_flat_nzi_taxonomy` stays on v1 until its `ROOT_ID` is remapped.

### Verified against the live v2 REST endpoint (2026-09-11)

Every claim above was checked with a read-only session against
`api-new.netzeroinsights.com`, and the whole `NetZeroAPI(api_version="v2")`
client was run end to end (`search_startups`, `get_startup_count`,
`get_startup_details`, `get_company_funding_rounds`, `get_investor_details`)
producing legacy-shaped records. Specifically:

* **Auth:** the token arrives in an `access_token` response header with an
  empty body; `POST /auth/logout` invalidates it (a later call is 403).
* **No `pageSize` cap at 100** — `pageSize=100` returns 100 rows. The
  short-page guard stays as insurance.
* **Entity IDs are unchanged** — company 668 is Sunfire in both versions, so
  the Postgres cache and every historical join survive the cutover.
* **Every mapped filter field was tested for whether it actually narrows
  results** (the endpoint drops unknown fields silently, so a wrong name is
  invisible). All honoured except the cases called out above.
* **Vocabularies:** `type.label` on real deals is exactly the v1 strings
  (`Seed`, `Series A`, `Late VC`, `Debt`, `Grant`, …); `fundingType.label` is
  `Equity`/`Debt`/`Grant`/`Other`. NZI's `DEAL_TYPE` lookup has every label in
  `DISCLOSED_STAGES_ORDERED` except `Series I`, `Series J` and
  `Post IPO - Equity` (v2 has `PIPE`); `INVESTOR_TYPE` has all 33 raw types in
  the Excel mapping plus a new `Crowdfunding Platform`.
* **Sorting:** `sortField` must be a v2 enum value (`fundingAmount` is a 400);
  the client raises on anything it cannot map.

Two NZI-side bugs to be aware of: `dealInclude.equityStageIDs` on a *company*
search is an HTTP 500 (Hibernate path error; it works on a deal search), and
malformed request bodies come back as 500 rather than 400, so the client's
transient-error retry will back off through four attempts before surfacing
them.

### Before flipping the default to v2

1. **Strict column selection in `process_nzi`.** `filter_format_columns` does
   `df[keep_columns]`, which raises `KeyError` on any listed column the record
   lacks. Eight v1 company columns have no v2 equivalent (`directURL`,
   `eutopiaScore`, `facebookURL`, `infrastructureProjectsCount`,
   `revenueYear`, `revenuesRange`, `trlFiveYearsPrior`, `trlLastThreeYears`),
   none of which any analysis reads — but their absence will crash the
   pipeline until that selection is made lenient or the columns are dropped
   from `ORIGINAL_COMPANY_DETAILS_COLUMNS`.
2. Re-run the survival-rate pipeline on a small cohort and diff the stage
   buckets against a v1 run.
3. Decide what `get_flat_nzi_taxonomy` does once v1 goes away (2027-02-28).

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Authentication

```python
from netzero_api import NetZeroAPI

# Initialize the client with your credentials
client = NetZeroAPI(username="your_username", password="your_password")

# The client automatically handles authentication
# When you're done, you can logout
client.logout()
```

### Getting Startups

```python
from netzero_api import NetZeroAPI, StartupFilter, Sorting

# Create a filter for startups
startup_filter = StartupFilter(
    name="Solar",
    stage=["Seed", "Series A"],
    founded_date={"min": "2020-01-01", "max": "2023-12-31"},
    location="United States",
    sustainability=["Renewable Energy", "Energy Storage"]
)

# Create sorting criteria
sorting = Sorting(field="name", order="asc")

# Get a list of startups
startups = client.get_startups(
    filter=startup_filter,
    sorting=sorting,
    limit=10  # Optional: limit the number of results
)

# Get all startups with pagination
for page in client.get_startups(
    filter=startup_filter,
    sorting=sorting,
    page_size=100  # Number of items per page
):
    for startup in page['results']:
        print(startup['name'])

# Get detailed information about a specific startup
startup_detail = client.get_startup_detail(startup_id=123)
```

### Getting Deals

```python
from netzero_api import NetZeroAPI, DealFilter, Sorting

# Create a filter for deals
deal_filter = DealFilter(
    acquisition_date_from="2020-01-01",
    acquisition_date_to="2023-12-31",
    dates_from="2022-01-01",
    dates_to="2023-12-31",
    last_round_days=[30, 60, 90],
    amount_from=1000000,
    amount_to=5000000,
    types=[1, 2],  # Deal type IDs
    allow_null_amounts=True,
    number_from=1,
    number_to=5,
    investors=[1, 2],  # Investor IDs
    total_funding_from=1000000,
    total_funding_to=5000000,
    financing_instruments=["Convertible Note", "SAFE"],
    equity_stages=[1, 2],  # Equity stage IDs
    exit_stages=[1, 2]  # Exit stage IDs
)

# Create sorting criteria
sorting = Sorting(field="date", order="desc")

# Get a list of deals
deals = client.get_deals(
    filter=deal_filter,
    sorting=sorting,
    limit=10  # Optional: limit the number of results
)

# Get all deals with pagination
for page in client.get_deals(
    filter=deal_filter,
    sorting=sorting,
    page_size=100  # Number of items per page
):
    for deal in page['results']:
        print(deal['name'])

# Get detailed information about a specific deal
deal_detail = client.get_deal_detail(deal_id=456)
```

### Getting Investors

```python
from netzero_api import NetZeroAPI, InvestorFilter, Sorting

# Create a filter for investors
investor_filter = InvestorFilter(
    investor_type_ids=[1, 2],  # Investor type IDs
    include_other_investor_types=True,
    investor_deals_from=10,
    investor_deals_to=100,
    investor_searchable_locations=[1, 2],  # Location IDs
    investor_regions=[1, 2],  # Region IDs
    co_investors=[1, 2],  # Investor IDs
    investments=[1, 2],  # Startup IDs
    investor_ids=[1, 2],  # Investor IDs
    investor_founded_dates_from="2000-01-01",
    investor_founded_dates_to="2020-12-31"
)

# Create sorting criteria
sorting = Sorting(field="name", order="asc")

# Get a list of investors
investors = client.get_investors(
    filter=investor_filter,
    sorting=sorting,
    limit=10  # Optional: limit the number of results
)

# Get all investors with pagination
for page in client.get_investors(
    filter=investor_filter,
    sorting=sorting,
    page_size=100  # Number of items per page
):
    for investor in page['results']:
        print(investor['name'])

# Get detailed information about a specific investor
investor_detail = client.get_investor_detail(investor_id=789)
```

## Filter Classes

The library provides several filter classes to help construct complex queries:

### StartupFilter
```python
startup_filter = StartupFilter(
    searchable_locations=[1, 2],  # Location IDs
    stages=[1, 2],  # Stage IDs
    fundings=[1, 2],  # Funding range IDs
    employees_from=10,
    employees_to=100,
    fundings_from=1000000,
    fundings_to=5000000,
    tags=[1, 2],  # Tag IDs
    tags_mode="AND",  # "AND" or "OR"
    trls=[5, 6, 7],  # TRL IDs
    financial_stage_ids=[1, 2],  # Financial stage IDs
    sustainabilities=[1, 2],  # Sustainability IDs
    founded_dates=[{"from": "2020-01-01", "to": "2023-12-31"}],
    founded_dates_from="2020-01-01",
    founded_dates_to="2023-12-31",
    raised_date_from="2022-01-01",
    raised_date_to="2023-12-31",
    last_round_dates=[{"from": "2022-01-01", "to": "2023-12-31"}],
    number_of_round_from=1,
    number_of_round_to=5,
    funding_types=[{"type": "Equity"}],
    sdgs=[7, 13],  # SDG goal IDs
    wildcards=["solar", "renewable"],
    wildcards_fields=[{"field": "name"}, {"field": "description"}],
    investors=[1, 2],  # Investor IDs
    last_funding_types=[{"type": "Series A"}],
    last_fundings_from=[1000000],
    last_fundings_to=[5000000],
    patent_search=["battery", "storage"],
    patents_status=[{"status": "Granted"}],
    application_date_from="2020-01-01",
    application_date_to="2023-12-31"
)
```

### DealFilter
```python
deal_filter = DealFilter(
    acquisition_date_from="2020-01-01",
    acquisition_date_to="2023-12-31",
    dates_from="2022-01-01",
    dates_to="2023-12-31",
    last_round_days=[30, 60, 90],
    amount_from=1000000,
    amount_to=5000000,
    types=[1, 2],  # Deal type IDs
    allow_null_amounts=True,
    number_from=1,
    number_to=5,
    investors=[1, 2],  # Investor IDs
    total_funding_from=1000000,
    total_funding_to=5000000,
    financing_instruments=["Convertible Note", "SAFE"],
    equity_stages=[1, 2],  # Equity stage IDs
    exit_stages=[1, 2]  # Exit stage IDs
)
```

### InvestorFilter
```python
investor_filter = InvestorFilter(
    investor_type_ids=[1, 2],  # Investor type IDs
    include_other_investor_types=True,
    investor_deals_from=10,
    investor_deals_to=100,
    investor_searchable_locations=[1, 2],  # Location IDs
    investor_regions=[1, 2],  # Region IDs
    co_investors=[1, 2],  # Investor IDs
    investments=[1, 2],  # Startup IDs
    investor_ids=[1, 2],  # Investor IDs
    investor_founded_dates_from="2000-01-01",
    investor_founded_dates_to="2020-12-31"
)
```

### ContactFilter
```python
contact_filter = ContactFilter(
    client_id=123,  # Required: Startup ID
    decision_maker=True,
    role_id=1  # Role ID
)
```

### InvestorContactFilter
```python
investor_contact_filter = InvestorContactFilter(
    investor_id=456,  # Required: Investor ID
    decision_maker=True,
    role_id=1  # Role ID
)
```

## Sorting

Use the `Sorting` class to specify how results should be ordered:

```python
# Sort by name in ascending order
sorting = Sorting(field="name", order="asc")

# Sort by date in descending order
sorting = Sorting(field="date", order="desc")
```

## Error Handling

The client will raise exceptions for HTTP errors (4xx, 5xx) and other request-related issues. You can handle these using try/except blocks:

```python
try:
    startups = client.get_startups()
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"Request Error: {e}")
```

## Session Management

The client automatically manages the session cookie and handles authentication. The session will expire after 30 minutes of inactivity. You can manually logout using the `logout()` method if needed.
