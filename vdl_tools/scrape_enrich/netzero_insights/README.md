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
* **`foundedDate` changed type.** v1 returned a date, v2 returns an integer
  `foundedYear`. The alias exists so the column is present, not because the
  values are interchangeable — check any code that parses it as a date.
* **Some filters are gone.** Filtering a search by company ID (`ids`) has no v2
  equivalent, and `taxonomyItems` became `tagIDs` **keyed by `tagID`, not by
  the taxonomy item's `id`**. Both raise a `ValueError` explaining the
  replacement rather than silently returning a wider result set. Translate
  taxonomy item IDs with `NetZeroAPI.get_taxonomy_item_tag_ids()`.
* **`investorIDs` moved.** On a company search it is now a deal-filter
  predicate; `create_search_filter(include_investors=...)` handles this.
* **`get_funding_round_details()` raises on v2** — NZI documents no
  deal-by-ID endpoint. Use `get_company_funding_rounds()` or `search_deals()`.
* **Taxonomy endpoints were not republished under the v2 host.** They are still
  documented against `api.netzeroinsights.com` only.

### Before flipping the default to v2

Nothing below has been run against the live v2 API yet:

1. Confirm `POST /auth/login` returns a token for our account, and whether it
   arrives in the response header or body (`_extract_access_token` reads both).
2. Compare one company, one investor and one company's deals fetched both ways
   and diff the normalised records.
3. Confirm the `roundType` / `financingType` deal mappings — these are marked
   `# UNVERIFIED` in `api_v2.py` because v2 exposes `type`, `fundingType`,
   `equityStage`, `exitStage` and `capitalStage` where v1 had two fields.
4. Re-run the `process_nzi` survival-rate pipeline on a small cohort and check
   the stage buckets match a v1 run.

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
