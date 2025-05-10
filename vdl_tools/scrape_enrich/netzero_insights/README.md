# NetZero Insights API Client

A Python client library for interacting with the NetZero Insights API.

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
