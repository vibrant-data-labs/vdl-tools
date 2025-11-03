# LinkedIn Employee Profile Loader

## Overview

The `employee_loader_psql.py` script provides a flexible way to retrieve LinkedIn employee profiles from the Coresignal API and store them in PostgreSQL database. It supports both **Base** and **Clean** employee data models.

## Features

- ✅ **Flexible Model Support**: Works with both `LinkedInBaseEmployee` and `LinkedInCleanEmployee` models
- ✅ **Smart Caching**: Checks database before making API calls to avoid duplicate queries
- ✅ **Original URL Tracking**: Maintains the original URL you provided for easy joining back to your data
- ✅ **Batch Processing**: Commits data in configurable batches for efficiency
- ✅ **Error Handling**: Robust error handling with logging for failed queries
- ✅ **Returns DataFrame**: Returns a pandas DataFrame for immediate use in your workflow

## Database Models

### LinkedInBaseEmployee

- **Table**: `linkedin_base_employee`
- **Primary Key**: `id` (BigInteger)
- **Key Fields**: `full_name`, `profile_url`, `headline`, `experience`, `education`, etc.
- **API Endpoint**: Coresignal `employee_base` endpoint

### LinkedInCleanEmployee

- **Table**: `linkedin_clean_employee`
- **Primary Key**: `member_id` (BigInteger)
- **Key Fields**: `member_full_name`, `member_websites_linkedin`, `member_job_title`, etc.
- **API Endpoint**: Coresignal `employee_clean` endpoint

## Usage

### Basic Usage

```python
from vdl_tools.linkedin import get_employee_profiles
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.config_utils import get_configuration

config = get_configuration()

# Your LinkedIn profile URLs
urls = [
    'https://www.linkedin.com/in/john-doe/',
    'https://www.linkedin.com/in/jane-smith/',
    'https://www.linkedin.com/in/bob-jones/'
]

with get_session() as session:
    # Get Base Employee profiles
    df = get_employee_profiles(
        urls=urls,
        session=session,
        api_key=config['linkedin']['coresignal_api_key'],
        model_type='base',  # or 'clean'
        skip_existing=True,
        n_per_commit=10,
    )

print(f"Retrieved {len(df)} profiles")
print(df[['id', 'full_name', 'original_url']].head())
```

### Using Clean Model

```python
with get_session() as session:
    df = get_employee_profiles(
        urls=urls,
        session=session,
        api_key=config['linkedin']['coresignal_api_key'],
        model_type='clean',  # Use clean model
        skip_existing=True,
    )

print(df[['member_id', 'member_full_name', 'original_url']].head())
```

### Joining Back to Original Data

The returned DataFrame includes an `original_url` column that matches the URLs you provided:

```python
# Your original data
original_df = pd.DataFrame({
    'company': ['Company A', 'Company B'],
    'linkedin_url': [
        'https://www.linkedin.com/in/john-doe/',
        'https://www.linkedin.com/in/jane-smith/'
    ]
})

# Get profiles
with get_session() as session:
    profiles_df = get_employee_profiles(
        urls=original_df['linkedin_url'].tolist(),
        session=session,
        api_key=config['linkedin']['coresignal_api_key'],
        model_type='base',
    )

# Join back to original data
result = original_df.merge(
    profiles_df,
    left_on='linkedin_url',
    right_on='original_url',
    how='left'
)
```

## Parameters

### `get_employee_profiles()`

| Parameter       | Type                  | Default  | Description                             |
| --------------- | --------------------- | -------- | --------------------------------------- |
| `urls`          | `list[str]`           | Required | List of LinkedIn profile URLs           |
| `session`       | SQLAlchemy Session    | Required | Database session                        |
| `api_key`       | `str`                 | Required | Coresignal API key                      |
| `model_type`    | `'base'` or `'clean'` | `'base'` | Which model to use                      |
| `skip_existing` | `bool`                | `True`   | Skip profiles already in database       |
| `n_per_commit`  | `int`                 | `10`     | Number of profiles to commit at once    |
| `max_errors`    | `int`                 | `5`      | Maximum retry count for failed profiles |

## Return Value

Returns a pandas DataFrame with:

- All columns from the chosen model (Base or Clean)
- `original_url`: The URL you provided for this profile

## Examples

### Example 1: Bulk Profile Enrichment

```python
# Load your data with LinkedIn URLs
companies_df = pd.read_csv('companies_with_employees.csv')

with get_session() as session:
    # Get all employee profiles
    employees_df = get_employee_profiles(
        urls=companies_df['employee_linkedin_url'].tolist(),
        session=session,
        api_key=config['linkedin']['coresignal_api_key'],
        model_type='clean',
        skip_existing=True,
    )

# Join back
enriched = companies_df.merge(
    employees_df,
    left_on='employee_linkedin_url',
    right_on='original_url'
)

# Now you have employee details merged with your original data
print(enriched[['company_name', 'member_full_name', 'member_job_title']])
```

### Example 2: Force Refresh (Skip Cache)

```python
with get_session() as session:
    # Force re-fetch even if in database
    df = get_employee_profiles(
        urls=urls,
        session=session,
        api_key=config['linkedin']['coresignal_api_key'],
        model_type='base',
        skip_existing=False,  # Don't skip existing
    )
```

## Notes

- LinkedIn IDs are automatically extracted from URLs using `extract_linkedin_id()`
- Invalid URLs or URLs without extractable IDs are automatically filtered out
- The script uses batch commits for efficiency and to prevent memory issues with large datasets
- All nested/array fields (experience, education, etc.) are stored as JSONB in the database
- Failed API calls are logged but don't stop the entire process

## Database Schema

Both models store nested data (arrays, objects) as JSONB columns. To query nested data:

```python
from sqlalchemy import text

# Query employees with specific skills
query = text("""
    SELECT member_id, member_full_name
    FROM linkedin_clean_employee
    WHERE member_skills @> '["Python"]'
""")

results = session.execute(query).fetchall()
```

## Troubleshooting

**Q: No profiles are returned**

- Check that your URLs are valid LinkedIn profile URLs
- Verify your API key is correct and has available credits
- Check logs for API errors

**Q: Some profiles are missing**

- Check Coresignal API credits (logged with each request)
- Verify profiles exist in Coresignal's database (not all profiles are available)
- Check if `skip_existing=True` is excluding profiles you want to refresh

**Q: How do I see what's in the database?**

```python
from vdl_tools.shared_tools.database_cache.database_models.linkedin_base_employee import LinkedInBaseEmployee

with get_session() as session:
    count = session.query(LinkedInBaseEmployee).count()
    print(f"Total base employee profiles in database: {count}")
```

## Related Files

- `linkedin_base_employee.py` - Base employee model definition
- `linkedin_clean_employee.py` - Clean employee model definition
- `coresignal_query.py` - API query functions
- `linkedin_url.py` - URL parsing utilities
