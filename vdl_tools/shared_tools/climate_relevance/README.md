# Climate Relevance Classification

This module provides climate relevance classification using fine-tuned GPT models with PostgreSQL caching.

## Overview

The `ClimateRelevanceCache` class extends `PromptResponseCacheSQL` to provide:

- **Binary Classification**: Determines if organizations/text are climate relevant (1) or not (0)
- **Fine-tuned Models**: Uses specialized GPT models trained for climate relevance
- **Probability Scores**: Extracts confidence scores from model logprobs
- **PostgreSQL Caching**: Efficient database-backed caching for repeated queries
- **Bulk Processing**: Efficient batch processing with threading

## Available Models

- `CB_FT_MODEL`: Original Crunchbase fine-tuned model
- `CD_FT_MODEL`: Candid fine-tuned model  
- `CB_CD_MODEL_4OMINI`: Combined model on GPT-4o-mini (recommended)
- `CB_CD_MODEL_4OMINI_TAILWIND`: Tailwind-specific model
- `CB_MODEL_ARPAH`: ARPA-H specific model

## Usage

### Single Prediction

```python
from vdl_tools.shared_tools.climate_relevance import ClimateRelevanceCache
from vdl_tools.shared_tools.database_cache.database_utils import get_session

session = get_session()
cache = ClimateRelevanceCache(session=session)

prediction, probability = cache.get_climate_relevance(
    given_id="tesla_example",
    text="Tesla manufactures electric vehicles and clean energy solutions."
)

print(f"Prediction: {prediction}, Confidence: {probability}")
# Output: Prediction: 1, Confidence: 0.95
```

### Bulk Processing

```python
data = [
    ("org_1", "Tesla makes electric cars"),
    ("org_2", "McDonald's serves fast food"),
]

results = cache.bulk_get_climate_relevance(data)

for org_id, (prediction, probability) in results.items():
    print(f"{org_id}: {prediction} ({probability})")
```

### DataFrame Integration

#### Method 1: Using `generate_predictions` Function (Recommended)

```python
import pandas as pd
from vdl_tools.shared_tools.climate_relevance import generate_predictions

df = pd.DataFrame({
    'uuid': ['org_1', 'org_2'],
    'description': ['Solar panel manufacturer', 'Restaurant chain']
})

# Generate predictions for DataFrame (standalone function)
predictions = generate_predictions(
    df=df,
    column_text='description',
    idn='uuid'
)

# predictions is a dict: {'org_1': (1, 0.95), 'org_2': (0, 0.98)}
for org_id, (prediction, probability) in predictions.items():
    relevance = "Climate Relevant" if prediction == 1 else "Not Climate Relevant"
    print(f"{org_id}: {relevance} (confidence: {probability})")
```

**See example_usage.py for more examples.

### DataFrame Function Parameters

The standalone `generate_predictions` function and `predict_dataframe` method support flexible column configuration:

```python
from vdl_tools.shared_tools.climate_relevance import generate_predictions

# Standalone function with custom parameters
predictions = generate_predictions(
    df=df,
    column_text='company_description',  # Text column name
    idn='company_id',                   # ID column name
    model=CB_CD_MODEL_4OMINI,          # Override model
    max_workers=5,                      # Parallel processing
    n_per_commit=100,                   # Database batching
    use_cached_results=True,            # Use cache
    label_override_dict={'id_1': 1},    # Manual overrides
    system_prompt="Custom system prompt",  # Custom prompts
    prompt_format="Custom format: {text} ->",  # Custom format
)

# Works with any DataFrame structure
df_custom = pd.DataFrame({
    'organization_id': ['A', 'B', 'C'],
    'org_description': ['Solar company', 'Oil company', 'Wind farm'],
    'sector': ['Energy', 'Energy', 'Energy']
})

predictions = generate_predictions(
    df=df_custom,
    column_text='org_description',
    idn='organization_id'
)
```

## Configuration

### Custom Prompts

```python
cache = ClimateRelevanceCache(
    session=session,
    system_prompt="You are a climate expert specializing in energy transition.",
    prompt_format="Is this organization relevant to climate solutions? {text} ->",
    model=ClimateRelevanceCache.CB_CD_MODEL_4OMINI
)
```

### Performance Tuning

```python
results = cache.bulk_get_climate_relevance(
    data,
    max_workers=5,        # Parallel threads
    n_per_commit=100,     # Database commit frequency
    max_errors=2,         # Error tolerance per item
)
```

## Migration from Legacy Script

The new implementation replaces `gpt_relevant_for_thinning.py` with these improvements:

- **Database Caching**: PostgreSQL instead of JSONLines files
- **Better Error Handling**: Automatic retry and error tracking
- **Consistent API**: Follows same patterns as other VDL tools
- **Thread Safety**: Safe for concurrent access
- **Monitoring**: Built-in logging and progress tracking
- **DataFrame Integration**: Native pandas support with `generate_predictions()` and `predict_dataframe()`

### Legacy vs New API

**Legacy Script:**
```python
predictions = generate_predictions(
    df, chunk_size, column_text, save_path, model, idn, max_workers
)
df['prediction'], df['probability'] = zip(*df[idn].map(predictions))
```

**New Implementation:**
```python
from vdl_tools.shared_tools.climate_relevance import generate_predictions

# Standalone function (similar interface to legacy)
predictions = generate_predictions(df, column_text, idn, model)

# Or use class methods for more control
with get_session() as session:
    cache = ClimateRelevanceCache(session=session)
    df_with_predictions = cache.predict_dataframe(df, column_text, idn, model)
```

## Return Values

- **Prediction**: `0` (not climate relevant), `1` (climate relevant), or `None` (error)
- **Probability**: Float between 0-1 representing model confidence, or `None` if unavailable

## Error Handling

The system automatically handles:
- Invalid model responses
- Network timeouts
- Rate limiting
- Malformed text inputs

Errors are logged and stored in the database for analysis.
