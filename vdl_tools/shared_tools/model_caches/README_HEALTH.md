# Health Relevance Classification (ARPA-H)

This module provides health relevance classification using fine-tuned GPT models with PostgreSQL caching, specifically designed for ARPA-H mission alignment.

## Overview

The health relevance system extends `RelevanceCache` to provide:

- **Binary Classification**: Determines if organizations/text are health/biomedical relevant (1) or not (0)
- **Fine-tuned Models**: Uses specialized GPT models trained for ARPA-H relevance
- **Probability Scores**: Extracts confidence scores from model logprobs
- **PostgreSQL Caching**: Efficient database-backed caching for repeated queries
- **Bulk Processing**: Efficient batch processing with threading

## Available Models

- `ARPAH_MODEL`: Original ARPA-H model (`ft:gpt-4.1-mini-2025-04-14:vibrant-data-labs:arpa-h:Bzr8QOcl`)
- `ARPAH_CONSERVATIVE`: Conservative model without "maybe" classifications (recommended) (`ft:gpt-4.1-mini-2025-04-14:vibrant-data-labs:without-maybe:CMM6fpnL`)

The conservative model is recommended as it provides clearer binary decisions.

## Usage

### Basic DataFrame Processing

```python
import pandas as pd
from vdl_tools.shared_tools.model_caches import generate_health_relevance_predictions

df = pd.DataFrame({
    'uuid': ['org_1', 'org_2'],
    'description': ['Biotech developing cancer therapies', 'Restaurant chain']
})

# Generate predictions
df_with_predictions = generate_health_relevance_predictions(
    df=df,
    column_text='description',
    idn='uuid'
)

# Results are added as columns
print(df_with_predictions[['uuid', 'prediction', 'probability']])
# Output:
#   uuid  prediction  probability
#   org_1      1          0.95
#   org_2      0          0.98
```

### Using Specific Models

```python
from vdl_tools.shared_tools.model_caches import generate_health_relevance_predictions
from vdl_tools.shared_tools.model_caches.health_relevance_cache import ARPAH_MODEL

df_with_predictions = generate_health_relevance_predictions(
    df=df,
    column_text='description',
    idn='uuid',
    model=ARPAH_MODEL  # Use less conservative model
)
```

### Manual Label Overrides

```python
df_with_predictions = generate_health_relevance_predictions(
    df=df,
    column_text='description',
    idn='uuid',
    label_override_dict={
        'org_known_relevant': 1,
        'org_known_not_relevant': 0
    }
)
```

### Configuration Options

```python
df_with_predictions = generate_health_relevance_predictions(
    df=df,
    column_text='company_description',  # Text column name
    idn='company_id',                   # ID column name
    model=ARPAH_CONSERVATIVE,           # Model to use
    max_workers=5,                      # Parallel threads
    n_per_commit=100,                   # Database commit frequency
    use_cached_results=True,            # Use cache
    system_prompt="Custom system prompt",  # Override prompt
    prompt_format="Custom format: {text} ->",  # Override format
    prompt_name="custom_health_relevance",  # Cache namespace
)
```

## Custom Prompts

While the default prompts are optimized for ARPA-H mission alignment, you can customize:

```python
df_with_predictions = generate_health_relevance_predictions(
    df=df,
    column_text='description',
    idn='uuid',
    system_prompt="You are an expert in biomedical innovation.",
    prompt_format="Is this relevant to advancing human health? {text} ->"
)
```

## Return Values

- **prediction**: `0` (not health relevant), `1` (health relevant), or `None` (error)
- **probability**: Float between 0-1 representing model confidence, or `None` if unavailable

## Examples

See `example_health_usage.py` for complete examples including:

- Basic usage with caching
- Using different models
- Manual label overrides
- Custom prompts

## Comparison with Climate Relevance

Both modules share the same underlying architecture (`RelevanceCache`) but differ in:

| Feature           | Climate                  | Health (ARPA-H)                |
| ----------------- | ------------------------ | ------------------------------ |
| **Default Model** | `CB_CD_MODEL_4OMINI`     | `ARPAH_CONSERVATIVE`           |
| **System Prompt** | Climate change expert    | Health & biomedical expert     |
| **Focus**         | Climate crisis solutions | Health & biomedical innovation |
| **Prompt Format** | Climate crisis relevance | ARPA-H mission alignment       |

## Database Caching

All predictions are cached in PostgreSQL with the following key:

- `prompt_id`: Generated from prompt string
- `given_id`: Your provided ID (e.g., organization UUID)
- `model_name`: The specific fine-tuned model used
- `text_id`: Hash of the input text

This means:

- ✅ Same text with same ID = cache hit
- ❌ Changed text with same ID = re-run (text change detected)
- ❌ Different model = re-run (model-specific cache)

## Error Handling

The system automatically handles:

- Invalid model responses
- Network timeouts
- Rate limiting
- Malformed text inputs

Errors are logged and stored in the database for analysis. Items with errors can be automatically retried up to `max_errors` times (default: 1).

## Performance Notes

- **Batch processing**: Processes multiple items in parallel using ThreadPoolExecutor
- **Database commits**: Batched at `n_per_commit` intervals (default: 50)
- **API calls**: Only made for uncached items
- **Cache lookup**: Single SQL query for all items

For 10,000 organizations:

- First run: ~30-60 minutes (depending on OpenAI rate limits)
- Subsequent runs: <30 seconds (cache hits)
