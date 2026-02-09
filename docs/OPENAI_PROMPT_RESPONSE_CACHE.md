# OpenAI Prompt/Response Cache — Usage Guide

This document describes how to use the SQL-backed prompt/response caches in `vdl-tools`: **PromptResponseCacheSQL**, **InstructorPRC**, and **FewShotCache**. They store OpenAI API inputs and outputs in a database so repeated calls with the same inputs can return cached results instead of calling the API again.

---

## Table of contents

1. [Prerequisites](#prerequisites)
2. [PromptResponseCacheSQL](#promptresponsecachesql)
3. [InstructorPRC (prompt_response_cache_instructor)](#instructorprc-prompt_response_cache_instructor)
4. [FewShotCache (few_shot_cache)](#fewshotcache-few_shot_cache)
5. [Running the examples](#running-the-examples)

---

## Prerequisites

- **Database**: A PostgreSQL database configured via your config (e.g. `config.sample.ini` / `get_configuration()`). The cache uses tables `prompt` and `prompt_response`.

- **OpenAI API**: Set `OPENAI_API_KEY` (or configure it in your config) for live API calls when the cache misses.

- **Imports**:

  ```python
  from vdl_tools.shared_tools.database_cache.database_utils import get_session
  from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
  from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
  from vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache import FewShotCache
  ```

---

## PromptResponseCacheSQL

**Module:** `vdl_tools.shared_tools.openai.prompt_response_cache_sql`

A generic SQL-backed cache for OpenAI Responses API completions. Cache keys are `(prompt_id, given_id, text_id)` where `text_id` is a hash of the input text. You can optionally scope by model with `filter_by_model=True`.

### Constructor

```python
PromptResponseCacheSQL(
    session,                    # SQLAlchemy session (e.g. from get_session())
    prompt=None,                # Optional: Prompt instance
    prompt_str=None,            # Optional: raw prompt string (creates/registers prompt)
    prompt_id=None,             # Optional: id of existing prompt in DB
    prompt_name="",             # Name when creating from prompt_str
    prompt_description="",      # Description when creating from prompt_str
    filter_by_model=False,      # If True, cache is keyed by model name too
    model="gpt-4.1-mini",       # OpenAI model for API calls (and cache when filter_by_model)
    store_results=True,         # If False, do not persist to DB (no cache fill)
)
```

You must provide at least one of: `prompt`, `prompt_str`, or `prompt_id`.

### Single completion: `get_cache_or_run`

**Inputs**

| Argument            | Type  | Description                                                |
|---------------------|--------|------------------------------------------------------------|
| `given_id`          | `str`  | User-defined id for the request (e.g. URL, row id).       |
| `text`              | `str`  | Input text sent to the model (cache is keyed by its hash).|
| `use_cached_result` | `bool` | If `True`, return cached response when available. Default `True`. |
| `**kwargs`          |        | Passed to the OpenAI API (e.g. `text_format`, `reasoning`).|

**Output**

- **On cache hit or successful API call:** A `dict` with keys from the cached/created `PromptResponse` row, including:
  - `prompt_id`, `given_id`, `model_name`, `text_id`, `input_text`
  - `response_text` — main text output (string)
  - `response_full` — full API response (JSON string or JSONB)
  - `num_errors`, `date_added`, `date_updated`
- **On API failure (and error stored):** `None`

**Example**

```python
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

with get_session() as session:
    prc = PromptResponseCacheSQL(
        session=session,
        model="gpt-5-nano",
        prompt_str="Give the world capital of the country.",
    )
    out = prc.get_cache_or_run(
        given_id="test",
        text="France",
        use_cached_result=False,
    )
    print(out["response_text"])
```

**Example output**

```
Paris
```

With a Pydantic response model and optional reasoning:

```python
from pydantic import BaseModel

class Response(BaseModel):
    capital: str

with get_session() as session:
    prc = PromptResponseCacheSQL(
        session=session,
        model="gpt-4.1-mini",
        prompt_str="Give the world capital of the country.",
    )
    out = prc.get_cache_or_run(
        given_id="test",
        text="France",
        use_cached_result=False,
        text_format=Response,
    )
    print(out["response_text"])
```

**Example output**

```json
{"capital":"Paris"}
```

With a reasoning model (gpt-5-nano) and structured output, including reading the reasoning effort and summary from `response_full`:

```python
import json
from pydantic import BaseModel
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

class Response(BaseModel):
    capital: str

with get_session() as session:
    prc = PromptResponseCacheSQL(
        session=session,
        model="gpt-5-nano",
        prompt_str="Give the world capital of the country.",
    )
    out = prc.get_cache_or_run(
        given_id="test",
        text="France",
        use_cached_result=False,
        text_format=Response,
        reasoning={"effort": "high", "summary": "auto"},
    )
    print("GPT-5-Nano response with response model:", out["response_text"])

    # Parse response_full to access reasoning metadata and summary
    response_from_api_as_dict = json.loads(out["response_full"])
    print("Reasoning effort:", response_from_api_as_dict["reasoning"]["effort"])

    reasoning_list = [
        x for x in response_from_api_as_dict["output"]
        if x["type"] == "reasoning"
    ][0]["summary"]
    reasoning_summary_full = ""
    for i, reasoning in enumerate(reasoning_list):
        reasoning_summary_full += f"Step {i+1}: {reasoning['text']}\n\n"
    print("Reasoning summary:", reasoning_summary_full)
```

**Example output (response_text)**

```json
{"capital":"Paris"}
```

**Example output (reasoning effort)**

```
high
```

**Example output (reasoning summary)**

```
Step 1: **Clarifying user request**

The user asks for the "world capital" of France, which seems like they want the capital city, Paris. ...

Step 2: **Understanding response format**

The user has specified that the output should be a simple JSON object, specifically with a "capital" field. ...

Step 3: **Finalizing JSON output**

I need to keep the response concise, so I'll format it as {"capital":"Paris"}. ...
```

**Without storing (dry run / testing)**

```python
prc = PromptResponseCacheSQL(
    session=session,
    model="gpt-4.1-mini",
    prompt_str="Give the world capital of the country.",
    store_results=False,
)
out = prc.get_cache_or_run(
    given_id="test_cambodia_no_storage",
    text="Cambodia",
    use_cached_result=True,
)
print(out["response_text"])
```

**Example output**

```
The capital of Cambodia is Phnom Penh.
```

### Bulk completions: `bulk_get_cache_or_run`

**Inputs**

| Argument            | Type                     | Description                                      |
|---------------------|--------------------------|--------------------------------------------------|
| `given_ids_texts`   | `list[tuple[str, str]]`  | Pairs of `(given_id, text)`.                    |
| `use_cached_result` | `bool`                   | Use cache when available. Default `True`.       |
| `n_per_commit`      | `int`                    | Chunk size for DB commits. Default `50`.        |
| `max_workers`       | `int`                    | Parallel workers for API calls. Default `3`.   |
| `max_errors`        | `int`                    | Max errors per (given_id, text) before skip. Default `1`. |
| `**kwargs`          |                          | Passed to the OpenAI API for each call.         |

**Output**

- `dict[str, dict]`: map from `given_id` to the same response dict shape as `get_cache_or_run` (only for succeeded or cached entries).

**Example**

```python
results = prc.bulk_get_cache_or_run(
    given_ids_texts=[
        ("id1", "France"),
        ("id2", "Japan"),
    ],
    use_cached_result=True,
    max_workers=3,
)
for gid, data in results.items():
    print(gid, data["response_text"])
```

### Model-specific kwargs

- **All models:** `response_format` / `text_format` (e.g. Pydantic model for structured output).
- **Reasoning models** (e.g. `gpt-5-nano`): `reasoning={"effort": "high", "summary": "auto"}` etc.
- **Non-reasoning models** (e.g. `gpt-4.1-mini`): `temperature`, `max_tokens`, `stop`, etc. as supported by the API.

---

## InstructorPRC (prompt_response_cache_instructor)

**Module:** `vdl_tools.shared_tools.openai.prompt_response_cache_instructor`

A subclass of `PromptResponseCacheSQL` that **always** uses a Pydantic response model. The prompt stored in the cache is the user prompt plus the response model’s JSON schema, so cache identity includes the output shape.

### Constructor

```python
InstructorPRC(
    session,
    prompt_str,           # User-facing prompt text
    response_model,       # Pydantic model class (e.g. BaseModel subclass)
    prompt_name=None,
    prompt_id=None,
    model="gpt-4.1-mini",
    filter_by_model=False,
    store_results=True,
)
```

### Single completion: `get_cache_or_run`

Same signature and semantics as `PromptResponseCacheSQL.get_cache_or_run`. The API is always called with `text_format=response_model`. You can still pass other model-specific kwargs.

**Inputs:** `given_id`, `text`, `use_cached_result=True`, `**kwargs`.

**Output:** Same dict shape as SQL cache: `response_text` (string, often JSON), `response_full`, etc.

**Example**

```python
from pydantic import BaseModel
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC

class Response(BaseModel):
    capital: str

with get_session() as session:
    prc = InstructorPRC(
        session=session,
        model="gpt-5-nano",
        prompt_str="Give the world capital of the country.",
        response_model=Response,
    )
    out = prc.get_cache_or_run(
        given_id="test",
        text="France",
        use_cached_result=False,
    )
    print(out["response_text"])
```

**Example output**

```json
{"capital": "Paris"}
```

**Example (gpt-4.1-mini)**

```python
prc = InstructorPRC(
    session=session,
    model="gpt-4.1-mini",
    prompt_str="Give the world capital of the country.",
    response_model=Response,
)
out = prc.get_cache_or_run(given_id="test", text="France", use_cached_result=False)
print(out["response_text"])
```

**Example output**

```json
{"capital":"Paris"}
```

### Bulk: `bulk_get_cache_or_run`

Same as `PromptResponseCacheSQL.bulk_get_cache_or_run`: input list of `(given_id, text)`, output `dict[given_id, response_dict]`.

---

## FewShotCache (few_shot_cache)

**Module:** `vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache`

An `InstructorPRC` subclass for **taxonomy relevance classification**: given an entity and an activity (category), it asks the model whether the entity is relevant to the category, using few-shot examples. The response is always structured as `IsRelevant` (`{"is_relevant": bool}`). Cache keys include the full input (entity, activity, examples).

### Constructor

```python
FewShotCache(
    session,
    model="gpt-4.1-mini",
    filter_by_model=False,
    prompt_str=DEFAULT_PROMPT,   # e.g. climate expert intro
    prompt_name="taxonomy_few_shot",
    store_results=True,
)
```

### Single classification: `get_cache_or_run`

**Inputs**

| Argument               | Type                          | Description                                   |
|------------------------|-------------------------------|-----------------------------------------------|
| `given_id`             | `str`                         | User-defined id for the request.              |
| `activity_entity_dict` | `EntityActivityDict` or `dict`| Keys: `entity_description`, `activity_description`; optional `activity_name`. |
| `examples_dicts`       | `list[dict]` or `None`        | Few-shot examples; default uses built-in `EXAMPLES`. |
| `use_cached_result`    | `bool`                        | Use cache when available. Default `True`.    |
| `**kwargs`             |                               | Passed to the OpenAI API.                     |

**Output**

- Same as the base cache: `dict` with `response_text`, `response_full`, etc., or `None` on stored error.
- `response_text` is a JSON string: `{"is_relevant": true}` or `{"is_relevant": false}`.

**Example**

```python
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache import FewShotCache, EXAMPLES_CLASSED

with get_session() as session:
    few_shot_cache = FewShotCache(
        session=session,
        model="gpt-5-nano",
        prompt_str="Always reply with False.",
    )
    response = few_shot_cache.get_cache_or_run(
        given_id="test",
        activity_entity_dict={
            "entity_description": "DogRunner trains dogs to run",
            "activity_description": "Exercise for Pets: Things that make pets exercise",
        },
        examples_dicts=EXAMPLES_CLASSED,
        use_cached_result=False,
    )
    print(response["response_text"])
```

**Example output**

```json
{"is_relevant": false}
```

### Bulk: `bulk_get_cache_or_run`

**Inputs**

| Argument                | Type                                      | Description |
|-------------------------|-------------------------------------------|-------------|
| `given_ids_texts`       | `list[tuple[str, EntityActivityDictExamplesDict]]` | Pairs of `(given_id, entity_activity_dict_examples_dict)`. |
| `use_cached_result`     | `bool`                                    | Default `True`. |
| `n_per_commit`          | `int`                                     | Default `50`. |
| `max_workers`           | `int`                                     | Default `5`. |
| `max_errors`             | `int`                                     | Default `1`. |
| `return_parsed_results` | `bool`                                    | If `True`, return `{id: {"is_relevant": bool}}`. If `False`, return full response dicts. Default `True`. |
| `**kwargs`              |                                           | Passed to the API. |

**Output**

- If `return_parsed_results=True`: `dict[str, dict]` mapping `given_id` to `{"is_relevant": bool}`.
- If `return_parsed_results=False`: `dict[str, dict]` mapping `given_id` to the full cache response dict (same shape as single `get_cache_or_run`).

### Input models

- **EntityActivityDict:** `entity_description`, `activity_description`, optional `acitvity_name`.
- **ExampleDict:** `entity_description`, `activity_description`, `relevant` (bool), optional `activity_name`.
- **EntityActivityDictExamplesDict:** `entity_activity_dict`, optional `examples_dicts` (list of `ExampleDict`).

---

## Running the examples

Activate the virtualenv, then run the module `__main__` blocks or the snippets above.

```bash
pyenv activate vdl-tools-312
```

**Run all examples in prompt_response_cache_sql:**

```bash
python vdl_tools/shared_tools/openai/prompt_response_cache_sql.py
```

**Example console output:**

```
GPT-5-Nano response: Paris
GPT-4.1-Mini response: The capital of France is Paris.
GPT-4.1-Mini response with response model: {"capital":"Paris"}
GPT-5-Nano response with response model: {"capital":"Paris"}
GPT-5-Nano response with response model reasoning effort: high
GPT-5-Nano response with response model reasoning summary: Step 1: ...
GPT-4.1-Mini response without storing: The capital of Cambodia is Phnom Penh.
```

**Run prompt_response_cache_instructor examples:**

```bash
python vdl_tools/shared_tools/openai/prompt_response_cache_instructor.py
```

**Example console output:**

```
GPT-5-Nano response: {"capital": "Paris"}
GPT-4.1-Mini response: {"capital":"Paris"}
```

**FewShotCache:** The `if __name__ == "__main__"` block in `few_shot_cache.py` uses a debugger. To run a standalone example without it:

```bash
python -c "
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.taxonomy_mapping.few_shot_cache import FewShotCache, EXAMPLES_CLASSED

with get_session() as session:
    c = FewShotCache(session=session, model='gpt-5-nano', prompt_str='Always reply with False.')
    r = c.get_cache_or_run(
        given_id='test',
        activity_entity_dict={
            'entity_description': 'DogRunner trains dogs to run',
            'activity_description': 'Exercise for Pets: Things that make pets exercise',
        },
        examples_dicts=EXAMPLES_CLASSED,
        use_cached_result=False,
    )
    print(r['response_text'])
"
```

**Example output:** `{"is_relevant": false}`

---

## Summary

| Component               | Use case                          | Main methods                | Response shape (single)                    |
|------------------------|-----------------------------------|-----------------------------|--------------------------------------------|
| **PromptResponseCacheSQL** | Generic prompt/response caching   | `get_cache_or_run`, `bulk_get_cache_or_run` | `dict` with `response_text`, `response_full`, etc. |
| **InstructorPRC**      | Caching with fixed Pydantic schema| Same                        | Same; `response_text` is JSON from schema. |
| **FewShotCache**       | Entity–activity relevance (few-shot) | Same + custom bulk return   | Same; `response_text` is `{"is_relevant": bool}`. |

All caches key by prompt (and optionally model when `filter_by_model=True`). Changing the input text (or entity/activity/examples for FewShotCache) produces a new cache key and may trigger a new API call.
