# Tutorial: OpenAI and Vercel AI Gateway with the SQL prompt cache

`PromptResponseCacheSQL` can call OpenAI directly or call models through Vercel AI Gateway. Both paths use the OpenAI Python SDK's Responses interface, so single requests, bulk requests, response storage, and Pydantic structured outputs use the same interface.

This tutorial covers `PromptResponseCacheSQL`. `InstructorPRC` and `FewShotCache` have their own constructors or completion overrides; do not assume they accept or honor the gateway options described here. See the [cache usage guide](OPENAI_PROMPT_RESPONSE_CACHE.md) for those classes.

## What changes when you choose Vercel?

| Setting or behavior | Direct OpenAI (default) | Vercel AI Gateway |
| --- | --- | --- |
| Constructor | `api_backend="openai"`, or omit it | `api_backend="vercel"` |
| Credentials | `OPENAI_API_KEY`, with the existing config fallback | `AI_GATEWAY_API_KEY`, or `[vercel] vercel_api_key` in config |
| Model name | For example, `gpt-4.1-mini` | Creator/model ID, for example, `anthropic/claude-sonnet-4.6` |
| Serving provider | OpenAI | Vercel chooses by default; optional preference or restriction |
| Request parameters | Responses API kwargs | Responses API kwargs, plus provider options through `extra_body` |
| Input truncation | Existing OpenAI token counting and context-budget calculation | No automatic OpenAI-based truncation; the gateway enforces model limits |
| Returned cache result | Dictionary containing `response_text`, `response_full`, and cache metadata | Same dictionary shape |

The `anthropic/` prefix identifies the **model creator**. The **serving provider** could be Anthropic or another supported host such as Bedrock. `api_backend` selects which service receives the request; `model_provider` expresses a preferred serving provider within Vercel.

Vercel documents its compatible endpoint and parameters in the [OpenResponses guide](https://vercel.com/docs/ai-gateway/sdks-and-apis/openresponses).

## 1. Configure credentials and the database

Use a Vercel **AI Gateway API key**. Add this section to `config.ini`:

```ini
[vercel]
vercel_api_key = your-gateway-key
```

The existing configuration loader reads `config.ini` in the working directory, or the file specified by `VDL_GLOBAL_CONFIG_PATH`. The key is read when the gateway client is first created; restart the process after changing it.

Alternatively, set the environment variable, which takes precedence over the config value:

```bash
export AI_GATEWAY_API_KEY="your-gateway-key"
```

For direct OpenAI requests, keep your existing OpenAI configuration, or set:

```bash
export OPENAI_API_KEY="your-openai-key"
```

Gateway-only requests do not need an OpenAI key. The shared module initializes direct OpenAI clients lazily, when they are first used, instead of requiring OpenAI credentials at import time. The gateway client is also created on first use and reused.

Both paths still require the existing PostgreSQL configuration and cache tables. Follow the [cache prerequisites](OPENAI_PROMPT_RESPONSE_CACHE.md#prerequisites). This change adds no database migration to an installation already using `request_hash` and `request_kwargs`.

## 2. Make a gateway request

```python
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL

with get_session() as session:
    cache = PromptResponseCacheSQL(
        session=session,
        prompt_str="Summarize the input in one sentence.",
        model="anthropic/claude-sonnet-4.6",
        api_backend="vercel",
        filter_by_model=True,
    )
    result = cache.get_cache_or_run(
        given_id="example-1",
        text="A community garden supplies fresh vegetables to local families.",
        max_output_tokens=1000,
    )
    if result is None:
        raise RuntimeError("Completion failed; inspect the logged API error.")
    print(result["response_text"])
```

No `model_provider` is supplied, so Vercel selects the serving provider. Use a model ID available to your gateway account.

To use direct OpenAI instead, change the constructor to `api_backend="openai"` and `model="gpt-4.1-mini"`. Existing callers that omit `api_backend` continue using OpenAI; model names do not automatically switch the backend.

## 3. Pass reasoning and other parameters

Pass model-supported Responses parameters on each call, just as for direct OpenAI:

```python
# Run inside the session block above, using a model that supports reasoning.
result = cache.get_cache_or_run(
    given_id="example-2",
    text="Explain the tradeoffs between two approaches to storing rainwater.",
    reasoning={"effort": "high"},
    max_output_tokens=4000,
)
```

Vercel translates `reasoning.effort` into the serving provider's reasoning configuration. Supported effort levels and their effects depend on the model and route; see [Vercel's reasoning documentation](https://vercel.com/docs/ai-gateway/sdks-and-apis/openresponses/reasoning).

Other parameters, such as `temperature`, `top_p`, `tools`, and `tool_choice`, are forwarded unchanged. Only send parameters supported by the selected model. This wrapper uses the Responses API: use `max_output_tokens`, and express reasoning as `reasoning={"effort": ...}` rather than a Chat Completions `reasoning_effort` argument.

For options outside the SDK's named parameters, use `extra_body`. Vercel's provider-specific configuration lives under the camel-case `providerOptions` field within that body.

## 4. Prefer or restrict a serving provider

To prefer Bedrock while retaining Vercel's fallback behavior, add this constructor argument:

```python
cache = PromptResponseCacheSQL(
    session=session,
    prompt_str="Summarize the input in one sentence.",
    api_backend="vercel",
    model="anthropic/claude-sonnet-4.6",
    model_provider="bedrock",
    filter_by_model=True,
)
```

This adds `providerOptions.gateway.order = ["bedrock"]`. Omit `model_provider` or set it to `None` for automatic routing. Passing `model_provider` with the direct OpenAI backend raises `ValueError`.

For an explicit preference order on an individual call:

```python
result = cache.get_cache_or_run(
    given_id="example-3",
    text="Summarize our garden project.",
    extra_body={
        "providerOptions": {
            "gateway": {"order": ["anthropic", "bedrock"]},
        },
    },
)
```

An explicit per-call `order` overrides the constructor's provider preference. Other provider options are preserved, and the caller's dictionary is not mutated.

To require a particular provider, use `only`:

```python
result = cache.get_cache_or_run(
    given_id="example-4",
    text="Summarize our garden project.",
    extra_body={
        "providerOptions": {
            "gateway": {"only": ["bedrock"]},
        },
    },
)
```

A restriction can make a request fail when the allowed provider cannot serve it. Use a provider supported for the selected model. See [Vercel's routing documentation](https://vercel.com/docs/ai-gateway/sdks-and-apis/openresponses/advanced) for routing, restrictions, and model fallbacks.

## 5. Use structured output or bulk requests

A Pydantic schema uses the existing `text_format` argument:

```python
from pydantic import BaseModel

class Summary(BaseModel):
    summary: str

# Run inside an active session, using the cache constructed above.
result = cache.get_cache_or_run(
    given_id="structured-1",
    text="The garden supplies vegetables to 40 families.",
    text_format=Summary,
)
if result is not None:
    parsed = Summary.model_validate_json(result["response_text"])
    print(parsed.summary)
```

The cache still returns a dictionary; `text_format` does not change it into a Pydantic object. Model support for schema-constrained output varies; see [Vercel's structured-output documentation](https://vercel.com/docs/ai-gateway/sdks-and-apis/openresponses/structured-outputs).

Bulk calls accept the same request parameters and constructor routing:

```python
results = cache.bulk_get_cache_or_run(
    given_ids_texts=[
        ("garden-1", "The garden supplies vegetables to 40 families."),
        ("garden-2", "Volunteers teach composting every Saturday."),
    ],
    max_output_tokens=1000,
    max_workers=4,
)
for given_id, result in results.items():
    print(given_id, result["response_text"])
```

Failed bulk items are omitted from the results. Single-call API failures return `None`. Errors are recorded when response writes are enabled.

## 6. Understand cache reuse before comparing models

Set **`filter_by_model=True`** when comparing backends, models, providers, or hyperparameters. With it enabled, reads match the model name and a `request_hash` derived from output-affecting request options, in addition to the prompt and input identity.

- Vercel calls include `api_backend="vercel"` in the cache identity.
- `model_provider` is folded into `extra_body.providerOptions.gateway.order` before hashing.
- Reasoning settings, supported allowlisted hyperparameters, and the entire `extra_body` contribute to the hash.
- The hash captures requested routing, not the actual provider selected by automatic routing or fallback. Repeating the same request can reuse the first cached result even if Vercel would choose a different provider now.
- The legacy default, **`filter_by_model=False`**, shares cached results across model, backend, and parameter identities. Changing providers alone will not force an API call with that default.

Existing direct OpenAI calls without `extra_body` keep their previous hashes. Calls with no output-affecting kwargs still hash to the empty string. Existing calls that pass `extra_body` now get a different hash because that field was previously ignored; with model filtering enabled, they populate new entries. No existing rows are deleted. The repository's pre-existing `extra_body` call in `model_benchmark.py` calls the SDK directly and does not use this SQL cache.

The allowlist is `KWARG_KEYS_THAT_AFFECT_OUTPUT` in `database_models/prompt.py`. Not every SDK argument is part of the cache identity. In particular, `text_format` is excluded: changing only the Pydantic schema can reuse an earlier response. Use a distinct prompt for a distinct output schema, or explicitly refresh the response.

To force a fresh completion without storing that response:

```python
result = cache.get_cache_or_run(
    given_id="experiment-1",
    text="Summarize our garden project.",
    read_from_cache=False,
    write_to_cache=False,
)
```

These flags control response-cache reads and writes. The constructor still uses the database to look up or register the prompt. `store_results=False` also disables response persistence, but does not disable cache reads. Prefer these explicit flags to the deprecated `use_cached_result` argument.

## Validation

Run the offline tests from the repository root:

```bash
python -m pytest vdl_tools/shared_tools/tests/test_vercel_prompt_response_cache.py -q
```

They exercise the real OpenAI SDK with mocked HTTP responses, including request serialization, structured parsing, provider routing, errors, single/bulk response persistence calls, and legacy cache-hash compatibility. Database calls are mocked; PostgreSQL upserts are compiled and inspected. These tests do not validate live provider availability or execute SQL against PostgreSQL.
