# Refactor `PromptResponseCacheSQL` for throughput, correctness, and ergonomics

> **Living plan.** Updated as steps land. Reference this from future sessions
> when picking up Steps 2–4.

## Status

| Step | Status | PR |
|---|---|---|
| 1. API-only workers + bulk upsert | ✅ Shipped | #126 |
| 3. `read_from_cache` / `write_to_cache` per-call flags | ✅ Shipped | _this PR_ |
| 2. Tenacity retry on validation/JSON parse errors | ⏳ Not started | — |
| 4. `request_hash` column for kwargs-aware cache keys | ⏳ Not started | — |

> Step 3 shipped ahead of Step 2: the two are independent (Step 2 lives in
> `openai_api_utils.py`, Step 3 in the cache flag plumbing) and Step 3 was
> the higher-utility lever for in-flight runs.

## Context

`PromptResponseCacheSQL.bulk_get_cache_or_run` showed ~constant wall-clock
(~32–35s for 150 items) regardless of `max_workers` when run locally against
a remote Postgres, while the colleague's uncached `hierarchical_taxonomy_mapping`
scaled linearly to the same workload in ~6s. EC2 numbers were closer but the
cached version still trailed (3.84s vs 2.86s @ 200 workers).

Root cause: workers called `self.session.merge()` from inside the thread pool
(`_get_completion_store` in `prompt_response_cache_sql.py`). SQLAlchemy
`Session` is not thread-safe, and each `merge()` issued a `SELECT` round-trip
before INSERT/UPDATE. On a remote DB (~30–50ms RTT), 150 items × 1 SELECT ≈
~7s of pure latency that workers cannot overlap. On EC2 (~1ms RTT) the
absolute hit shrunk but the concurrency ceiling remained.

Alongside the perf issue, three correctness/ergonomics items surfaced:

- Pydantic `EOF while parsing` errors from truncated model output need retry
  on the API call, not as a post-hoc surface.
- Cache key currently ignores model-behavior-changing kwargs (`reasoning`,
  `tools`/web search, `temperature`, future agent kwargs), so different
  request shapes collide on the same row. Note `text_format` does **not**
  belong in the key — we store the raw `response_full` and structured
  parsing happens at retrieval time, so the same row can be re-parsed
  against a different Pydantic model safely.
- `use_cached_result` conflates read and write semantics; a separate
  `write_to_cache` flag is needed for safe re-runs and dry-run testing.

Intended outcome: bulk cache throughput should be limited by the OpenAI API
rather than the DB; transient model JSON errors should retry transparently;
cache keys should be unambiguous; and callers should be able to disable
reads and writes independently.

## Discovery notes

- Alembic is wired up at `alembic.ini` →
  `vdl_tools/shared_tools/database_cache/data_migrations/`. Recent migrations
  in `data_migrations/versions/` show the team uses `op.alter_column`,
  `op.create_table`, `op.add_column`. Schema changes belong in a new
  Alembic revision, not in `recreate_db`.
- `PromptResponse` PK is composite on `(prompt_id, given_id, model_name)` —
  Step 4 will extend this to add `request_hash`.
- Sibling caches share the same anti-pattern:
  `vdl_tools/shared_tools/openai/embedding_cache.py` and
  `vdl_tools/shared_tools/taxonomy_mapping/few_shot_cache.py` both called
  `session.merge()` from worker threads. All three were refactored together
  in Step 1.
- `tenacity` is already used in `few_shot_cache.py` (retries on
  `openai.APIConnectionError`) and `scrape_enrich/primer/engines_utils/engines.py`.
  Pattern is established; we extend it to validation errors in Step 2.
- `openai_api_utils.get_completion` is the single shared call site for both
  `PromptResponseCacheSQL` and downstream caches — adding retry there in
  Step 2 benefits everything.

## Step 1 — Workers call API only; main thread bulk-upserts (all three caches) ✅

**Status: shipped in PR #126.**

Removed the per-row DB round-trip from the hot path so workers scale
with the API, not the DB. Applied uniformly across the three caches that
shared the anti-pattern.

**Files touched**
- `vdl_tools/shared_tools/openai/prompt_response_cache_sql.py` (primary)
- `vdl_tools/shared_tools/openai/embedding_cache.py`
- `vdl_tools/shared_tools/taxonomy_mapping/few_shot_cache.py`

**What changed**
- Worker fn `_api_only` takes `(given_id, text)` and returns
  `(given_id, text, response_or_error, error_flag)`. Workers never touch
  `self.session`.
- Chunk loop in `_bulk_get_cache_or_run`:
  1. `executor.map(_api_only, chunk)` collects results in the main thread.
  2. Build two lists of row dicts: `success_rows` and `error_rows` via
     `_build_success_row` / `_build_error_row`.
  3. Issue **one** `INSERT ... ON CONFLICT DO UPDATE` per list using
     `sqlalchemy.dialects.postgresql.insert`:
     - success: `set_` overwrites `response_full`, `response_text`,
       `text_id`, `input_text`, `num_errors=NULL`.
     - error: `set_` sets `response_full` and
       `num_errors = COALESCE(prompt_response.num_errors, 0) + 1`.
  4. `self.session.commit()` once per chunk (unchanged cadence).
- Single-row path (`_get_cache_or_run` / `get_cache_or_run`) still uses
  `session.merge` — not on the perf-critical path. `store_item` /
  `store_error` are now thin wrappers around the row builders.
- `FewShotCache` overrides `_build_success_row` / `_build_error_row` to
  JSON-encode dict `input_text` and emit `IsRelevant`-shaped `response_text`.
- `EmbeddingCache` got the same treatment — workers were already API-only,
  but main-thread per-item `merge()` was the bottleneck. Replaced with the
  same bulk upsert pattern on `(model_name, text_id)`.
- **Defaults bumped** to match the new ceiling:
  - `PromptResponseCacheSQL.bulk_get_cache_or_run`: `max_workers` 3 → 20,
    `n_per_commit` 50 → 200.
  - `FewShotCache.bulk_get_cache_or_run`: `max_workers` 5 → 20,
    `n_per_commit` 50 → 200.
  - `EmbeddingCache.bulk_get_cache_or_run`: `max_workers` 3 → 10
    (`n_per_commit` 1500 unchanged).

**Benchmark (local client + remote RDS, gpt-5.4-nano, 150 countries)**

| Workers | Before cached | After cached | Speedup | Raw API |
|---:|---:|---:|---:|---:|
| 50  | 33.6s | **9.0s** | 3.7× | 6.9s |
| 100 | 32.1s | **9.6s** | 3.3× | 7.3s |
| 150 | 35.5s | **9.6s** | 3.7× | 7.0s |
| 200 | 32.7s | **9.3s** | 3.5× | 6.0s |

Cached-vs-raw ratio: ~5× → ~1.3×. Throughput now scales with the OpenAI
API rather than flatlining on DB round-trips.

## Step 2 — Retry on validation / JSON parse errors in `get_completion`

Goal: the `EOF while parsing` Pydantic errors are transient model
truncation — retry at the source so callers never see them under normal
conditions.

**Files touched**
- `vdl_tools/shared_tools/openai/openai_api_utils.py`
- `pyproject.toml` (add `tenacity` as an explicit dependency; currently
  transitive)

**Changes**
- Wrap the body of `get_completion` in a `tenacity.retry`:
  ```python
  @retry(
      stop=stop_after_attempt(3),
      wait=wait_random_exponential(multiplier=1, max=15),
      retry=retry_if_exception_type((
          pydantic.ValidationError,
          json.JSONDecodeError,
          openai.LengthFinishReasonError,  # confirm name in installed openai version
      )),
      reraise=True,
  )
  ```
- Log the retry (use the existing module `logger`) including a truncated
  view of the offending payload so we can diagnose model regressions.
- Do **not** retry on `openai.APIError` family here — the OpenAI client
  already retries those (`max_retries=4`). Stay surgical.
- `few_shot_cache.FewShotCache.get_completion` already has a tenacity
  layer for `APIConnectionError`; leave it alone — it composes cleanly
  on top.

**Verification**
- Add a unit-style test that monkeypatches `CLIENT.responses.parse` to
  raise `pydantic.ValidationError` twice then return a valid response;
  assert one successful return and two retries.
- Run the original benchmark script — should see no regression on the
  happy path, and the `EOF while parsing` traceback should disappear from
  real runs over time.

## Step 3 — `read_from_cache` / `write_to_cache` per-call flags ✅

**Status: shipped.**

Decoupled the cache read path from the write path so callers can force
re-runs, do read-only diagnostic passes, or run pure passthroughs without
constructor-level changes. Applied across all three caches.

**Files touched**
- `vdl_tools/shared_tools/openai/prompt_response_cache_sql.py`
- `vdl_tools/shared_tools/openai/embedding_cache.py`
- `vdl_tools/shared_tools/taxonomy_mapping/few_shot_cache.py`

**What changed**
- Added two orthogonal kwargs to `get_cache_or_run` and
  `bulk_get_cache_or_run` (plus their underscored internals):
  - `read_from_cache: bool = True` — gate the lookup phase.
  - `write_to_cache: bool = True` — gate the upsert + commit phase.
- New module-level helper `_resolve_cache_flags(...)` in
  `prompt_response_cache_sql.py` (imported by `embedding_cache.py`):
  translates a legacy `use_cached_result` kwarg into `read_from_cache`
  and emits a `DeprecationWarning` (`stacklevel=3`). Writes are
  unaffected by the legacy flag — preserves historical behavior
  (`use_cached_result=False` was "force refresh," not "read-only").
- Module-level sentinel `_UNSET` lets us distinguish "caller didn't pass
  `use_cached_result`" from "caller passed `True`," so existing callers
  don't spam warnings.
- Write path is the conjunction of `write_to_cache AND
  self.store_results` — constructor flag still wins as a hard off.
- Single-row read-only path (`write_to_cache=False`) builds the
  response dict from `_build_success_row` + `_row_to_response_dict`
  so callers get the same return shape without a DB roundtrip.
- Public method signatures kept fully backwards compatible — all new
  kwargs default to `True`, deprecated alias still works.

**Behavior matrix**

| `read_from_cache` | `write_to_cache` | Behavior |
|---|---|---|
| T | T | Default. Read cache, miss → API + persist. |
| F | T | Force refresh. Bypass cache, persist fresh result. |
| T | F | Read-only / diagnostic. Cache hits returned; misses run API but **don't** write. |
| F | F | Pure passthrough. No DB I/O at all. |

**Verification (smoke test `/tmp/smoke_read_write_flags.py`)**

All four quadrants asserted end-to-end against the real database:
- Q1 (T,T): row persisted on miss; second call returns same row. ✓
- Q2 (F,T): force-refresh bumps `date_updated`. ✓
- Q3 (T,F): hit returns cache, miss runs API but no DB row created. ✓
- Q4 (F,F): API result returned, no DB row created. ✓
- Legacy `use_cached_result=False` still force-refreshes AND emits one
  `DeprecationWarning` per call. ✓
- Single-row `get_cache_or_run(read_from_cache=F, write_to_cache=F)`
  matches bulk behavior. ✓

**Migration**
- 151 references to `use_cached_result` across the codebase remain
  working unchanged. They'll emit a `DeprecationWarning` on use.
- Migrate at leisure — no urgency. The alias has no planned removal date
  in this PR.

## Step 4 — Add `request_hash` to the cache key

Goal: prevent calls that produce semantically different model output
(different `reasoning` effort, with vs without `tools`/web search,
different `temperature`, etc.) from sharing a row, while still letting
the same underlying response be re-parsed against different
`text_format` Pydantic models without a cache miss.

**Key design call**
- `text_format` is **excluded** from the hash. We persist
  `response_full` raw; parsing into a Pydantic class happens at
  retrieval time, so the same row is reusable across schema choices.
  This also avoids invalidating the cache every time a caller tweaks
  a response model.
- `request_hash` is built from an **explicit allowlist** of kwargs
  known to change model output. Anything not on the list is ignored,
  which keeps the key stable as we add forwarding pass-throughs.

**Allowlist (initial)**
- `reasoning` (dict; `effort`, `summary`)
- `tools` (list; presence of web search, file search, etc.)
- `tool_choice`
- `temperature`
- `top_p`
- `max_output_tokens`
- `seed`
- `service_tier`

Everything else (`text_format`, `return_all`, `metadata`,
`stream`, etc.) is dropped before hashing. New items get added to
the list explicitly via PR — opt-in keeps cache invalidation
predictable.

**Files touched**
- `vdl_tools/shared_tools/database_cache/database_models/prompt.py`
- New Alembic revision under
  `vdl_tools/shared_tools/database_cache/data_migrations/versions/`
- `vdl_tools/shared_tools/openai/prompt_response_cache_sql.py`

**Schema change**
- Add `request_hash VARCHAR NOT NULL DEFAULT ''` to `prompt_response`.
- Drop the existing PK and recreate it as
  `(prompt_id, given_id, model_name, request_hash)`.
- Backfill: existing rows get `''` from the default — they continue
  to match calls whose allowlisted kwargs are empty or all-default.

**Code change**
- New helper in `prompt_response_cache_sql.py`:
  ```python
  KWARG_KEYS_THAT_AFFECT_OUTPUT = frozenset({
      "reasoning",
      "tools",
      "tool_choice",
      "temperature",
      "top_p",
      "max_output_tokens",
      "seed",
      "service_tier",
  })

  def _request_hash(kwargs: dict) -> str:
      norm = {}
      for k in sorted(kwargs):
          if k not in KWARG_KEYS_THAT_AFFECT_OUTPUT:
              continue
          v = kwargs[k]
          # Sort dict/list contents so structural equality maps to
          # the same hash. Tools list ordering shouldn't bust the key.
          norm[k] = json.loads(json.dumps(v, sort_keys=True, default=str))
      if not norm:
          return ""  # legacy-compatible: matches existing rows
      return make_uuid(
          json.dumps(norm, sort_keys=True),
          namespace_text=PROMPT_RESPONSE_NAMESPACE_TEXT,
      )
  ```
  Reuse `make_uuid` from `database_models/prompt.py` to stay
  consistent with `text_id` / `prompt_id` hashing.
- Empty allowlist intersect ⇒ `request_hash = ''`, matching legacy
  rows seamlessly (backfill safe).
- Pass `request_hash` through every lookup (`get_prompt_response_obj`,
  `get_prompt_response_obj_bulk`) and every upsert
  (`_upsert_success_rows`, `_upsert_error_rows`).
- Document the allowlist in the class docstring and call out that
  callers passing model-behavior kwargs must check the list — new
  kwargs default to "cosmetic" (cache reuse).

**Verification**
- Same prompt + given_id with `reasoning={"effort":"low"}` and then
  `reasoning={"effort":"high"}` ⇒ two distinct rows.
- Same prompt + given_id with `tools=[{"type":"web_search_preview"}]`
  vs no `tools` ⇒ two distinct rows.
- Same prompt + given_id with `text_format=ResponseA` and then
  `text_format=ResponseB` ⇒ **single row**, second call is a cache hit.
- Run twice with identical model-affecting kwargs; assert single row,
  second call cache hit.
- Run with no model-affecting kwargs against legacy rows (where
  `request_hash = ''`); assert cache hit (backfill compatibility).
- `alembic upgrade head` on a scratch DB, then `alembic downgrade -1`,
  then `alembic upgrade head` — confirm reversibility.

## Out of scope (explicit non-goals for this plan)

- Removing the `load_prompts(self.session)` table scan in
  `_set_prompt_obj` (cosmetic). Worth a one-line fix later but not on
  the perf-critical path.
- `print(prompt_obj)` debug line in `register_prompt`. Same.
- The unused `tuple_` import in `prompt_response_cache_sql.py`. Same.
- ~~Generalizing `read_from_cache` / `write_to_cache` (Step 3) into
  `embedding_cache.py`'s API — that cache lacks `store_results` today,
  so flag rework there is a separate decision.~~ ✅ Done — Step 3
  rolled the flags out to `embedding_cache.py` directly. Note that
  `EmbeddingCache` still has no `store_results` constructor flag, so
  the only write gate is `write_to_cache`. Adding a constructor-level
  `store_results` to `EmbeddingCache` is still optional follow-up.
- Replacing `datetime.utcnow()` with `datetime.now(dt.UTC)` in the
  bulk-upsert helpers — Python 3.12 deprecation. Spawned as a separate
  task; touches the same files as Step 1 but unrelated to flag work.

## Suggested PR breakdown

Each step is independently shippable and reversible:
1. PR1: Step 1 (perf). ✅ #126. Big win, no API surface change, no schema change.
2. PR2: Step 3 (flags). ✅ _this PR_. Additive, deprecation alias keeps callers working.
3. PR3: Step 2 (retry). Small, isolated, benefits all callers.
4. PR4: Step 4 (`request_hash`). Schema migration; coordinate deployment.
