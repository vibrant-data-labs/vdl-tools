# parquet_cache — Usage Guide

A drop-in replacement for `pd.read_json` / `DataFrame.to_json` that writes
Parquet instead, works transparently with local paths and `s3://` URIs, and
caches S3 reads locally so you download each file once — with a built-in
safety net that prevents stale reads.

> **Location:** `vdl_tools.shared_tools.parquet_cache`

---

## TL;DR

- **Writing:** `write_dataframe(df, uri)` — works for local paths, `s3://`, and `file://`.
- **Reading:** `read_dataframe(uri)` — same URI styles. S3 reads are automatically cached locally.
- **The cache is safe.** If someone else pushes a new version to S3, your next `read_dataframe` call detects it and re-downloads automatically. You cannot accidentally read stale data unless you explicitly opt out.

---

## Table of contents

1. ["Am I going to read a stale local copy?" — short answer: no](#am-i-going-to-read-a-stale-local-copy)
2. [When to use this module](#when-to-use-this-module)
3. [Quick start](#quick-start)
4. [Reading and writing local files](#reading-and-writing-local-files)
5. [Reading and writing S3](#reading-and-writing-s3)
6. [How the cache works](#how-the-cache-works)
7. [Controlling cache behavior](#controlling-cache-behavior)
8. [Multi-file writes and reads](#multi-file-writes-and-reads)
9. [Lineage: "where did this file come from?"](#lineage-where-did-this-file-come-from)
10. [Common recipes](#common-recipes)
11. [Troubleshooting](#troubleshooting)

---

## "Am I going to read a stale local copy?"

**Short answer: no, not by default.** Every time you call `read_dataframe` on an `s3://` URI, the module quietly asks S3 *"has this file changed since I last downloaded it?"* (a cheap HEAD request — ~10ms, no data transfer) and compares the answer to what's in your local cache.

- If S3's answer matches your cache → serve from local disk (instant, free).
- If S3's answer differs → re-download the new version, update your cache, serve you the new data.

This is all automatic. You write the same `read_dataframe("s3://…")` call whether you're the first person to ever read this file or the 100th. The cache just makes the subsequent reads fast; it never makes them wrong.

**Three scenarios to make this concrete:**

| Scenario | What happens |
|---|---|
| You wrote the file 5 minutes ago and you read it now | Local cache hit. Zero bytes downloaded. |
| A teammate pushed a new version 1 minute ago | HEAD detects the change. Re-download. Your analysis uses the new data. |
| You're offline (no network) | Default: read fails (safer than serving potentially stale data). Can opt into "serve whatever's local" for airplane use — see [Controlling cache behavior](#controlling-cache-behavior). |

If you're worried about ever working on old data, the correct action is **"just call `read_dataframe`"** — not "manually clear the cache." The module already does the right thing.

---

## When to use this module

Use `parquet_cache` when you have:

- **A pandas DataFrame that you want to persist** — to local disk, to S3, or to somewhere teammates can read.
- **DataFrames with dict or list columns** (common in Crunchbase, Candid, and other API-derived data). This module handles them automatically; `pd.to_json` tends to lose types and `pd.to_parquet` fails outright.
- **Shared data on S3 that multiple teammates read repeatedly** — the cache means each person downloads each file once, not once per script run.

**Don't use this module for:**

- Files that aren't DataFrames (Excel, images, raw JSON blobs). Use `vdl_tools.shared_tools.s3_tools` for those.
- Tiny files that never change. Local Parquet still works great, but the caching ceremony is overkill.

---

## Quick start

```python
from vdl_tools.shared_tools.parquet_cache import write_dataframe, read_dataframe
import pandas as pd

df = pd.DataFrame({"org": ["Acme", "Beta"], "funding": [100_000, 50_000]})

# Write somewhere — local or S3, same call
write_dataframe(df, "s3://shared-data-new/my_project/orgs.parquet")

# Read it back — anywhere you'd use pd.read_json
df_reloaded = read_dataframe("s3://shared-data-new/my_project/orgs.parquet")
```

That's 90% of what you need. Everything below is elaboration.

---

## Reading and writing local files

Works exactly like you'd expect:

```python
from vdl_tools.shared_tools.parquet_cache import write_dataframe, read_dataframe

# Write to a local path
write_dataframe(df, "/Users/me/data/orgs.parquet")

# Also accepts pathlib.Path
from pathlib import Path
write_dataframe(df, Path.home() / "data" / "orgs.parquet")

# file:// URIs are accepted too
write_dataframe(df, "file:///Users/me/data/orgs.parquet")

# Reading — same deal
df = read_dataframe("/Users/me/data/orgs.parquet")
```

For local files, there is **no cache involved** — the file IS on your disk, so `read_dataframe` reads it directly. The `use_cache`/`cache_dir`/`check_remote` parameters are all ignored. You don't need to think about them.

Parent directories are created automatically on write.

---

## Reading and writing S3

```python
# Write — include the bucket in the URI
write_dataframe(df, "s3://shared-data-new/my_project/orgs.parquet")
# Output: Wrote 5 rows → s3://shared-data-new/my_project/orgs.parquet (0 json cols)

# Read — same URI, regardless of who wrote it or when
df = read_dataframe("s3://shared-data-new/my_project/orgs.parquet")
```

**Credentials.** The module reads AWS credentials from the standard vdl-tools `config.ini` under the `[aws]` section (same place everything else reads from). If that's not present, it falls back to the boto3 default credential chain (env vars, `~/.aws/credentials`, or an IAM role on EC2). You don't need to pass anything.

**First vs. subsequent reads.** The first person to read a given S3 URI downloads the file. Every subsequent read — by the same person, on the same machine — is served from the local cache, validated against S3 via a HEAD request. The pattern stays identical; only the speed changes.

---

## How the cache works

Three things you need to know:

### 1. Where the cache lives

By default: `~/.cache/vdl-tools/parquet/`

Override per-call: pass `cache_dir="/some/other/path"` to `read_dataframe`.

Override globally: set the `VDL_PARQUET_CACHE_DIR` environment variable.

### 2. What the cache looks like (and why filenames look weird)

Inside the cache directory, you'll see files like:

```
~/.cache/vdl-tools/parquet/
  bebd5f1b4efbad8af533a917e20b1d4ab5f1e96a3646ab76e819f75969677828
  22a914e0d3861451cef4cc10e53203e29b73e94a7d7c54e5b616987489bea5b2
  cache
```

The long hex names are SHA-256 hashes of the S3 URI — not the actual filenames. This prevents collisions (two S3 paths could share the same `organizations.parquet` filename). The `cache` file is a JSON index mapping each S3 URI to its hash and last-seen version fingerprint.

You shouldn't ever need to inspect these by hand. But if you saw `.../vdl-tools/parquet/` and wondered "what is this garbage?", that's what it is.

### 3. How "is this stale?" gets answered

Every time you call `read_dataframe("s3://...")`:

1. The module looks up the URI in its cache index. If it's never been seen, download it fresh and done.
2. If it has been seen, the module makes a **HEAD request** to S3 — basically asking "what's the current version fingerprint (ETag) of this object?"
3. It compares that fingerprint to the one stored when the local copy was downloaded.
4. **Match** → serve the local copy.
5. **Mismatch** → download the new version, update the cache, serve that.

The HEAD request is tiny (<1KB, ~10–20 ms). You pay it on every read, but you save the much larger download cost whenever the file hasn't changed — which is most of the time.

Concurrent writers are handled automatically: if your teammate updates a shared file while you're mid-analysis, your next `read_dataframe` call picks up their change on its next HEAD check.

---

## Controlling cache behavior

The defaults are what you want 95% of the time. These knobs are escape hatches for specific situations:

| What you want | How |
|---|---|
| **Fresh download, ignore cache entirely** | `read_dataframe(uri, use_cache=False)` |
| **Use the local cache without calling S3** (offline / airplane mode) | `read_dataframe(uri, check_remote=False)` |
| **Different cache location** (isolate a project, or use fast local SSD) | `read_dataframe(uri, cache_dir="/path/to/cache")` |
| **All reads in this session use a custom cache** | `export VDL_PARQUET_CACHE_DIR=/path/to/cache` before running |
| **Clean up old cached files** | `prune_cache(keep_recent_days=30)` |

### Force a fresh download

You almost never need this. The default ETag check already handles "get me the latest version." But if you want absolute certainty — say, you're debugging and suspect something weird — pass `use_cache=False`:

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/orgs.parquet",
    use_cache=False,                            # skip cache, download fresh
)
```

### Read without hitting the network

Useful on a flight or when S3 is flaky and you're OK with your current local copy:

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/orgs.parquet",
    check_remote=False,                         # don't call S3 to validate
)
```

This serves whatever's in your local cache without checking if S3 has something newer. If the file isn't in your cache at all, this still fails — `check_remote=False` isn't magic; it's "trust whatever's local."

---

## Multi-file writes and reads

When you have a logical group of DataFrames that belong together — for example, the 5 Crunchbase raw tables — use the multi-file helpers:

```python
from vdl_tools.shared_tools.parquet_cache import write_dataframes, read_dataframes

# Writes {dir_uri}/organizations.parquet, {dir_uri}/funding_rounds.parquet, etc.
write_dataframes(
    {"organizations": df_orgs, "funding_rounds": df_fr, "founders": df_founders},
    dir_uri="s3://shared-data-new/ed_tracker/2026_04_17",
    lineage={"source": "crunchbase", "search_terms": ["education", "edtech"]},
)

# Reads them back by name
tables = read_dataframes(
    dir_uri="s3://shared-data-new/ed_tracker/2026_04_17",
    names=["organizations", "funding_rounds", "founders"],
)
df_orgs = tables["organizations"]
```

The `dir_uri` is a **container URI** — S3 prefix or local directory — not a file. Each DataFrame becomes `{dir_uri}/{name}.parquet`. Caching, lineage, and credentials behave exactly like single-file reads/writes.

---

## Lineage: "where did this file come from?"

Every file written by `write_dataframe` carries optional metadata in its Parquet footer. Useful when someone looks at a file three months later and wonders what it is.

```python
write_dataframe(
    df,
    "s3://shared-data-new/ed_tracker/2026_04_17/cb_companies_cleaned.parquet",
    lineage={
        "source": "ed_tracker.prepare_raw_data",
        "filter_yr": 2017,
        "search_terms": ["edtech", "education"],
        "n_rows": len(df),
    },
)
```

To read the lineage back later without loading the data:

```python
from vdl_tools.shared_tools.parquet_cache import get_lineage

meta = get_lineage("s3://shared-data-new/ed_tracker/2026_04_17/cb_companies_cleaned.parquet")
print(meta)
# {'created_at': '2026-04-17T14:23:05+00:00',
#  'source': 'ed_tracker.prepare_raw_data',
#  'filter_yr': 2017,
#  'search_terms': ['edtech', 'education'],
#  'n_rows': 12043}
```

A `created_at` timestamp is added automatically. This is a metadata-only read — it doesn't download the data, just the ~1KB footer.

---

## Common recipes

### 1. Writer-reader in the same script

If you write a file and then read it back within the same script/session, the read uses your cache (which you just populated). No second S3 download needed.

```python
uri = "s3://shared-data-new/my_project/funders.parquet"
write_dataframe(df_funders, uri)
df_reloaded = read_dataframe(uri)   # served from local cache, not re-downloaded
```

### 2. Writing several related tables together

When a logical unit contains multiple DataFrames — say, a funding dataset split into `grants`, `orgs`, and `people` — write them under a shared directory URI:

```python
write_dataframes(
    {
        "grants": df_grants,
        "orgs":   df_orgs,
        "people": df_people,
    },
    dir_uri="s3://shared-data-new/my_project/2026_04_17",
    lineage={"source": "my_pipeline.py", "run_id": "20260417-a"},
)
```

Produces `s3://shared-data-new/my_project/2026_04_17/grants.parquet`, `…/orgs.parquet`, `…/people.parquet`.

### 3. Reading someone else's shared data

You (or a teammate) wrote to an S3 prefix yesterday. Today you need to load it:

```python
tables = read_dataframes(
    dir_uri="s3://shared-data-new/my_project/2026_04_17",
    names=["grants", "orgs", "people"],
)
df_grants = tables["grants"]
```

First call downloads each file once (and caches it). Subsequent calls are cache hits with a HEAD check to confirm nothing changed upstream.

If you just want one of the files, use the single-file API:

```python
df_grants = read_dataframe(
    "s3://shared-data-new/my_project/2026_04_17/grants.parquet",
)
```

### 4. Offline (airplane) mode

You've worked with this data before, you're on a plane, you want to keep working:

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/funders.parquet",
    check_remote=False,                         # don't call S3; trust local cache
)
```

Only works if you've read this URI at least once before. If it's not in your local cache, there's nothing to serve.

### 5. Forcing a fresh download

Rare, but sometimes you want to guarantee you're bypassing any local cache (e.g., you suspect your cache is corrupted, or you're debugging):

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/funders.parquet",
    use_cache=False,                            # skip cache entirely; download now
)
```

### 6. Project-isolated cache

Useful when you don't want one project's cache to compete with another for disk space, or when you want a cache on a fast SSD separate from home:

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/funders.parquet",
    cache_dir="/Volumes/fast_ssd/caches/my_project",
)
```

Or, once at the top of your script, for all reads in this session:

```python
import os
os.environ["VDL_PARQUET_CACHE_DIR"] = "/Volumes/fast_ssd/caches/my_project"
```

### 7. Column pruning — faster reads on wide tables

If you only need a handful of columns from a wide file (say, 3 out of 80), ask for just those columns. Parquet reads just the bytes you need:

```python
df = read_dataframe(
    "s3://shared-data-new/my_project/big_table.parquet",
    columns=["org_name", "funding_total", "year"],
)
```

This is a real win on multi-hundred-MB files: 5–20× faster than reading everything and discarding.

### 8. Cache cleanup

Old cache files pile up over time. Run this occasionally:

```python
from vdl_tools.shared_tools.parquet_cache import prune_cache
prune_cache(keep_recent_days=30)          # deletes files not accessed in 30+ days
```

Safe to run anytime — anything needed will be transparently re-downloaded on next read.

---

## Built on top of this module

Some VDL pipelines wrap `parquet_cache` with domain-specific helpers. If you're working on one of those, you'll use the wrapper instead of calling `read_dataframe` directly:

- **Crunchbase pipeline** — `query_companies_extended(save_to_uri=...)` pulls from the CB API and writes the 5 raw tables to S3 using `write_dataframes` under the hood. `load_search_results_from_parquet(dir_uri=...)` reads them back. See `vdl_tools/scrape_enrich/crunchbase/organizations_api_db.py`.

These wrappers pass the same `use_cache` / `cache_dir` / `check_remote` knobs through, so everything in this guide applies.

---

## Troubleshooting

### "It's slow" — first read

The first read of any S3 file downloads it fully. A 500 MB file over typical office bandwidth takes tens of seconds. Subsequent reads are near-instant. This is working as designed.

### "It's slow" — every read

If every read is slow, not just the first, the cache isn't persisting between runs. Check:

- Is `VDL_PARQUET_CACHE_DIR` set to something ephemeral (like `/tmp`) that gets wiped?
- Is your default cache dir (`~/.cache/vdl-tools/parquet/`) writable? Look for permission errors in the logs.
- Are you passing different `cache_dir` values on different calls? Each cache dir is independent.

### "My teammate updated the file but I'm still seeing old data"

You probably aren't — the default behavior re-downloads on ETag mismatch. But if you suspect otherwise:

1. Check you're not passing `check_remote=False` anywhere (that suppresses the HEAD check).
2. Run once with `use_cache=False` to force a fresh download and compare.
3. Ask your teammate to confirm they actually pushed the update (it happens).

### "ArrowInvalid: Could not convert '…' with type str: tried to convert to double"

The DataFrame has a column with mixed types — mostly numeric with at least one string (or vice versa). Parquet requires one type per column; this one has two.

This is almost always **bad input data**, not a bug in the helper. The error message names the exact offending value and column. Find the bad row upstream:

```python
bad = df[df["Funding_2017"].apply(lambda v: isinstance(v, str))]
print(bad)
```

Either clean the value or drop the row, then re-write.

### "AttributeError: module 'pyarrow' has no attribute 'PyExtensionType'"

You're on an old version of `datasets` that doesn't work with pyarrow 17+. Run:

```bash
pip install -U 'datasets>=2.18.0'
```

### "Cache files have hash names, I can't find the one I want"

That's by design — hash names prevent collisions. If you really need to find the cached copy of a specific URI, check the `cache` JSON file in your cache directory:

```python
import json
from vdl_tools.shared_tools.parquet_cache import DEFAULT_CACHE_DIR
print(json.dumps(json.loads((DEFAULT_CACHE_DIR / "cache").read_text()), indent=2))
```

That file maps each URI to its hash and its last-seen ETag.

---

## One-page summary

```python
from vdl_tools.shared_tools.parquet_cache import (
    write_dataframe, read_dataframe,       # single file
    write_dataframes, read_dataframes,     # multi-file under a directory URI
    get_lineage, prune_cache,
)

# Write — local or S3, same call
write_dataframe(df, "s3://bucket/key.parquet")
write_dataframe(df, "/local/path.parquet")
write_dataframe(df, uri, lineage={"source": "my_pipeline", "n_rows": len(df)})

# Read — safe to use freely; cache handles freshness for you
df = read_dataframe("s3://bucket/key.parquet")                # default: cached, ETag-checked
df = read_dataframe(uri, use_cache=False)                     # force fresh download
df = read_dataframe(uri, check_remote=False)                  # offline; trust local
df = read_dataframe(uri, cache_dir="/my/cache")               # custom cache location
df = read_dataframe(uri, columns=["a", "b"])                  # column pruning (fast!)

# Metadata-only
meta = get_lineage("s3://bucket/key.parquet")

# Maintenance
prune_cache(keep_recent_days=30)
```
