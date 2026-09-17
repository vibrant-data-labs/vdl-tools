---
name: vdl-tools-expert
description: Expertise for vdl-tools, VDL's shared internal Python package (editable install at /Users/ericberlow/VDL/vdl-tools, imported as `vdl_tools` in every VDL project repo). Covers the shared conventions all project repos rely on — paths.ini/config.ini resolution via project_config (CWD-relative, read at import time), parquet_cache artifacts with lineage footers, the centralized Crunchbase funding-type vocabulary (cb_funding_types raw slugs vs display names), funding calculations (VENTURE_BACKED_ROUNDS vs raised_from_venture_rounds), DB-backed LLM prompt/response caching, and where the major pipelines live (scrape_enrich, taxonomy_mapping, org_type_classifier, climate_landscape, netzero_insights). USE THIS PROACTIVELY in ANY VDL repo whenever code imports vdl_tools, whenever you're about to write a helper that might already exist here (path config, Google Sheets, NetZero API, network build/layout/clustering, LLM calls with caching, py2mappr), or whenever you're editing vdl-tools itself (changes here affect every project). Read BEFORE reimplementing anything or modifying shared pipeline logic.
---

# vdl-tools — Shared Toolkit Expert

`vdl-tools` is VDL's shared internal package: the single place for reusable pipelines
and utilities across the team and across project repos. It is an **editable install**
at `/Users/ericberlow/VDL/vdl-tools`, wired into the Python 3.12 env
(`~/.pyenv/versions/3.12.11/envs/python-3-12`) via a `.pth` file — edits take effect
immediately in every repo, no reinstall.

**Prime directive: check here before writing a local helper.** The most common
reinvented wheels: paths.ini parsing, Google Sheets access, NetZero Insights API
calls, network build/layout/Leiden clustering (`tag2network`), LLM calls with
caching, and `py2mappr` map publishing. If vdl-tools has a near-miss that would need
a small change, **flag it** rather than silently forking a local copy.

## Keeping this skill accurate

If a function, path, constant, or behavior described here no longer matches the code,
fix this file in the same turn and mention it in the end-of-turn summary. Unsure if
it's drift? Leave a `TODO(skill-audit):` note. This skill is young — **add sections
as gotchas surface** in project work (that's how it grows).

## Hot-list of shared conventions and gotchas

### 1. Path config: always `project_config`, never hand-rolled
Every VDL repo keeps a `paths.ini` (and optional `config.ini`) at the repo root.
Read them ONLY via `vdl_tools.shared_tools.project_config`:

- `get_paths()` → `[paths]` section as a dict of absolute `Path`s, with `{key}`
  substitution between entries
- `get_s3_paths()` → `[s3_paths]` section (S3 URIs, usually versioned via a
  `s3_project_version` key)
- `get_project_config()` → `config.ini`

**These resolve relative to the current working directory** — which is why every
runnable script needs the plain-script bootstrap (`os.chdir(project_root)` +
`sys.path.insert`) BEFORE any `vdl_tools` import: parts of vdl_tools read config at
import time, so import order matters.

### 2. Parquet artifacts carry lineage — check it before trusting an artifact
`vdl_tools.shared_tools.parquet_cache` is the artifact I/O layer:

- `read_dataframe(uri)` / `write_dataframe(df, uri, lineage={...})` — S3 parquet
  with a lineage footer (source, created_at, row_count, plus whatever the writer adds)
- `get_lineage(uri)` — read the footer without loading the frame

**Artifacts are point-in-time snapshots.** Adding a column in code does not add it to
artifacts already on S3 — a "missing" column usually means the artifact predates the
change, not that something drops it. Before debugging a "dropped" column, check
`get_lineage(uri)['created_at']` against when the column was introduced. Backfill
scripts that patch an artifact should extend its lineage with a `backfill` block
(see climate-landscape's `backfill_venture_backed.py` for the pattern).

### 3. Crunchbase funding-type vocabulary is centralized in `cb_funding_types`
`vdl_tools.shared_tools.cb_funding_types` is the single source of truth for round
types (introduced in #190). Two vocabularies:

- **Raw slugs** (`series_a`, `grant`, …) in `FUNDING_TYPES_RAW_COL`
  (`'Funding Types Raw'`) — what classifiers and set logic operate on
- **Display names** in `'Funding Types'` — assigned only via `to_display`;
  `as_raw` maps back

Helpers in `cb_funding_calculations` normalize via `as_raw` first, so they accept
either column. `unknown_slugs()` warns when Crunchbase introduces a round type the
vocabulary doesn't know — heed that warning, don't suppress it.

### 4. Two different "venture" definitions — don't conflate them
- `cb_funding_calculations.VENTURE_BACKED_ROUNDS` → the **narrow** set used by
  `climate_landscape.venture_backed_flag.add_venture_backed_flag` (seed…series_j,
  angel, pre-seed, convertible note, corporate round, secondary market, product
  crowdfunding; plus a grant-only + For-Profit rule). Requires the org-type
  prediction column, so it runs AFTER the org-type classifier.
- `raised_from_venture_rounds` → **broader** (also counts private equity and
  post-IPO) and is used only to infer org type.

### 5. LLM calls route through a DB-backed prompt/response cache
Scraping, summarization, relevance models, and taxonomy mapping all cache in SQL
keyed on (text, prompt, model). Re-running with unchanged inputs issues **no API
calls** — so full pipeline re-runs are cheap replays. Corollary: **changing how the
input text is derived invalidates the cache** for every row; keep text-derivation
formulas identical when refactoring (e.g. `choose_longer_text` for taxonomy mapping).

### 6. Editing vdl-tools affects every project repo
This package is imported by all VDL repos (climate-landscape,
elemental-catalytic-capital, oneearth, …). Before changing shared behavior, grep the
sibling repos under `/Users/ericberlow/VDL/` for callers. Project repos reference
vdl-tools changes by PR number in their commit messages (e.g. "vdl-tools #191") —
keep doing that so data artifacts can be traced to the code that made them.

## Where things live (top-level map)

| Area | Module |
|---|---|
| Path / project config | `shared_tools/project_config.py` |
| S3 parquet artifacts + lineage | `shared_tools/parquet_cache.py` |
| Crunchbase round-type vocabulary | `shared_tools/cb_funding_types.py` |
| Funding-derived classifications (org type, stage, grant/loan flags) | `shared_tools/cb_funding_calculations.py` |
| Crunchbase / Candid / LinkedIn prep + combine | `scrape_enrich/prepare_crunchbase.py`, `prepare_candid.py`, `combine_crunchbase_candid_linkedin.py` |
| Climate-landscape enrichment pipeline + venture_backed | `shared_tools/climate_landscape/` |
| One Earth / Drawdown hierarchical taxonomy mapping | `shared_tools/taxonomy_mapping/` |
| Org-type classifier | `shared_tools/org_type_classifier/` |
| NetZero Insights processing (ECC project) | `scrape_enrich/netzero_insights/` — see the `elemental-catalytic-capital-expert` skill |
| Keyword tagging from text | `scrape_enrich/tags_from_text.py` |
| Per-org attribute overrides from a checked-in patch JSON | `shared_tools/org_patches.py` (`apply_org_patches`; added 2026-09-16 on branch `org-patches` — drop this note once merged) |
| Common pandas/text utilities | `shared_tools/common_functions.py` |

Per-project depth belongs in each repo's own project skill
(`.claude/skills/<project>-expert/`); this skill stays at the shared-conventions level.
