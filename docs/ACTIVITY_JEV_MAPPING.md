# Activity JEV mapping: usage guide

Classify organizations, or any text, onto a taxonomy with Jev. The shared code extracts
what each organization does from its website, asks Jev one yes/no question per taxonomy
node for every activity, keeps the probabilities, picks matches with an explicit policy,
and rolls them up to organizations with tracker columns, statuses and funding shares.

> **Location:** `vdl_tools.shared_tools.taxonomy_mapping.activity_jev` (`mapping.py`,
> `activities.py`), the taxonomy packages `taxonomy_mapping.one_earth` and
> `taxonomy_mapping.drawdown`, and `vdl_tools.shared_tools.activity_extraction`.
> Design rules and source revisions: [implementation contract](activity_jev_contract.md).

---

## TL;DR

- **A whole project in one call:** `run_activity_mapping(orgs, jobs, run_id=..., directory=..., id_col=..., name_col=..., url_col=...)`.
- **Your own text and taxonomy, no extraction:** `map_subjects(subjects, taxonomy, policy=...)`.
- **Change the policy for free:** `select_matches(saved_scores, taxonomy, new_policy)` makes no requests.
- **Reruns are free.** Extraction and every Jev answer go through the SQL prompt cache; unchanged inputs make no calls.
- **Everything is traceable.** Each file's Parquet footer holds the workbook hash, question hash, policy, model and extraction lineage (`get_lineage`).

---

## Table of contents

1. [How it fits together](#how-it-fits-together)
2. [Vocabulary](#vocabulary)
3. [Quick start: classify a project's organizations](#quick-start-classify-a-projects-organizations)
4. [Map your own text onto your own taxonomy](#map-your-own-text-onto-your-own-taxonomy)
5. [Change the policy without paying again](#change-the-policy-without-paying-again)
6. [Which activities count: union or core first](#which-activities-count-union-or-core-first)
7. [Tracker settings](#tracker-settings)
8. [Lineage: what produced this file?](#lineage-what-produced-this-file)
9. [Combining runs](#combining-runs)
10. [Adding a taxonomy](#adding-a-taxonomy)
11. [Cost, caching and reruns](#cost-caching-and-reruns)
12. [Testing without model calls](#testing-without-model-calls)
13. [Troubleshooting](#troubleshooting)

The captured outputs below come from the 500-organization `zein-test` run on the
2026-10-22 Climate Landscape pull (`s3://climate-landscape-data/2026_10_22_new_cb_cd_pull/results/activity_jev/zein-test`),
read back on 2026-11-03 without any model calls, and from one small live call in
[section 4](#map-your-own-text-onto-your-own-taxonomy).

---

## How it fits together

```
organizations ──► activity_extraction.extract_frame ──► activities (3 per org, median)
  (your columns)      one cached LLM call per website        │
                                                             ▼  activities.build_activity_subjects
                                                      subjects (id + text)
                                                             │
taxonomy workbook ──► one_earth / drawdown.load_taxonomy     ▼  mapping.score_subjects
                      (one Jev question per node) ──────► probabilities, every node
                                                             │  mapping.select_matches(policy)
                                                             ▼
                                                      matched paths per activity
                                                             │  activities.export_extraction(rollup)
                                                             ▼
                                organization paths, tracker columns, statuses, funding
```

*`run_activity_mapping` runs the whole chain and writes each table. `mapping.py` is the
generic half (any text, any taxonomy); `activities.py` is everything specific to
activities and trackers.*

---

## Vocabulary

| Term | What it is | Example |
| -- | -- | -- |
| Subject | One piece of text with an ID that Jev scores against every node | An extracted activity, or an organization's own description |
| Taxonomy | Levels, nodes with definitions, and one Jev question per node | Drawdown: Sector, Cluster, Solution; 198 nodes |
| Policy | How probabilities become matches | `MatchPolicy((0.7, 0.5), descend=0.6)`: every node at 0.7 or above; 0.5 only when nothing reached 0.7; children at 0.6 under a match |
| Roll-up | Which activities' matches make up an organization's result | `"union"` (all activities) or `"core_first"` (core activities first) |
| Job | One taxonomy plus its tracker settings | `MappingJob(taxonomy, policy, "drawdown_category", rollup="core_first")` |
| Run | One extraction mapped onto one or more jobs, written to one directory | `results_path/activity_jev/pilot-001/` |

---

## Quick start: classify a project's organizations

```python
from vdl_tools.shared_tools.taxonomy_mapping import drawdown, one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.activities import MappingJob, run_activity_mapping

jobs = {
    "one_earth": MappingJob(
        one_earth.load_taxonomy(one_earth_workbook), one_earth.DEFAULT_POLICY, "one_earth_category",
        rollup=one_earth.DEFAULT_ROLLUP,
    ),
    "drawdown": MappingJob(
        drawdown.load_taxonomy(drawdown_workbook), drawdown.DEFAULT_POLICY, "drawdown_category",
        rollup=drawdown.DEFAULT_ROLLUP,
    ),
}
run = run_activity_mapping(
    orgs,                                   # one row per organization, any other columns kept
    jobs,
    run_id="pilot-001",
    directory="s3://my-bucket/my_project/activity_jev/pilot-001",
    id_col="org_id", name_col="org_name", url_col="homepage",   # your column names
)
```

What happens, in order:

1. **Website text.** Each URL is read through the shared scraper (`scrape_websites_psql`):
   cached pages are served, missing sites are scraped once. Parked, blocked, not-found,
   garbled and empty sites are skipped with the same verdicts the website summarizer uses.
   Already have the text? Pass `website_texts={org_id: text}` and `url_col` becomes optional.
2. **Extraction.** One cached `gpt-5.4-mini` call per organization returns its activities,
   each with a prominence (`core`, `secondary`, `minor`) and evidence.
3. **Mapping.** For each job, every activity is scored against every node and matched with
   the job's policy, then rolled up to the organization.
4. **Files.** Each job's files are written as soon as it finishes; `organizations.parquet`
   is written last, so its presence means the run completed.

| File | One row per | Holds |
| -- | -- | -- |
| `organizations.parquet` | organization | Your frame plus each job's columns and `activity_extraction_status`, `activity_n_activities`, `activity_source_sha256`, `activity_extraction_run_id` |
| `<job>_paths.parquet` | organization and matched path | Supporting activity IDs, per-activity evidence, best probability and tier, `mapping_status` |
| `<job>_funding.parquet` | organization and funded path | `FundingFrac`, summing to 1 per matched organization |
| `<job>_activity_paths.parquet` | activity and matched node | Every activity's matches, before the roll-up |
| `<job>_scores.parquet`, `<job>_usage.parquet` | activity and node; activity and request group | Raw probabilities; request status, model and tokens |

The returned `run` holds the same tables in memory: `run.organizations`,
`run.extraction` (its `organizations` and `activities`), `run.results["drawdown"]`
(`paths`, `funding`, `activity_paths`, `scores`, `usage`, `lineage`) and `run.lineage`.

One organization's row from the `zein-test` run (selected columns):

```text
{
 "id": "01-0515381",
 "Organization": "Biodiversity Research Institute",
 "one_earth_category": "Nature Education & Communication",
 "all_level2_one_earth_category": ["Nature Education & Communication", "Nature Technology (Engineering & Science)"],
 "one_earth_mapping_status": "matched",
 "drawdown_category": null,
 "all_level2_drawdown_category": [],
 "drawdown_mapping_status": "unmatched",
 "activity_extraction_status": "success",
 "activity_n_activities": 3
}
```

Its three activities are wildlife science research (core), Minamata Convention mercury
support (secondary) and a quarterly e-zine (minor). None of them is a Drawdown solution;
the mercury activity's best Drawdown node scored 0.19, so the organization is `unmatched`
there rather than forced into the closest node.

Every organization gets a status per job: `matched`, `unmatched`, `partial_failure`
(some requests failed but others matched), `scoring_failed`, or the extraction outcome
`missing_text`, `empty` or `request_failed`. No organization is ever dropped.

---

## Map your own text onto your own taxonomy

No extraction, no tracker: build a taxonomy from a workbook and score any text. This is
the starting point for a new project. Run live on 2026-11-03 with `write_to_cache=False`,
so it left nothing in the shared cache:

```python
import pandas as pd

from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.mapping import (
    MatchPolicy,
    load_workbook,
    map_subjects,
)

# A two-level taxonomy: one sheet per level, a Definition per node.
with pd.ExcelWriter("food_system.xlsx") as book:
    pd.DataFrame([
        {"Sector": "Agriculture", "Definition": "Growing crops or raising livestock."},
        {"Sector": "Food waste", "Definition": "Preventing, recovering or recycling wasted food."},
    ]).to_excel(book, sheet_name="Sectors", index=False)
    pd.DataFrame([
        {"Sector": "Agriculture", "Practice": "Rotational grazing", "Definition": "Moving livestock between paddocks so pasture can recover."},
        {"Sector": "Agriculture", "Practice": "Cover cropping", "Definition": "Planting crops between harvests to protect and build soil."},
        {"Sector": "Food waste", "Practice": "Food rescue", "Definition": "Collecting surplus edible food and redistributing it to people."},
        {"Sector": "Food waste", "Practice": "Composting", "Definition": "Turning food scraps into compost."},
    ]).to_excel(book, sheet_name="Practices", index=False)


def questions(nodes, columns):
    """One yes/no Jev question per node, worded from its name and definition."""
    return nodes.assign(question=[
        {"type": "noul", "instructions": f"Does this organization work on {r.name}? {r.Definition}"}
        for r in nodes.itertuples()
    ])


taxonomy = load_workbook(
    "food_system", "food_system.xlsx",
    columns=("Sector", "Practice"), sheets=("Sectors", "Practices"), question_builder=questions,
)

# Subjects: any text with an ID. Here, each organization's own description.
subjects = pd.DataFrame([
    {"subject_id": "valley-food", "state": "We pick up surplus produce from farms and grocers and deliver it to food banks across Oregon."},
    {"subject_id": "green-pastures", "state": "Our ranchers rotate cattle across paddocks and plant winter rye after harvest to rebuild soil."},
    {"subject_id": "spoke-shop", "state": "We repair bicycles and teach riding safety classes."},
])

result = map_subjects(subjects, taxonomy, policy=MatchPolicy(), write_to_cache=False)
print(result.subject_paths[["subject_id", "Sector", "Practice", "prob", "tier"]].to_string(index=False))
print(result.usage[["subject_id", "error", "input_tokens", "output_tokens"]].to_string(index=False))
```

Captured output:

```text
    subject_id      Sector           Practice  prob  tier
green-pastures Agriculture     Cover cropping  0.93     1
green-pastures Agriculture Rotational grazing  0.95     1
   valley-food  Food waste        Food rescue  0.98     1
    subject_id error  input_tokens  output_tokens
   valley-food  None           430            221
green-pastures  None           430            221
    spoke-shop  None           420            221
```

The bicycle shop matched nothing, which is the right answer. Each subject was one
request of about 430 input tokens. `result.scores` keeps every node's probability for
every subject, including the ones below the threshold.

Each node's `question` dict follows TypeSafe's format (see
[JEV_VERCEL_CACHE.md](JEV_VERCEL_CACHE.md)); a `noul` question returns the probability
of "yes". Add `criteria` to a question when a node needs an explicit rule, as the
Drawdown stewardship and One Earth Cross-Cutting questions do.

---

## Change the policy without paying again

`select_matches` only reads probabilities, so a new threshold or descent rule costs
nothing. It re-applies to a finished run's saved scores:

```python
from vdl_tools.shared_tools.parquet_cache import get_lineage, read_dataframe
from vdl_tools.shared_tools.taxonomy_mapping import drawdown
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.mapping import MatchPolicy, ScoringResult, select_matches

run = "s3://climate-landscape-data/2026_10_22_new_cb_cd_pull/results/activity_jev/zein-test"
workbook = ".../shared-data-clean/data/taxonomies/Drawdown/DrawdownTaxonomy_VDL_2026-10-22.xlsx"
taxonomy = drawdown.load_taxonomy(workbook)  # the workbook the run used

as_subjects = {"activity_id": "subject_id"}
scored = ScoringResult(
    read_dataframe(f"{run}/drawdown_scores.parquet").rename(columns=as_subjects),
    read_dataframe(f"{run}/drawdown_usage.parquet").rename(columns=as_subjects),
    get_lineage(f"{run}/drawdown_scores.parquet"),
)
for policy in (drawdown.DEFAULT_POLICY, MatchPolicy(thresholds=(0.8, 0.6)), MatchPolicy()):
    paths = select_matches(scored, taxonomy, policy)
    print(policy)
    print(f"  {paths.subject_id.nunique()} activities matched, {len(paths)} paths, by depth {paths.depth.value_counts().sort_index().to_dict()}")
```

Captured output:

```text
MatchPolicy(thresholds=(0.7, 0.5), descend=0.6)
  878 activities matched, 987 paths, by depth {0: 208, 1: 422, 2: 357}
MatchPolicy(thresholds=(0.8, 0.6), descend=None)
  794 activities matched, 839 paths, by depth {0: 240, 1: 415, 2: 184}
MatchPolicy(thresholds=(0.7, 0.5), descend=None)
  878 activities matched, 958 paths, by depth {0: 243, 1: 434, 2: 281}
```

Depth 2 is Solution. Drawdown's descent step (children at 0.6 under a matched parent)
is what carries 76 more activity paths down to Solution than the same thresholds
without it. If the workbook or any question's wording has changed since the scores were
made, `select_matches` refuses: "Scores belong to a different taxonomy or question set;
re-run scoring".

---

## Which activities count: union or core first

Every activity is always scored and kept in `<job>_activity_paths`. The roll-up decides
which activities' matches reach the organization's tags and funding:

- **`"union"`**: every activity's matches. One Earth's default.
- **`"core_first"`**: the core activities' matches; if no core activity matched anything,
  core and secondary; if still nothing, all activities. Drawdown's default, adopted by the
  Drawdown prototype on 2026-10-26 after it agreed with the judge 517 of 600 times against
  500 for the union.

Set it per job with `MappingJob(..., rollup="core_first")`. On the `zein-test` run, for
Drawdown:

| | Union | Core first |
| -- | -- | -- |
| Organization–Sector pairs | 463 | 421 |
| Organization–Solution pairs | 274 | 235 |
| Matched organizations | 373 | 373 |
| Organizations whose Solutions changed | | 25 |

Symbrosia shows what changes. Its core activity is a seaweed feed additive; a minor one
is a seaweed restoration project:

| Roll-up | Solution | Supporting activities | Funding share |
| -- | -- | -- | -- |
| Union | Use Feed Additives | 2 | 1/3 |
| Union | Protect Seaweed Ecosystems | 1 | 1/3 |
| Union | Restore Seaweed Ecosystems | 1 | 1/3 |
| Core first | Use Feed Additives | 1 | 1 |

Core first can also drop a fair match. Clean Power Alliance keeps only Improve Grid
Efficiency, from its core demand-response activity; the Deploy Distributed Solar PV and
Boost Appliance & Equipment Efficiency matches from its secondary "clean energy rebates
and appliance access" activity are dropped. Funding always splits equally across the kept paths (the existing
`redistribute_funding_fracs` allocation); probability, prominence and activity counts
never weight it.

---

## Tracker settings

Everything a dashboard expects is a `MappingJob` setting, so the shared code assumes no
tracker. Climate Landscape's Drawdown job, abridged from `process_map_activities.py`:

```python
MappingJob(
    taxonomy=drawdown.load_taxonomy(workbook),
    policy=drawdown.DEFAULT_POLICY,
    mapping_name="drawdown_category",            # level0_drawdown_category, all_level2_..., drawdown_category
    format_paths=format_drawdown_paths,          # display tweaks on the result rows ("Sector: Cluster")
    attribute_columns={"SPEED OF ACTION": "drawdown_speed_of_action"},  # node attributes listed per org
    pad_levels=4,                                # empty level3 columns the loaders expect
    funding_level=2,                             # allocate funding at Solution
    rollup=drawdown.DEFAULT_ROLLUP,              # "core_first"
)
```

`run_activity_mapping(..., file_names={"drawdown_paths": "drawdown_taxonomy_mapping_results.parquet",
"organizations": "cb_cd_li_meta_mapped.parquet"})` renames the result, funding and
organization files to the names existing loaders read. Leave every setting out and you get
the taxonomy's own levels, funding at its deepest level, the union roll-up and default file
names.

---

## Lineage: what produced this file?

Every file carries its identities in the Parquet footer; reading it downloads no data:

```python
import json

from vdl_tools.shared_tools.parquet_cache import get_lineage

run = "s3://climate-landscape-data/2026_10_22_new_cb_cd_pull/results/activity_jev/zein-test"
lineage = get_lineage(f"{run}/drawdown_taxonomy_mapping_results.parquet")
keep = ("taxonomy", "taxonomy_sha256", "questions_sha256", "policy", "model", "n_nodes",
        "n_groups", "n_subjects", "n_failed_requests", "max_funding_level", "status_counts")
print(json.dumps({k: lineage[k] for k in keep}, indent=2))
```

Captured output:

```text
{
  "taxonomy": "drawdown",
  "taxonomy_sha256": "2c012d562f08e14562b1e38a8edaeeb740d80a8c3f68d625f6597c71d00be6f8",
  "questions_sha256": "bbd9fdde351930f544a486964ebbe709ccbaff1fc28c04d96443602af5c00707",
  "policy": {
    "thresholds": [
      0.7,
      0.5
    ],
    "descend": 0.6
  },
  "model": "typesafe-ai/jev",
  "n_nodes": 198,
  "n_groups": 2,
  "n_subjects": 1351,
  "n_failed_requests": 0,
  "max_funding_level": 2,
  "status_counts": {
    "matched": 373,
    "missing_text": 69,
    "unmatched": 53,
    "empty": 5
  }
}
```

The organizations file's footer adds `run_id`, `n_orgs`, `columns` (the ID, name and URL
columns used), `input` (whatever you pass as `input_lineage`), `extraction` (prompt, schema
and model hashes plus status counts) and `taxonomies` (each job's footer as above,
including `rollup` and `export_columns`). Runs made before 2026-10-30, such as
`zein-test`, have `run_label` instead of `run_id` and no `columns`; runs made before
2026-11-03 have no `rollup`, and used the union.

---

## Combining runs

Each taxonomy can be run, rerun and approved on its own. `assemble_mapping_runs` copies
the selected jobs' columns from finished runs onto a base frame by ID, leaving every other
column alone, so an approved legacy Drawdown mapping can sit beside a new One Earth run:

```python
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.activities import assemble_mapping_runs

combined = assemble_mapping_runs(
    base_frame,
    {"one_earth": "s3://.../activity_jev/one-earth-002/organizations.parquet"},
    id_col="org_id",
)
```

It reads the export columns from each run's lineage and raises "covers a different
organization cohort" when a run's IDs differ from `base_frame`'s. It makes no model calls
and writes nothing.

---

## Adding a taxonomy

A taxonomy is a small package beside `one_earth/` and `drawdown/`; nothing in
`activity_jev` changes:

```python
# vdl_tools/shared_tools/taxonomy_mapping/food_system/__init__.py
from ..activity_jev.mapping import MatchPolicy, load_workbook

COLUMNS = ("Sector", "Practice")
SHEETS = ("Sectors", "Practices")
DEFAULT_POLICY = MatchPolicy()
DEFAULT_ROLLUP = "union"


def load_taxonomy(path):
    return load_workbook("food_system", path, columns=COLUMNS, sheets=SHEETS, question_builder=build_questions)


def build_questions(nodes, columns):
    return nodes.assign(question=[
        {"type": "noul", "instructions": f"Does this organization work on {r.name}? {r.Definition}"}
        for r in nodes.itertuples()
    ])
```

The workbook needs one sheet per level with the path columns so far and a `Definition`;
an `Exclude` column of 1 drops a node and its descendants. Any depth works, from one
level up. Pass `attributes=(...)` to `load_workbook` to keep extra sheet columns on the
nodes for `attribute_columns`.

---

## Cost, caching and reruns

What the `zein-test` run used, for 500 organizations, of which 426 had usable website
text and 1,351 activities:

| Taxonomy | Nodes | Request groups | Jev requests | Median input tokens per request | Median output tokens | Total input | Total output |
| -- | -- | -- | -- | -- | -- | -- | -- |
| One Earth | 137 | 1 | 1,351 | 21,772 | 4,963 | 29.4M | 6.7M |
| Drawdown | 198 | 2 | 2,702 | 31,895 | 3,601 | 86.2M | 9.7M |

Jev requests are activities × request groups, and nearly all input tokens are the
taxonomy's questions, sent once per activity per group. Drawdown therefore costs about
three times One Earth. Price a full run from these per-activity numbers and the current
`typesafe-ai/jev` gateway rate before launching it. Extraction adds one `gpt-5.4-mini`
call per organization with text, on up to 300,000 characters of website text; this
snapshot does not record its tokens.

What is cached, and what makes a rerun call the model again:

| Step | Cache key | Calls again when |
| -- | -- | -- |
| Extraction | Prompt text, response schema, model; organization ID; name plus website text | The website text, name, prompt or schema changes |
| Jev scoring | One request group's questions and routing; a hash of organization ID and activity text | Any question in that group is reworded, or the activity text changes |
| Selection, roll-up, export, funding | Not cached; deterministic and free | Never calls a model |

A new `run_id` alone does not cause cache misses. Each Jev request group stops after 10
HTTP 5xx responses, counting retries; responses already received are committed to the
cache, so rerunning resumes where it stopped.

---

## Testing without model calls

The two model calls are module functions; patch them. `tests/activity_jev/conftest.py`
has fixtures for exactly this:

```python
import pytest

from vdl_tools.shared_tools.activity_extraction import pipeline
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev import mapping


@pytest.fixture
def use_extraction(monkeypatch):
    """Answer extraction requests with ``fake(given_ids_texts, prompt, **kw) -> {uid: profile}``."""
    return lambda fake: monkeypatch.setattr(pipeline, "run_extraction_prompt", fake)


@pytest.fixture
def use_jev(monkeypatch):
    """Answer Jev gateway requests with ``fake(questions, ids_texts, **kw) -> {cache_id: row}``."""
    return lambda fake: monkeypatch.setattr(mapping, "jev_gateway_request", fake)
```

A fake Jev reply is `{"response_text": json.dumps({node_id: {"noul": 0.8}, ...})}` per
cache ID; see `fake_jev` in the same file. Patch `mapping.jev_gateway_request`, not a
copy imported elsewhere.

---

## Troubleshooting

| Message | Cause | Fix |
| -- | -- | -- |
| `Pass url_col to read website text, or supply website_texts` | No website column and no supplied text | Pass `url_col=`, or `website_texts={org_id: text}` |
| `Missing organization columns: [...]` | A named column is not in the frame | Check `id_col`, `name_col`, `url_col` |
| `Organization IDs must be non-null and unique` | Duplicate or blank IDs (compared as strings) | Deduplicate first; `1` and `"1"` collide |
| `Scores belong to a different taxonomy or question set; re-run scoring` | The workbook or a question changed since scoring | Load the workbook the run used (its hash is in the lineage), or re-score |
| `Taxonomy question exceeds request budget: <node>` | One question is over the 48k-token request budget | Shorten that node's definition or criteria |
| `Unknown rollup ...` | A roll-up other than `"union"` or `"core_first"` | Use one of the two |
| `... covers a different organization cohort` | Assembling a run made on other organizations | Assemble onto the same cohort, or rerun the job on it |
| Many organizations `missing_text` | Websites absent, unreachable, or junk verdicts | Check the scrape cache, or supply `website_texts` |

---

## One-page summary

```python
from vdl_tools.shared_tools.taxonomy_mapping import drawdown, one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.activities import (
    MappingJob, run_activity_mapping, export_extraction, assemble_mapping_runs,
)
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.mapping import (
    MatchPolicy, ScoringResult, load_workbook, map_subjects, select_matches,
)

# A project: extract once, map each job, write files with lineage
job = MappingJob(drawdown.load_taxonomy(wb), drawdown.DEFAULT_POLICY, "drawdown_category",
                 rollup=drawdown.DEFAULT_ROLLUP)
run = run_activity_mapping(orgs, {"drawdown": job}, run_id="pilot-001", directory=out,
                           id_col="org_id", name_col="org_name", url_col="homepage")

# Any text, any taxonomy
result = map_subjects(subjects, taxonomy, policy=MatchPolicy())   # subjects: subject_id, state

# New policy on saved scores: no requests
paths = select_matches(scored, taxonomy, MatchPolicy(thresholds=(0.8, 0.6)))

# Combine selected runs by ID
combined = assemble_mapping_runs(base, {"drawdown": f"{out}/organizations.parquet"}, id_col="org_id")
```
