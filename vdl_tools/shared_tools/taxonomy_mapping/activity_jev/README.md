# General JEV taxonomy mapping

The engine accepts supplied subjects and one taxonomy per call. It does not extract
text, name a taxonomy, allocate funding or write tracker columns. See
[the implementation contract](../../../../docs/activity_jev_contract.md).

## Running a project

`runs.run_activity_mapping` is the entry point for classifying a project's
organizations from their websites. It extracts activities once, maps them onto each
job's taxonomy, rolls the matches up to organizations and writes every table through
`parquet_cache` with its lineage. The caller names its own columns; nothing assumes
`id`, `Organization` or `Website`.

```python
from vdl_tools.shared_tools.taxonomy_mapping import one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.runs import MappingJob, run_activity_mapping

jobs = {"one_earth": MappingJob(one_earth.load_taxonomy(workbook), one_earth.DEFAULT_POLICY, "one_earth_category")}
run = run_activity_mapping(
    orgs, jobs, run_id="pilot-001", directory="s3://bucket/project/activity_jev/pilot-001",
    id_col="org_id", name_col="org_name", url_col="homepage",
)
# run.organizations: orgs + mapping columns + activity_extraction_status etc.
# Files: organizations, <job>_paths, <job>_funding, <job>_scores, <job>_usage, <job>_activity_paths
```

A `MappingJob` also takes the tracker conventions a consumer wants: `format_paths` to
reshape result rows, `attribute_columns` to list node attributes per organization,
`pad_levels` to emit empty deeper level columns, and `funding_level` to stop funding
above the deepest level. `file_names` renames the result, funding and organization files.
Each taxonomy's files are written as soon as it finishes. `assemble_mapping_runs`
combines selected taxonomies from finished runs by ID.

```python
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import map_subjects
from vdl_tools.shared_tools.taxonomy_mapping.one_earth import DEFAULT_POLICY, load_taxonomy

# This package does not import Drawdown or read its workbook.
taxonomy = load_taxonomy(one_earth_workbook)
# subjects: subject_id, state; optional cache_id.
result = map_subjects(subjects, taxonomy, policy=DEFAULT_POLICY)
# result.subject_paths, scores, usage, lineage
```

## Layout

| Package | Holds |
| -- | -- |
| `taxonomy_mapping/activity_jev/` | `engine` (`score_subjects`, `select_matches`, `map_subjects`), `taxonomies` (`Taxonomy`, `load_workbook`), `subjects` (`build_activity_subjects`), `exports` (`rollup_paths`, `export_extraction`: tracker-shaped columns, statuses, funding), `runs` (`MappingJob`, `run_activity_mapping`, `assemble_mapping_runs`) |
| `taxonomy_mapping/one_earth/` | `load_taxonomy`, question wording (`cc_routing`), `DEFAULT_POLICY` |
| `taxonomy_mapping/drawdown/` | `load_taxonomy`, question wording (`stewardship_yes`), `DEFAULT_POLICY` |

The two taxonomy packages are independent of each other and carry one production
wording each; a wording change shows up as a new `questions_sha256`. The core supports
any configured depth: construct a `Taxonomy` or call `load_workbook` with a question
builder. A third taxonomy needs its own package and no core edits.

## Extraction and export

The runner is built from these pieces, which remain usable on their own.
`activity_extraction.pipeline.extract_frame(orgs, run_id=..., id_col=..., name_col=...,
url_col=...)` extracts website activities once per organization from the text the
shared scraper holds for its website, or from supplied `website_texts` keyed by
organization ID. `ExtractionResult.save/load` persist the snapshot as Parquet through
`parquet_cache` with the lineage in the footers.

`exports.export_extraction(organizations, extraction, taxonomy, mapped, id_col=...,
name_col=..., mapping_name=...)` rolls activity matches up to organizations and attaches
the legacy mapping columns, per-organization statuses and funding. Padding, funding depth,
column names and display conventions are the consumer's choice; Climate Landscape keeps
its One Earth and Drawdown settings in `process_map_activities.py` (four padded levels,
level-2 funding).

`select_matches(result, taxonomy, policy)` makes no requests, so a policy change
re-applies to the same scores; scores from a different workbook or question set are
rejected.


Activity extraction is independent of taxonomy mapping. Its result contains schema
fields and extraction provenance. Before JEV scoring, call
`subjects.build_activity_subjects(extraction.activities)` to construct `subject_id`,
the six-field scoring text, and stable JEV cache IDs. Other consumers can use the
extracted activities directly or supply their own extraction schema.
