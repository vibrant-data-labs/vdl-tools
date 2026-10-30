# General JEV taxonomy mapping

The engine accepts supplied subjects and one taxonomy per call. It does not extract
text, name a taxonomy, allocate funding or write tracker columns. See
[the implementation contract](../../../../docs/activity_jev_contract.md).

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
| `taxonomy_mapping/activity_jev/` | `engine` (`score_subjects`, `select_matches`, `map_subjects`), `taxonomies` (`Taxonomy`, `load_workbook`), `exports` (`rollup_paths`, `export_extraction`: tracker-shaped columns, statuses, funding) |
| `taxonomy_mapping/one_earth/` | `load_taxonomy`, question wording (`cc_routing`), `DEFAULT_POLICY` |
| `taxonomy_mapping/drawdown/` | `load_taxonomy`, question wording (`stewardship_yes`), `DEFAULT_POLICY` |

The two taxonomy packages are independent of each other and carry one production
wording each; a wording change shows up as a new `questions_sha256`. The core supports
any configured depth: construct a `Taxonomy` or call `load_workbook` with a question
builder. A third taxonomy needs its own package and no core edits.

## Extraction and export

`activity_extraction.pipeline.extract_frame` extracts website activities once per
organization from the text the shared scraper holds for its website; `ExtractionResult.save/load` persist the snapshot as Parquet through
`parquet_cache` with the lineage in the footers. Rename `activity_id` to `subject_id`
before calling the engine.

`exports.export_extraction(organizations, extraction, taxonomy, mapped, mapping_name=...,
format_paths=...)` rolls activity matches up to organizations and attaches the legacy
mapping columns (four levels), per-organization statuses and level-2 funding. Which
column name and display conventions to use is the consumer's choice; Climate Landscape
keeps its One Earth and Drawdown settings in `process_map_activities.py`.

`select_matches(result, taxonomy, policy)` makes no requests, so a policy change
re-applies to the same scores; scores from a different workbook or question set are
rejected.
