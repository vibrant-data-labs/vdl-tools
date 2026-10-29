# General JEV taxonomy mapping

The engine accepts supplied subjects and one taxonomy per call. It does not import
activity extraction, name a taxonomy, allocate funding or modify tracker columns.
See [the implementation contract](../../../../docs/activity_jev_contract.md).

```python
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import MatchPolicy
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.pipeline import map_subjects, group_matches
from vdl_tools.shared_tools.taxonomy_mapping.one_earth import load_taxonomy, default_policy

# This package does not import Drawdown or read its workbook or configuration.
taxonomy = load_taxonomy(one_earth_workbook)
# subjects: subject_id, state; optional entity_id, kind and evidence fields.
result = map_subjects(subjects, taxonomy, policy=default_policy())
# result.subject_paths, scores, usage, nodes, lineage
entities = group_matches(result.subject_paths, subjects, taxonomy)  # optional entity union
```

## Layout

| Package | Holds |
| -- | -- |
| `taxonomy_mapping/activity_jev/` | `engine` (scoring, offline selection), `taxonomies` (`Taxonomy`, `load_workbook`), `pipeline` (`map_subjects`, `group_matches`), `activity_rollup`, `exports` |
| `taxonomy_mapping/one_earth/` | One Earth loader, question wording, default policy, compatibility export |
| `taxonomy_mapping/drawdown/` | Drawdown loader, question wording, default policy, compatibility export |

The two taxonomy packages are independent of each other. Both currently use
activity-worded questions through Solution. The core supports arbitrary configured
depths and other subject kinds: construct a `Taxonomy` with explicit levels, nodes
and questions, or call `load_workbook` with a question builder. A third taxonomy
needs its own package and no core edits. Match question wording to the supplied
subject kind; supplying `kind` enables an explicit consistency check.

## Extraction and compatibility exports

Extraction is an optional upstream step. `ExtractionResult.save(directory)` and
`ExtractionResult.load(directory)` persist immutable, checksummed snapshots. For
extracted activities, rename `activity_id` to `subject_id` before calling the core.
Scores keep subject IDs; evidence remains in the supplied subject table.

`score_activities` and `apply_policy` are the same operations keyed by `activity_id`.
Each taxonomy package's `export_extraction(organizations, extraction, taxonomy, mapped)`
rolls activity matches up to organizations (`activity_rollup.rollup_paths`) and
attaches the legacy tracker columns and level-2 funding through `exports`. Those
output conventions are outside generic inference.

## Reselecting saved scores

`select_matches(result, taxonomy, policy)` makes no requests. Policy changes reuse
saved probabilities; a changed payload, variant, question set or subject kind is
rejected. Older activity manifests with per-group question hashes are checked using
those hashes. Gateway credentials and caller configuration come from the existing
JEV SQL adapter.
