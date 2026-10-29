# General JEV taxonomy mapping

The engine accepts supplied subjects and one taxonomy per call. It does not import
activity extraction, load a particular taxonomy, allocate funding or modify tracker
columns. See [the implementation contract](../../../../docs/activity_jev_contract.md).

```python
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.engine import MatchPolicy
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.pipeline import map_subjects, group_matches
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.one_earth import load_taxonomy, default_policy

# This loader does not import Drawdown or access its workbook/configuration.
taxonomy = load_taxonomy(one_earth_workbook)
# subjects: subject_id, state; optional entity_id, kind and evidence fields.
result = map_subjects(subjects, taxonomy, policy=default_policy())
# result.subject_paths, scores, usage, nodes, lineage
entities = group_matches(result.subject_paths, subjects, taxonomy)  # optional entity union
```

One Earth and Drawdown have independent loader, question and policy modules.
Both currently use activity-worded questions through Solution. The core supports
arbitrary configured depths and other subject kinds: construct a `Taxonomy` with
explicit levels/nodes/questions, or use `load_workbook` with a question builder.
A third taxonomy requires no core edits. Match question wording to the supplied
subject kind; supplying `kind` enables an explicit consistency check.

Extraction is an optional upstream step. `ExtractionResult.save(directory)` and
`ExtractionResult.load(directory)` persist immutable, checksummed snapshots.
For extracted activities, rename `activity_id` to `subject_id` before calling
the core. Scores keep subject IDs; evidence remains in the supplied subject table.

`score_activities`, `apply_policy`, `rollup_paths` and `map_extraction` remain
compatibility APIs. `map_extraction` loads only the selected built-in configuration
and calls its optional legacy exporter. The exporters preserve native-list
mapping columns and evidence; first-adopter funding remains capped at level 2.
Those output conventions are outside generic inference.

Score existing activity records with one selected loader:

```bash
python -m vdl_tools.shared_tools.taxonomy_mapping.activity_jev \
  --activities activities.parquet \
  --taxonomy drawdown --taxonomy-path DrawdownTaxonomy_VDL_2026-10-22.xlsx \
  --output-dir local_data/jev/score-001
```

For another taxonomy or direct organization text, use supplied subjects and a
JSON specification (`name`, `columns`, `nodes` containing built questions;
optional `levels`, `variant`, `subject_kind`):

```bash
python -m vdl_tools.shared_tools.taxonomy_mapping.activity_jev \
  --subjects subjects.parquet --taxonomy-spec education.json \
  --namespace education-tracker --output-dir local_data/jev/education-001
```

Reselect saved probabilities without SQL or model access:

```bash
python -m vdl_tools.shared_tools.taxonomy_mapping.activity_jev \
  --stored-run local_data/jev/education-001 --taxonomy-spec education.json \
  --thresholds 0.8,0.6 --descend none --output-dir local_data/jev/education-002
```

Policy changes reuse probabilities; changed payloads, variants, questions or
subject kinds are rejected. Older activity manifests with per-group question
hashes are checked using those hashes. Output directories must be new. The CLI
does not publish tracker/funding outputs. Caller configuration and gateway
credentials use the existing JEV SQL adapter.
