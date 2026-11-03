# General JEV taxonomy mapping

Map text onto any taxonomy with Jev, and classify a project's organizations from their
website activities. Worked examples, with output from a real run, are in the
[usage guide](../../../../docs/ACTIVITY_JEV_MAPPING.md); the design rules and source
revisions are in the [implementation contract](../../../../docs/activity_jev_contract.md).

## Layout

| Module | Holds | Knows about |
| -- | -- | -- |
| `activity_jev/mapping.py` | `Taxonomy`, `load_workbook`, `MatchPolicy`, `score_subjects`, `select_matches`, `map_subjects`, `jev_gateway_request` | Any text with an ID (a "subject") and one taxonomy |
| `activity_jev/activities.py` | `run_activity_mapping` (the entry point for a project), `MappingJob`, `build_activity_subjects`, `export_extraction`, `assemble_mapping_runs` | Extracted activities, organizations, tracker columns, funding and run files |
| `one_earth/`, `drawdown/` | `load_taxonomy`, question wording, `DEFAULT_POLICY`, `DEFAULT_ROLLUP` | One taxonomy each |

`mapping.py` never imports `activities.py`, a taxonomy package or a tracker; it maps any
text, with any configured hierarchy depth. `activities.py` imports `mapping.py` and the
shared extraction (`activity_extraction.pipeline.extract_frame`). The two taxonomy
packages are independent of each other and carry one production wording each; a wording
change shows up as a new `questions_sha256`. A third taxonomy needs its own package and
no changes here.

## In one call

```python
from vdl_tools.shared_tools.taxonomy_mapping import one_earth
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.activities import MappingJob, run_activity_mapping

jobs = {"one_earth": MappingJob(one_earth.load_taxonomy(workbook), one_earth.DEFAULT_POLICY, "one_earth_category")}
run = run_activity_mapping(
    orgs, jobs, run_id="pilot-001", directory="s3://bucket/project/activity_jev/pilot-001",
    id_col="org_id", name_col="org_name", url_col="homepage",
)
```

The caller names its own columns, and every tracker convention is a `MappingJob`
setting: `format_paths`, `attribute_columns`, `pad_levels`, `funding_level` and
`rollup`. `select_matches` makes no requests, so a policy change re-applies to saved
scores; scores from a different workbook or question set are rejected.
