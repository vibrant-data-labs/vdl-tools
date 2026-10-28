# Activity-level Jev mapping

See [`docs/activity_jev_contract.md`](../../../../docs/activity_jev_contract.md) for source revisions, contracts and open release gates.

The library consumes the shared extractor's activity records, maps One Earth and Drawdown through Solution, and retains raw scores for offline policy changes. It needs no source-project checkout. The caller owns configuration through `VDL_GLOBAL_CONFIG_PATH` and gateway credentials through the existing Jev adapter.

```python
from vdl_tools.shared_tools.activity_extraction.pipeline import extract_frame
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.taxonomies import load_taxonomy
from vdl_tools.shared_tools.taxonomy_mapping.activity_jev.pipeline import map_extraction

extracted = extract_frame(orgs, run_id="pilot-001")
taxonomy = load_taxonomy("drawdown", "DrawdownTaxonomy_VDL_2026-10-22.xlsx")
result = map_extraction(orgs, extracted, taxonomy)
# result.organizations, paths, activity_paths, scores, usage, nodes, funding, lineage
```

Score existing parsed activities:

```bash
python -m vdl_tools.shared_tools.taxonomy_mapping.activity_jev \
  --activities activity_extraction_activities.parquet \
  --taxonomy drawdown --taxonomy-path DrawdownTaxonomy_VDL_2026-10-22.xlsx \
  --output-dir local_data/jev/score-001
```

Reselect the saved probabilities with a different rule, without SQL or model calls:

```bash
python -m vdl_tools.shared_tools.taxonomy_mapping.activity_jev \
  --stored-run local_data/jev/score-001 \
  --taxonomy drawdown --taxonomy-path DrawdownTaxonomy_VDL_2026-10-22.xlsx \
  --thresholds 0.8,0.6 --descend none --output-dir local_data/jev/select-002
```

Use `--variant base` only for an explicitly chosen comparison; defaults preserve the source activity variants (`cc_routing`, `stewardship_yes`). Output directories must be new. The standalone CLI emits activity and organization path tables, not production funding artifacts; `map_extraction` supplies compatible metadata and allocation outputs.
