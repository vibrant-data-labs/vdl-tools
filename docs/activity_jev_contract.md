# Activity extraction and Jev mapping · D1 / VIB-78

Implementation contract, 2026-10-28. Canonical project spec: [Linear](https://linear.app/vibrant-data-labs/document/spec-activity-parsing-and-jev-taxonomy-mapping-d415afa01680).

## Source revisions

| Component | Source and revision | Reused behavior |
| -- | -- | -- |
| Shared infrastructure | vdl-tools `40a33ad45936ff53fadaa6ee310bb97c8a33bd93` | SQL Jev adapter; structured response cache; mapping columns; funding allocation |
| Extraction | oneearth `d6811ca9286b846cd2093e1b5327a8346393143a`, `activity_extraction/` | General prompt and schema, text assembly/caps, cached website reader, structured cache, rendering, grounding |
| One Earth questions | Same revision, `jev/jev_mapping.py` | Activity wording and `cc_routing`; independent node probabilities; ancestor implication |
| Drawdown questions and rules | drawdown `dfc03de9cd7b3fd4331631ab6153647db7b386ce`, `jev/jev_drawdown.py` | `stewardship_yes`; 0.7/0.5 tiers; 0.6 descent; full-path prefix deduplication |

Fresh remote references were inspected on 2026-10-28. The original main checkouts and their unrelated edits were left intact. The generic code does not import either source repo or set their config paths. Templates retain the source wording; package imports and request grouping are adapted.

## Pinned taxonomy inputs

Zein chose upgrading to the newest taxonomy versions on 2026-10-28. Climate Landscape vendors the reviewed workbooks and records their repository revisions and SHA-256 values in `data/taxonomy/activity_jev_sources.json`.

| Taxonomy | Newest committed workbook reviewed | Included nodes through Solution |
| -- | -- | -- |
| One Earth | `OE Solutions Terms 20260514_expanded_VDL.xlsx` from `d6811ca` | 5 Pillars, 17 Sub-Pillars, 115 included Solutions |
| Drawdown | `DrawdownTaxonomy_VDL_2026-10-22.xlsx` from `dfc03de` | 9 Sectors, 26 Clusters, 163 Solutions |

The One Earth filename is still May 14 in the newest source revision. Its content hash, not a latest-file glob, identifies the actual input. Excluded ancestors suppress their descendants. Missing definitions, duplicate full paths, and orphan paths fail validation. No Sub-Term or taxonomy Activity sheets are read.

## Input and extraction

* One unique, non-null organization ID, name, and Website column per organization. Caller column names are configurable; IDs are normalized internally and restored at export.
* Read the existing SQL website cache, or accept caller-provided `SiteText` pages. No new scrape, 990 content, archived descriptions, or implicit repo-local overlay is used by `extract_frame`.
* Keep the general source prompt and schema. Production extraction uses `gpt-5.4-mini`, medium reasoning, through the existing structured SQL cache, including its schema fingerprint and model isolation.
* Keep all extracted activities, including minor, one-off and past activities. Importance is metadata and never an assignment or funding weight.
* Return one profile/status record for every input organization: `success`, `empty`, `missing_text`, or `request_failed`. A failed or unparsable model response never silently removes its organization.

`ExtractionResult.organizations` records the rendered profile, source hash, source cache table, coverage, activity count, extraction run, and status. `activities` records every schema field, evidence, organization identity and run-qualified activity identity.

Activity IDs are SHA-256 over `[extraction_run_id, org_uid, activity_index]`; they are only stable within a fixed extraction run. Jev cache IDs hash `[org_uid, six-field activity state]`, so a new run ID does not invalidate unchanged activity requests. No durable identity is inferred across changed activities.

## Scores and policies

`ActivityTaxonomy` holds three level columns, level specs, exact node questions, workbook hash, path and variant. Nodes use hashes of taxonomy name plus full ancestor path, preventing same-name children from colliding.

`ScoringResult` holds every successful `(activity_id, node_id, probability)` plus one usage/status record per activity/request group. Each group records the question hash and returned model. Successful raw gateway responses remain in the existing SQL cache. Jev's gateway alias is not a version pin; the observed response metadata must be retained when judging reproducibility.

Questions are packed deterministically under a 48k cl100k token estimate, with activity state limited to 8k. This leaves context headroom under the gateway's nominal 64k limit; tokenizer estimates are not a contractual guarantee. Oversized individual questions fail instead of truncating definitions. This replaces Drawdown's October-specific fixed sector groups with reusable bounded groups.

Missing, extra, nonnumeric, nonfinite, or out-of-range answers fail the request. Malformed responses are cached as errors; malformed old success rows are refreshed. If any request group fails, that activity contributes no matches. Other activities still run; the failed group's successful peers and raw scores remain reviewable.

The initial, versioned development policy follows the inspected activity prototypes:

* Both: all nodes at 0.7 or above; only activities with no hit use all nodes at 0.5 or above.
* One Earth: no third tier or descent by default.
* Drawdown: recursively admit children at 0.6 under matched parents, through Solution only.

These are implementation baselines, not validated production thresholds. `apply_policy` makes no requests and rejects a different workbook/variant. The shared CLI can reselect a stored scoring run without SQL or API access.

## Rollup and consumer outputs

* Take the deduplicated union of every activity's paths. Remove shallow rows implied by deeper paths, preserving the source prototypes' representation. Keep the raw activity-path table for complete provenance.
* Each organization path records supporting activity IDs, per-activity evidence and source hashes, best probability, best tier, and support count. Probability and importance never determine dollars.
* Reuse `attach_mapping_columns` and `distribute_funding_from_matches(max_level=2)`. Preserve native-list legacy mapping columns and blank level-3 columns. Unmatched organizations stay in mapped metadata and have explicit statuses; existing serving consumers emit their No Match rows.
* Funding fractions sum to one for each matched organization at the existing allocation grain. Hierarchical parent and child rows are never summed as separate allocations.
* Preserve Drawdown Solution properties and the legacy `Sector: Cluster` display prefix. The October workbook already carries the new sector/cluster structure, so Climate Landscape emits regrouped aliases without applying the June crosswalk.

## Validation and release

Offline fixtures cover source accounting, full-path identity, exclusion, thresholds and descent, request completeness, cache identity, union evidence, allocation, and end-to-end candidate exports. They prove implementation behavior, not classification quality.

Q5/VIB-76 still defines judges, human reference labels, minimum sample sizes, and release targets. D2/VIB-79 cannot freeze the real evaluation cohorts until that decision. Q6/VIB-77 still controls production budget, backfill scope and release authority. Candidate runs use immutable labelled directories; neither the shared library nor the new integration promotes production results.
