# Activity extraction and Jev mapping · D1 / VIB-78

Implementation contract, updated 2026-10-30. Canonical project spec: [Linear](https://linear.app/vibrant-data-labs/document/spec-general-jev-taxonomy-mapping-d415afa01680).

## General mapping boundary and independent loading

`map_subjects(subjects, taxonomy, policy=...)` accepts supplied text with `subject_id`
and returns scores, statuses and paths. Extraction and the tracker-shaped export are
separate operations. `Taxonomy` and the parameterized `load_workbook` reader accept
configured levels and explicit built questions; the core has no fixed Solution ceiling.
One- and four-level Education fixtures demonstrate direct organization input without
core changes.

`taxonomy_mapping.one_earth.load_taxonomy` and `taxonomy_mapping.drawdown.load_taxonomy`
are separate packages that load only their own definitions and question wording, one
production wording each (`cc_routing`, `stewardship_yes`); the core has no
name-switching dispatcher and no variant mechanism. Tracker column names and display
conventions live in Climate Landscape. Climate Landscape loads only the selected
taxonomy's workbook; shared-data may hold both, and a missing or invalid sibling
workbook does not block the selected loader.

Climate Landscape obtains activities through the `InstructorPRC` prompt cache on
all runs; it writes no separate extraction snapshot and requires no extraction-run
path. The output prefix is derived from the existing S3 `results_path` and run label.
It maps selected taxonomies in turn, writes each taxonomy's files as it finishes, and
records the input, extraction and per-taxonomy identities in the mapped Parquet's
lineage footer (`get_lineage`). Errors leave completed mapping files in place, and
reruns reuse unchanged cached responses. Progress logs cover website-cache reads,
extraction, JEV request groups, and output writes. The shared prompt cache uses its existing chunk-level logging.
`assemble_mapping_runs` reads each selected run's mapped Parquet and joins its export
columns by organization ID, preserving an unselected taxonomy's approved legacy
columns. No assembly or mapping function promotes production outputs.

## Source revisions

| Component | Source and revision | Reused behavior |
| -- | -- | -- |
| Shared infrastructure | vdl-tools `40a33ad45936ff53fadaa6ee310bb97c8a33bd93` | SQL Jev adapter; structured response cache; mapping columns; funding allocation |
| Extraction | oneearth `d6811ca9286b846cd2093e1b5327a8346393143a`, `activity_extraction/` | General prompt and schema, rendering |
| One Earth questions | Same revision, `jev/jev_mapping.py` | Activity wording and `cc_routing`; independent node probabilities; ancestor implication |
| Drawdown questions and rules | drawdown `dfc03de9cd7b3fd4331631ab6153647db7b386ce`, `jev/jev_drawdown.py` | `stewardship_yes`; 0.7/0.5 tiers; 0.6 descent; full-path prefix deduplication |

Fresh remote references were inspected on 2026-10-28. The original main checkouts and their unrelated edits were left intact. The generic code does not import either source repo or set their config paths. Templates retain the source wording with the prototype's activity substitutions pre-applied; package imports and request grouping are adapted.

## Pinned taxonomy inputs

Zein chose upgrading to the newest taxonomy versions on 2026-10-28. Shared-data owns the reviewed workbooks under `data/taxonomies/oneearth` and `data/taxonomies/Drawdown`; their source repository revisions are the table above, and each run records the hash of the workbook it used in its lineage. Climate Landscape references these shared files through `paths.ini`; it stores no taxonomy payloads.

| Taxonomy | Newest committed workbook reviewed | Included nodes through Solution |
| -- | -- | -- |
| One Earth | `OE Solutions Terms 20260514_expanded_VDL.xlsx` from `d6811ca` | 5 Pillars, 17 Sub-Pillars, 115 included Solutions |
| Drawdown | `DrawdownTaxonomy_VDL_2026-10-22.xlsx` from `dfc03de` | 9 Sectors, 26 Clusters, 163 Solutions |

The One Earth filename is still May 14 in the newest source revision. Its content hash, not a latest-file glob, identifies the actual input. Excluded ancestors suppress their descendants. Missing definitions, duplicate full paths, and orphan paths fail validation. No Sub-Term or taxonomy Activity sheets are read.

## Input and extraction

* One unique, non-null `id`, `Organization` and `Website` per organization. IDs are strings in every output.
* Website text comes from the shared scraper (`scrape_websites_psql`: pages already in the SQL scrape cache are served, missing sites are scraped once) with the website summarizer's junk verdicts applied (parked, blocked, not-found, garbled, dead and empty sites are skipped); callers may supply `website_texts` directly. No 990 content or archived descriptions are used by `extract_frame`.
* Keep the general source prompt and schema. Production extraction uses `gpt-5.4-mini`, medium reasoning, through the shared `InstructorPRC` cache, which puts the response schema in the prompt identity and keys rows by model (`filter_by_model=True`). A cached row that no longer validates raises rather than being dropped.
* Keep all extracted activities, including minor, one-off and past activities. Importance is metadata and never an assignment or funding weight.
* Return one profile/status record for every input organization: `success`, `empty`, `missing_text`, or `request_failed`. A failed or unparsable model response never silently removes its organization.

Extraction has no taxonomy or JEV dependency and returns the supplied schema's activity fields without scoring text or mapping cache keys. The downstream `build_activity_subjects` adapter creates those inputs for JEV.

`ExtractionResult.organizations` records the rendered profile, source hash, text length, coverage, activity count, extraction run, and status. `activities` records every schema field, evidence, organization identity and run-qualified activity identity.

Activity IDs are SHA-256 over `[extraction_run_id, org_uid, activity_index]`; they are only stable within a fixed extraction run. `activity_jev.subjects.build_activity_subjects` builds the JEV text and cache IDs from `[org_uid, six-field activity state]`, so a new run ID does not invalidate unchanged activity requests. No durable identity is inferred across changed activities.

## Scores and policies

`Taxonomy` holds configured path columns, exact node questions, payload hash and path. Nodes use hashes of taxonomy name plus full ancestor path, preventing same-name children from colliding. The two first-adopter loaders select three levels through Solution.

`ScoringResult` holds every successful `(subject_id, node_id, probability)` plus one usage/status record per subject/request group. Cache IDs hash subject identity and supplied state; the JEV activity adapter supplies `cache_id` from organization and activity state so an unchanged activity reuses its cached answers across runs. Each group records the question hash and returned model. Successful raw gateway responses remain in the existing SQL cache. Jev's gateway alias is not a version pin; retain observed response metadata when judging reproducibility.

Questions are packed deterministically under a 48k cl100k token estimate. This leaves context headroom under the gateway's nominal 64k limit; tokenizer estimates are not a contractual guarantee. Oversized individual questions fail instead of truncating definitions. This replaces Drawdown's October-specific fixed sector groups with reusable bounded groups.

Missing, extra, nonnumeric, nonfinite, or out-of-range answers fail the request and are cached as errors, so a later run retries them. If any request group fails, that activity contributes no matches. Other activities still run; the failed group's successful peers and raw scores remain reviewable.

The initial, versioned development policy follows the inspected activity prototypes:

* Both: all nodes at 0.7 or above; only activities with no hit use all nodes at 0.5 or above.
* One Earth: no third tier or descent by default.
* Drawdown: recursively admit children at 0.6 under matched parents, through Solution only.

These are first-adopter implementation baselines (`DEFAULT_POLICY` in each package), not validated production thresholds. `select_matches` makes no requests and rejects scores from a different payload or question set; it needs neither SQL nor gateway access.

## Rollup and consumer outputs

* Take the deduplicated union of every activity's paths. Remove shallow rows implied by deeper paths, preserving the source prototypes' representation. Keep the raw activity-path table for complete provenance.
* Each organization path records supporting activity IDs, per-activity evidence and source hashes, best probability, best tier, and support count. Probability and importance never determine dollars.
* Reuse `attach_mapping_columns` and `distribute_funding_from_matches(max_level=2)` through `exports.export_extraction`. Preserve native-list legacy mapping columns and blank level-3 columns. Unmatched organizations stay in mapped metadata and have explicit statuses; existing serving consumers emit their No Match rows.
* Funding fractions sum to one for each matched organization at the existing allocation grain. Hierarchical parent and child rows are never summed as separate allocations.
* Climate Landscape preserves Drawdown Solution properties and the legacy `Sector: Cluster` display prefix in its export settings. The October workbook already carries the new sector/cluster structure, so Climate Landscape emits regrouped aliases without applying the June crosswalk.

## Validation and release

Offline fixtures cover source accounting, full-path identity, exclusion, thresholds and descent, request completeness, cache identity, union evidence, allocation, and end-to-end candidate exports. They prove implementation behavior, not classification quality.

Q5/VIB-76 still defines judges, human reference labels, minimum sample sizes, and release targets. D2/VIB-79 cannot freeze the real evaluation cohorts until that decision. Q6/VIB-77 still controls production budget, backfill scope and release authority. Candidate runs use separate labelled S3 prefixes; neither the shared library nor the new integration promotes production results.
