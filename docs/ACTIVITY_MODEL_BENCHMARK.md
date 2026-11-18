# Open-weight models for activity extraction: benchmark notes

Can an open-weight model on Baseten replace `gpt-5.4-mini` (medium effort) for the website
activity extraction in `vdl_tools/shared_tools/activity_extraction`? This page records what
was measured, how the judges were built and why they changed, and what is still open. Work
from 2026-11-05 to 2026-11-17.

Code: `model_probe.py` (extractions), `model_judges.py` (judges), and `benchmark/` (reports
rebuilt from the cache; its `__init__.py` lists the rebuild commands in order).

## Where it stands

DeepSeek V4.1 Flash is the only candidate still in the running. Against the baseline, on 453
organizations:

- It finds more of the organization's work: it misses 0.17 lines of work per org against
  0.44. Pooled F1 is 0.877 against 0.823, and the per-org gain is +0.037 [+0.009, +0.064].
- It makes more errors: 14.2% of its activities against 10.4% (per org +0.038
  [+0.011, +0.064]). It also lists more activities per org.
- It costs about half as much ($9.74 against $17.48 per 1,000 orgs) but is about 3.5 times
  slower (35.6 s median against 10.0 s).

Not decided yet. Two things are worth settling first:

- **Human check of the judge doubts.** One reader's pass over a random draw found about a
  third of the precision judge's material flags cosmetic or doubtful, and most of DeepSeek's
  reported misses not real work (see "Failure modes"). A team check of about 50 items would
  confirm or reject that before anything is re-weighted.
- **The extraction prompt changes** in `data_out/activity_extraction_judges/prompt_adjustments.md`
  (fundraising, membership administration, current vs past work, credit for partners' work).
  They should be made and re-measured with the same judges, for both models.

## Setup

- **Baseline:** `gpt-5.4-mini`, medium reasoning effort, through OpenAI. Its stored rows (prompt
  `5415e5d5…`, request hash `c05a0fb2…`) are never re-run. A timed re-run under its own hash
  gave same-time latency for the 300 sample.
- **Inputs:** each org gets exactly the text its baseline row received
  (`prompt_response.input_text`), so every model sees the same input.
  - **153 cohort:** the seed-42 cohort the colleagues' alignment judge had scored, mostly
    Candid nonprofits.
  - **300 sample** (`data_out/activity_extraction_probe/sample300.json`, seed 42, tracker schema
    `cft_v2_zein_2026_10_22`): 200 Crunchbase for-profits and 100 Candid nonprofits. It includes
    only orgs with a scraped website: description-only inputs ("URL: unknown", about 22% of
    those groups) were excluded on purpose.
- **Settings:** output cap 24,000 tokens. Each candidate runs at its Baseten default reasoning,
  except Nemotron, whose reasoning is off by default and was turned on.
- **Caching:** every call goes through the Postgres prompt cache. Probe keys are
  `probe_v1|<mode>|<org>`, and judge keys are `bench_v2_<run>|<org>`. The baseline keeps its
  `bench_v1_` verdicts, since its extraction never changes. No existing row is overwritten. A
  new judge version or setting gets a new prompt row or request hash, so it gets new rows.

## Models tried and why they were dropped

| Model (Baseten unless noted) | Outcome |
|---|---|
| DeepSeek V4 Pro | Ignores the requested JSON schema: no valid output in schema mode, and it fails the canary. Usable only with the schema pasted into the prompt. Dropped. |
| tencent/hy3 on DeepInfra, through Vercel | Every probe call failed. Dropped. |
| Vercel routes to the same Baseten weights | Same behaviour as calling Baseten directly. Baseten direct was used from then on. |
| GLM 5.3 (full) | Probed only, as a quality ceiling. It returned valid profiles but was slow (50 s median on 10 orgs). Not benchmarked. |
| DeepSeek V4 Flash 0731 | Stops reasoning at 4,096 tokens. Raising the cap works only through chat completions; the Responses endpoint ignores `reasoning.max_tokens`. Replaced by V4.1 Flash on 2026-11-12. |
| GLM 5.3 Flash | Highest recall, but the most errors (21.6% against the baseline's 10.2% on the 153). Dropped 2026-11-13. |
| Nemotron 3.5 Super VL | Turning reasoning on cut its errors from 21% to 14% and its over-splitting from 10.7% to 2.8%, but its recall stayed at the baseline's (0.50 missed lines per org). Dropped 2026-11-13. |
| **DeepSeek V4.1 Flash 1017** | Kept. Its evidence quotes are verbatim 99–100% of the time. |

Every model that passed honored the strict `json_schema` format, as the canary mode confirmed:
it uses invented key names that only enforced decoding can produce.

Reasoning controls on Baseten, verified by calls:

- **DeepSeek V4.1 Flash:** reasons up to 75% of `max_output_tokens` and ignores
  `reasoning.max_tokens`, so raise the output cap to give it more room.
- **GLM 5.3 Flash:** has no reasoning budget at all, and at effort max it can spend the whole
  output cap on reasoning.
- **Nemotron:** reasoning is off unless an effort is sent.

## Results

All 453 orgs. The judges are precision v3.2 and recall v2.1 (`gpt-6-sol`), with alignment and
miss matching on `gpt-5.4-mini` medium. Intervals are per-org paired bootstrap 95% CIs
(DeepSeek − baseline). The full table by data source is in
`data_out/activity_extraction_judges/two_model_summary.md`.

| | Baseline | DeepSeek V4.1 |
|---|---|---|
| Activity errors | 10.4% | 14.2% |
| Missed lines of work per org | 0.44 | 0.17 |
| Pooled recall, low / central / high | 0.742 / 0.761 / 0.781 | 0.874 / 0.896 / 0.919 |
| Pooled F1, low / central / high | 0.812 / 0.823 / 0.835 | 0.866 / 0.877 / 0.888 |
| Median latency, same time, 8 parallel calls | 10.0 s | 35.6 s |
| List price per 1,000 orgs | $17.48 | $9.74 |

Prices are list prices on the run dates: OpenAI from `MODEL_DATA`, Baseten from its live
catalog (DeepSeek V4.1 Flash $0.30 per million input tokens and $1.20 per million output
tokens). `latency_price` reads the costs saved in the probe's `probe_calls.csv` files, which
are kept for that reason. Re-running the probe would re-price the calls from that day's
catalog, so the costs would no longer be the run dates' prices.

**By data source:**

- **Crunchbase for-profits:** DeepSeek errs more (16.4% against 9.7%), and its F1 gain is
  small (+0.024, the interval includes 0).
- **Candid nonprofits:** the baseline misses 0.74 lines of work per org. DeepSeek's F1 gain is
  +0.048, with the interval at about 0.

## How the judges were built, and why they changed

Every version stays in `model_judges.py`, so its stored verdicts can still be read back.
Current runs use precision v3.2 and recall v2.1. The reports from earlier versions were
deleted, since their judges were superseded and the verdicts are still in the cache. The
reasons for each change:

| Version | What changed, and why |
|---|---|
| Precision v1 (the colleagues' original) | Checked only name, what happens and who. Its duplicate rule split on location, which the extraction prompt says is no reason to split. Still read for one thing: its stored inputs hold the 153 cohort's descriptions on record. |
| Precision v2 (2026-11-09) | Showed every field and applied the prompt's own granularity rule. Counted any unsupported field as an error. In the blind human review (80 items), the human flagged 3 of 22 activities where v2 flagged 17, so v2's error rates (43–54% for the open models) were mostly cosmetic noise. |
| Precision v3 / v3.1 (2026-11-10) | Copied the summarization benchmark's judge: each faulty field gets a support level (inferred or fabricated) and an impact (material or none). Only material issues count, and severity is computed in code, not asked as a number. A work check comes first: is this concrete work, current, and done by this organization? Goals are taken at face value; only mission language that names no work is excluded. Agreement with the held-out human answers rose from 6 of 18 (v2) to 14 of 18. |
| Precision v3.2 (2026-11-12) | v3.1 plus a blind recurrence rating, compared in code with the model's own. Only past against ongoing or recurring counts as a mismatch; one-off against past is minor. The prompt text is otherwise unchanged, and a test enforces that. |
| Recall v1 | Over-reported: menu labels and link titles taken as evidence, communications and perks taken as work, and an audience or site difference taken as a reason to split. |
| Recall v2.1 | Excludes those, and names the nearest listed activity and how the miss differs. |
| Pooled reference | Each model had been scored against its own reference. Now one shared list of lines of work per org is built from the pairwise alignments, using union-find. The recall judges' misses are then matched to it. The range is reported because only about half of the line-of-work misses looked real in the human review. |

Judge noise is real: re-judging with one rule changed flipped 4 of 60 items between material
and not. Treat small differences in agreement as noise.

## Failure modes (read by hand, 2026-11-16)

From 80 errors and 40 misses read in full. The second round was a plain random draw of 20
errors and 10 misses per model. The item-by-item tags are in
`data_out/activity_extraction_judges/failure_dossier/reading_notes.md`.

| Error category | Random draw, baseline / DeepSeek (of 20 each) |
|---|---|
| Wrong doer: credit for what the org promotes, sponsors, links to, or a partner or customer does; sometimes a role given to customers or partners that the source doesn't give them | 9 / 7 |
| Borrowed detail: a place or claim from another product, page, office, the org's history or outside news | 7 / 5 |
| Sector stretched from clients or uses | 1 / 2 |
| Invented detail | 0 / 2 |
| No concrete work behind it (tagline, headline, appeal, marketing claim) | 0 / 2 |
| Ended work shown as current | 1 / 1 |
| Past work the model itself labelled past | 1 / 1 |
| A feature of a listed activity, listed on its own | 1 / 0 |
| *Judge questionable: the flag looks cosmetic or doubtful* | *7 / 8* |

- **who_carries_it_out is the main failure for both models.** It is also one of the six fields
  the taxonomy mapping scores (activity, what happens, who carries it out, aim, sector,
  methods). Geography, evidence and certainty are not scored.
- **Some errors don't affect the taxonomy.** Geography-only errors and past work the model
  labelled past are two of them. The mapping contract deliberately keeps and scores past
  activities, so the judge's "past is unsupported" rule disagrees with how the pipeline uses
  them. Without those two kinds, error rates are 7.9% (baseline) and 11.2% (DeepSeek).
- **Most of DeepSeek's misses don't look like missed program work.** Across both rounds, about
  9 of 22 of the baseline's sampled misses were real distinct lines of work, against about 3
  of 18 of DeepSeek's. The rest were fundraising, content marketing, internal support or
  mission-only lines. The evidence for misses often sits off the program pages: staff bios,
  social posts, grant announcements.

## Open questions and next steps

1. **Human check of about 50 items:** the judge-questionable errors, plus about 10 misses per
   model. It decides whether to relax the judge on cosmetic fields and whether the low end of
   the recall range is too harsh on DeepSeek.
2. **Measure materiality where it matters:** correct the flagged words in the activities
   flagged on mapped fields, re-run the taxonomy mapping (on the `ztawilvdl/activity-jev`
   branch), and count how many categories change. Not costed yet.
3. **Extraction prompt changes** from `prompt_adjustments.md`, then a re-run for both models
   with the same judges.
4. **Decide the policy on past work.** The mapping keeps it; the judge counts it as an error.

## What is kept on disk, and what can be rebuilt

Everything the reports use is in the Postgres cache: every extraction, judge verdict,
alignment, miss match, wall-clock latency (saved in the response metadata) and token count.
Every CSV and report under `data_out/activity_extraction_judges/` can be rebuilt at no cost
with the commands in `benchmark/__init__.py`. A rebuild on 2026-11-17 reproduced the saved
files exactly, apart from bootstrap intervals, which moved by up to 0.003 until the row order
was made fixed.

These exist only on disk (gitignored `data_out/`) and must be kept:

- `activity_extraction_judges/review/human/`, `human_overrides.json`, `key.json`, `items.json`
  and `rubric.md`: the blind human review of 80 verdicts (2026-11-09/10). The key says which
  blind item came from which model and judge version, and without it the answers can't be
  scored. The overrides are the human's own corrections, approved 2026-11-10; the review
  page's shared store holds the answers as first saved.
- `activity_extraction_judges/review/claude_labels/` and `failure_dossier/reading_notes.md`:
  labels and tags written by hand.
- `activity_extraction_probe/sample300.json`: the 300 sample. The tracker schema it was drawn
  from can change.
- `activity_extraction_judges/prompt_adjustments.md`: the pending prompt changes.
- `activity_extraction_probe/sample300_deepseek/`, `sample300_baseline/` and `cohort153_v2/`:
  the probe runs behind the current results. Their `probe_calls.csv` files hold the costs at
  the run dates' prices.

Everything else from earlier rounds was deleted on 2026-11-17 because it was superseded:

- **Reports:** the first probes, the 16k-cap cohort run, the v2, v3.1 and four-model v3.2 judge
  reports, and the run logs. The probe and judge rows behind them are still in the cache.
- **Helper scripts:** the calibration scripts (`review/calibrate_v3*.py`, `compare.py`), the
  review-page builder, and the scripts for the candidate-vs-candidate alignment and the GLM
  rerun. Their results are summarised above, and their judge calls are still in the cache.
  Re-running them would mean rewriting them.
- **Reasoning-budget test** (`reasoning_budget_test.json`): 20 direct Baseten calls that never
  went through the cache, so it can't be rebuilt without paying for the calls again. Its
  findings are the reasoning controls listed under "Models tried and why they were dropped".
