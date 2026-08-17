# Hierarchical taxonomy mapping — shared tools

LLM-driven classifier and judge for mapping entities (organizations,
projects, abstracts, …) onto an arbitrary hierarchical taxonomy.
Used across at least four projects (One Earth, OE Levers of Change,
Drawdown, hopper_dean) and designed to scale to a fifth taxonomy
with a small per-project driver.

This README covers:

1. [What's in this folder](#components)
2. [Project folder conventions](#project-folder-conventions)
3. [The end-to-end iteration workflow](#iteration-workflow) — including the [two levers](#two-levers-prompt-edits-vs-taxonomy-definition-edits)
4. [Lessons learned about prompt and taxonomy iteration](#lessons-learned)
5. [Adding a new taxonomy](#adding-a-new-taxonomy)
6. [Evaluating a taxonomy for redundancy & nesting](#evaluating-a-taxonomy-for-redundancy--nesting-overlap-analysis)

---

## Components

```
vdl_tools/shared_tools/taxonomy_mapping/
├── hierarchical_taxonomy_mapping.py     # classifier engine
├── hierarchical_taxonomy_judge.py       # judge engine
├── analyze_judge_results.py             # judge-results analyzer (Tier 1)
├── analyze_taxonomy_overlap.py          # term overlap / nesting analyzer
├── oe_hierarchical_taxonomy_mapping.py  # OE-specific classifier wiring
└── …                                    # other domain-agnostic helpers
```

**`hierarchical_taxonomy_mapping.py`** — domain-agnostic classifier
engine. Walks any N-level taxonomy top-down per entity, prompting an
LLM with candidate nodes at each level. Project drivers supply a
level spec, domain intro, and modes; the engine handles
classification, parallelism, and per-row / collapsed output writing.

**`hierarchical_taxonomy_judge.py`** — domain-agnostic LLM-as-judge
engine. Reads any per-row classification, scores each match
(good / weak / bad with a one-sentence reason), produces a top-level
alignment verdict per entity, writes standardized outputs (scored,
verdicts, summary, per-entity, markdown report). Configured by a
`JudgeConfig` dataclass that captures the variable parts (level
spec, taxonomy intro, entity column conventions, etc.).

**`analyze_judge_results.py`** — judge-output analyzer. Reads a
scored xlsx (+ companion verdicts + per-row files when present)
and produces a markdown report covering Pillar × Level errors, per
Sub-Pillar breakdowns, top bad/weak nodes by level, failure-reason
clustering, alignment verdicts, and NoMatch entity samples. The
**cheap inner-iteration tool** — runs in milliseconds against
already-judged data, no API spend.

**`analyze_taxonomy_overlap.py`** — term overlap / nesting analyzer,
for evaluating a taxonomy's DESIGN against how entities actually
mapped: which term pairs share most of their entities (same-parent =
redundancy candidates, cross-branch = co-practice). No LLM calls;
one-call driver; full guide in
[Evaluating a taxonomy for redundancy & nesting](#evaluating-a-taxonomy-for-redundancy--nesting-overlap-analysis).

## Per-project drivers (live in each project, not here)

Each project has a thin driver per role (classifier driver, judge
driver). All drivers import from the engines above and supply only
the project-specific config:

| Project | Classifier driver | Judge driver |
|---|---|---|
| One Earth (CFT orgs) | `oneearth/scripts/run_oe_hierarchical_mapping.py` | `oneearth/scripts/evaluate_mapping_quality.py` |
| OE Levers of Change | `oneearth/scripts/run_oe_loc_mapping.py` | `oneearth/scripts/evaluate_loc_mapping_quality.py` |
| Drawdown (CFT orgs) | `drawdown/taxonomy/run_drawdown_hierarchical_mapping.py` | `drawdown/taxonomy/evaluate_drawdown_mapping_quality.py` |
| hopper_dean (research grants) | `hopper_dean/query/project_taxonomy_mapping.py` | `hopper_dean/query/evaluate_oe_mapping_quality.py` |

A driver is typically 100–200 lines: a `JudgeConfig` (for the
judge) or level spec + system prompt parts (for the classifier),
default paths, and an argparse `main()` that dispatches to the
shared engine.

---

## Project folder conventions

Each consuming project is its own git repo. The shared convention
for what's tracked vs gitignored:

| Folder | Tracked? | Contains |
|---|---|---|
| `taxonomy_mapping/` (or `taxonomy/`, `query/` — the production code dir) | ✓ committed | Production drivers, libraries, judges. The folder name varies by project (`oneearth/taxonomy_mapping/`, `drawdown/taxonomy/`, `hopper_dean/query/`) but the role is the same. |
| `data/taxonomy/` (or `local/`) | ✓ committed | Small project-local taxonomy input xlsx. |
| `data/reports/` | ✓ committed | Human-readable reports (markdown + HTML): analysis reports, judge-quality reports, full-pool summary reports. These ARE the artifacts you share with collaborators. |
| `local_data/results/` | ✗ gitignored | Regenerable classifier and judge outputs (xlsx, csv). Large and reproducible from the prompt + taxonomy + sample seed, so we don't version them. |
| `scripts/` | ✗ gitignored | Ad-hoc experiment scripts. Convention: anything in `scripts/` is throwaway / personal. Promote to the production folder when it stabilizes. |
| `.env` / `config.ini` | ✗ gitignored | Secrets. |

The split rationale: `data/` is for **inputs and curated outputs**;
`local_data/` is for **regenerable bulk artifacts**. The semantic
distinction "data the project depends on" vs "data the project
produced" maps cleanly to "track in git" vs "don't".

Genuinely cross-project resources (e.g., the OE main taxonomy that
several projects use) live in a separate `shared-data/taxonomies/…`
location, outside any project's repo.

A standard `.gitignore` for any of these projects looks like:

```gitignore
# Folder conventions: data/ committed, local_data/ + scripts/ ignored
local_data/
scripts/

# Secrets
.env
config.ini
*.pem
*.key

# Python
__pycache__/
*.pyc
.pytest_cache/
.ipynb_checkpoints/
.venv/
*.egg-info/

# OS / editors
.DS_Store
.idea/
.vscode/
*.swp

# Office app lock files
~$*
```

---

## Iteration workflow

The work of refining a classifier or taxonomy follows a tight loop.
Each pass is intentionally cheap so you can iterate often.

### Two levers: prompt edits vs taxonomy definition edits

Quality improvement has two co-equal levers, and the right choice
depends on the shape of the error you're fixing:

- **Prompt edits** — the project driver's `DOMAIN_INTRO`, `MODES`,
  and `RULE_OVERRIDES` (passed through `build_system_prompt`).
  These apply uniformly to every per-level decision. Best for
  **cross-cutting matching algorithm logic**: evidence-only,
  qualifier lock, specificity, user-vs-provider, mode-of-operation
  framing. A prompt rule is the right tool when an error pattern
  repeats across many nodes (e.g., users mis-tagged as providers
  in every Solution).

- **Taxonomy definition edits** — the `Definition` /
  `expanded_definition` cells in the taxonomy xlsx, saved as a
  dated-successor file (see Lesson #1). The classifier reads the
  candidate definitions at every level decision, so per-node text
  hits the LLM at the exact per-decision point. Best for **per-node
  positive scope, negative scope, sibling disambiguation, and
  exclusionary rules**. A definition edit is the right tool when
  an error concentrates on one or two specific nodes.

**When the taxonomy is read-only** (e.g., a third-party reference
framework you don't own), prompt rules are the only lever. See
Lesson #2 for the anchor-effect trap that affects prompt-only
iteration with cheap classifier models.

**Where taxonomy edits have the highest leverage**: at the **Pillar
and Sub-Pillar levels**, where errors propagate through the engine's
gating walk. A wrong Pillar match means the entity never visits the
right Sub-Pillar's children, and a Sub-Pillar with stretched scope
mis-routes a whole branch of descendants. The ed_tracker
`Learning & School Models` case (a +22pp Sub-Pillar quality jump
from one paragraph rewrite + one Exclusionary Rule extension —
Lesson #9) is the canonical example. See `check_taxonomy_coherence.py`
for the audit that catches the failure modes safe definition edits
must avoid (coverage / completeness / cohesion).

```
┌──────────────────────────────────────────────────────────────┐
│                  Inner loop (≈ 5 minutes)                    │
│                                                              │
│  1. classify 200 sample  ──▶ 2. judge sample                 │
│         (1 min)                  (1.5 min)                   │
│                                       │                      │
│                                       ▼                      │
│  4. apply taxonomy / prompt    ◀──  3. analyze: find         │
│     edit (dated successor or          where errors are       │
│     prompt clause)                    concentrated           │
│                                       (instant — analyzer)   │
│                                                              │
│  ─── re-run from 1 with the candidate, compare ───           │
└──────────────────────────────────────────────────────────────┘
                       │
                       ▼  (when judge-only signal saturates)
┌──────────────────────────────────────────────────────────────┐
│                  Outer loop (≈ 30–60 minutes)                │
│                                                              │
│  5. classify full pool  ──▶ 6. find zero/low-match nodes,    │
│     (20–45 min)              NoMatch patterns at scale,      │
│                              targeted entity recheck         │
│                                                              │
│  ─── back to inner loop with new hypotheses ───              │
└──────────────────────────────────────────────────────────────┘
```

### Inner loop in detail

**Step 1 — Classify 200 sample**:
```bash
python <project>/run_<x>_hierarchical_mapping.py --limit 200 --seed 42 --workers 32
```
Writes `*_sample200_seed42.xlsx` (per-row) and `*_collapsed.xlsx`.
~1 min at `gpt-5.4-nano`.

**Step 2 — Judge the sample**:
```bash
python <project>/evaluate_<x>_mapping_quality.py
```
Writes `*_quality_scored.xlsx` (per-match scores), `*_quality_pillar_verdicts.xlsx`
(or `_sector_`), `*_quality_summary.xlsx`, `*_quality_per_entity.xlsx`, and
`*_quality_report.md`. ~1.5 min at `gpt-4.1`.

**Step 3 — Analyze where errors are concentrated**:
```bash
python vdl_tools/shared_tools/taxonomy_mapping/analyze_judge_results.py \
    --scored <project>/.../sample200_seed42_quality_scored.xlsx \
    --html
```
Writes `*_quality_analysis.{md,html}` with seven sections:

1. **Top × Level error breakdown** — for each Pillar/Sector × each
   level (Pillar / Sub-Pillar / Solution / Sub-Term), good/weak/bad
   counts and mean score. Surfaces which pillar's leaves are weakest.
2. **Per Sub-Pillar / SectorCluster breakdown** — sorted by mean
   score. Quickly identifies the worst-performing buckets.
3. **Top bad-firing nodes by level** — which specific Solutions /
   Sub-Terms / etc. were scored "bad" most often, plus up to 3
   sample reasons each. Where the next prompt-edit hypotheses
   typically come from.
4. **Top weak-firing nodes by level** — same shape for "weak".
   Often where the most-bounded gains live.
5. **Failure-reason clustering** — bucketing of the judge's bad-
   match reasons by canonical complaint shape (`no_mention`,
   `qualifier_mismatch`, `uses_not_provides`, etc.). Surfaces the
   dominant failure modes.
6. **Pillar / Sector alignment verdicts** — entity-level
   correctness rate plus samples of wrong / mixed / `no_X_expected`.
7. **NoMatch entities in the sample** — with description previews.

The analyzer takes a few hundred ms to run; it's cheap to re-run
after any prompt or taxonomy edit.

**Step 4 — Apply edit, re-run**: change the prompt (in a project's
classifier driver) or the taxonomy (save as a dated-successor xlsx
in shared-data/taxonomies — see [lessons learned](#lessons-learned)).
Use the analyzer's output to decide which lever to pull:

- Errors concentrated on **one or two specific nodes** (Section 3
  dominated by a single node, or Section 1 showing one Sub-Pillar's
  branch with much weaker mean than its siblings), and the taxonomy
  is editable → **tighten that node's definition**. Especially when
  the node is a Pillar or Sub-Pillar, since the error gates every
  descendant.
- Errors **systematic across many nodes following the same pattern**
  (e.g., user-vs-provider conflation showing up in many Solutions,
  same failure-reason cluster from Section 5 across multiple nodes)
  → **prompt rule override**. Add or sharpen a `RULE_OVERRIDES` entry
  in the project driver.
- A whole Sub-Pillar mis-firing with the same wrong-direction
  pattern → run `check_taxonomy_coherence.py` on that parent first
  (look at the coverage / completeness / cohesion gaps), then
  decide. A cohesion gap usually signals that the right fix is
  structural (split / rename / move a child) rather than a wording
  patch.

Re-run from step 1. The analyzer's deltas between iterations will
show whether the change moved the right metrics.

### Outer loop — when to step out

The inner loop is enough when the judge surfaces actionable
patterns. Step out to the full-pool classification when:

- The 200-sample is too small to reveal a category (e.g., 1 zero-
  match Solution doesn't reach the sample at all)
- You suspect a Sub-Pillar redistribution effect that needs whole-
  pool data to see
- You want to verify a gap closure landed at scale (e.g., the 21
  alt-protein orgs after Sustainable Rangelands definition revision)

Full-pool classification:
```bash
python <project>/run_<x>_hierarchical_mapping.py --limit all --workers 64
```
20–45 min at the cheap classifier model. Don't judge the full pool
— that's expensive and rarely yields insight beyond what the 200-
sample judge already shows.

### Comparing iterations

After classifying + judging a candidate version, compare it to the
prior baseline:

1. Run the analyzer on both scored files
2. Diff the two `_quality_analysis.md` reports manually (or use a
   future `compare_judge_results.py`)

Quick at-a-glance comparison from the headline numbers in each
report's Provenance section:

> Headline: **1554 matches scored** across **189 entities** →
> 64.6% good, 23.9% weak, 11.5% bad, mean = **0.766**

Movement on `good`, `weak`, and `bad` percentages is your top-line
score. Movement on the Pillar × Level table tells you *where* the
change took effect. Section 3 (top bad-firing nodes) lets you check
whether the failure patterns you targeted actually decreased.

### Promotion / revert

After a candidate proves better:
1. **Promote**: rename the candidate's per-row + collapsed files to
   the canonical paths (overwriting the prior canonical), update any
   downstream reports that cite numbers from the sample.
2. **Revert**: undo the prompt / taxonomy edit, delete the
   candidate's outputs.

Keep prior canonical files for at least one iteration in case you
need to compare; clean up older `_v<N>_*.xlsx` files periodically.

---

## Lessons learned

These are non-obvious patterns that have repeatedly shown up in
real iteration work. Reading them once will save you wrong turns.

### 1. Use dated-successor taxonomy files, not in-place edits

When editing taxonomy definitions (Pillar / Sub-Pillar / Solution
definitions in the xlsx), save as a new dated file:

```
OE Solutions Terms 20250502_expanded_VDL.xlsx   (prior canonical)
OE Solutions Terms 20260512_expanded_VDL.xlsx   (new, with revisions)
```

The classifier's `find_latest_taxonomy()` picks the latest by date.
This gives you trivial rollback and keeps a chronological record.

### 2. The "anchor effect" — categorical rule lists can backfire

If you've discovered N concrete failure patterns ("alt-protein orgs
miss Meat-free Proteins", "battery makers mis-routed to Renewable
Power", etc.), it's tempting to add a long categorical rule listing
all N with "REQUIRES X" framing. **This often backfires at cheap
classifier models** (`gpt-5.4-nano` in our use). Listing N specific
node names in the prompt makes the model more eager to fire those
nodes, not more disciplined about them.

In our experiments adding 5 ecosystem-named-node categorical rules
to the LoC and OE prompts increased the bad-match rate by ~1pp
across the board. Reverting to a tighter rule body with fewer
specific anchors recovered the precision.

What works better than long categorical rule lists:
- Edit the **taxonomy definitions** instead. A Sub-Pillar definition
  that enumerates its Solutions in plain language solves the gate-
  closure problem (the walk reaches the right Solutions) without
  loading the prompt with anchor terms.
- One or two narrow categorical rules with concrete examples
  ("Coastal Wetlands Solutions REQUIRE 'salt marsh', 'mangrove',
  'seagrass', or 'tidal' explicitly") work fine. The threshold
  appears to be around 3–4 categorical rules before the anchor
  effect dominates.

### 3. Trimming verbose definitions helps too

Once you've added Sub-Pillar enumeration text to fix a gate
problem, the resulting definitions can become bloated. Trim the
scaffolding ("On the demand-side and infrastructure side, this
includes…" → just list the items). Removing redundant climate-
mechanism re-statements (already in the Pillar definition) and
keeping the concrete Solution enumerations typically improves
quality by 1–2pp.

### 4. Generic-principle prompt rules don't help cheap models

We tried twice to add general-principle prompt rules (e.g.,
"Defining-noun lock: the description must support the node's
defining noun explicitly"). Both times the cheap model ignored or
mis-interpreted them, and quality regressed. **Cheap models need
concrete, narrow categorical instructions, not general principles.**

The exception: the **classifier engine's generic rule body** in
`hierarchical_taxonomy_mapping.py` (`PROMPT_RULE_KEYS`,
`_default_rule_bodies`) provides reasonable defaults that
*together* set the expected behavior. Project drivers should
override these rules with **domain-specific** language, not with
new general principles.

### 5. The user-vs-provider conflation is real and recurring

Entities that *use* a technology often get mis-classified as
*providers* of that technology. A solar developer gets tagged with
"Solar VPP Aggregation"; a battery manufacturer gets tagged with
"Electric Freight Trucks". The narrow categorical rule
("Activities that name a deliverable require the entity to be the
provider, not a user") works well when added to the cross-sector
rule. Watch for this pattern; it shows up in every taxonomy.

### 6. Pillar errors propagate to Sub-Pillar gates

When the classifier mis-pillars an entity, it never visits the
right Sub-Pillar's children. So a "1% bad Pillar rate" can show up
as a "10% NoMatch rate" at the leaves. Always check Section 1 of
the analyzer report (Pillar × Level) before drilling into specific
nodes — Pillar-level errors are the most consequential.

### 7. Walk depth is a useful proxy

If a prompt edit increases the share of entities reaching
SubTerm-level matches without changing the bad-match rate, it's a
net positive (more entities classified more specifically with no
quality loss). Section 1 of the analyzer report indirectly shows
this — but the most direct view is the per-row file's
`deepest_match` column distribution before vs after.

### 8. The judge is a quality validator, not an oracle

`gpt-4.1` as the judge model is stronger than the `gpt-5.4-nano`
classifier, so judge agreement is a useful signal. But:

- The judge is also stochastic at ~5% level even at `temperature=0`
- It systematically penalizes loose-but-correct matches at the
  Solution / Sub-Term level (the "weak" tier captures this)
- It rarely catches systematic gate-closure bugs (Sub-Pillars that
  never fire) — only full-pool analysis surfaces those

Use the judge for iteration *between* methods on the same sample;
use full-pool inspection for taxonomy-coverage gaps.

### 9. Prompt edits vs taxonomy definition edits — when to use which

The decision principle that emerges from a year of iteration work:

- **Per-node scope** (what *this node* covers, what it excludes,
  how it differs from its siblings) → taxonomy definition edit.
  The classifier reads candidate definitions at every per-level
  decision, so per-node text is the exact context the LLM uses
  when deciding "does this entity belong here or at the sibling
  node?". Prompt rules are awkward at expressing per-node scope.
- **Cross-cutting matching algorithm logic** (how every match
  decision should be made, regardless of which node is involved) →
  prompt rule override. Examples: evidence-only ("the name is not
  evidence"), qualifier lock ("Offshore in the name means the
  description must say offshore"), specificity ("don't go deeper
  than the description supports"), user-vs-provider.

The canonical case is the ed_tracker `Learning & School Models`
Sub-Pillar: a +22pp Sub-Pillar quality jump (75.0% → 97.2% good in
a 200-entity judge run) came entirely from two surgical taxonomy
edits — rewriting the L&SM first paragraph to honestly span both
its scopes, and adding an accreditee exclusion to Governance &
Accountability. Zero prompt changes. A prompt rule could not have
expressed "L&SM covers school models AND OST, not just school
models" cleanly; the taxonomy definition can.

**Use the cohesion guard.** Prior LLM-driven revision passes can
satisfy *completeness* (every child signposted by the parent) by
stretching a parent's scope across heterogeneous children — gluing
in a second scope paragraph for a child that doesn't fit the
parent's primary frame. This breaks *cohesion* (the parent + its
children no longer describe a single coherent category) and
silently degrades downstream classification. The third check in
`check_taxonomy_coherence.py` (Cohesion) catches this pattern, and
revision passes that respect a `cohesion_blocker` field refuse to
auto-glue rather than producing a confused parent.

**Front-load the load-bearing scope content.** The judge truncates
each per-match taxonomy definition at `DEFINITION_TRUNCATE_CHARS`
(currently 1500 chars; see
`hierarchical_taxonomy_judge.py`) when assembling its prompt. The
cap was originally 600, raised in May 2026 because modern context
windows make the token-budget rationale obsolete. The attention-
discipline rationale still applies: LLMs attend disproportionately
to early tokens in any document, so even with a generous cap,
load-bearing content (positive scope, exclusionary rules, sibling-
disambiguation) should live in the **first ~1000 characters** of
the `expanded_definition`. Don't bury an exclusionary rule or a
dual-scope statement deep in the body — it will technically fit
within the cap but the judge's effective attention may not reach
it. (See Lessons #3 and the L&SM / Traditional Schools cases
above for what happens when load-bearing content sits past the
effective-attention boundary.)

**Read-only taxonomies.** When you cannot edit the taxonomy (third-
party reference framework, frozen domain-authority definitions),
prompt rules are the only lever. Lesson #2 (anchor effect) is the
relevant guidance there: keep the rule body tight, prefer one or
two narrow categorical rules with concrete examples over long lists
of node-name anchors.

---

## Adding a new taxonomy

To stand up a fifth taxonomy mapping (say, an adaptation
framework):

### 1. Write a classifier driver

Pattern: copy an existing one (e.g.,
`oneearth/scripts/run_oe_loc_mapping.py`) and adapt:

```python
import vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_mapping as _htm

LEVELS = [
    {"idx": 0, "name": "Pillar", "sheet": "Pillars", "key_col": "Pillar",
     "output_col": "Pillar", "parent_filters": []},
    {"idx": 1, "name": "Solution", ...},
    ...
]

DOMAIN_INTRO = "You are classifying ... [domain framing]"
MODES = [{"name": "direct", "definition": "..."}, ...]  # or omit for no-mode walk
RULE_OVERRIDES = {"cross_sector": "...", "specificity": "..."}  # override specific rules

SYSTEM_PROMPT = _htm.build_system_prompt(
    levels=LEVELS, domain_intro=DOMAIN_INTRO,
    modes=MODES, rules=RULE_OVERRIDES,
)

def load_taxonomy(path):
    return _htm.load_taxonomy(path, LEVELS)

def map_my_taxonomy(entities, ...):
    return _htm.classify_entities(...)
```

### 2. Write a judge driver

Pattern: copy `evaluate_loc_mapping_quality.py` or
`evaluate_drawdown_mapping_quality.py`. Build a `JudgeConfig`:

```python
from vdl_tools.shared_tools.taxonomy_mapping.hierarchical_taxonomy_judge import (
    JudgeConfig, run_judge_evaluation,
)

CONFIG = JudgeConfig(
    levels=tuple(LEVELS),
    taxonomy_label="My Taxonomy",
    taxonomy_intro="...",
    new_level_cols={lvl["name"]: lvl["output_col"] for lvl in LEVELS},
    load_taxonomy_tables=lambda: load_taxonomy(MY_TAXONOMY_FILE),
    taxonomy_label_path_fn=lambda: MY_TAXONOMY_FILE,
    build_openai_client=my_build_openai_client,
    # Override if your top-level isn't "Pillar":
    # verdict_top_label="Sector",  # auto-derives sector_alignment / chosen_sectors
    # Override if your entity isn't an organization:
    # entity_id_col="abstract_id", entity_name_col="title",
    # entity_description_col="abstract", entity_label_in_prompt="Project title",
    # entity_label_plural="projects",
    # scoring_rubric=MY_RESEARCH_RUBRIC,  # full replacement
    # strictness=MY_RESEARCH_STRICTNESS,
)

if __name__ == "__main__":
    # standard argparse → run_judge_evaluation(CONFIG, ...)
    ...
```

### 3. Run the iteration loop

The `analyze_judge_results.py` tool works on any project's output
without configuration — just point it at the judge's scored xlsx.

---

## Evaluating a taxonomy for redundancy & nesting (overlap analysis)

`analyze_taxonomy_overlap.py` answers a design question the judge cannot:
**do the taxonomy's terms actually mean different things in practice?**
It looks at which entities got mapped to which terms and finds pairs of
terms whose entity sets largely coincide. Two readings:

- **Same-parent overlap = redundancy candidates.** If 78% of the
  organizations in *Conservation Easements* are also in *Land Trusts*
  (siblings under one parent), the two terms may be one concept wearing
  two names — merge them or sharpen the definitions.
- **Cross-branch overlap = co-practice.** If two terms in different
  branches share most of their entities, that's how organizations bundle
  work in the real world — useful signal, not a taxonomy flaw.

No LLM calls, no API spend, runs in seconds (~40s for the largest level
tried: 1,232 terms / 21,558 pairs). Importable without OpenAI/DB config.

### What you need

1. **A per-row mapping output** from the classifier engine (the
   `..._full_....xlsx` file with one row per entity-path — NOT the
   `_collapsed` file).
2. **The taxonomy workbook** the walk used (for term definitions in
   tooltips and the summary).
3. **The same level spec** the walk used (the `MY_LEVELS` list of dicts
   your run driver already defines). Tip: paste a copy into the analysis
   driver rather than importing the mapping library — that import pulls
   the OpenAI config and fails on machines without it.

### How to run it for any taxonomy

Copy an existing driver (`oneearth/taxonomy_mapping/analyze_oe_cooccurrence.py`
or `drawdown/taxonomy_mapping/analyze_drawdown_cooccurrence.py`), change
the paths, and hit Run in PyCharm. The whole thing is one call:

```python
import vdl_tools.shared_tools.taxonomy_mapping.analyze_taxonomy_overlap as ato

ato.run_overlap_analysis(
    MAPPING_FILE,      # per-row classifier output (xlsx)
    TAXONOMY_FILE,     # the taxonomy workbook (definitions)
    MY_LEVELS,         # the walk's level spec, verbatim
    REPORT_DIR,        # where charts + summary land (data/reports/...)
    xlsx_path=PAIRS_XLSX,          # full pair tables (local_data/...)
    file_prefix="mytax_",          # so several taxonomies can share a dir
)
```

Optional knobs (all have sensible defaults):

| knob | what it does | default |
|---|---|---|
| `level_indices=[1, 2]` | which levels to analyze | every level with ≥2 terms |
| `color_level={2: 1, 3: 1}` | which ANCESTOR level colors the chart dots (pick one with ≤ ~11 values) | immediate parent |
| `summary_level_indices=[0, 1, 2]` | which levels get detail sections in the summary — leave deep levels (1,000+ terms) out; they stay in the counts table as "(counts only)" | all analyzed |
| `nested_min=0.60` | containment threshold for "nested" | 0.60 |
| `taxonomy_tables=...` | pre-loaded definition tables, for taxonomies needing a custom loader (e.g. OE's synthesized Sub-Terms) | loaded from `TAXONOMY_FILE` |
| `group_colors={...}` | fixed group→hex palette | automatic assignment |
| `per_row=df` + `subset_label="..."` | analyze one entity cohort (see below) | whole mapping file |

### Analyzing one cohort at a time

To compare how a taxonomy behaves for different kinds of entity (for-profit
vs nonprofit, one region at a time), call `run_overlap_analysis` once per
cohort with a pre-filtered `per_row`, its own `file_prefix`, and a
`subset_label`. The label leads every chart title and the summary heading,
so two cohorts' charts are never confusable once they leave the folder.

```python
for label, prefix, keep in [("For-profit only", "fp_", df.kind == "For Profit"),
                            ("Nonprofit only",  "np_", df.kind == "Non Profit")]:
    ato.run_overlap_analysis(MAPPING_FILE, TAXONOMY_FILE, MY_LEVELS, REPORT_DIR,
                             per_row=df[keep], file_prefix=prefix,
                             subset_label=label)
```

Everything is recomputed **within** the cohort — term sizes, prevalence,
lift, and the structural ceiling all use the cohort as the denominator. That
is what makes each cohort's numbers internally valid, and it also means they
are not comparable to a full-pool run: the same pair shows a different lift
in each. Check cohort sizes before reading anything into the result — a
small cohort leaves most terms too thin (n≤3) to support a containment
claim, which is what the summary's "terms with ≤3 entities" column is for.

### What you get

Per analyzed level, in `REPORT_DIR`:

- `{prefix}{level}_nesting_scatter.html/.png` — containment vs Jaccard for
  every co-occurring pair; the shaded box is the nested region; dot area =
  entity count of the smaller term. Hover any dot for the full stats and
  both definitions; click a legend entry to highlight.
- `{prefix}{level}_nesting_dumbbell.html/.png` — one row per nested pair,
  strongest first. Filled dot = smaller term, hollow = larger, grey tick =
  chance.
- `{prefix}taxonomy_overlap_summary.md/.html` — **read this first**: per
  level, the top redundancy candidates and co-practice pairs, plus how
  many terms are thin (≤3 entities) or share no entity with any other term.
- One xlsx of full pair tables (`xlsx_path`), provenance sheet first.

Every artifact names the exact taxonomy + mapping files it read — the
mapping output doesn't record which taxonomy produced it, so confirm the
mapping run postdates the taxonomy file.

### How to read the numbers

| metric | meaning | use it for |
|---|---|---|
| **containment** | share of the SMALLER term's entities also carrying the larger | nesting — "A is basically inside B" |
| **jaccard** | shared / union | near-equal twins; under-ranks size-mismatched pairs |
| **lift** | containment ÷ larger term's base rate | "how much more than chance"; the only metric safe to compare across branches |
| **ceiling** | max containment the parent-gated walk permits | context for cross-parent pairs — they CAN'T reach 100% by construction |

Caveats the tool bakes into its outputs, worth knowing anyway: term
identity is the full ancestor path (same-named terms under different
parents are never merged — labels get a "(parent)" suffix when names
collide); rows are paths, so entity–term membership is deduplicated before
counting; big levels truncate charts to the top pairs by shared entities
(always announced, never silent); a term matched to 1–3 entities can hit
100% containment on no real evidence — the summary counts these per level;
and **top-level overlap depends on the prompt** (a near-exclusive pillar
prompt makes low level-0 overlap a design artifact, not a finding).

Existing drivers to crib from: LoC
(`oneearth/taxonomy_mapping/analyze_loc_cooccurrence.py`, includes a fixed
hand-solved palette and legacy xlsx column names), One Earth
(`.../analyze_oe_cooccurrence.py`, custom taxonomy loader), Drawdown
(`drawdown/taxonomy_mapping/analyze_drawdown_cooccurrence.py`, the
minimal template).

---

## Quick command reference

```bash
# Classify a 200-sample (any project)
python <project>/run_<x>_hierarchical_mapping.py --limit 200 --seed 42

# Judge the sample
python <project>/evaluate_<x>_mapping_quality.py

# Analyze the judge results (cheap iteration tool)
python vdl_tools/shared_tools/taxonomy_mapping/analyze_judge_results.py \
    --scored <project>/.../sample200_seed42_quality_scored.xlsx --html

# Full-pool classification (only when needed)
python <project>/run_<x>_hierarchical_mapping.py --limit all --workers 64

# Spot-check entities (ad hoc, no tool yet — use pandas one-liners)
```

For project-specific notes (taxonomy paths, output conventions),
see each project's README or the driver docstrings.
