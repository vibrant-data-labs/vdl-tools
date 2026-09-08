# Replacing gpt-4.1-mini for website summarization

**Status:** results final at n=181 orgs. Integration not built.
**Author:** Lara · **Branch:** `open-model-summarization-eval` ·
**Code:** `vdl_tools/shared_tools/web_summarization/model_benchmark.py`

---

## TL;DR

We benchmarked 11 models against `gpt-4.1-mini` on 181 real orgs, judged blind
against the actual scraped source text.

**1. No cheap model is measurably more accurate than what we run today.** Several
are *equivalent* at 2–3× lower cost with open weights. That's a cost and
vendor-independence win, not a quality one.

**2. Exactly one model is more faithful: `tencent/hy3` with reasoning enabled** —
**0 fabrications in 181 orgs** versus the baseline's 13. It costs **6.3× the
latency** (~8 days for a full pass) and ~60% of its spend goes to reasoning
tokens we discard. Every other model tested, reasoning off, sits at 12–19
fabrications.

**3. Our current model fabricates in roughly 7% of summaries** — inventing a US
state in a project list, a company suffix, a merged founding date. Fluent,
specific, and they reach the dashboard as fact.

**4. Recommendation:** switch for cost and open weights, not for quality.
`tencent/hy3` pinned to `deepinfra` is cheapest ($0.96 vs $2.64 per 1k orgs) and
the judge prefers it; `deepseek-v3.2` is fastest and MIT-licensed. Neither is an
accuracy upgrade. Pay the latency for `hy3` with reasoning only if a fabricated
fact on the dashboard is expensive.

**Two gotchas that cost us days:** the same model id behaves differently
depending on which provider serves it (see [Model hosting](#model-hosting-the-same-model-id-is-not-one-thing)),
and **structured output silently fails on open models** through Vercel — which
blocks moving taxonomy/classification the same way.

---

## Goal

Replace `gpt-4.1-mini` in the summarization step of `process_enrich` with
something cheaper and preferably open-weight, so we can self-host, fine-tune and
avoid lock-in. Constraints in priority order: **faithfulness** (summaries feed a
public dashboard and the taxonomy mapper, so invented facts propagate), **cost**
(~110,000 orgs), **latency** (a full pass shouldn't take a week).

---

## Method

**Platform: Vercel AI Gateway.** Chosen over Baseten because it supports the
Responses API, which `openai_api_utils.get_completion` already uses — so pointing
summarization at an open model is a `base_url` + key change, not a rewrite.

**The benchmark is standalone** and touches no production code. Per org it runs
the real production prompt over the real packed source text (`make_group_text`),
then grades two ways:

1. **Deterministic checks** (free): empty output, `----` delimiter leakage,
   refusals, repetition, length. These give an absolute *rate*, which a
   head-to-head can't.
2. **Blind pairwise judging** by `gpt-5.6-luna` at high reasoning effort, called
   against OpenAI directly. A/B order randomized, model identities stripped. The
   judge **sees the source text**, so faithfulness is checked against what the
   site actually says. It returns a verdict (`faithful` /
   `unsupported_inference` / `fabricated`), an impact judgment, facet coverage,
   and the specific unsupported claims quoted. Analysis fields precede the
   verdict in the schema, so it can't decide first and rationalize after.

**Severity** is computed in code from those labels — so weights can be re-tuned
over saved results without re-judging:

| | impact: none | impact: material |
|---|---|---|
| `faithful` | 0.0 | 0.0 |
| `unsupported_inference` | 0.5 | 1.0 |
| `fabricated` | **1.0** | **3.0** |

Calibrated with 11 worked examples verified against scraped source, e.g. adding
**Louisiana** to a list of three real states → `fabricated`/`material` (3.0);
saying "**over** 2,850 partners" where the source says "2,850" → drift, not
invention (0.5); calling an org a **nonprofit** when the source describes
grantmaking and a member network → faithful (0.0).

**Sampling:** stratified across 7 `(project, data_source)` strata discovered from
the project schemas, plus an **unattributed** stratum. That last one matters —
project tables only hold orgs that *passed* relevance, but summarization also
runs *before* relevance for orgs with thin descriptions, and those are the
hardest cases.

**Judge validation:** 8 of its claims were checked by grepping the scraped
source. 6 held, 1 was ambiguous, 1 was wrong — so treat fabrication counts as
**±1–2**.

---

## Results (n=181 orgs)

Latency is a **multiple of the baseline in the same run** — runs used 15
concurrent workers, which roughly doubles per-call times, so only within-run
ratios are meaningful.

| Model | fabricated | latency× | $/1k | reasons? | W-L-T | win% | 95% CI |
|---|---|---|---|---|---|---|---|
| `tencent/hy3` *(novita)* | **0/181** | 6.31× | $1.99 | **yes** | 117-52-12 | 69% | **[0.62, 0.76]** |
| `tencent/hy3` *(deepinfra)* | 12/181 | 1.90× | **$0.96** | no | 108-64-9 | 63% | **[0.55, 0.70]** |
| `deepseek/deepseek-v3.2` | 13–14/181 | **0.81×** | $1.68 | no | 95-71-15 | 52–57% | [0.45, 0.65] |
| `deepseek/deepseek-v4-flash` | 16/181 | 1.69× | $1.34 | no* | 92-78-11 | 54% | [0.47, 0.61] |
| `openai/gpt-4.1-mini` *(current)* | 8–13/181 | 1.00× | $2.64 | no | — | — | — |
| `inclusionai/ling-3.0-flash` | 19/181 | 0.44× | $0.47 | no | 97-79-5 | 55% | [0.48, 0.62] |
| `inclusionai/ling-3.0-tiny-free` | 48/181 | 0.52× | free | no | 60-116-5 | 34% | **[0.27, 0.41]** |

\* routing-dependent: measured 0 reasoning tokens across this run despite
reasoning in isolated tests.

Earlier rounds eliminated `zai/glm-4.7-flash` (27% fabrication),
`openai/gpt-oss-120b` (22%), `alibaba/qwen3-next-80b-a3b-instruct` (20%) and
`mistral/ministral-8b` (research licence only).

**The pattern:** every non-reasoning model clusters at 12–19 fabrications. The
one reasoning deployment sits at 0.

### How to read this

**Win rate** is the proportion of *decisive* paired comparisons won; ties are
dropped. H₀ is that it equals 0.5. The test is a two-sided exact binomial (a sign
test) — appropriate for paired preference data. Bonferroni for 7 comparisons puts
the threshold at **p < 0.0071**.

**Fabrications are compared with McNemar's exact test** on paired outcomes, not
by comparing totals. The baseline alone scored 13, 8 and 11 across three runs, so
eyeballing totals is worthless.

| Model | judge preference | accuracy vs baseline (McNemar) |
|---|---|---|
| `hy3` *(novita)* | 69%, p<0.0001 ✓ | **0 vs 13, p=0.0002** ✓ |
| `hy3` *(deepinfra)* | 63%, p=0.0010 ✓ | no difference, 12 vs 11, p=1.00 |
| `deepseek-v3.2` | 57%, 52% (p=0.07, 0.65) | fewer total errors, p=0.0073 — *just misses* |
| `deepseek-v4-flash` | 54% (p=0.32) | no difference, p=0.69 |
| `ling-3.0-flash` | 55% (p=0.20) | possibly *more*, 19 vs 8, p=0.035 |
| `ling-3.0-tiny-free` | 34%, p<0.0001 ✗ | **48 vs 8, p<0.0001** ✗ |

`deepseek-v3.2` trending toward fewer errors across two independent runs is worth
noting — a consistent direction is weak evidence, not none. It doesn't clear the
bar, and its own win rate moved 5pp between runs.

**The two metrics can disagree, and here they do.** `hy3`/deepinfra is preferred
while fabricating identically; `deepseek-v3.2` errs less while not being
preferred. Win rate has more power but is **not verifiable against ground truth**;
error counts are partly verifiable but say nothing about whether the summary is
*useful*. Report both — where they disagree, that's the finding.

---

## Recommendations

**Only one model is demonstrably more accurate: `tencent/hy3` with reasoning.**
Zero fabrications in 181. Cost: 6.31× latency (~8 days per full pass at 5
workers) and ~60% of spend on discarded reasoning tokens. Worth it only if a
fabricated fact reaching the dashboard is expensive.

**Otherwise it's a cost-and-latency choice among equivalents** — all
non-reasoning, all performing like the current model:

| | $/1k | latency× | licence | note |
|---|---|---|---|---|
| `tencent/hy3` (deepinfra) | **$0.96** | 1.90× | open | cheapest; judge prefers it, no accuracy gain |
| `deepseek/deepseek-v3.2` | $1.68 | **0.81×** | MIT | fastest; trends toward fewer errors |
| `deepseek/deepseek-v4-flash` | $1.34 | 1.69× | MIT | routing-dependent behaviour |
| `openai/gpt-4.1-mini` *(today)* | $2.64 | 1.00× | closed | — |

Either of the first two is a defensible switch. **Neither is an accuracy
upgrade.**

**Not recommended:** `inclusionai/ling-3.0-flash`, despite being cheapest ($0.47)
and fastest (0.44×) — its fabrication signal points the wrong way (19 vs 8,
p=0.035, doesn't survive correction). Worth a dedicated re-test if throughput
ever becomes the binding constraint.

---

## Model hosting: the same model id is not one thing

The most transferable finding, and the one that cost three rounds of wrong
conclusions. `reasoning.effort` appeared to be ignored on several models, so we
concluded reasoning couldn't be disabled through Vercel. Wrong — **the variable
was provider routing, not the parameter.**

`tencent/hy3`, same request:

| provider | reasoning tokens | latency |
|---|---|---|
| novita *(what auto-routing picks)* | ~2,500 | 52.7s |
| deepinfra | **0** | **9.4s** |

`GET /v1/models/{model}/endpoints` confirms it: deepinfra declares only
`max_tokens, stop, temperature` for hy3 — no reasoning parameter — while novita
declares full support.

**Practical rules**

1. **Check the provider list before treating behaviour as a model property.** Of
   the models tested only hy3 had a split; `deepseek-v3.2` (4 providers, none
   reason), `deepseek-v4-flash` (9, all declare it) and `ling-3.0-flash` (1) are
   uniform.
2. **Pin with `only`, not `order`.** `order` leaves a fallback available, so a
   transient error silently reroutes to a different deployment. `only` returns a
   **400** if the provider can't serve the model — a visible failure beats
   contaminated data.
3. **From Python it must go through `extra_body`.** The `provider_options=` form
   in Vercel's own Python example raises `TypeError` in the `openai` package:

   ```python
   extra_body={"providerOptions": {"gateway": {"only": ["deepinfra"]}}}
   ```

4. **Record the resolved provider** — `provider_metadata.gateway.routing.resolvedProvider`.
5. **Model slugs are not stable.** `ling-3.0-flash-free` was retired mid-project
   and 404'd every call in one run.

---

## Other findings

**Our current model fabricates in ~7% of summaries.** `gpt-4.1-mini` invented
"Ohmium **Technologies**" (the company is just "Ohmium"), placed a project in
**Louisiana** where the source names three other states, and merged two
organizations' founding dates. The baseline was never a safe reference.

**Cheap and fast doesn't mean good.** `zai/glm-4.7-flash` is the cheapest input
price tested, MIT, fast — and fabricates in 27% of summaries, losing 34 of 41
head-to-heads at p<0.0001. The floor is lower than expected too:
`ling-3.0-tiny-free` (7.9B MoE, **1.3B active**) fabricated in 48 of 181.

**Reasoning helps — but this is n=1.** `hy3`/novita is the only reasoning
deployment in the final dataset. The effect is large and cleanly isolated (same
weights, same orgs, same prompt: 0 vs 12 fabrications) but rests on a single
model. Whether reasoning helps summarization *in general* is not established here.

**The workload is input-bound, so output price barely matters.** ~4,000 input
tokens against ~200 output. `deepseek-v3.2` costs *more* than `v4-flash` despite
generating fewer tokens, because its input rate is 40% higher. Provider
prompt-caching discounts are irrelevant (each org's text is unique), and tiered
long-context pricing bites — headline per-token prices understate real cost.

**Structured output silently fails on open models through Vercel.** The gateway
forwards a JSON schema; upstream providers ignore it. `gpt-oss-120b` returned
`{"answer": ...}` instead of the requested key **every time** — well-formed JSON
with invented keys. Summarization is unaffected (plain text), but this **blocks
`InstructorPRC`**, so taxonomy mapping and classification can't move to open
models without a JSON-in-prompt fallback. OpenAI models through Vercel are fine.

**Use paired tests for anything measured per-org.** Both models saw the same
source text; comparing aggregates throws that pairing away and most of the
statistical power with it. Eyeballing totals would have called `ling-3.0-flash`
(19 fabrications) equivalent to the baseline (8) — the paired test says otherwise.

**Facet coverage doesn't discriminate.** The taxonomy mapper may only assign a
category when the summary explicitly states supporting language, so we expected
summaries to be the bottleneck. They aren't — all models cover
activities/beneficiaries/sector/geography/mechanism at 4.6–4.9 of 5. Weak
taxonomy recall has a cause downstream, not here.

**Some sources contradict themselves.** `overbrookcenter.org` gives three
founding narratives; `edmentum.com` states two different efficacy figures one
sentence apart. Every model trips on these and no model choice fixes it — roughly
1 org in 41.

---

## Next steps

**Decide first:** switch for cost and open weights, or pay the latency for zero
fabrications. The benchmark can't make that call — it depends on what a
fabricated fact on the dashboard costs us.

**Then, three code changes:**

**1. Add `MODEL_DATA` entries** — `openai_constants.py:1`. Context window and
per-token prices for whichever model is chosen. Not optional:
`make_page_text.py:59,63` does a bare lookup, so it `KeyError`s before any API
call without one.

```python
"tencent/hy3": {
    "model_name": "tencent/hy3",
    "max_context_window": 262_144,
    "max_output_tokens": 32_768,
    "input_cost_per_token": 0.14 / 1_000_000,
    "output_cost_per_token": 0.58 / 1_000_000,
},
```

**2. Per-model client routing** — `openai_api_utils.py:20`. `CLIENT` is built at
import time against OpenAI with no `base_url`, and `get_completion` uses it
unconditionally (line 177), so every call goes to `api.openai.com` whatever the
model string says.

Suggested shape: keep the module-level `CLIENT` as the default and resolve a
client per model from `MODEL_DATA`, so no call site changes:

```python
# openai_api_utils.py
@lru_cache(maxsize=None)          # cache: one connection pool per model, not per call
def client_for(model: str) -> OpenAI:
    cfg = MODEL_DATA.get(model, {})
    if not cfg.get("base_url"):
        return CLIENT                              # unchanged path for OpenAI models
    return OpenAI(base_url=cfg["base_url"],
                  api_key=get_configuration()[cfg["api_key_ref"]]["api_key"],
                  max_retries=4)

# in get_completion, replacing CLIENT.responses.parse(...)
response = client_for(model).responses.parse(**response_kwargs)
```

`MODEL_DATA` then carries `base_url`, `api_key_ref` and (for pinned models)
`provider` alongside the existing fields — one place to look, and existing
OpenAI models keep the current behaviour exactly.

**3. If pinning a provider, add `extra_body` to the cache key** —
`database_models/prompt.py:26`. The allowlist decides which kwargs form
`request_hash`, and `extra_body` isn't in it, so a novita-generated summary would
be served for a deepinfra request. We've shown those differ substantially, so
without this the pin is decorative:

```python
KWARG_KEYS_THAT_AFFECT_OUTPUT = frozenset({
    "reasoning", "tools", "tool_choice", "temperature",
    "top_p", "max_output_tokens", "seed", "service_tier",
    "extra_body",        # provider pinning changes the deployment, so the output
})
```

Existing rows are unaffected — they were written without `extra_body`, so their
hashes don't change.

**Still open:** taxonomy/classification can't follow onto open models until
structured output has a JSON-in-prompt fallback.

**Two unrelated bugs found in passing:**

- `is_reasoning_model()` matches the bare prefix `gpt-5`, so every gateway slug
  (`openai/gpt-5-nano`) fails the check and takes the wrong call shape.
- `MAX_SUMMARY_LENGTH` (500 tokens) is defined but referenced nowhere, and the
  prompt carries no length instruction — so summary length is currently
  uncontrolled in both directions. Across 2,114 benchmark summaries the median is
  ~200 tokens and only **0.57% exceed 500**, but the tail is real: one
  `hy3-deepinfra` summary ran **3,778 tokens**.

  **If we add a limit, it belongs in the prompt, not in `max_output_tokens`.**
  `max_output_tokens` is a hard stop, not a budget the model writes toward, and
  it fails two ways:

  | cap exceeded by | result |
  |---|---|
  | a non-reasoning model | **truncated mid-sentence** — measured: `"...It is governed by a volunteer"` |
  | a reasoning model | **no visible output at all** — the cap is spent on thinking |

  Both set `status: "incomplete"` with `incomplete_details.reason`, but the cache
  reads `output_text` and never checks status
  (`prompt_response_cache_sql.py:566`), so a fragment or an empty string is
  stored as though it were a finished summary.

  A prompt instruction avoids this — the model plans its ending. Measured on
  `deepseek-v3.2` with `"Please keep the summary under 250 words."`: 0/6 over the
  limit, 6/6 ended on terminal punctuation. **Caveat: it anchors length upward.**
  Median went 141w → 207w — the model reads the limit as a target. Set any
  ceiling well above the natural median (~165 words) so it clips outliers without
  dragging the middle, and re-check cost, since output tokens are the expensive
  side.

  A `status` check is worth adding regardless of the length question: **any**
  truncation or provider hiccup returning `incomplete` currently lands in the
  cache as a blank or partial summary. The benchmark catches these via its
  `is_empty` check; production has no equivalent.

**Caveats on the results above:** judge false-positive rate ~12% (hand-measured),
so fabrication counts are ±1–2. Win rate is one judge's preference and cannot be
verified against ground truth, unlike fabrication claims. The rubric's `impact`
dimension is weakly calibrated — read severity alongside raw counts, never alone.
Latency was measured at 15 workers; only within-run ratios are meaningful.
`ministral-8b` is Mistral Research License — commercial use needs a paid licence,
unlike the MIT/Apache options.
