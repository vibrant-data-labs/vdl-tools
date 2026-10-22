# Firecrawl scraping adapter: implementation plan

Status: draft v2 for team discussion, 2026-10-22. Decision pending per the VDL rule that new
tools get discussed before adoption.

v2 incorporates the data-engineer and data-scientist reviews of v1 (same day). The main
changes: vendor-error retry semantics, a working per-org token cap, text validation before
any paid run, lineage below the row, a per-org pipeline instead of a backend flag, unrelated
issues split into their own PRs, and corrected cost and size figures.

Evidence lives in PR #237 (branch `zeclaude/kind-chaum-8f9459`):
`vdl_tools/scrape_enrich/scraper/vendor_pilot/README.md` holds the method, the result tables
and the "Production adapter notes" that this plan turns into work.

## 1. Why

The in-house scraper (`vdl_tools/scrape_enrich/scraper/`) fetches with httpx, falls back to a
local Playwright Chromium for JS shells and bot walls, and extracts text with
`unstructured.partition_html`. It runs on laptops and fails in ways that depend on the machine:
Chromium installs and memory, the `unstructured[all-docs]` install, home-IP rate limits, and
host security checks.

Two benchmarks in October 2026 measured a vendor against it on the same URLs the same day.
"Usable" means the home page answered 2xx and the text passes `classify_text_quality`.

| Benchmark | Firecrawl | In-house | Notes |
|---|---|---|---|
| Phase 1: 1,000 cached home URLs, stratified toward hard cases | 74% | 61 to 63% | Olostep 73% but reports 200 for 404s; Spider 67%, cannot render JS shells |
| Phase 2: 2,000 CFT orgs end to end, home plus chosen subpages | 86.0% | 78.0% | 4.67 credits per org, 5.5 pages/s on the Standard plan, billing matched per-page credits exactly |

Where the phase 2 gap comes from (data-scientist review of the artifacts):

- On the 1,707 orgs that were already in the cache: 85.6% versus 82.1%.
- On the 293 orgs never cached before: 88.4% versus 53.9%. These supply 104 of the 173
  rescues, and 69 of the 74 in-house walls there are "403 forbidden" or the host security
  check.
- The README's "cache historical 82.4%" is text of unknown age and matches today's in-house
  rate on the same orgs, so it is not an independent baseline.

Firecrawl rescues 173 orgs end to end and loses 13 (about 11 standard errors on the
discordant pairs). It is the only vendor tested that both handles those pages and reports
the target's real status code and final URL. Note that all 93 rescued bot walls came back on
the basic proxy; nothing in the pilot measures the stealth tier.

## 2. Scope

In scope:

- A Firecrawl-backed fetch-and-extract pipeline alongside the existing in-house one, writing
  to new cache tables with the columns the old ones lack.
- The text pipeline that sits on top: cleaner, gate order, per-org token cap, deterministic
  page selection.
- Validation of the LLM outputs built on the new text before the first paid refresh.
- Migrating every reader, then retiring the in-house path and its dependencies.

Out of scope, each with its own PR or issue so this one stays reviewable:

- #226 (unify the domain key in portfolio_comparison: 21 call sites, and `resolve_redirect`
  is used by matching code on domains that are never scraped).
- #232 (blank and malformed website fields in data prep). The client's `invalid_url` guard
  in this plan is the backstop, not the fix.
- Retiring `website_quality`'s separate redirect and liveness pass. It needs every project on
  the new tables and it replaces a deliberate fresh check; Phase 7 at the earliest.
- The in-house-first hybrid. Both reviews reject it: it splits one org's pages across two
  extractors (1,268 orgs in-house, 452 Firecrawl, 280 mixed in the phase 2 replay) to save
  roughly $80 to $230 per refresh, and it needs the code Phase 7 deletes.
- Embedding-based page selection to replace the English `PATHS_TO_KEEP` allow-list.
- Coresignal Multi-source fields for dedupe and dead-site fallback.

## 3. Target architecture

A separate function, `scrape_websites_firecrawl`, that shares helpers with
`scrape_websites_psql` rather than threading a `backend` flag through that 330-line
function. The old function keeps its chunk-then-fan-out shape and its tables; the new one is
a per-org pipeline, which is where the pilot's 5.5 pages per second came from.

```
scrape_websites_firecrawl(urls, run_config)
  |
  |-- scrape_runs row: run_id, git sha, request profile, balance before, started_at
  |
  |-- cache lookup on web_pages_scraped_v2 / web_pages_parsed_v2
  |     skip when parsed row is ok and fetched_at within max_age_days
  |
  |-- per org, bounded by one account-wide semaphore (25 Standard / 50 Growth) and rpm:
  |     FirecrawlClient.scrape(home)        formats [markdown, links], onlyMainContent=false
  |     status gate on metadata.statusCode  non-2xx: store row, failure_reason, no subpages
  |     same_site_paths(links, final_host)  -> filter_links (deterministic, capped)
  |     FirecrawlClient.scrape(subpage) x N onlyMainContent=true (pending Phase 5 result)
  |     gate markers on plain text          parked / wall / blocked-by-extension
  |     clean_markdown -> parsed_html       versioned
  |     dedupe lines across pages, cap at 8k tokens, make_group_text -> combined_text
  |     classify_text_quality(combined)     -> web_pages_parsed_v2.text_quality
  |     write page rows + parsed row, commit every N pages
  |
  '-- scrape_runs row: balance after, credits used, stop reason, ended_at
```

Where it runs: a long-running process with the cache as its checkpoint. At the pilot's
throughput a CFT refresh is a few hours and the whole cache about a day. The vendor's
account-wide concurrency is the bottleneck, so two processes oversubscribe it and Lambda adds
nothing. Orchestration, cleaning and combine still run on the caller's machine; only the
browser and the extraction move to the vendor.

Production basics the pilot had and the pipeline keeps: `--dry-run` (reports cache misses,
estimated credits and current balance, fetches nothing), a fake backend for tests, a per-run
credit ceiling, and `concurrencyQueueDurationMs` logged so queueing on the vendor side is
visible.

## 4. Data model

New tables rather than a migration of the 17 GB `web_pages_scraped`. Same keys so
`make_group_text` and the cache lookups are unchanged; the status column keeps its old name
because `_format_scraped_sites` and the enrichment pipeline read `response_status_code`.

`scrape_runs`

| Column | Content |
|---|---|
| `run_id` (PK), `started_at`, `ended_at` | |
| `git_sha`, `extractor_version`, `request_profile` | the request settings per page type (formats, `onlyMainContent`, proxy, timeout) |
| `plan_concurrency`, `plan_rpm`, `credit_ceiling` | |
| `credits_before`, `credits_after`, `credits_used`, `stop_reason` | |

`web_pages_scraped_v2`

| Column | Source | Closes |
|---|---|---|
| `cleaned_key` (PK), `home_url`, `subpath`, `page_type` | ours, as today | |
| `run_id` | `scrape_runs` | |
| `source_url`, `final_url`, `final_host` | `metadata.sourceURL`, `metadata.url`, derived | #227 |
| `response_status_code` | `metadata.statusCode`, the target's real code | |
| `failure_reason` | `dead` / `http_error` / `blocked` / `invalid_url` / `vendor_error` | #227 |
| `markdown` | as returned; kept on non-2xx rows too | |
| `parsed_html` | cleaned text; empty string on non-2xx rows because the combine step reads every row for the home URL | |
| `extractor` | `firecrawl-md-v1`, bumped with the cleaner, cap or dedupe rules | |
| `text_quality` | per-page verdict | #198 |
| `proxy_used`, `credits_used`, `content_type`, `language`, `scrape_id` | metadata | |
| `num_errors`, `fetched_at` | ours; see section 6 for what increments `num_errors` | |

`web_pages_parsed_v2`: `cleaned_home_key` (PK), `home_url`, `run_id`, `combined_text`,
`source_keys` (the page rows it was built from), `extractor`, `prompt_token_budget`,
`text_quality`, `num_errors`, `combined_at`.

`org_site_keys` (added 2026-10-22 at Zein's request): the link from a source org id to the
site fetched for it, so a Crunchbase uuid, an EIN or an NZI id finds the cached site and text
when the URL itself is in doubt. Columns: `id_type` (`crunchbase`, `ein`, `nzi`,
`givingtuesday`, `linkedin`, `coresignal`), `id_value` (normalized: EIN digits only, uuid
lowercase), `cleaned_home_key`, `source_url`, `final_host`, `run_id`, `first_seen`,
`last_seen`; primary key (`id_type`, `id_value`, `cleaned_home_key`). One id may map to
several home keys over time and one home key to several ids; lookups return the newest
`last_seen` first. The pipeline takes an optional `org_ids` mapping and upserts rows; a
`site_for_org(id_type, id_value)` helper reads them; a one-time backfill from the project org
frames makes already-cached orgs resolvable. Linear: D16 (VIB-70).

Notes:

- `rawHtml` is not requested. The pilot fetched it and never used it; the vendor's median is
  164 to 190 kB per page. `return_raw_html=True` returns `markdown` for this pipeline.
- Markdown averages 15.6 kB per page, so the whole cache is about 7.7 GB uncompressed and a
  full refresh about 9.5 GB, before `parsed_html`, `combined_text` and the new rows in the
  29 GB `prompt_response` table that re-summarization adds. Measure real growth on the
  2,000-org validation run (Phase 5). Dropping the old table at Phase 7 frees 17 GB.
- The `extractor` tag makes `parsed_html` and `combined_text` re-derivable from `markdown`
  by a backfill without spending credits.

## 5. Text pipeline

These rules were tested against the pilot artifacts during review and are the part of the
plan that most affects the LLM outputs.

**Gate order.** Run the parked and wall markers from `text_quality.py` on plain text with link
labels kept (`markdown_to_plain`), before cleaning: dropping menu-only link lines deletes
labels like "buy this domain" and turned two parked pages `ok` in the review's replay. Add a
marker for Firecrawl's own "ERR_BLOCKED_BY_CLIENT / blocked by an extension" page, which
sites redirecting to ad domains return and which passed as `ok` on about 5 of 2,000 orgs;
that marker needs no length cap. Only the thin check runs on the cleaned text.

**Cleaner** (`markdown_clean.py`, pure, `CLEANER_VERSION`). Drop image markup, collapse
`[label](url)` to `label` unless `add_section_links`, drop menu-only link lines, remove blank
lines, then the existing `clean_scraped_text`. On the phase 2 pages the median page goes from
11,037 characters raw to about 3,400 (4,793 without the menu-line drop); in-house today is
2,649.

**Per-org cap and cross-page dedupe.** `make_group_text` budgets against gpt-4.1-mini's
1,047,576-token window, so it never truncates today: on raw markdown 331 of 2,000 orgs exceed
32k tokens and carry 64% of all tokens, and one site alone is a million. Add a per-org cap of
8k tokens on cleaned text and remove lines repeated across an org's pages (41% of characters
on raw markdown, 18% after cleaning, 6% in-house). Ship cap, dedupe and cleaner under one
`extractor` version so caches bust once.

**Deterministic page selection.** `filter_links` iterates `set(links)` before applying
`max_per_subpath`, so which pages survive the cap depends on the hash seed; the review
reproduced three different `/news` picks with three seeds, and a section hits the cap for
241 of 2,000 orgs. Sort with `deterministic_len_key` before capping. This is a live bug in the
shared code and should land as its own small PR before Phase 4, since it changes the in-house
path too. About 17% of picks are news, blog or resources posts whose content churns between
refreshes; capping those at one or two per org is a candidate change, measured separately.

**`onlyMainContent` on subpages.** Full page on the home page (the links array needs it);
main content only on subpages is the proposal, validated in Phase 5 with sentence recall of
the in-house text inside the main-content subpages plus the duplicated-line share, not with
the usable rate, which cannot tell a 36k-character text from a 10k one. Phase 1's 60-URL
main-content-only run already showed about 26% less prose and 3 pages flipping to thin.

## 6. Error and refresh semantics

- Only target-side failures increment `num_errors`: `dead`, `http_error`, `blocked`,
  `invalid_url`. Vendor-side failures (408, exhausted 429, 5xx that is not `SCRAPE_*`,
  transport errors) are stored with `failure_reason = vendor_error` and `num_errors = 0`, so
  the next run retries them. The CFT caller passes `max_errors=1`, so without this rule a
  single 408 is a permanent skip and sends the org to the paid Coresignal fallback; phase 2
  alone produced 20 of them.
- A credit stop (402) or auth failure (401) halts in-flight work and writes nothing for pages
  still in flight.
- `SCRAPE_*` 500s are terminal (`dead`). Blank and malformed URLs are rejected before any
  request as `invalid_url` with no credit spent.
- The LinkedIn fallback in the enrichment pipeline keys off `failure_reason` and
  `text_quality` and skips `vendor_error`.
- `max_age_days` on `fetched_at` defines "refresh": with `skip_existing=True` a second refresh
  today costs nothing and returns stale text; with `False` it refetches dead domains.
- A successful re-combine sets `num_errors = 0` (#228); the same one-line fix goes into the
  old path.
- Retention: old `scrape_runs` and superseded page rows follow the TTL policy that the RDS
  runbook (#150) requires.

## 7. Work plan

Estimates are working days for one engineer who knows the pipeline.

### Phase 0: prerequisites and zero-credit validation (2 days plus waiting)

- RDS per the runbook agreed on #150: `AllocatedStorage` to 250 (max 300), gp2 to gp3, and
  a TTL policy. The volume was 182 of 200 GB in July; the sizes in section 4 do not fit
  without it.
- Firecrawl plan: Standard billed monthly ($99, 100k credits, 25 browsers, 500 rpm) is the
  resting plan and is active. Refresh months move to Growth or a top-up.
- Merge PR #237 so the benchmark, the adapter notes and the `vendor_pilot` scripts are on
  main as regression tooling.
- Rescore from the saved artifacts, for free: run the cleaner, dedupe and cap over
  `firecrawl_e2e_pages.parquet`, recombine with the production `make_group_text`, recompute
  usable (the review's cleaned-text replay gave 85.5% against 86.0%, so the headline is not a
  length artifact); run the new link rule over the phase 1 links arrays and diff the picks
  against the pilot's.
- Paired LLM test on the 1,547 orgs usable on both runs: summarize both texts, run summary
  of summaries and the One Earth mapping, and compare against a noise floor (the same text
  re-run with the cache bypassed) on summary embedding cosine, taxonomy node Jaccard per
  level, relevance flips, and an LLM judge plus 50 human spot checks, oversampling the 173
  rescues and the largest length ratios. This is the number the team needs to decide, not
  the fetch rate.

### Phase 1: Firecrawl client (2.5 days)

`vdl_tools/scrape_enrich/scraper/firecrawl_client.py`, lifted from the pilot's `_common.py`
and `run_firecrawl.py`: typed `scrape()` capturing `url` and `sourceURL` explicitly (the
pilot's `KEEP_METADATA` omits them); semaphore and rpm limiter from a `[scraping_vendors]`
config section; retries with `Retry-After` on 429, 5xx and transport errors; the error
semantics of section 6; the credit ceiling; `dry_run`; a fake transport for tests; balance
read before and after a run. Tests with an httpx mock transport cover retry paths, terminal
codes, the 402 stop, the status gate and metadata capture including redirects.

### Phase 2: text pipeline (2 days)

`markdown_clean.py`, `markdown_to_plain`, the gate order, the per-org cap and cross-page
dedupe in the combine helper. Golden tests on saved pilot markdown including the parked
pages and the blocked-by-extension pages the review found.

### Phase 3: models and migration (1.5 days)

ORM models for `scrape_runs`, `WebPagesScrapedV2`, `WebPagesParsedV2` and `OrgSiteKey`
(section 4); Alembic migration with the `drop_table` lines for `microapp_configs`, `microapp_domains` and
`grantor_recipient_labeled_set` stripped; `delete_cached_urls(backend=...)` reaching the new
tables with the existing tests run against both.

### Phase 4: per-org pipeline and id lookup (5 to 6 days)

`scrape_websites_firecrawl` as in section 3: cache lookup with `max_age_days`, home fetch,
`same_site_paths(links, final_host)` with `urllib.parse` (exact netloc compare with `www.`
normalization, fragments and query stripped, extension filter on `.path`; compare against the
final host because 11% of home pages land elsewhere and their links are dropped today),
subpage fetches, gate, clean, dedupe, cap, combine, verdicts, run record, commits every N
pages. An orchestration test with the fake backend in the style of `StubBrowserScraper` in
`test_js_shell_fallback.py`: home failure fetches no subpages, vendor error leaves
`num_errors` at 0, subpage selection from vendor links, verdict persistence, `num_errors`
reset on re-combine. Delete the dead `choose_org_pages` and `filter_pages`.

Id lookup (D16, VIB-70): the pipeline accepts an optional `org_ids` mapping from home URL to
`(id_type, id_value)` pairs and upserts `org_site_keys` with the run id and first and last
seen; a `site_for_org(id_type, id_value)` helper returns the home key, final host, verdict and
text, newest first; a one-time backfill from `cb_cd_li_meta_mapped` and the ed_tracker org
frame against existing home keys makes cached orgs resolvable before the first refresh. Test
that two ids on one site and one id on two historical sites both resolve.

### Phase 5: validation (3 days plus run time; about 10k credits)

- About 200 orgs with main-content-only subpages (roughly 700 to 1,000 credits), scored on
  sentence recall and duplicated-line share against the full-page run. Decide the subpage
  setting on that.
- The 2,000-org phase 2 sample through the production path. Hard gate before any refresh:
  usable at or above 85%, credits per org near 4.7, no 429s at the configured concurrency,
  and the paired LLM measures of Phase 0 within the noise floor.
- Measure summarizer input tokens per org and real storage growth on both v2 tables and
  `prompt_response`.
- The ~850 ed_tracker host-security-check domains (#229): run them and report the recovered
  share; for any residue, test `proxy: "stealth"` on a sample before pinning it.

### Phase 6: readers and the first refresh (2 to 3 days)

Readers that move to the new tables: `climate_landscape/enrichment_pipeline.py` (which also
passes its Crunchbase uuid, EIN and NZI id columns as `org_ids`),
`web_summarization/website_summarization_psql.py` (reads the stored verdict instead of
reclassifying), `portfolio_comparison/enrichment/scrape.py` (the self-heal path's
`verify_ssl=False` has no vendor equivalent; `skip_existing=False` only),
`web_summarization/model_benchmark.py` (raw SQL on `web_pages_scraped`),
`website_quality.combined_texts` (must read the same table the summarizer read), and
`notebooks/scraped_pages_exploration.py`.

First refresh, two options for the team:

- Read-through: reuse the old `web_pages_parsed` rows that are `ok` and within
  `max_age_days`, send only new, failed or stale orgs to Firecrawl. Captures most of the gain
  for a fraction of the credits and the re-summarization, at the cost of one refresh with two
  extractors across orgs.
- Full re-scrape on the new extractor, timed to coincide with a deliberate taxonomy refresh,
  since every org's summary, summary of summaries, taxonomy mapping and relevance call will
  re-run.

Either way, later full refreshes with `maxAge=0` should carry a material-change gate: keep the
previous summary when the line-shingle Jaccard against the previous text is at least 0.9.

### Phase 7: retire the in-house path (2 days)

After every project has run once on the new pipeline:

- Delete `async_scraper.py`, `page_scraper.py`, `chrome_utils.py`, `direct_loader.py` (move
  its `HEADERS` and `BROWSER_HEADERS` to a constants module for `get_page_redirects` and
  `website_quality`), the in-house branch of `website_processor.py`, and
  `scrape_websites_psql` itself.
- `pyproject.toml`: drop `playwright`, `playwright-stealth`, `selenium`, `webdriver_manager`,
  `unstructured[all-docs]` and the `playwright install` post-install hooks;
  `tests/test_dependencies.py` imports selenium and changes with it. `torch` and
  `transformers` stay: they are direct dependencies used elsewhere.
- `wikipedia/scraper.py` is the other Selenium user: port it to `Wikipedia-API` (already a
  dependency) or keep Selenium for it alone.
- Retire `website_quality`'s redirect pass and its S3 domain cache in favor of `final_url`
  and `failure_reason` (#227 follow-up).
- Close #175 and #229 as superseded; close #139 with the new `same_site_paths`. The
  characterization tests #139 refers to are on an unmerged remote branch, not main.
- Snapshot the old `web_pages_scraped` and `web_pages_parsed` to S3, then drop them.

## 8. Issue and TODO map

| Item | Where it lives | Addressed in |
|---|---|---|
| #227 store final URL and failure reason | TODOs in `async_scraper.py` and `scrape_websites.py` | Phase 3 schema, Phase 4 writes, Phase 7 retires the redirect pass |
| #228 re-combine does not reset `num_errors` | combine loop | Phase 4, both paths |
| #229 host security-check pages | ~850 ed_tracker domains | Phase 5 measurement; stealth only if measured |
| #175 fixed 2 s browser wait | `_fetch_browser_impl` | Superseded at Phase 7 |
| #198 quality gate inside the scraping layer | `text_quality.py`, `summarize_scraped_df` | Phase 3 columns, Phase 4 persistence, Phase 6 readers |
| #139 rewrite `filter_anchors` with `urllib.parse` | `website_processor.py` | Phase 4 `same_site_paths`; old path retired in Phase 7 |
| #150 RDS storage at 91% | infra | Phase 0 prerequisite per the runbook; Phase 7 drops 17 GB |
| `filter_links` nondeterministic cap | `page_choice/choose_pages.py` | Own PR before Phase 4 (new issue) |
| #232 blank and malformed website fields | data prep | Own PR; Phase 1 `invalid_url` guard is the backstop |
| #226 unify domain key in portfolio_comparison | `intake/normalize.py` | Own PR |
| TODO `website_processor.py:157` and `:177` (extractor fallback, `"empty"` sentinel) | in-house extractor | Obsolete at Phase 7 |
| TODO `page_choice/constants.py:1` | link rules | `mission` is already in the list; re-read the TODO before acting |
| FIXME `choose_pages.py:153` | dead code | Deleted in Phase 4 |
| Pilot README caveat on `KEEP_METADATA` | `vendor_pilot/run_firecrawl.py` | Phase 1 |
| FIXME `enrichment_pipeline.py:586` column naming | pipeline | Not addressed; unrelated |

## 9. Costs

Credits are one per fetched page; failures are free; stealth retries billed one credit in
the pilot. Phase 2 measured 4.67 credits per org including subpages.

Org counts: the parquet the pilot sampled from (`cb_cd_li_meta_mapped`, August 2026) has
18,480 relevant orgs with 16,811 distinct domains. The working figure of 30k orgs used in
discussion needs a source; it may be the pre-relevance universe. Both are shown.

| Run | Credits | Plan for that month | Cost |
|---|---|---|---|
| Incremental run, new orgs only | a few thousand | Standard, already paid | $0 marginal |
| CFT refresh, 16.8k domains | ~79k | Standard | $0 marginal |
| CFT refresh if 30k orgs | ~140k | Growth, or Standard plus 40k top-up | ~$333 to $400, or $99 plus ~$100 |
| ed_tracker refresh, ~24k domains | ~110k | Standard plus 10k top-up | ~$125 |
| Whole cache, ~130k homes | ~607k | Scale (1M, one-month rollover) or Growth plus top-ups | ~$599 |

LLM costs per 30k orgs, summarizer input only, gpt-4.1-mini (review's recomputation from the
pilot texts):

| Text fed to the summarizer | Cost |
|---|---|
| In-house | ~$48 |
| Firecrawl raw markdown, uncapped | ~$232 |
| Cleaned, uncapped | ~$71 to $90 |
| Cleaned, deduped, capped at 8k tokens | ~$43 |

The first refresh on a new extractor is the re-summarization; there is no separate one-time
cost. What is not yet quantified: the summary-of-summaries, taxonomy-mapping and relevance
calls that re-run when summaries change. The Phase 0 paired test measures them.

Storage: section 4. Compute: no browser or extractor runs locally; orchestration still does.

## 10. Risks and open questions

- The LLM outputs may shift even when the fetch is better. Phase 0's paired test decides
  whether the shift is noise or a change in what the tracker says about an org.
- The subpage `onlyMainContent` setting is unmeasured; Phase 5 decides it on recall, not on
  the usable rate.
- First-refresh strategy (read-through versus full re-scrape with a taxonomy refresh) is a
  team decision with a comparability cost either way.
- Page-selection churn: news and blog picks change between refreshes regardless of vendor;
  the deterministic sort removes the seed dependence but not the churn.
- Exit path: Olostep with our own 404 detection, or self-hosted Firecrawl without its
  anti-bot engine. The hybrid is not the exit; it needs the code Phase 7 deletes.
- Credits do not roll over below Scale. Refresh months need a deliberate plan change and a
  cancel-back afterward; the credit ceiling and the 402 stop prevent an unplanned overrun.
- The vendor's account-wide concurrency means one pipeline process at a time; a second
  project running concurrently queues behind the first.

## 11. Estimate

| Phase | Days |
|---|---|
| 0 Prerequisites and zero-credit validation | 2 plus waiting on RDS and the plan |
| 1 Client | 2.5 |
| 2 Text pipeline | 2 |
| 3 Models and migration | 1.5 |
| 4 Per-org pipeline and id lookup | 5 to 6 |
| 5 Validation | 3 plus run time |
| 6 Readers and first refresh | 2 to 3 |
| 7 Retire in-house path | 2 |
| Total, this PR's scope | 20 to 23 working days |
| Separate PRs: `filter_links` determinism, #232, #226 | 0.5, 1 to 2, 2 |

Phases 1 through 3 are independent and can run in parallel; Phase 4 needs all three; Phase 5
needs 4; Phase 6 needs 5's gate to pass; Phase 7 waits for every project to have run on the
new pipeline.
