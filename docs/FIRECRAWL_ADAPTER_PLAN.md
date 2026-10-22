# Firecrawl scraping adapter: implementation plan

Status: draft for team discussion, 2026-10-22. Decision pending per the VDL rule that new
tools get discussed before adoption.

Evidence lives on branch `zeclaude/kind-chaum-8f9459` (not yet merged):
`vdl_tools/scrape_enrich/scraper/vendor_pilot/README.md` holds the method, the result tables
and the "Production adapter notes" that this plan turns into work.

## 1. Why

The in-house scraper (`vdl_tools/scrape_enrich/scraper/`) fetches with httpx, falls back to a
local Playwright Chromium for JS shells and bot walls, and extracts text with
`unstructured.partition_html`. It runs on laptops and fails in ways that depend on the machine:
Chromium installs and memory, the `unstructured[all-docs]` install (torch, transformers, PDF
stack, imported in exactly one place), home-IP rate limits, and host security checks.

Two benchmarks in October 2026 measured a vendor against it on the same URLs the same day.
"Usable" means the home page answered 2xx and the text passes `classify_text_quality`.

| Benchmark | Firecrawl | In-house | Notes |
|---|---|---|---|
| Phase 1: 1,000 cached home URLs, stratified toward hard cases | 74% | 61 to 63% | Olostep 73% but reports 200 for 404s; Spider 67%, cannot render JS shells |
| Phase 2: 2,000 CFT orgs end to end, home plus chosen subpages | 86.0% | 78.0% | 4.67 credits per org, 5.5 pages/s on the Standard plan, billing matched per-page credits exactly |
| Phase 2 hybrid, in-house first and Firecrawl on failure | 86.6% | | 0.81 credits per org |

Firecrawl rescues 173 of 2,000 orgs end to end (bot walls, dead-to-us sites, thin extractions)
and loses 13. It is the only vendor tested that both handles those pages and reports the
target's real status code and final URL.

## 2. Scope

In scope:

- A Firecrawl-backed fetch-and-extract layer behind the existing `scrape_websites_psql`
  orchestration.
- New cache tables with the columns the old ones lack (final URL, failure reason, extractor,
  quality verdict, credits).
- Migrating every caller, fixing the data-prep and domain-key issues that feed it, and
  retiring the Playwright, Selenium and `unstructured` dependencies.

Out of scope, recorded so they are not lost:

- The in-house-first hybrid (section 8). Build Firecrawl-only first; the router is cheap to
  add later because the quality gate already yields the routing signal.
- Embedding-based page selection to replace the English `PATHS_TO_KEEP` allow-list
  (idea noted on #139).
- Coresignal Multi-source fields for dedupe and dead-site fallback (separate evaluation).

## 3. Target architecture

```
scrape_websites_psql(urls, backend="firecrawl")            orchestration: unchanged shape
  |
  |-- cache lookup on web_pages_scraped_v2 / web_pages_parsed_v2   (skip_existing, max_errors)
  |
  |-- FirecrawlClient.scrape(home_url)                      POST /v2/scrape
  |     formats [markdown, rawHtml, links], onlyMainContent=false, maxAge=0, proxy=auto
  |     semaphore = plan browsers (25 Standard / 50 Growth), limiter = plan rpm
  |     retries on 429/5xx/transport with Retry-After; SCRAPE_* 500s terminal; 402/401 stop
  |
  |-- status gate: metadata.statusCode 2xx, else store failure row (dead/404/blocked)
  |-- same_site_paths(links, home_url) -> filter_links(PATHS_TO_KEEP, max_per_subpath=6)
  |
  |-- FirecrawlClient.scrape(subpage) x N                   onlyMainContent=true, no links
  |
  |-- clean_markdown(markdown) -> parsed_html               pure, versioned (firecrawl-md-v1)
  |-- classify_text_quality(page)                           per-page verdict stored
  |
  |-- make_group_text(home + subpages) -> combined_text     unchanged
  '-- classify_text_quality(combined) -> web_pages_parsed_v2.text_quality
```

Only the fetch and the text extraction change. The cache keys (`cleaned_key`, `home_url`,
`subpath`, `page_type`), the subpage rules, the token-budgeted combine and the quality gate are
the production functions called as they are today.

## 4. Data model

New tables rather than a migration of the 17 GB `web_pages_scraped`. Same keys so the lookups
and `make_group_text` are unchanged.

`web_pages_scraped_v2`

| Column | Source | Closes |
|---|---|---|
| `cleaned_key` (PK), `home_url`, `subpath`, `page_type` | ours, as today | |
| `source_url`, `final_url`, `final_host` | `metadata.sourceURL`, `metadata.url`, derived | #227 |
| `status_code` | `metadata.statusCode`, the target's real code | |
| `failure_reason` | `dead` / `http_error` / `blocked` / `invalid_url` / `vendor_error` | #227 |
| `markdown` | as returned, images and link URLs intact | |
| `parsed_html` | cleaned text; what every downstream reader uses | |
| `extractor` | `firecrawl-md-v1` | |
| `text_quality` | per-page `classify_text_quality` verdict | #198 |
| `proxy_used`, `credits_used`, `content_type`, `language`, `scrape_id` | metadata | |
| `num_errors`, `fetched_at` | ours | |

`web_pages_parsed_v2`: `cleaned_home_key` (PK), `home_url`, `combined_text`, `num_errors`,
`text_quality`, `extractor`, `combined_at`.

Notes:

- `rawHtml` is requested but not stored by default (median 156 kB per page, about 75 GB
  for the whole cache); `return_raw_html` keeps its meaning for one-off debugging.
- Markdown adds about 5 GB uncompressed for 490k pages, likely under 2 GB after TOAST
  compression. The old `web_pages_scraped` can be dropped once every project has moved,
  which is a net reduction.
- The `extractor` tag makes `parsed_html` re-derivable: a backfill can rerun a newer
  `clean_markdown` over `markdown` without spending credits.

## 5. Work plan

Estimates are working days for one engineer who knows the pipeline. Each phase lists the
issues and TODOs it closes.

### Phase 0: prerequisites (0.5 day of work, plus waiting on others)

- Raise `MaxAllocatedStorage` on `vdl-database-1` before any new table lands. The instance
  was at 182 of 200 GB in July (#150); a new text table on a 91%-full volume is how a
  pipeline run takes the dashboards down.
- Firecrawl plan: Standard billed monthly ($99, 100k credits, 25 browsers, 500 rpm) is the
  resting plan and is already active. Refresh months go to Growth (500k credits) or a top-up.
- Open the PR for the pilot branch so the benchmark and the adapter notes are reviewable
  and the `vendor_pilot` scripts stay as regression tooling.

### Phase 1: Firecrawl client (2 days)

New module `vdl_tools/scrape_enrich/scraper/firecrawl_client.py`, lifted from the pilot's
`_common.py` and `run_firecrawl.py`:

- `scrape(url, *, only_main_content, formats)` returning a typed result with markdown, raw
  HTML, links array and the full metadata. Capture `url` and `sourceURL` explicitly; the
  pilot runner's `KEEP_METADATA` list omits them.
- Concurrency semaphore and rpm limiter sized from a `[scraping_vendors]` config section
  (`firecrawl_api_key`, `firecrawl_concurrency`, `firecrawl_rpm`).
- Retries with backoff on 429, 5xx and transport errors honoring `Retry-After`; `SCRAPE_*`
  500s are terminal (dead targets); 402 and 401 raise a stop that halts in-flight work.
- Reject blank and malformed URLs before the request (no credit spent), recording
  `failure_reason = invalid_url`. Pairs with #232.
- Credit accounting: read the balance endpoint before and after a run and log the delta
  against the sum of per-page `creditsUsed`.
- Tests with an httpx mock transport: retry paths, terminal codes, stop on 402, status gate,
  metadata capture including redirects.

### Phase 2: markdown cleaner (1 day)

New module `vdl_tools/scrape_enrich/scraper/markdown_clean.py`: a pure function with a
`CLEANER_VERSION` constant. Drop image markup, collapse `[label](url)` to `label` unless
`add_section_links`, drop menu-only link lines, remove blank lines, then the existing
`clean_scraped_text`. Measured target on the phase 2 pages: median 11,037 characters raw to
about 4,700 cleaned (in-house today: 2,649). Golden tests on saved pilot markdown.

### Phase 3: models and migration (1 day)

- ORM models `WebPagesScrapedV2`, `WebPagesParsedV2` in
  `database_models/web_scraping.py`; Alembic migration under `data_migrations/versions/`.
- Strip the `drop_table` lines autogenerate emits for `microapp_configs`,
  `microapp_domains` and `grantor_recipient_labeled_set` before running it.
- `delete_cached_urls` gains a `backend` argument so invalidation reaches the v2 tables
  (the existing tests in `test_cache_invalidation.py` run against both).

### Phase 4: orchestration (3 to 4 days)

In `scrape_websites.py`:

- `scrape_websites_psql(..., backend="firecrawl" | "inhouse")` selecting the client and the
  table pair. Default stays `inhouse` until Phase 5 flips callers.
- Home fetch with `onlyMainContent=false`; subpages with `true` (links not needed there).
  This is the unmeasured assumption; Phase 6 verifies it.
- `same_site_paths(links, home_url)`: Firecrawl returns absolute URLs, so the vendor path
  gets the `urllib.parse` implementation #139 asked for (exact netloc compare with `www.`
  normalization, fragments and query stripped, extension filter on `.path`). No cache-key
  drift because the v2 table starts empty. The old `filter_anchors` stays untouched behind
  the `inhouse` backend with its characterization tests until Phase 7.
- Fold the `PATHS_TO_KEEP` TODO (match `mission` without the `our-` prefix) into the same
  revision, for the same no-drift reason; remove the `print` and the dict/dataclass hack in
  `choose_pages.choose_org_pages` while there.
- Store the per-page verdict and, in the combine loop, the combined verdict on
  `web_pages_parsed_v2.text_quality` (#198). Set `num_errors=0` on a successful re-combine
  (#228); apply the same one-line fix to the old path.
- Write `final_url`, `final_host`, `failure_reason` on every row (#227).
- `return_raw_html=True` returns `markdown` for the vendor backend.
- Tests: an orchestration test with a stub backend, in the style of `StubBrowserScraper`
  in `test_js_shell_fallback.py`, covering home failure (no subpages fetched), subpage
  selection from vendor links, verdict persistence and `num_errors` reset.

### Phase 5: callers and feeders (2 to 3 days)

- `climate_landscape/enrichment_pipeline.py`: pass `backend="firecrawl"`; the LinkedIn
  fallback for failed sites keys off `failure_reason` and `text_quality` rather than only
  scrape failures (#198, third bullet).
- `web_summarization/website_summarization_psql.py`: thread `backend` through
  `summarize_website` and `scrape_and_summarize_websites`; the summarizer already skips
  non-`ok` verdicts (#214) and can read the stored one instead of reclassifying.
- `portfolio_comparison/enrichment/scrape.py`: the self-heal path's `verify_ssl=False`
  has no vendor equivalent; replace with `skip_existing=False` only. Switch the module's
  `normalize_domain` and `identity_domain` calls to `url_keys.website_domain` and delete
  `resolve_redirect` and `domains_converge`, which the stored `final_url` makes redundant
  (#226).
- `scrape_enrich/website_quality.py`: read liveness and final domain from
  `web_pages_scraped_v2` and retire the separate 30-minute redirect pass and its S3 domain
  cache (#227 follow-up).
- Data prep (#232): blank websites to null and the ~150 malformed values repaired in the
  Candid, Crunchbase and GivingTuesday cleaning before `cb_cd_cleaned`, so the scraper never
  sees them. The client's `invalid_url` guard is the backstop.
- `notebooks/scraped_pages_exploration.py`: point at the v2 tables.

### Phase 6: validation and first refresh (2 days plus run time)

- Re-run the 2,000-org phase 2 sample through the production path and compare to the
  pilot: usable at or above 86%, credits per org near 4.7, no 429s at the configured
  concurrency.
- Measure the subpage `onlyMainContent=true` change: combined text size and usable rate
  against the pilot's full-page run. If usable drops, keep `false` and rely on the cleaner.
- Measure summarizer input tokens per org before and after, on the same orgs.
- Check the ~850 ed_tracker host-security-check domains (#229): Firecrawl's `proxy: auto`
  recovered 93 bot walls in phase 2; confirm on this set and, for any residue, pin
  `proxy: "stealth"` for those keys.
- First production use: the next CFT refresh (about 30k orgs, about 140k credits) on
  Growth or Standard plus top-up, with `backend="firecrawl"` and the old tables untouched.

### Phase 7: retire the in-house path (1 to 2 days)

After every project has run once on the vendor backend:

- Delete `async_scraper.py`, `page_scraper.py`, `chrome_utils.py`, the in-house branch of
  `website_processor.py` (keep `extract_website_links` only if the old backend survives),
  and move `BROWSER_HEADERS` to a small constants module for `website_quality` and
  `get_page_redirects`.
- Drop `playwright`, `playwright-stealth`, `selenium`, `webdriver_manager` and
  `unstructured[all-docs]` from `pyproject.toml`. `unstructured` has a single importer.
  `wikipedia/scraper.py` is the only other Selenium user and needs a decision: port to
  `Wikipedia-API` (already a dependency) or keep Selenium for it alone.
- Close #175 and #229 as superseded; close #139 with the vendor-path implementation.
- Drop the old `web_pages_scraped` and `web_pages_parsed` after a final snapshot to S3.

## 6. Issue and TODO map

| Item | Where it lives | Addressed in |
|---|---|---|
| #227 store final URL and failure reason | `async_scraper.py` TODO on `response.url`; `scrape_websites.py` TODO in `process_scraped_content` | Phase 3 schema, Phase 4 writes, Phase 5 `website_quality` reads them |
| #228 re-combine does not reset `num_errors` | combine loop in `scrape_websites.py` | Phase 4, both backends |
| #229 host security-check pages | ~850 ed_tracker domains, several climate companies | Phase 6 verification; vendor proxy escalation |
| #175 fixed 2 s browser wait | `_fetch_browser_impl` | Superseded at Phase 7; Firecrawl `waitFor` available if a residue appears |
| #198 quality gate inside the scraping layer | `text_quality.py`, `summarize_scraped_df` | Phase 3 columns, Phase 4 persistence, Phase 5 consumers read the verdict |
| #232 blank and malformed website fields | data prep for Candid, Crunchbase, GivingTuesday | Phase 5 data prep; Phase 1 `invalid_url` guard |
| #139 rewrite `filter_anchors` with `urllib.parse` | `website_processor.py`, characterization tests | Phase 4 vendor path; old path retired in Phase 7 |
| #226 unify domain key in portfolio_comparison | `intake/normalize.py` | Phase 5 |
| #150 RDS storage at 91% | infra | Phase 0 prerequisite; Phase 7 drops the 17 GB table |
| TODO `website_processor.py:157` fallback sees only `<p>`/`<section>` | in-house extractor | Obsolete with vendor markdown; removed in Phase 7 |
| TODO `website_processor.py:177` `"empty"` sentinel leaks as content | in-house extractor | Same |
| TODO `page_choice/constants.py:1` match `mission` without `our-` | link rules | Phase 4 |
| FIXME `choose_pages.py:153` dict/dataclass hack and stray `print` | link rules | Phase 4 |
| Pilot README caveat: `KEEP_METADATA` omits `url`/`sourceURL` | `vendor_pilot/run_firecrawl.py` | Phase 1 captures both explicitly |
| FIXME `enrichment_pipeline.py:586` column naming | pipeline | Not addressed; unrelated to scraping |

## 7. Costs

Credits are one per fetched page; failures are free; stealth retries billed one credit in
the pilot. Phase 2 measured 4.67 credits per org including subpages.

| Run | Pages | Credits | Plan for that month | Cost |
|---|---|---|---|---|
| Incremental run, new orgs only | a few thousand | same | Standard, already paid | $0 marginal |
| CFT refresh, ~30k orgs | ~140k | ~140k | Growth, or Standard plus 40k top-up | ~$333 to $400, or $99 plus ~$100 |
| ed_tracker refresh, ~24k domains | ~110k | ~110k | Standard plus 10k top-up | ~$125 |
| Whole cache, ~130k homes | ~607k | ~607k | Scale (1M, one-month rollover) or Growth plus top-ups | ~$599 |

Secondary costs:

- Summarizer tokens: cleaned vendor text is about 1.8x the in-house size per page before the
  subpage `onlyMainContent` change; expect at most roughly $110 instead of $36 per CFT refresh
  on gpt-4.1-mini, less after Phase 6 tuning.
- One-time re-summarization: every org re-scraped with the new extractor gets new text, so
  its summary cache entry misses once. About $60 per 30k orgs; a few hundred dollars for the
  whole cache, spread across refreshes.
- RDS: markdown adds under 2 GB compressed; dropping the old table at Phase 7 frees 17 GB.
- Compute: nothing runs locally except cache writes; the laptop and the EC2 box are out of
  the loop.

## 8. Risks and open questions

- Subpage `onlyMainContent=true` is unmeasured. Phase 6 checks it before it ships as the
  default.
- Mixed caches during transition. A project on `backend="inhouse"` keeps reading the old
  tables; nothing is shared between the pairs, so there is no partial state, but two
  projects on different backends will summarize the same org from different text until
  both have moved.
- Link-rule changes shift page selection. The `urllib.parse` rewrite and the `mission`
  match add candidates; the cap of 6 per subpath bounds credits, and the phase 2 subpage
  count of 3.8 per org is the baseline to compare against in Phase 6.
- Vendor dependence. Firecrawl is AGPL and self-hostable without its anti-bot engine, so
  the fallback is the hybrid, not a self-host. Olostep is the measured second choice if
  we add our own 404 detection.
- Credits do not roll over below Scale. Refresh months need a deliberate plan change and a
  cancel-back afterward; the client's 402 stop and a per-run credit ceiling prevent an
  unplanned overrun.
- The hybrid (86.6% usable at 17% of the credits) stays on the table. It keeps `httpx` and
  a local extractor in production, so it is a credit optimization to add once the vendor
  path exists, not a reason to delay it.

## 9. Estimate

| Phase | Days |
|---|---|
| 0 Prerequisites | 0.5 |
| 1 Client | 2 |
| 2 Cleaner | 1 |
| 3 Models and migration | 1 |
| 4 Orchestration | 3 to 4 |
| 5 Callers and feeders | 2 to 3 |
| 6 Validation and first refresh | 2 plus run time |
| 7 Retire in-house path | 1 to 2 |
| Total | 12 to 16 working days |

Phases 1 through 3 are independent of each other and can run in parallel; Phase 4 needs all
three; Phase 5 needs 4; Phase 7 waits for every project to have run on the new backend.
