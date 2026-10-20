# Vendor pilot: can Firecrawl or Spider replace the in-house fetch-and-extract layer?

**Status (2026-10-20):** Firecrawl measured on the full 1,000-URL sample.
Spider not run yet (no API key in config.ini). Results and recommendation
are in [Results](#results).

## The question

The in-house scraper (`vdl_tools/scrape_enrich/scraper/`: httpx first,
Playwright Chromium with stealth as the fallback, text via
`unstructured.partition_html`) is fragile on developer laptops. The idea is
to **buy only the fetch-and-extract step** and keep everything around it:

| keep | buy |
|---|---|
| Postgres cache (`web_pages_scraped`, `web_pages_parsed`) | fetching the page (HTTP, JS rendering, bot walls, proxies) |
| subpage selection (`filter_links` + `PATHS_TO_KEEP` in `web_summarization/page_choice/`) | turning HTML into text |
| the `text_quality.classify_text_quality` gate | (optionally) extracting the page's links |

So the benchmark asks, per vendor: on the same home URLs the cache already
holds, does one vendor call give (a) text at least as usable as our
`parsed_html`, (b) links our subpage rules can work from, (c) at what
speed and cost.

## Method

### Sample: 1,000 home URLs, stratified by how the cache fared

Drawn read-only from `web_pages_scraped` index rows (`page_type =
'PageType.INDEX'`, 130,483 rows) left-joined to `web_pages_parsed` on the
home key. Strata are assigned in priority order so they are disjoint:

| stratum | definition | population | sampled |
|---|---|---|---|
| `status_429` | index row has `response_status_code = 429` | 514 | 100 |
| `status_0` | status 0: no HTTP response at all (DNS, TLS, timeout) | 17,265 | 100 |
| `status_404` | status 404 | 1,247 | 100 |
| `empty_combined` | any other status but `combined_text` empty or missing (fetch "worked", extraction produced nothing; 151 of the 200 are status 200, 20 are 202) | 8,576 | 200 |
| `ok_200` | status 200, non-empty `parsed_html`, non-empty `combined_text` | 102,657 | 500 |

Exclusions: platform and free-mail domains (`url_keys.website_domain(url)
== ""`, i.e. LinkedIn, Facebook, Wix-hosted `wixsite.com` is *not*
excluded), PDF home URLs, duplicate domains. One URL per domain. Sampled
with Postgres `setseed(0.42)` + `ORDER BY random()`, then shuffled so any
prefix of the sample is still stratified (a run cut short by credits stays
representative). The parquet carries the seed, per-stratum populations and
the SQL in its lineage footer (`parquet_cache.get_lineage`).

Our side of the comparison is frozen in the sample: `parsed_html`, its
length, and `classify_text_quality(parsed_html, num_errors)`:

| stratum | ok | thin | blocked | parked | garbled | empty | dead |
|---|---|---|---|---|---|---|---|
| ok_200 | 466 | 15 | 12 | 4 | 3 | 0 | 0 |
| empty_combined | 18 | 16 | 2 | 0 | 0 | 133 | 31 |
| status_429 / status_0 / status_404 | 0 | 0 | 0 | 0 | 0 | 0 | 100 each |

(So 7% of what the cache calls a success is junk by our own gate, which is
the baseline a vendor has to beat.)

### Vendor settings

One call per home URL, vendor cache disabled, no retries on the target
beyond what the vendor does itself. API-level 429/5xx are retried with
backoff (max 4 attempts); 402 stops the run.

**Firecrawl** `POST /v2/scrape`: `formats=['markdown','rawHtml','links']`,
`maxAge=0`, `proxy='auto'`, `timeout=60000`, `onlyMainContent=false`.
Free plan: 1,000 credits/month, 10 `/scrape` requests/min, 2 concurrent
browsers; the runner sits at those limits (`--rpm 10 --concurrency 2`).

`onlyMainContent` deserves its own line. Firecrawl's default (`true`)
strips header, nav and footer from the markdown **and from the `links`
array**. Probed on one Wix site in the sample: default gave 0 internal
links, `false` gave 8 (`/about-3`, `/meet-the-team`, `/projects-6`, ...),
which are exactly the paths `PATHS_TO_KEEP` selects. The main run therefore
uses `false`. The cost is nav/footer text in the markdown, which our
`unstructured` extraction (`skip_headers_and_footers=True`) does not have;
`--only-main-content` runs the default on the first 60 sampled URLs as
`firecrawl_mainonly` to size that effect.

**Spider** `POST /scrape`: `return_format=['markdown','raw']`,
`return_page_links=true`, `request='smart'`, `cache=false`,
`metadata=true`; 20 concurrent. Runner written, not run (no key).

Both runners talk REST through `httpx` (already a dependency) rather than
the vendor SDKs: the pilot is throwaway and a new dependency needs a team
decision first.

### Metrics (`compare.py`)

Per URL and vendor:

- **fetched**: the API returned content (markdown or raw HTML).
- **2xx**: fetched and the vendor reports a 2xx status for the target
  (a 404 page is "fetched" but not 2xx).
- **verdict**: `classify_text_quality` on the vendor's markdown with image
  and link syntax, bare URLs and line markup stripped first
  (`markdown_to_plain`), so URLs do not pad the 200-char thin threshold.
  `num_errors=1` when the fetch failed, so an empty result reads `dead`
  like ours.
- **recovered**: our verdict was not `ok` and the vendor's is.
  **lost**: ours was `ok` and the vendor's is not.
- **link recall**: internal paths `filter_anchors` keeps from the `<a href>`
  tags BeautifulSoup finds in the vendor's *raw HTML* (what
  `extract_website_links` does in production) versus from the vendor's
  *links array* (same-host absolute URLs reduced to paths, the adapter
  production would need). Recall = share of the HTML-derived paths that the
  array also yields. **pick recall** is the same after `filter_links(...,
  PATHS_TO_KEEP, max_per_subpath=6)`, i.e. the pages production would
  actually scrape next; it is noisier because `filter_links` caps per
  top-level path in set-iteration order.
- **wall seconds** per page (client-side, includes the vendor's queue),
  **pages/second** for the run as a whole (from the run meta), per-response
  **credits** as the vendor reports them, and the **account balance** read
  from the vendor's credits endpoint before and after each run.

### Running it

From the repo root, in the `vdl-tools-312` env, with
`[scraping_vendors] firecrawl_api_key` (and `spider_api_key`) in
`config.ini`:

```bash
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_urls
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl --limit 3     # smoke
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl                 # ~100 min on the free plan
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl --only-main-content --limit 60
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_spider
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare
```

Everything lands under `.cache/vendor_pilot/` (gitignored): the sample,
one `<vendor>_results.jsonl` (append-only, resumable: rerunning skips URLs
already fetched, `--no-resume` starts over), `<vendor>_results.parquet`
with lineage, `<vendor>_run_meta.json` (one entry per run: timing, settings,
balance before/after), `comparison_results.parquet` (one row per vendor x
URL), `comparison_summary.parquet` and `comparison_summary.md`.

## Results

<!-- RESULTS -->

## Caveats and dead ends

- **Firecrawl's `links` array is only usable with `onlyMainContent=false`.**
  With the default it holds main-content links only (donate buttons,
  external partners), none of the nav links our subpage rules select. See
  the probe above. Either send `false`, or keep our BeautifulSoup extraction
  over `rawHtml` (same credit, no change to our link logic).
- **Pages/second on the free plan is the plan's rate limit**, not the
  vendor's throughput: 10 requests/min, 2 concurrent browsers. Paid plans
  advertise 100 to 5,000 requests/min; the per-page wall time (which
  includes their queue) is the number that transfers.
- **`proxy='auto'` can bill more than 1 credit**: Firecrawl escalates to
  its stealth pool when the basic fetch is blocked and bills that at a
  higher rate. Per-response `creditsUsed` is recorded, so the stratum
  tables show where the escalations happened.
- **Markdown is not our `parsed_html`.** The vendor's text is a different
  extraction of the same page; verdict shares are comparable, lengths are
  only roughly so. With `onlyMainContent=false` nav text can lift a page
  our gate calls `thin` over the 200-char line; the `firecrawl_mainonly`
  subset sizes that.
- **The cache is historical.** A site that was `status_0` or `429` months
  ago may simply be alive today; "recovered" overstates what the vendor
  adds versus a fresh in-house fetch. The same-day in-house baseline on a
  60-site panel lives in `.cache/scraper_benchmark/` (sibling run,
  2026-10-19): current scraper 37/60 usable, median 3.0 s/page; Olostep
  40/60 html-usable, 43/60 native-usable, median 8.6 s/page, 49 credits
  reported for 60 requests with 11 responses not reporting credits.
- **Olostep and Jina Reader were not run here.** Olostep's key is in
  config.ini and the harness takes a new vendor in ~40 lines, but the
  sibling run above already covers it; Jina has no key.
- **Sample reproducibility**: `setseed` + `ORDER BY random()` reproduces
  the draw only against the same table state. The parquet, not the query,
  is the artifact of record.
- **Vendor SDKs were not used** (see above); the Spider Python client, for
  what it is worth, posts to `/crawl` with `limit=1` rather than `/scrape`,
  and neither SDK has an async client.

## Files

| file | role |
|---|---|
| `sample_urls.py` | stratified read-only draw from the cache -> `sample_urls.parquet` |
| `_common.py` | config keys, rate limiter, retry/stop policy, resumable JSONL, run meta, parquet with lineage |
| `run_firecrawl.py` | Firecrawl v2 scrape runner (`--only-main-content` variant) |
| `run_spider.py` | Spider.cloud scrape runner |
| `compare.py` | verdicts, link recall, per-stratum summary -> `comparison_*` |
