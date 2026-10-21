# Vendor pilot: can Firecrawl or Spider replace the in-house fetch-and-extract layer?

**Status (2026-10-20):** Firecrawl measured on the full 1,000-URL sample,
against the in-house scraper run the same day on the same URLs from a
laptop and from an EC2 instance. Spider not run (no API key in config.ini
yet; runner ready). Short version: Firecrawl gets usable text from 74% of
the sample versus 61 to 63% for us, with identical reachability, a 1%
expensive-path rate versus our 25%, similar per-page latency, 1 credit per
fetched page, and a links array that stands in for our link extraction when
`onlyMainContent=false`. See [Results](#results) and
[Recommendation](#recommendation).

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
`return_page_links=true`, `request='smart'` (HTTP first, Chrome only when
Spider decides it is needed), `cache=false`, `metadata=true`; 20
concurrent; a 403 (exhausted balance) stops the run. Pay-as-you-go,
metered on bytes and CPU time, 10,000 credits = $1; the per-page
`costs.total_cost` is the spend (it matched the balance delta exactly on
the smoke run). `--request-mode chrome` runs the same sample with
rendering forced, as `spider_chrome`, because the smart run left most
JavaScript app shells unrendered.

**Olostep** `POST /v1/scrapes`: `formats=['html','markdown']`,
`links_on_page={}` (absolute URLs), `max_age` default 0 (fresh),
`remove_css_selectors=["script","style","noscript"]` sent as a JSON
array; 10 concurrent, API attempts capped at 2, run capped at 1,000
requests. The setting asked for was `"none"` (keep nav and footer so the
links array compares with Firecrawl full-page), but `"none"` also leaves
`<style>` and `<script>` bodies in the markdown (one 6.6k-character page
came back as 43k characters, mostly CSS), the JSON-*stringified* list the
docs describe is silently ignored, and `"default"` strips nav links from
`links_on_page` and timed out more often, so the array form is what ran.
Target failures come back as the API's own 400 (`dns_resolution_failed`),
502 (`tls_error`) and 504 (`scrape_poll_timeout`, about 55 s) and are
recorded, not retried; 402 (`credits_exhausted`) stops the run. 1 credit
per fetched page, failures free; the API reports no balance and
`cost_usd` came back null.

**In-house baseline** (`run_inhouse.py`): the production path for an
index page without the cache: `AsyncScraper.scrape_url` (httpx, then
Playwright Chromium with stealth on a JS wall or JS shell), then
`_process_scraped_with_browser_retry` (`unstructured` text extraction,
browser retry when the text is empty or a bot wall). Same concurrency as
`scrape_websites_psql` (20 HTTP, 3 browser, 3 per host). Run on the
laptop (residential IP) and, via `remote_baseline.sh`, on the EC2 box
`zein_playground` (r5.xlarge, datacenter IP, shared with a running
ed_tracker job, under `nice`).

All vendor runners talk REST through `httpx` (already a dependency)
rather than the vendor SDKs: the pilot is throwaway and a new dependency
needs a team decision first.

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
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl                 # ~110 min on the free plan
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl --only-main-content --limit 60
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_spider                    # ~6 min
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_spider --request-mode chrome
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_olostep                   # stops when the plan's credits run out
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse --label laptop    # ~5 min
SSH_OPTS="-i ~/.ssh/key.pem" vdl_tools/scrape_enrich/scraper/vendor_pilot/remote_baseline.sh push  ec2-user@HOST
SSH_OPTS="-i ~/.ssh/key.pem" vdl_tools/scrape_enrich/scraper/vendor_pilot/remote_baseline.sh run   ec2-user@HOST
SSH_OPTS="-i ~/.ssh/key.pem" vdl_tools/scrape_enrich/scraper/vendor_pilot/remote_baseline.sh pull  ec2-user@HOST
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare
```

Everything lands under `.cache/vendor_pilot/` (gitignored): the sample,
one `<vendor>_results.jsonl` (append-only, resumable: rerunning skips URLs
already fetched, `--no-resume` starts over), `<vendor>_results.parquet`
with lineage, `<vendor>_run_meta.json` (one entry per run: timing, settings,
balance before/after), `comparison_results.parquet` (one row per vendor x
URL), `comparison_summary.parquet` and `comparison_summary.md`.

## Results

### Summary

Same 1,000 home URLs, fetched the same day (2026-10-20). "Usable" means
the site answered 2xx and the text passes `classify_text_quality`.

| | Firecrawl (full page) | in-house, laptop | in-house, EC2 r5.xlarge |
|---|---|---|---|
| usable | **74%** | 63% | 61% |
| fetched with 2xx | 77% | 77% | 75% |
| expensive path (stealth proxy / browser fallback) | 1% | 26% | 25% |
| median s per fetched page | 3.4 | 1.1 | 2.4 |
| p95 s per page, all attempts | 24.5 | 18.4 | 35.3 |
| throughput achieved | 0.15 pages/s (free plan cap: 10 req/min) | 3.9 pages/s | 2.2 pages/s |
| cost | 895 credits for 1,000 URLs; failed fetches billed 0 | laptop time | instance time, shared with a production job |

**Reachability is the same; the gain is extraction and bot walls.** Both
sides got a 2xx from 77% of the sample. Paired per URL, Firecrawl versus
the laptop run: usable on both 621, Firecrawl only 117, laptop only 9,
neither 253. The 117 Firecrawl-only pages are 55 bot walls our browser
path still cannot get through, 26 pages where our extraction returned thin
text, 20 where it returned nothing even after the browser retry, and 16
that did not resolve or connect from the laptop. The 9 laptop-only pages
are 3 Firecrawl timeouts, 2 vendor 500s, 2 pages Firecrawl saw as 404 and
one as 403. On the stratum built from cache rows whose extraction came
back empty (`empty_combined`), Firecrawl is usable on 68% against our 32
to 34%.

**The links array is a usable substitute, with `onlyMainContent=false`.**
On the 603 URLs whose raw HTML holds internal links, the array recovers a
median 93% of the paths our BeautifulSoup extraction finds (at least 90%
on 64% of pages, under 50% on 8%). After `filter_links` with
`PATHS_TO_KEEP`, the subpages production would choose match with a median
recall of 1.0 (at least 0.9 on 95% of the 445 pages where we pick any).
With Firecrawl's default `onlyMainContent=true` the median recall is 0.31.

**Full-page markdown pads the text.** On the 60 URLs fetched both ways,
verdicts agree on 55; nav and footer text lifted 3 of 9 thin pages over
the 200-character line. Markdown runs 7,586 characters median full-page
against 4,983 main-content-only (2,365 vs 1,758 after stripping link
syntax), and the links array 24 against 9. The padding also lets a 404
page with a long nav pass the gate (40% of the `status_404` stratum reads
"ok" full-page, 12% main-content-only), which is why the usable metric
requires a 2xx.

**Billing and failures.** The balance moved 1,442 to 547 over the full run:
895 credits, exactly the sum of per-page `creditsUsed`, one per fetched
page. The 12 stealth-proxy escalations billed 1 credit each in both places
(the docs quote a higher stealth rate; not observed here). The whole pilot
cost 949 credits. The 105 failed fetches: 60 DNS failures, 15 "all
engines failed", 13 API timeouts (408), 7 connection or tunnel errors, 5
vendor rate-limit 429s that outlasted the harness's 4 attempts, 2 TLS
errors, 3 invalid URLs. 32 URLs hit the 4-attempt retry on terminal 500s
(median 58 s each), which inflates the all-attempts p95; over fetched
pages the p95 is 15.2 s.

**Laptop versus EC2.** The two in-house runs agree on the verdict for 96%
of URLs. The datacenter IP costs 0.4 points of bot walls (6.3% vs 5.9%)
and 8 points on the historically rate-limited stratum. The latency gap is
the machine, not the IP: 4 vCPUs shared with a running ed_tracker job,
where each Chromium fallback took 30 s median against 15 s on the laptop.

**Olostep** (sibling run, 2026-10-19, 60-site panel, not this sample):
40/60 html-usable against 37/60 for the in-house scraper, 8.6 s median
per page, 49 credits reported for 60 requests. Not re-run here.

### Recommendation

**Firecrawl can replace the fetch-and-extract layer behind the existing
cache**, and on this sample it is a clear improvement where it matters
(bot walls, JS shells, pages our extraction returns empty) at no loss in
reachability. Adopt it on these terms:

1. Request `formats=['markdown','rawHtml','links']` with
   `onlyMainContent=false` and `maxAge=0`. Take internal links from the
   array (same-host absolute URL to path, as `compare.vendor_links_as_hrefs`
   does) and keep `filter_links` + `PATHS_TO_KEEP` unchanged. Keeping
   `rawHtml` costs nothing extra and leaves our BeautifulSoup path as a
   fallback.
2. Gate on `metadata.statusCode` (2xx) before `classify_text_quality`;
   full-page markdown of a 404 or a parked page can pass the text gate on
   its own.
3. Store the markdown as `parsed_html`. It carries about a third more
   text than our main-content extraction (nav, footer), which the
   summarizer tolerates and which flipped 3 of 60 verdicts from thin to
   ok. If that padding turns out to matter, run `unstructured` over
   `rawHtml` instead, which keeps today's `parsed_html` semantics for the
   same credit.
4. Treat HTTP 500 with a `SCRAPE_*` code as a terminal failure (dead
   target), and respect the plan's requests-per-minute with backoff: at the
   free plan's 10/min, retries pushed 5 URLs into 429s.
5. Budget: 1 credit per fetched page, failures free. A full refresh of
   the cache (130k homes, 490k pages) is about 490k credits, roughly
   $2,450 at the Hobby plan's marginal $0.005 per credit; larger plans
   quote lower per-credit rates. The free plan's 10 requests/min is for
   pilots only; Standard (500/min, 25 browsers) would put a full refresh
   at about a day of wall time at the measured 3.4 s median.

**Not measured yet**: Spider on the same sample (key pending), subpage
fetches (same call, not sampled here), and throughput and billing at paid
concurrency. Next steps, in order: run `run_spider.py` when the key lands;
a 10k-URL run on a paid plan to confirm throughput and per-credit cost at
scale; then the cache adapter (markdown to `parsed_html`, links array to
`filter_links`) as a `scrape_websites_psql` backend.

### Headline tables (generated by `compare.py`; full per-vendor tables, verdict crosstabs and fetch-mode mixes are in `.cache/vendor_pilot/comparison_summary.md`)

Columns: `firecrawl_mainonly` = Firecrawl default `onlyMainContent=true`, first 60 sampled URLs; `firecrawl` = `onlyMainContent=false`, all 1,000; `inhouse_ec2` / `inhouse_laptop` = our scraper on the same 1,000 URLs the same day, from a datacenter and a residential IP.

#### Usable: site answered 2xx AND the text passes `classify_text_quality`

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 100% | 95% | 89% | 90% |
| empty_combined | 64% | 68% | 32% | 34% |
| status_429 | 89% | 90% | 79% | 87% |
| status_0 | 14% | 32% | 13% | 16% |
| status_404 | 12% | 8% | 6% | 7% |
| ALL | 70% | 74% | 61% | 63% |

#### Share fetched with a 2xx status

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 100% | 97% | 96% | 97% |
| empty_combined | 64% | 75% | 79% | 82% |
| status_429 | 89% | 90% | 84% | 90% |
| status_0 | 29% | 34% | 19% | 22% |
| status_404 | 12% | 9% | 8% | 8% |
| ALL | 72% | 77% | 75% | 77% |

#### Share whose text passes the gate regardless of status (a 404 page with a long nav can pass)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 100% | 95% | 89% | 90% |
| empty_combined | 64% | 70% | 32% | 34% |
| status_429 | 100% | 98% | 79% | 87% |
| status_0 | 14% | 34% | 13% | 16% |
| status_404 | 12% | 40% | 6% | 7% |
| ALL | 72% | 78% | 61% | 63% |

#### Share needing the expensive path (browser fallback / stealth proxy)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 0% | 1% | 24% | 23% |
| empty_combined | 0% | 3% | 60% | 60% |
| status_429 | 0% | 0% | 3% | 7% |
| status_0 | 0% | 2% | 5% | 9% |
| status_404 | 0% | 0% | 3% | 4% |
| ALL | 0% | 1% | 25% | 26% |

#### Median seconds per fetched page

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 2.8 | 3.2 | 1.8 | 0.9 |
| empty_combined | 4.5 | 3.8 | 27.2 | 13.8 |
| status_429 | 3.7 | 5.2 | 1.7 | 0.7 |
| status_0 | 1.8 | 4.2 | 2.3 | 1.7 |
| status_404 | 1.8 | 1.8 | 2.3 | 8.4 |
| ALL | 3.0 | 3.4 | 2.4 | 1.1 |

#### p95 seconds per page (all attempts)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 7.7 | 15.5 | 34.7 | 18.2 |
| empty_combined | 65.3 | 60.4 | 37.9 | 19.9 |
| status_429 | 8.5 | 17.3 | 5.2 | 13.2 |
| status_0 | 2.7 | 101.7 | 8.8 | 12.9 |
| status_404 | 9.3 | 13.7 | 7.2 | 14.7 |
| ALL | 17.9 | 24.5 | 35.3 | 18.4 |

#### Median recall of our HTML link extraction by the vendor links array

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop |
|---|---|---|---|---|
| ok_200 | 0.31 | 0.93 | — | — |
| empty_combined | 0.21 | 0.95 | — | — |
| status_429 | 0.63 | 0.95 | — | — |
| status_0 | 0.38 | 0.97 | — | — |
| status_404 | 0.40 | 0.00 | — | — |
| ALL | 0.31 | 0.93 | — | — |


## Caveats and dead ends

- **Firecrawl's `links` array is only usable with `onlyMainContent=false`.**
  With the default it holds main-content links only (donate buttons,
  external partners), none of the nav links our subpage rules select. See
  the probe above. Either send `false`, or keep our BeautifulSoup extraction
  over `rawHtml` (same credit, no change to our link logic).
- **A text gate alone is not enough on full-page markdown.** A 404 page
  or a parked page with a long nav exceeds the 200-char thin threshold;
  `compare.py` reports "usable" as 2xx and ok for that reason.
- **The harness retried terminal errors in the full Firecrawl run.**
  Firecrawl answers a dead target with HTTP 500 and a `SCRAPE_*` code;
  the run treated 500 as transient and retried up to 4 times with backoff
  (32 URLs, 14 s of sleep each, plus 3 wasted rate-limited calls). Fixed in
  `run_firecrawl.py` after the run; the all-attempts p95 carries it.
- **5 URLs were lost to vendor 429s**: the harness's own limiter sat at
  the plan's 10/min, but retries on top of it exceeded the limit at times.
  They would succeed on a rerun (resume handles it); they count as failed
  here.
- **Stealth escalations billed 1 credit** in this run (12 pages), although
  the pricing docs quote a higher stealth rate. Observed on the free plan;
  check on a paid plan before budgeting on it.
- **The EC2 baseline shared its box with a production job** (ed_tracker,
  one core at 100%) and ran under `nice`, which is most of why its browser
  fallbacks took twice as long as the laptop's.
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
| `run_spider.py` | Spider.cloud scrape runner (`--request-mode chrome` variant) |
| `run_olostep.py` | Olostep scrape runner (request budget, selector list) |
| `run_inhouse.py` | the in-house scraper on the same sample, no cache writes (`--label laptop|ec2`) |
| `remote_baseline.sh` | push the package and sample to an EC2 host, run the baseline under `nice`, pull results |
| `compare.py` | verdicts, usable share, link recall, fetch-mode mix, per-stratum summary and headline pivots -> `comparison_*` |
