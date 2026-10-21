# Vendor pilot: can Firecrawl or Spider replace the in-house fetch-and-extract layer?

**Status (2026-10-21):** Firecrawl, Olostep and Spider (plain and
browser-rendered) measured on the same 1,000-URL sample, against the
in-house scraper run the same day on the same URLs from a laptop and from
an EC2 instance. Short version: Firecrawl gets usable text from 74% of the
sample, Olostep 73% once its unreported 404s are discounted, Spider 67%
(66% with rendering forced), the in-house scraper 61 to 63%. Firecrawl is
the recommendation: it is the only vendor that both handles the
JavaScript-shell and bot-wall pages we fail on today and reports the
target's real status code, which the text gate needs. See
[Results](#results) and [Recommendation](#recommendation).

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

Same 1,000 home URLs, fetched the same day. "Usable" means the site
answered 2xx and the text passes `classify_text_quality`. Olostep reports
status 200 for every page it fetches, so its "as reported" figures include
51 pages the other vendors see as 404; the strict figure removes them.

| | Firecrawl | Olostep | Spider (smart) | Spider (chrome) | in-house, laptop | in-house, EC2 |
|---|---|---|---|---|---|---|
| usable | **74%** | 79% as reported, **73%** strict | 67% | 66% | 63% | 61% |
| fetched with 2xx | 77% | 88% as reported | 80% | 82% | 77% | 75% |
| any content returned | 90% | 88% | 91% | 97% | 77% | 75% |
| expensive path | 1% stealth proxy | always rendered | not reported | always rendered | 26% browser | 25% browser |
| median s per fetched page | 3.4 | 5.8 | 2.1 | 11.4 | 1.1 | 2.4 |
| p95 s per page, all attempts | 24.5 | 56.0 | 37.7 | 102.6 | 18.4 | 35.3 |
| throughput achieved | 0.15 pages/s (free plan cap) | 0.93 pages/s (10 concurrent) | 2.6 pages/s (20 concurrent) | 0.93 pages/s (20 concurrent) | 3.9 pages/s | 2.2 pages/s |
| links array vs our HTML extraction, median recall | 0.93 | 0.93 (0.30 on the empty-extraction stratum) | 1.00 | 1.00 | n/a | n/a |
| billed for the 1,000 URLs | 895 credits | 876 credits over exactly 1,000 calls | $0.37 | $1.41 | laptop time | instance time |
| list price per 1,000 credits or pages | Hobby $3.20, Standard $0.83, Growth $0.67 | Starter $1.80, Standard $0.50, Scale $0.40 | $0.37 measured (bytes + CPU) | $1.41 measured | | |

**Reachability is the same; the differences are extraction, rendering
and bot walls.** Every fetcher got a 2xx from 75 to 80% of the sample
(Olostep's 88% is its own fetch status, not the target's). Paired per URL
against the in-house laptop run, usable on the vendor only / on the laptop
only:

| vendor | vendor only | laptop only | what the vendor-only pages were on the laptop |
|---|---|---|---|
| Firecrawl | 117 | 9 | 55 bot walls, 26 thin, 20 empty, 16 unreachable |
| Olostep (strict) | 130 | 25 | 56 bot walls, 28 unreachable, 26 thin, 18 empty |
| Spider (smart) | 104 | 63 | 56 bot walls, 26 unreachable, 13 thin, 9 empty |

and against Firecrawl: Olostep (strict) wins 28 and loses 31 (a wash: its
losses are 17 pages where its own scrape timed out or failed TLS and 14
thin extractions; 8 of its wins are pages Firecrawl saw as HTTP 402, which
Olostep reports as 200); Spider smart wins 23 and loses 90; Spider chrome
wins 42 and loses 119. On the stratum built from cache rows whose
extraction came back empty (`empty_combined`), Firecrawl and Olostep are
usable on 68 and 67%, Spider on 50% (58% rendered), the in-house scraper
on 32 to 34%.

**Spider's gap is rendering.** Of its 124 pages that answered 2xx but came
back thin or empty in smart mode, 95 are JavaScript app shells (median raw
HTML 3 KB) fetched over plain HTTP. Forcing `request='chrome'` made 26 of
those 95 usable; Firecrawl makes 67 of them usable. 83 chrome-mode pages
came back as an empty document (`<html><head></head><body></body></html>`)
with a 525 or 504 status, so Spider's browser path fails where Firecrawl's
and Olostep's succeed, at four times the price and five times the latency
of its smart mode.

**Links.** With `onlyMainContent=false` Firecrawl's links array recovers
a median 93% of the internal links our BeautifulSoup extraction finds in
its raw HTML (at least 90% on 64% of pages); after `filter_links` the
chosen subpages match with a median recall of 1.0 (at least 0.9 on 95% of
the pages where we pick any). Olostep's array is comparable overall (0.93,
at least 0.9 on 52%) but drops to 0.30 on the empty-extraction stratum,
exactly the pages a vendor is for; after `filter_links` it is 1.0 median,
at least 0.9 on 91%. Spider's array matches our extraction on 94% of pages,
by construction: both come from the same HTTP source. With Firecrawl's
default `onlyMainContent=true` the median recall is 0.31.

**Full-page markdown pads the text.** On the 60 URLs Firecrawl fetched
both ways, verdicts agree on 55; nav and footer text lifted 3 of 9 thin
pages over the 200-character line. Markdown runs 7,586 characters median
full-page against 4,983 main-content-only (2,365 vs 1,758 after stripping
link syntax), and the links array 24 against 9. The padding also lets a
404 page with a long nav pass the gate (40% of the `status_404` stratum
reads "ok" full-page, 12% main-content-only), which is why the usable
metric requires a 2xx, and why Olostep's missing status codes matter.

**Billing and failures.**

- *Firecrawl*: the balance moved 1,442 to 547 over the full run, 895
  credits, exactly the sum of per-page `creditsUsed`: one per fetched page,
  the 105 failures free, the 12 stealth-proxy escalations billed 1 credit
  each (the docs quote a higher stealth rate; not observed on the free
  plan). The pilot cost 949 credits in all. Failures: 60 DNS, 15 "all
  engines failed", 13 API timeouts (408), 7 connection or tunnel errors, 5
  vendor rate-limit 429s that outlasted the harness's attempts, 2 TLS, 3
  invalid URLs.
- *Olostep*: 876 credits over exactly 1,000 API calls (the run's cap),
  one per fetched page, failures free; `cost_usd` came back null on the
  free plan and $0.92 on the 521 pages fetched after the top-up (about
  $1.76 per 1,000 fetched pages, the Starter rate). The run was split
  across two sessions: the free plan's credits ran out after 479 URLs
  (`402 credits_exhausted`), Zein bought credits, and the resume fetched
  the remaining 521 under the same settings and the same cap. Failures:
  57 DNS, 50 of its own 55-second scrape timeouts, 17 TLS, 1 empty.
- *Spider*: $0.37 for the smart run and $1.41 for the chrome run from the
  per-page `costs.total_cost` (10,000 credits = $1), which matched the
  `/data/credits` balance delta on every run that did not overlap a
  top-up (the balance rose from 2,488 to 248,819 credits during the smart
  run). Cost is 98% correlated with raw HTML size. Failures (smart): 49
  "domain does not exist", 4 empty, 2 "Malicious URL not allowed", 2
  unreachable, 1 geo-blocked, others; chrome: 29.
- *In-house*: 231 of 1,000 fetches failed on the laptop (119 HTTP errors,
  106 dead links), 255 went through the Playwright fallback.

**Laptop versus EC2.** The two in-house runs agree on the verdict for 96%
of URLs. The datacenter IP costs 0.4 points of bot walls (6.3% vs 5.9%)
and 8 points on the historically rate-limited stratum. The latency gap is
the machine, not the IP: 4 vCPUs shared with a running ed_tracker job,
where each Chromium fallback took 30 s median against 15 s on the laptop.

### Recommendation

**Adopt Firecrawl for the fetch-and-extract layer behind the existing
cache.** It is the only vendor that is best or tied on usable text *and*
reports the target's real status code, and its advantage is concentrated
where our scraper fails today: JavaScript shells, bot walls and pages our
extraction returns empty. Terms:

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
   the cache (130k homes, 490k pages, about 440k fetched) fits in one
   month of the Growth plan ($333/month, 500k credits, 50 concurrent
   browsers, 5,000 requests/min); Standard ($83/month, 100k credits, 25
   browsers) covers incremental runs. At the measured 3.4 s median, 25
   concurrent browsers put a full refresh at roughly a day of wall time.

**Olostep** is the second choice: text parity with Firecrawl (73% strict
vs 74%) at a lower list price ($0.50 vs $0.83 per 1,000 on the Standard
plans), but it reports 200 for every page it fetches (the 2xx gate cannot
work; 51 of its "usable" pages are 404s the others catch), 5% of pages
hit its 55-second timeout, median latency is 5.8 s, and its links array
misses most of our links on the empty-extraction stratum. Choose it only
if price dominates and we add our own 404-page detection.

**Spider** is the cheapest ($0.37 per 1,000 pages) and the fastest over
HTTP, with a perfect links array and the same strength on bot walls, but
its rendering is the weakest: it leaves the JavaScript shells that are our
main failure class unrendered, and forcing Chrome recovers a quarter of
them at four times the price and five times the latency, with 83 empty
documents. It does not fit fetch-and-extract across our population on its
own. A two-tier design (Spider smart first, Firecrawl for the pages that
come back thin or empty) would cut Firecrawl credits by roughly two
thirds; worth costing only once the single-vendor path is in place.

**Not measured**: subpage fetches (same call, not sampled here) and
throughput and billing at paid concurrency. Next steps, in order: a
10k-URL run on a paid Firecrawl plan to confirm throughput and per-credit
cost at scale; then the cache adapter (markdown to `parsed_html`, links
array to `filter_links`) as a `scrape_websites_psql` backend.

### Headline tables (generated by `compare.py`; full per-vendor tables, verdict crosstabs and fetch-mode mixes are in `.cache/vendor_pilot/comparison_summary.md`)

Columns: `firecrawl` = `onlyMainContent=false`, all 1,000; `firecrawl_mainonly` = Firecrawl default, first 60 sampled URLs; `olostep` = all 1,000 across two sessions (free credits ran out at 479, resumed after a top-up); `spider` = `request='smart'`; `spider_chrome` = `request='chrome'`; `inhouse_ec2` / `inhouse_laptop` = our scraper on the same 1,000 URLs the same day (2026-10-20), from a datacenter and a residential IP. Olostep's 2xx and usable rows are inflated: it reports status 200 for pages the other vendors see as 404 (see Caveats).

#### URLs attempted (a run cut short by credits covers a stratified prefix of the sample)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 25 | 500 | 500 | 500 | 500 | 500 | 500 |
| empty_combined | 11 | 200 | 200 | 200 | 200 | 200 | 200 |
| status_429 | 9 | 100 | 100 | 100 | 100 | 100 | 100 |
| status_0 | 7 | 100 | 100 | 100 | 100 | 100 | 100 |
| status_404 | 8 | 100 | 100 | 100 | 100 | 100 | 100 |
| ALL | 60 | 1000 | 1000 | 1000 | 1000 | 1000 | 1000 |

#### Usable: site answered 2xx AND the text passes `classify_text_quality`

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 100% | 95% | 89% | 90% | 94% | 85% | 90% |
| empty_combined | 64% | 68% | 32% | 34% | 68% | 58% | 50% |
| status_429 | 89% | 90% | 79% | 87% | 97% | 69% | 95% |
| status_0 | 14% | 32% | 13% | 16% | 32% | 23% | 23% |
| status_404 | 12% | 8% | 6% | 7% | 54% | 25% | 6% |
| ALL | 70% | 74% | 61% | 63% | 79% | 66% | 67% |

#### Share fetched with a 2xx status

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 100% | 97% | 96% | 97% | 97% | 96% | 97% |
| empty_combined | 64% | 75% | 79% | 82% | 84% | 85% | 86% |
| status_429 | 89% | 90% | 84% | 90% | 100% | 92% | 98% |
| status_0 | 29% | 34% | 19% | 22% | 40% | 39% | 37% |
| status_404 | 12% | 9% | 8% | 8% | 83% | 34% | 8% |
| ALL | 72% | 77% | 75% | 77% | 88% | 82% | 80% |

#### Share whose text passes the gate regardless of status (a 404 page with a long nav can pass)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 100% | 95% | 89% | 90% | 94% | 86% | 90% |
| empty_combined | 64% | 70% | 32% | 34% | 68% | 58% | 50% |
| status_429 | 100% | 98% | 79% | 87% | 97% | 69% | 95% |
| status_0 | 14% | 34% | 13% | 16% | 32% | 25% | 25% |
| status_404 | 12% | 40% | 6% | 7% | 54% | 46% | 24% |
| ALL | 72% | 78% | 61% | 63% | 79% | 69% | 69% |

#### Share needing the expensive path (browser fallback / stealth proxy)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 0% | 1% | 24% | 23% | 0% | 0% | 0% |
| empty_combined | 0% | 3% | 60% | 60% | 0% | 0% | 0% |
| status_429 | 0% | 0% | 3% | 7% | 0% | 0% | 0% |
| status_0 | 0% | 2% | 5% | 9% | 0% | 0% | 0% |
| status_404 | 0% | 0% | 3% | 4% | 0% | 0% | 0% |
| ALL | 0% | 1% | 25% | 26% | 0% | 0% | 0% |

#### Median seconds per fetched page

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 2.8 | 3.2 | 1.8 | 0.9 | 6.1 | 10.6 | 2.0 |
| empty_combined | 4.5 | 3.8 | 27.2 | 13.8 | 5.7 | 12.0 | 2.6 |
| status_429 | 3.7 | 5.2 | 1.7 | 0.7 | 5.9 | 17.8 | 2.2 |
| status_0 | 1.8 | 4.2 | 2.3 | 1.7 | 4.8 | 15.8 | 2.2 |
| status_404 | 1.8 | 1.8 | 2.3 | 8.4 | 3.4 | 6.1 | 1.7 |
| ALL | 3.0 | 3.4 | 2.4 | 1.1 | 5.8 | 11.4 | 2.1 |

#### p95 seconds per page (all attempts)

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 7.7 | 15.5 | 34.7 | 18.2 | 17.4 | 78.3 | 11.1 |
| empty_combined | 65.3 | 60.4 | 37.9 | 19.9 | 56.1 | 105.2 | 51.6 |
| status_429 | 8.5 | 17.3 | 5.2 | 13.2 | 18.7 | 105.2 | 29.1 |
| status_0 | 2.7 | 101.7 | 8.8 | 12.9 | 60.5 | 105.2 | 75.2 |
| status_404 | 9.3 | 13.7 | 7.2 | 14.7 | 56.5 | 93.0 | 44.9 |
| ALL | 17.9 | 24.5 | 35.3 | 18.4 | 56.0 | 102.6 | 37.7 |

#### Median recall of our HTML link extraction by the vendor links array

| stratum | firecrawl_mainonly | firecrawl | inhouse_ec2 | inhouse_laptop | olostep | spider_chrome | spider |
|---|---|---|---|---|---|---|---|
| ok_200 | 0.31 | 0.93 | — | — | 0.82 | 1.00 | 1.00 |
| empty_combined | 0.21 | 0.95 | — | — | 0.30 | 1.00 | 1.00 |
| status_429 | 0.63 | 0.95 | — | — | 0.99 | 1.00 | 1.00 |
| status_0 | 0.38 | 0.97 | — | — | 1.00 | 1.00 | 1.00 |
| status_404 | 0.40 | 0.00 | — | — | 1.00 | 1.00 | 1.00 |
| ALL | 0.31 | 0.93 | — | — | 0.93 | 1.00 | 1.00 |

## Phase 2: end to end over 2,000 organizations (built, awaiting the paid run)

Phase 1 measured home pages. Phase 2 runs the whole per-org path
production takes, on a representative (not hard-case) sample, for
Firecrawl, for the in-house scraper the same day, and against the cache,
and simulates the hybrid Zein is considering.

**Sample** (`sample_orgs.py`): 2,000 organizations drawn uniformly from
the US Climate Finance Tracker org parquet,
`s3://climate-landscape-data/2026_08_04_oe_hierarchy_update/results/cb_cd_li_meta_mapped.parquet`
(18,480 rows; `prediction_relevant` true and a non-empty `Website`; one
org per domain; platform and free-mail domains dropped), seed 42. 1,707
of them are in the cache; their historical `combined_text` verdicts are
1,406 ok, 268 dead, 13 parked, 13 thin, 7 blocked; the cache holds a
median of 1 and a mean of 3.0 subpages per org. Each org carries its
cache row (home status, text verdicts, stored subpaths) for the
historical comparison. A `--source cache` fallback draws random index
rows instead.

**Per-org pipeline** (`_e2e.py`, the same for every runner): fetch the
home page; unless it is a single-page platform, choose subpages with
`filter_links` over the internal links (`PATHS_TO_KEEP`,
`max_per_subpath=6`, `subpage_type='about'`), clean each with
`scrape_websites.__clean_website_path`, join with `urljoin`; fetch each;
build the combined text with `make_group_text` over the 2xx pages (same
ordering, token budget and counting prompt as `scrape_websites_psql`);
classify it with `classify_text_quality`. "Usable" = home 2xx and
combined verdict ok. Where the internal links come from is recorded per
org:

- `run_firecrawl_e2e.py`: `POST /v2/scrape` per page with the phase-1
  settings (`markdown`, `rawHtml`, `links`; `onlyMainContent=false`;
  `maxAge=0`; `proxy='auto'`). Subpages come from `filter_anchors` +
  `filter_links` over the vendor's **links array** (`picks_array`); the
  set production would compute from `rawHtml` is recorded alongside
  (`picks_html`). Page text = markdown. Concurrency at the Standard plan's
  limits (25 in flight, 500/min); a 402 stops the run; 429/5xx retries are
  counted. `--fake` substitutes a deterministic vendor (no network, no
  credits) for dry runs.
- `run_inhouse_e2e.py`: production's own path (`AsyncScraper`,
  `_process_scraped_with_browser_retry`, `extract_website_links` over the
  fetched HTML, `unstructured` text), no cache writes, 20 HTTP + 3 browser
  concurrency. Page text = `parsed_html`.
- `run_hybrid_topup.py`: fetches through Firecrawl exactly the subpages the
  hybrid would send it (in-house home usable, in-house subpage not) that
  the vendor-only run did not already fetch, so the hybrid is assembled
  from real vendor results rather than assumed ones.
- `compare_e2e.py`: per org and aggregate: usable for vendor, in-house,
  cache and hybrid; credits (home + subpages, failures free); subpages
  chosen by each and their overlaps (vendor array vs vendor HTML, vs
  in-house picks, vs cache subpaths); wall time, pages/second and API
  retries from the run meta. The hybrid takes the vendor wholesale when
  the in-house home is not usable (the vendor-only run's home and
  subpages), otherwise only the in-house subpages that are not usable;
  its combined text is rebuilt with `make_group_text`; its credits are
  the vendor pages used.

**Dry run** (2026-10-21): the fake vendor over 40 orgs, the in-house
runner over 5 (all 5 usable; 36 pages; 6.2 subpages chosen per org), the
top-up and the comparison all run end to end from the repo root:

```bash
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_orgs
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e --fake --limit 40 --no-resume
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse_e2e --label laptop --limit 5
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_hybrid_topup --fake --inhouse inhouse_laptop
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare_e2e --vendor fake --inhouse inhouse_laptop
```

**The paid run** (not started; needs the Standard plan, roughly 9k
credits: 2,000 homes plus about 3 subpages each at 1 credit per fetched
page):

```bash
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e --limit 20   # smoke, ~100 credits
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e              # resumable
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_inhouse_e2e --label laptop  # same day
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_hybrid_topup --inhouse inhouse_laptop
python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare_e2e
```

<!-- PHASE2 RESULTS -->

## Caveats and dead ends

- **Firecrawl's `links` array is only usable with `onlyMainContent=false`.**
  With the default it holds main-content links only (donate buttons,
  external partners), none of the nav links our subpage rules select. See
  the probe above. Either send `false`, or keep our BeautifulSoup extraction
  over `rawHtml` (same credit, no change to our link logic).
- **Olostep reports status 200 for every page it fetches**, including
  the 33 pages in the `status_404` stratum that Firecrawl and Spider both
  report as 404 and the 8 pages Firecrawl saw as 402. Its "2xx" is its own
  fetch status. The README quotes a strict usable share with the pages the
  other two vendors both call 404 removed; production would need its own
  404-page detection on Olostep output.
- **Olostep's run was split across two sessions.** The free plan's
  credits ran out after 479 URLs (`402 credits_exhausted`, 2026-10-21
  14:16 PDT); Zein bought credits and the resume fetched the remaining
  521 (14:28 to 14:38 PDT) under the same settings and the same 1,000-call
  cap, which the run hit exactly. The shuffled sample keeps both halves
  stratified; the two halves' timings are 12 minutes apart.
- **Olostep's selector setting is not what was asked for** (`"none"`):
  see Vendor settings. The JSON-stringified form of a custom selector list
  is silently ignored by the API; send a JSON array.
- **Spider refused 2 URLs as "Malicious URL not allowed"** and
  geo-blocked 1 ("could not be served from country 'co'"); the smart run's
  balance was topped up mid-run, so its spend is the per-page cost sum.
- **Spider chrome returned 83 empty documents** with 525/504 statuses on
  pages Firecrawl and Olostep render; its browser path, not the targets,
  is the limit.
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
