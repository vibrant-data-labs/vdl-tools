# Firecrawl scraping adapter

Everything that turns a Firecrawl response into the text and subpage picks the rest of the
pipeline reads lives in this package, apart from the in-house scraper it replaces. When the
in-house path is retired (plan Phase 7, D14), its modules in `scraper/` are deleted and this
package is untouched. The plan is `docs/FIRECRAWL_ADAPTER_PLAN.md`; the canonical spec and
issues are Linear project P-VIB-2. The vendor benchmark that chose Firecrawl stays in
`scraper/vendor_pilot/`, since it compares four vendors.

| path | what it is | issue |
|---|---|---|
| `markdown_clean.py` | the text rules: browser chrome out, markers once per page, the cleaner, per-org dedupe and the 8k-token cap; `page_text` per page, `org_text` per org; `CLEANER_VERSION` | D2, wired in by D5 |
| `site_links.py` | `same_site_paths`: links on the host the page was served from, ahead of `filter_links` | D2, used by D7 |
| `client.py` | `FirecrawlClient`: `POST /v2/scrape` under the account limits, with retries, the target-versus-vendor failure split, the 402/401 stop, the credit ceiling, `dry_run` and the balance before and after a run | D4, used by D7 |
| `validation/` | one-off measurements over the pilot artifacts that the adoption decision rests on | D2, D3, D9 |
| `tests/` | unit, golden and determinism tests; `fixtures/markdown/` holds real pilot pages; `test_client.py` runs the client on an httpx mock transport | |

Still to come here: the per-org pipeline `scrape_websites_firecrawl` (D7, `pipeline.py`).
The ORM models for the new tables (D6) go with the other models in
`shared_tools/database_cache/database_models/`.

## The client (D4)

One `FirecrawlClient` per process ([VIB-56](https://linear.app/vibrant-data-labs/issue/VIB-56)).
It owns the account-wide limits, since Firecrawl counts browsers and requests per account:
25 in flight and 500 per minute on the Standard plan, read from `[scraping_vendors]`
(`firecrawl_api_key`, optional `firecrawl_concurrency` and `firecrawl_rpm`). The key is
never logged, printed or part of `repr`.

```python
from vdl_tools.scrape_enrich.scraper.firecrawl.client import FirecrawlClient, RunStopped

async with FirecrawlClient.from_config(credit_ceiling=10_000) as client:   # reads the balance
    result = await client.scrape("https://example.org")     # ScrapeResult; raises RunStopped on 402/401/ceiling
    result.ok, result.failure_reason, result.counts_as_error
    result.status_code, result.final_url, result.final_host, result.markdown, result.links
client.stats            # balance before and after, credits used, retries, failures, stop reason
```

Every page is sent the same profile (Q2): `formats [markdown, links]`, `onlyMainContent:
false` (Firecrawl's default is `true`, which also drops the navigation links the subpage
rules read), `skipTlsVerification: true` (the vendor's default flipped to verifying
certificates after the pilot, which turned 56 of 3,000 home pages into `SCRAPE_SSL_ERROR` in
the 2026-11-06 functional test; the pilot measured with it skipped), `maxAge 0`, `proxy auto`,
a 60 s page timeout. `payload_overrides` adds or replaces fields for one-off runs;
`client.request_profile` is what each page was sent, for `scrape_runs.request_profile`.

Lineage the client carries but does not use, so a result is a page row without a side
lookup and joins back to where it came from:

| field | what it is | set by |
|---|---|---|
| `requested_url` | the caller's string byte for byte: the join key back to the source frame's website column | the caller's argument |
| `url` | what was sent: `ensure_url_scheme` of the value when its host is the one `website_domain` finds (path and port kept), else the repaired host (`https://michigan bulls.com` → `https://michiganbulls.com`, `info@x.org` → `https://x.org`) | `normalize_url`, from the two `url_keys` helpers |
| `final_url`, `final_host` | where Firecrawl was served from after redirects | the vendor |
| `home_url` | the org's requested home URL, verbatim: the parent of a subpage and the cache's `cleaned_home_key`; never `final_url`, since the cache key is untouched | `scrape(..., home_url=)` |
| `page_type` | `"index"` for the home page, `"page"` for a subpage; plain strings, D6 picks the stored literal | `scrape(..., page_type=)` |
| `run_id` | the client's run, also on `client.stats` | `FirecrawlClient(run_id=)` |

The client cannot infer the last three: subpage URLs are built on the home page's
`final_url`, another host for 11% of orgs, and home and subpages get the same request.
Lookup by org id (a Crunchbase uuid, an EIN, an NZI id) is D16's `org_site_keys`, not this
module.

What a page's `failure_reason` means, and who pays for it (credits as the 2026-11-06
functional test measured them, where `credits_used` matched the balance change exactly):

| `failure_reason` | when | counts as an error | bills a credit |
|---|---|---|---|
| `dead` | a `SCRAPE_*` code other than `SCRAPE_TIMEOUT` (DNS, SSL, every engine failed, unsupported file), at API status 500 or 200 | yes | no |
| `http_error` | the target answered, but not 2xx (`metadata.statusCode`); the markdown is kept | yes | yes |
| `blocked` | the target answered 401, 403 or 429 | yes | yes |
| `invalid_url` | names no site, so never sent: blank, a placeholder, no dotted host, a platform or free-mail domain, as `url_keys.website_domain` judges it; or a 400 from Firecrawl for a bad top-level domain | yes | no |
| `vendor_error` | a 408 scrape timeout, a 429 or 5xx that outlived four attempts, a transport error, an answer with no status code | no | no |

Only target failures increment `num_errors`. The climate caller runs with `max_errors=1`, so
a vendor-side 408 recorded as an error would skip the org for good and send it to the paid
Coresignal fallback; the pilot saw 13 timeouts per 1,000 home pages. A 402 (out of credits)
or 401 (bad key) raises `RunStopped` for that page and every page not yet sent; pages
already in flight finish and are kept, and a page waiting to retry sends nothing more. The
credit ceiling stops the run the same way, and so does a 400 whose validation details name
anything but `url` (another field, or the body itself: an unrecognized key): that is our
request body (a bad `payload_overrides`), identical for every page, and must not become a
target failure on each of them. 429s and 5xx retry with `Retry-After` when the header is present, the "retry after 17s" in
Firecrawl's 429 body when it is not, else exponential backoff; a page holds its browser slot
through its retries.

Dry run, against today's `web_pages_scraped` and spending nothing (the balance is one
`GET /team/credit-usage`):

```
python -m vdl_tools.scrape_enrich.scraper.firecrawl.client --dry-run \
    --urls .cache/vendor_pilot/sample_urls.parquet --limit 20 --credits-per-miss 4.7
```

It prints the cached, to-fetch and invalid counts, the estimated credits (1 per miss for a
home page alone, 4.7 per org with its subpages in the pilot), the ceiling check and the
balance. D7 replaces the cache lookup with the v2 tables, which also know a row's age.

Tests run by path:

```
pytest vdl_tools/scrape_enrich/scraper/firecrawl/tests
```

From a worktree, set `PYTHONPATH` to the worktree root first; the editable install points at
the main checkout. A rule change that alters any golden output fails the tests until
`CLEANER_VERSION` is bumped and the files are regenerated with `REGENERATE_GOLDEN=1`.

## Phase 0 rescore: the production text rules (run 2026-10-30, zero credits)

D2 of the adapter project ([VIB-53](https://linear.app/vibrant-data-labs/issue/VIB-53)).
`validation/rescore_text_rules.py` replays the rules in `markdown_clean.py` and `site_links.py`
over the vendor pilot's saved artifacts: every 2xx phase 2 page through `markdown_clean.page_text`, every org through
`markdown_clean.org_text`, and the phase 1 links arrays through `same_site_paths`.

```
python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.rescore_text_rules --out-dir .cache/vendor_pilot
```

Outputs in `.cache/vendor_pilot/`: `rescore_orgs.parquet` (each org's Firecrawl combined
text under the production rules next to its in-house combined text from the same day; the
input to the paired LLM test, D3), `rescore_pages.parquet`, `rescore_picks.parquet` and
`rescore_summary.md`. Each parquet's lineage footer records `cleaner_version`, `extractor`,
the cap and the git sha. The numbers below are `CLEANER_VERSION` 1, `firecrawl-md-v1`.

| measure | pilot, raw markdown | production rules | in-house, same day |
|---|---|---|---|
| usable orgs (home 2xx, verdict ok) | 86.0% | 85.3% | 78.0% |
| median characters per 2xx page | 11,335 | 3,307 | 2,633 |
| tokens per org, mean / median / p90 | 21,726 / 9,988 / 51,606 | 3,114 / 2,103 / 7,999 | 4,371 / 1,907 / 10,750 |
| orgs over 8k tokens | 1,002 | 0 (228 cut by the cap) | 269 |
| summarizer input per 30k orgs, gpt-4.1-mini | $261 | $37 | $52 |
| characters in lines repeated across an org's pages | 50.5% | 0 (28.8% before dedupe) | 20.2% |

Paired with in-house: usable on both 1,547, Firecrawl only 159, in-house only 13, neither
281. Before the cap, cleaned and deduped text is 4,271 tokens per org on average, level with
in-house text.

Sixteen orgs usable in the pilot are not usable under the rules, and on inspection the rules
are right about them: 10 `thin` ("under construction", "Site Not Found", "Sorry, nothing
found", a countdown timer, a private WordPress site; raw URLs had pushed them past 200
characters), 4 `blocked` (three parked domains that redirect to an ad host, one for-sale page
behind a Cloudflare check), 2 `empty` (one-page sites whose cleaned home text is under the
50 characters `make_group_text` requires). Two orgs went the other way: the pilot marked them
parked because of a URL substring in raw markdown (`dan.com` inside another domain) or one
subpage selling coffee that "is available for purchase"; the rules remove URLs before the
markers and drop the junk page instead of the org.

What the data changed in the rules, against plan section 5 as written:

1. **Markers run once, per page, on plain text.** Running the wall markers again on cleaned
   text called 66 real pages walls: cleaning made them short and their footers still
   mentioned reCAPTCHA. The cleaned page and the combined text get the length check only;
   a junk home page makes the org junk, and other junk pages are left out of the text.
2. **Chrome's blocked-frame error page is noise, not a marker.** 363 pages of 61 orgs carry
   "This page has been blocked by an extension"; 356 are real pages with a refused analytics
   or shop frame. The block (and its "refused to connect" and "blocked by Chromium" variants)
   is stripped everywhere, and a page is `blocked` only when under 200 characters remain
   without it. The plan's "own marker with no length cap" would have marked 61 orgs blocked.
3. **Captcha widgets and notices are stripped the same way.** 1,389 pages carry the widget;
   on a short page the `captcha` wall marker took it for a challenge screen (8 pages, 2 of
   them home pages).
4. **Unicode spaces are normalized before `clean_scraped_text`.** Half the pages carry
   non-breaking spaces, and `clean_non_ascii_text` deletes them before its `\xa0`
   replacement runs, gluing words ("formed in 2017focused"). The shared function has the
   same bug for in-house text; fixing it there would change every in-house text and its cache
   key, so it is left for its own PR.
5. **Hard line breaks and bold markers go.** 88,927 trailing-backslash breaks in the phase 2
   pages, bold markers on 5,708.

Subpage picks on the 766 fetched phase 1 home pages: 88.8% identical, Jaccard median 1.00,
mean 0.87. The 92 home pages served from another host get 302 picks instead of none. On the
same host 27 pages differ, for three reasons: query strings are dropped (product variants
stop counting as separate pages), file links are dropped (one site's picks had been folder
names read off a PDF link), and `.php` pages are allowed.

The golden tests in `tests/test_markdown_clean.py` pin the rules down on real pages
from this run (`tests/fixtures/markdown/cases.json` says what each one is for),
including the two parked link-line pages, a frame-only redirect, a real page with a refused
frame, a reCAPTCHA form, a real wall and one four-page org for dedupe and the cap. A rule
change that alters any output fails them until `CLEANER_VERSION` is bumped and the files are
regenerated.

## D3 paired LLM test: what the new text does downstream (run 2026-10-30)

D3 of the adapter project ([VIB-54](https://linear.app/vibrant-data-labs/issue/VIB-54)), the
evidence for the adoption decision (Q1). `validation/paired_llm_test.py` runs each pilot org
through the tracker's own LLM stages three times: on its Firecrawl text under
`firecrawl-md-v1`, on the in-house text from the same day, and on the in-house text again.
The third run is the noise floor, because identical input still gives different output. The
stages are the website summary, the summary of summaries, relevance (only orgs whose
description is under 100 characters read the website summary), the Baseten adaptation filter
and the One Earth mapping, each with its production prompt, model and code path. The shared
prompt cache is neither read nor written; results are resumable JSONL in
`.cache/vendor_pilot/d3/`, and `d3_summary.md` there is the full report.

```
python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.paired_llm_test --out-dir .cache/vendor_pilot
```

### Orgs both scrapers could read (1,547)

| measure | Firecrawl vs in-house | in-house vs its rerun (noise) | difference, 95% interval |
|---|---|---|---|
| website summary, cosine | 0.956 | 0.970 | -0.014 (-0.017 to -0.012) |
| summary of summaries, cosine | 0.953 | 0.955 | -0.002 (-0.005 to 0.000) |
| One Earth level 1, orgs whose nodes changed | 31.9% | 28.8% | Jaccard -0.021 (-0.042 to 0.001) |
| One Earth level 2, orgs whose nodes changed | 40.5% | 40.1% | Jaccard -0.008 (-0.030 to 0.015) |
| relevance flips, 107 orgs with short descriptions | 11 | 5 | 5.6 points (0 to 11) |
| tracker membership flips | 15 (1.0%) | 9 (0.6%) | 0.4 points (-0.1 to 0.9) |

- The summary the tracker shows, the summary of summaries, moves no more than rerunning the
  same text does: 5.4% of orgs fall below the noise floor's 5th percentile, against 5% by
  chance. The website summary itself moves a little more (12.5%), as it should with
  different input, and the other text fields absorb it.
- The One Earth mapping changes one to three points more than its own noise at each level,
  and its own noise is large: rerunning it on identical text changes the level-1 nodes of 29%
  of orgs and the level-2 nodes of 40%. The mapper's instability, not the vendor, dominates.
- Membership: 9 orgs would leave the tracker and 6 would join, against 9 changes from rerun
  noise alone; the adaptation filter flips 0.3% either way.
- A gpt-4.1 judge on 300 sampled orgs, shown both scrapes and both website summaries in
  random order, preferred Firecrawl's summary for 108, the in-house one for 84 and called 108
  ties. It flagged an unsupported statement in 5.7% of Firecrawl summaries and 11.7% of
  in-house ones.

### Orgs only Firecrawl could read (159)

Their in-house chain had no website summary, so the comparison is with and without one, and
it moves everything, as new information should: the summary of summaries (cosine 0.885) and
the One Earth level-1 nodes of 44% of them. The judge checked each Firecrawl summary against
its scrape (96.9% faithful, 94.3% specific) and flagged 16 as not the organization's site.

Reading all 159 found more: **18 are registrar sale pages**, mostly GoDaddy's current
for-sale page, which puts the domain on its own line between "The domain name" and "is for
sale!" so no parked marker matched it. The judge flagged only 10 of them, because the other
8 summaries honestly say the domain is for sale; all 18 would still put a sale page's
summary in the tracker ("Chlorofill facilitates the buying and leasing of domain names"),
and Calxeda's was invented from the company's name. vibrant-data-labs/vdl-tools#251 adds a
rule to the shared gate for these pages. Rerun through the D2 rescore with that rule, usable
moves from 85.3% to 84.4%, Firecrawl-only rescues from 159 to 141, and the 1,547 paired orgs
are untouched; the same rule flips 36 for-sale pages in the in-house cache. The text is
unchanged, so `CLEANER_VERSION` stays 1 and the paired measures above stand.

The other 6 the judge flagged are not a text-rule problem: 2 gambling sites on lapsed
domains, which the tracker's junk-summary rule already catches; 2 squatters with unrelated
content (an SEO agency, a generic nonprofit template); and 2 that are the real successor
sites, Heila Technologies now part of Rehlko and Wyncote Audubon renamed Liberty Bird
Alliance. Those two belong to org dedupe through `final_host`, not to the junk gate.

### Cost

| stage | per 1,000 chain runs |
|---|---|
| website summary (gpt-4.1-mini) | $2.13 |
| summary of summaries (gpt-4.1-mini) | $0.46 |
| One Earth (gpt-5.4-mini; 62% of input is cached, billed here at 10%) | $7.40 |
| relevance (fine-tuned gpt-4o-mini, short descriptions only) | under $0.01 per org overall |
| adaptation (Baseten, CPU replicas) | not metered here; about 8 minutes for 2,000 texts at 6 replicas |

Re-running the downstream stages for an org whose text changed costs about $10 per 1,000
orgs, about $300 for a 30k-org refresh, plus Baseten time. D3 itself cost about $56 in OpenAI
calls (at most $83 if cached input were billed in full), including a $5 judge.

Two engineering notes from the run. The shared OpenAI client in `openai_api_utils` keeps the
library's 600-second timeout with four retries, so one silent connection stalled this run for
37 minutes; D3 uses a copy with a 90-second timeout, and production runs have the same
exposure. The adaptation stage sends chunks in parallel the way `bulk_predict` does since
#241, without that method's cache writes.

Human spot checks: `.cache/vendor_pilot/d3/d3_spot_checks.xlsx` holds 50 orgs (20 Firecrawl-only,
20 with the largest length difference, 10 at random) with both summaries, the judge's verdict
and empty columns for a reviewer.

## D9: main-content-only subpages (run 2026-11-03, 1,967 credits)

D9 of the adapter project ([VIB-61](https://linear.app/vibrant-data-labs/issue/VIB-61)), the
evidence for Q2: fetch subpages with Firecrawl's `onlyMainContent=true`, which strips navigation,
header and footer at the source, or full-page as the home page must be? `validation/subpage_main_content.py`
fetched every subpage of 200 seeded pilot orgs both ways on the same day and scored both with the
production text rules against the in-house text of the same pages.

```
python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.subpage_main_content --out-dir .cache/vendor_pilot
```

| measure (199 orgs, 977 subpages) | full page | main content only |
|---|---|---|
| sentence recall of the in-house text, pooled | 0.915 | 0.880 |
| median cleaned characters per subpage | 3,720 | 2,801 |
| duplicated-line share across an org's subpages, before dedupe | 19.2% | 12.4% |
| tokens per org after the rules (mean) | 3,721 | 3,642 |
| tokens per org before the 8k cap (mean) | 5,339 | 5,030 |
| pages ok full-page that go thin | | 24 of 976 (2.5%) |
| orgs that lose an ok combined text | | 0 of 199 |

Against Q2's default rule (main content only if recall is at least 0.95 of full page and thin
flips stay under 2%), recall passes at 0.961 and thin flips fail at 2.5%, so the default is
**full page on subpages**. The detail makes the same case. Main-content detection does not trim
evenly: on most pages it drops nothing (per-org median ratio 1.000), and on some it drops the
whole page, which are often the pages a summary needs: a team page of staff bios went from
29,814 characters to 10, a 2025 annual report from 17,155 to 20, an executive team page lost
the bios. What it buys is small, because the cleaner and cross-page dedupe already remove the
repeated navigation: about 6% fewer tokens before the cap and 2% after.

Production must therefore send `onlyMainContent: false` on every request, since Firecrawl's
default is `true`.

Firecrawl's ad blocker cannot be turned off. Every `blockAds: false` request failed with
`SCRAPE_ALL_ENGINES_FAILED` ("engines tried: pdf, document, image"), even for
https://example.com, so the browser engines refuse to run without it. The blocked-frame cleanup
in `markdown_clean.strip_blocked_frames` stays.
