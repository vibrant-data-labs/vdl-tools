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
| `validation/` | one-off measurements over the pilot artifacts that the adoption decision rests on | D2, D3 |
| `tests/` | unit, golden and determinism tests; `fixtures/markdown/` holds real pilot pages | |

Still to come here: the client (D4, `client.py`) and the per-org pipeline
`scrape_websites_firecrawl` (D7, `pipeline.py`). The ORM models for the new tables (D6) go
with the other models in `shared_tools/database_cache/database_models/`.

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
