"""``scrape_websites_firecrawl``: each org's website through Firecrawl into the v2 tables.

The per-org pipeline of ``docs/FIRECRAWL_ADAPTER_PLAN.md`` section 3, with the
error and refresh rules of section 6. For every home URL that is due:

1. fetch the home page (``FirecrawlClient.scrape``);
2. pick its subpages from the links Firecrawl returned, compared against the
   host the page was served from (``same_site_paths`` then ``filter_links``);
3. fetch the picks;
4. write one ``web_pages_scraped_v2`` row per page under the last-known-good
   policy, and one ``web_pages_parsed_v2`` row for the org, built by
   ``markdown_clean.combine_org`` from this run's picks only.

It writes the v2 tables and nothing else. ``web_pages_scraped`` and
``web_pages_parsed`` belong to the in-house scraper, whose ``skip_existing``
would otherwise read Firecrawl text as its own cache.

What "due" means (``is_due``), decided on the home page's row:

- never fetched: due;
- the last attempt was good: due only when ``max_age_days`` is given and the
  stored text is older than that;
- the last attempt was a vendor error: due (the next run retries it);
- the URL names no site (``invalid_url``): never due;
- the last attempt failed and the row has no good text: due while its
  ``num_errors`` is under ``max_errors``;
- otherwise (a failed attempt, with or without older good text): due once
  ``retry_failed_after_days`` have passed since the attempt, so a blocked
  site is neither lost nor hammered.

Last known good (``_write_page``). A page's text columns (``markdown``,
``parsed_html``, ``text_quality``, ``extractor``, ``response_status_code``,
the URL and metadata columns, ``fetched_at``, ``run_id``) describe the last
fetch that was good: a 2xx answer whose verdict is ``ok`` or ``thin``. A
later attempt that is not good changes only ``last_attempt_at``,
``last_attempt_result``, ``failure_reason``, ``attempts_since_fetch`` and
``num_errors``. When a good fetch replaces different good text, the old
version goes to ``web_pages_history_v2`` first, one version per page. A row
that never held good text is simply overwritten by each attempt.

``last_attempt_result``:

    ok           2xx and the verdict is ok or thin
    gone         404, 410, a dead host, or a 2xx page that is a parked domain
    unknown      blocked, another HTTP error, a vendor error, or a 2xx page
                 that is a bot wall, garbled or empty
    invalid_url  the value names no site; nothing was sent

Subpages follow the home page. The combined text is built from the home page
and the subpages this run picked, never from every row under the home key. A
picked page that is ``gone`` leaves the text; one that is ``unknown`` keeps
its last good text in it; a page in the previous ``source_keys`` that is no
longer picked gets ``superseded_at``. When the home page itself is not good
and the org already has text, the org's text is left alone: ``gone`` marks
it ``stale_reason = home_gone``, ``unknown`` changes nothing but the attempt
columns.

Credits. A run refuses to start without a ``credit_ceiling``; get the figure
from ``dry_run=True``, which reports how many orgs are due and what they
would cost and fetches nothing. A stop (ceiling, 402, 401) ends the run:
an org cut off mid-way writes nothing and is due again next time. The run's
credits come from ``client.stats``, never from a sum over rows.

What comes back is ``scrape_websites_psql(return_combined_res=True)``'s frame:
one row per org with ``home_url`` and ``combined_text``, null when the org
has no summarizable text, plus the verdict and attempt columns.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import subprocess
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from urllib.parse import urljoin

import pandas as pd

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
from vdl_tools.scrape_enrich.scraper.firecrawl.client import (
    DEAD,
    INVALID_URL,
    VENDOR_ERROR,
    DryRunReport,
    FirecrawlClient,
    RunStopped,
    ScrapeResult,
    _read_urls,
)
from vdl_tools.scrape_enrich.scraper.firecrawl.site_links import same_site_paths
from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.url_keys import (
    check_is_single_page_websites,
    clean_website_path,
    ensure_url_scheme,
    extract_website_name,
)
from vdl_tools.shared_tools.database_cache.database_models.web_scraping import (
    OrgSiteKey,
    ScrapeRun,
    WebPagesHistoryV2,
    WebPagesParsedV2,
    WebPagesScrapedV2,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP

CREDITS_PER_ORG = 4.67            # phase 2 of the pilot: 9,348 credits for 2,000 orgs
MAX_ERRORS = 5                    # scrape_websites_psql's default
MAX_PER_SUBPATH = 6               # scrape_websites_psql's default
RETRY_FAILED_AFTER_DAYS = 90      # the window website_quality trusts a liveness answer for
N_PER_COMMIT = 25                 # orgs per commit
LOOKUP_CHUNK = 1000

PAGE = "page"
OK, GONE, UNKNOWN, INVALID = "ok", "gone", "unknown", "invalid_url"
GONE_STATUSES = (404, 410)
HOME_GONE = "home_gone"


def _utcnow() -> dt.datetime:
    """Naive UTC, like every timestamp in the cache tables."""
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:  # noqa: BLE001 - lineage only
        return None


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def attempt_result(result: ScrapeResult, verdict: str | None) -> str:
    """ok, gone, unknown or invalid_url for one attempt; ``verdict`` is the page's when it answered 2xx."""
    if result.ok:
        if verdict in mc.COMBINABLE_VERDICTS:
            return OK
        return GONE if verdict == "parked" else UNKNOWN
    if result.failure_reason == INVALID_URL:
        return INVALID
    if result.failure_reason == DEAD or result.status_code in GONE_STATUSES:
        return GONE
    return UNKNOWN


def has_good_text(row) -> bool:
    """The row's text columns hold a fetch that passed the status gate and the text gate."""
    return row is not None and row.fetched_at is not None and row.text_quality in mc.COMBINABLE_VERDICTS


def is_due(row, now: dt.datetime, max_age_days: float | None, retry_failed_after_days: float,
           max_errors: int) -> bool:
    """Whether a page should be fetched now; the module docstring lists the cases."""
    if row is None:
        return True
    if row.last_attempt_result == OK:
        if max_age_days is None:
            return False
        return row.fetched_at is None or row.fetched_at < now - dt.timedelta(days=max_age_days)
    if row.failure_reason == VENDOR_ERROR:
        return True
    if row.last_attempt_result == INVALID:
        return False
    if not has_good_text(row) and (row.num_errors or 0) < max_errors:
        return True
    return row.last_attempt_at is None or row.last_attempt_at < now - dt.timedelta(days=retry_failed_after_days)


def pick_subpages(home: ScrapeResult, max_per_subpath: int = MAX_PER_SUBPATH) -> list[tuple[str, str]]:
    """(subpath, url) for each subpage of a fetched home page, in ``filter_links``'s order.

    Candidates are the links on the host the page was served from, so an org
    whose home page redirects to another host still gets its subpages, and
    the URLs are built on that host too.
    """
    base = home.final_url or home.url
    candidates = same_site_paths(home.links, base)
    if not candidates:
        return []
    picks, seen = [], set()
    for path in filter_links(candidates, keep_paths=PATHS_TO_KEEP, max_per_subpath=max_per_subpath):
        subpath = path.strip("/")
        key_part = clean_website_path(subpath) if subpath else None
        if not key_part or key_part in seen:
            continue
        seen.add(key_part)
        picks.append((subpath, urljoin(base, "/" + subpath)))
    return picks


def page_key(home_key: str, subpath: str) -> str:
    return f"{home_key}/{clean_website_path(subpath)}"


# ---------------------------------------------------------------------------
# Fetch: no database
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Org:
    home_url: str     # ensure_url_scheme(the caller's value)
    key: str          # extract_website_name: cleaned_home_key


@dataclass
class _OrgFetch:
    org: _Org
    home: ScrapeResult
    subpages: list = field(default_factory=list)   # (subpath, ScrapeResult), in pick order


async def _fetch_org(client: FirecrawlClient, org: _Org, *, had_good_home: bool, max_per_subpath: int,
                     single_page_websites: Iterable[str]) -> _OrgFetch:
    """The home page and, when it is worth it, its picks. Raises ``RunStopped``
    when any of the org's pages was cut off, so a half-fetched org is never written."""
    home = await client.scrape(org.home_url, home_url=org.home_url, page_type=mc.HOME_PAGE_TYPE)
    fetched = _OrgFetch(org, home)
    if not home.ok or check_is_single_page_websites(org.home_url, list(single_page_websites)):
        return fetched
    verdict = mc.page_text(home.markdown).verdict
    # A parked, walled or garbled home page makes the org that verdict whatever
    # its subpages say, so they are not paid for. An empty one (a splash page)
    # can still be carried by its subpages, unless the org already has a good
    # home page that this fetch will not replace.
    if not (verdict in mc.COMBINABLE_VERDICTS or (verdict == "empty" and not had_good_home)):
        return fetched
    picks = pick_subpages(home, max_per_subpath)
    results = await client.scrape_many([url for _, url in picks], home_url=org.home_url, page_type=PAGE)
    for subpath, url in picks:
        result = results[url]
        if isinstance(result, RunStopped):
            raise result
        fetched.subpages.append((subpath, result))
    return fetched


# ---------------------------------------------------------------------------
# Write: the last-known-good policy
# ---------------------------------------------------------------------------

def _keep_history(session, row: WebPagesScrapedV2) -> None:
    """Copy the row's current good text to the history table, one version per page."""
    session.query(WebPagesHistoryV2).filter(WebPagesHistoryV2.cleaned_key == row.cleaned_key).delete(
        synchronize_session=False)
    session.add(WebPagesHistoryV2(
        cleaned_key=row.cleaned_key, fetched_at=row.fetched_at, run_id=row.run_id, extractor=row.extractor,
        markdown=row.markdown, parsed_html=row.parsed_html, text_quality=row.text_quality,
    ))


def _write_page(session, existing: WebPagesScrapedV2 | None, *, key: str, home_url: str, subpath: str,
                page_type: str, result: ScrapeResult, now: dt.datetime) -> tuple[WebPagesScrapedV2, bool]:
    """Apply one attempt to a page row. Returns the row and whether its text
    columns now hold this attempt."""
    page = mc.page_text(result.markdown) if result.ok else None
    outcome = attempt_result(result, page.verdict if page else None)
    old_good = has_good_text(existing)
    row = existing
    if row is None:
        row = WebPagesScrapedV2(cleaned_key=key, home_url=home_url, subpath=subpath, page_type=page_type,
                                num_errors=0, attempts_since_fetch=0)
        session.add(row)
    replaces = outcome == OK or not old_good
    if replaces:
        if old_good and existing.markdown != result.markdown:
            _keep_history(session, existing)
        row.run_id = result.run_id
        row.source_url = result.source_url
        row.final_url = result.final_url
        row.final_host = result.final_host or None
        row.response_status_code = result.status_code
        row.markdown = result.markdown or None
        row.parsed_html = page.text if page else ""
        row.extractor = mc.EXTRACTOR if page else None
        row.text_quality = page.verdict if page else None
        row.proxy_used = result.proxy_used
        row.credits_used = result.credits_used
        row.content_type = result.content_type
        row.language = result.language
        row.scrape_id = result.scrape_id
        row.fetched_at = now if result.ok else None
        row.attempts_since_fetch = 0
    else:
        row.attempts_since_fetch = (row.attempts_since_fetch or 0) + 1
    row.failure_reason = result.failure_reason
    row.last_attempt_at = now
    row.last_attempt_result = outcome
    if outcome == OK:
        row.num_errors = 0
    elif result.failure_reason != VENDOR_ERROR:
        row.num_errors = (row.num_errors or 0) + 1
    row.superseded_at = None
    return row, replaces


def _in_text(row: WebPagesScrapedV2) -> bool:
    """A picked subpage whose stored text belongs in the combined text: not gone, and holding a 2xx fetch."""
    return row.last_attempt_result != GONE and row.fetched_at is not None and mc.is_2xx(row.response_status_code)


def _write_org(session, fetched: _OrgFetch, parsed: WebPagesParsedV2 | None, home_row: WebPagesScrapedV2 | None,
               *, now: dt.datetime, prompt_str: str, max_tokens: int) -> tuple[WebPagesParsedV2, WebPagesScrapedV2]:
    """One fetched org into the page rows and the org row. Returns the org row and the home page row."""
    org, result = fetched.org, fetched.home
    home, home_replaced = _write_page(session, home_row, key=org.key, home_url=org.home_url, subpath="/",
                                      page_type=mc.HOME_PAGE_TYPE, result=result, now=now)
    if parsed is None:
        parsed = WebPagesParsedV2(cleaned_home_key=org.key, home_url=org.home_url, num_errors=0)
        session.add(parsed)
    parsed.last_attempt_at = now
    parsed.last_attempt_result = home.last_attempt_result

    if not (result.ok and home_replaced):
        # The home page gave nothing new to build on. Text the org already has stays.
        if result.failure_reason != VENDOR_ERROR:
            parsed.num_errors = (parsed.num_errors or 0) + 1
        if parsed.combined_text:
            if home.last_attempt_result == GONE:
                parsed.stale_reason = HOME_GONE
        else:
            parsed.text_quality = classify_text_quality(None, parsed.num_errors or 0)
        return parsed, home

    keys = {subpath: page_key(org.key, subpath) for subpath, _ in fetched.subpages}
    existing = {}
    if keys:
        existing = {r.cleaned_key: r for r in session.query(WebPagesScrapedV2).filter(
            WebPagesScrapedV2.cleaned_key.in_(list(keys.values())))}
    rows = [home]
    for subpath, sub_result in fetched.subpages:
        row, _ = _write_page(session, existing.get(keys[subpath]), key=keys[subpath], home_url=org.home_url,
                             subpath=subpath, page_type=PAGE, result=sub_result, now=now)
        rows.append(row)

    # The home page is this run's 2xx fetch whatever its verdict: a parked or walled
    # home page has to reach combine_org, which makes the org that verdict.
    in_text = [home] + [r for r in rows[1:] if _in_text(r)]
    combined = mc.combine_org(
        org.home_url,
        [mc.StoredPage(r.subpath, r.page_type, r.response_status_code, r.markdown) for r in in_text],
        prompt_str, max_tokens,
    )
    # Every row in the text carries the cleaner version the text was built with,
    # including rows kept from an earlier run.
    for row, page in zip(in_text, combined.pages):
        row.parsed_html, row.text_quality, row.extractor = page.text, page.verdict, combined.org.extractor

    previous_keys = set(parsed.source_keys or [])
    parsed.home_url = org.home_url
    parsed.run_id = result.run_id
    parsed.combined_text = combined.org.text or None
    parsed.source_keys = [org.key if sp == "/" else keys[sp] for sp in combined.org.subpaths]
    parsed.extractor = combined.org.extractor
    parsed.prompt_token_budget = max_tokens
    parsed.text_quality = combined.org.verdict
    parsed.combined_at = now
    parsed.stale_reason = None
    parsed.num_errors = 0 if combined.org.verdict in mc.COMBINABLE_VERDICTS else (parsed.num_errors or 0) + 1

    dropped = previous_keys - set(keys.values()) - {org.key}
    if dropped:
        session.query(WebPagesScrapedV2).filter(WebPagesScrapedV2.cleaned_key.in_(sorted(dropped))).update(
            {WebPagesScrapedV2.superseded_at: now}, synchronize_session=False)
    return parsed, home


def _write_org_ids(session, org_ids: Mapping[str, Iterable[tuple[str, str]]], sites: dict,
                   now: dt.datetime) -> int:
    """Upsert ``org_site_keys`` for every id whose URL has a home page row.
    ``sites`` maps a home key to ``(source_url, final_host, run_id)``."""
    wanted = {}
    for url, pairs in org_ids.items():
        key = extract_website_name(url) if isinstance(url, str) else None
        if key not in sites:
            continue
        for id_type, id_value in pairs:
            if id_type and id_value is not None and str(id_value).strip():
                wanted[(str(id_type), str(id_value).strip(), key)] = sites[key]
    for (id_type, id_value, key), (source_url, final_host, site_run_id) in wanted.items():
        row = session.get(OrgSiteKey, (id_type, id_value, key))
        if row is None:
            row = OrgSiteKey(id_type=id_type, id_value=id_value, cleaned_home_key=key, first_seen=now)
            session.add(row)
        row.source_url = source_url
        row.final_host = final_host
        row.run_id = site_run_id    # the run that fetched the site, not the run that looked it up
        row.last_seen = now
    return len(wanted)


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

# What the cache lookup reads from each home page row. The text columns are
# left in the database: a refresh covers tens of thousands of orgs.
_HOME_COLUMNS = (
    WebPagesScrapedV2.cleaned_key, WebPagesScrapedV2.last_attempt_result, WebPagesScrapedV2.last_attempt_at,
    WebPagesScrapedV2.fetched_at, WebPagesScrapedV2.failure_reason, WebPagesScrapedV2.text_quality,
    WebPagesScrapedV2.num_errors, WebPagesScrapedV2.source_url, WebPagesScrapedV2.final_host,
    WebPagesScrapedV2.home_url, WebPagesScrapedV2.run_id,
)


def _chunks(keys: list[str]):
    for i in range(0, len(keys), LOOKUP_CHUNK):
        yield keys[i:i + LOOKUP_CHUNK]


def _frame_row(org: _Org, parsed: dict | None, failure_reason: str | None) -> dict:
    """One row of the returned frame from an org row's ``to_dict()``."""
    row = dict(parsed) if parsed is not None else {"cleaned_home_key": org.key}
    row["home_url"] = org.home_url   # this call's spelling: callers join failures back on it
    if row.get("text_quality") not in mc.COMBINABLE_VERDICTS:
        row["combined_text"] = None  # nothing a summarizer should read; the stored row keeps the text
    row["failure_reason"] = failure_reason
    return row


def _site(home) -> tuple:
    return (home.source_url or home.home_url, home.final_host, home.run_id)


async def scrape_websites_firecrawl_async(
    urls: Iterable[str],
    *,
    credit_ceiling: float | None = None,
    session=None,
    client: FirecrawlClient | None = None,
    max_age_days: float | None = None,
    retry_failed_after_days: float = RETRY_FAILED_AFTER_DAYS,
    max_errors: int = MAX_ERRORS,
    max_per_subpath: int = MAX_PER_SUBPATH,
    single_page_websites: Iterable[str] = (),
    n_per_commit: int = N_PER_COMMIT,
    org_ids: Mapping[str, Iterable[tuple[str, str]]] | None = None,
    summary_prompt: str | None = None,
    max_tokens: int = mc.ORG_TOKEN_CAP,
    dry_run: bool = False,
    credits_per_org: float = CREDITS_PER_ORG,
    clock: Callable[[], dt.datetime] = _utcnow,
) -> pd.DataFrame | DryRunReport:
    """See the module docstring. ``scrape_websites_firecrawl`` is the synchronous form.

    ``urls`` are home URLs; blanks are dropped and a URL repeated under one
    cache key is taken once. ``org_ids`` maps a home URL, as given in ``urls``,
    to its ``(id_type, id_value)`` pairs for ``org_site_keys``. ``client`` is
    for the tests; by default one is built from ``[scraping_vendors]``.
    """
    orgs, seen = [], set()
    for url in urls:
        key = extract_website_name(url) if isinstance(url, str) else None
        if key and key not in seen:
            seen.add(key)
            orgs.append(_Org(ensure_url_scheme(url.strip()), key))
    if not dry_run and not (credit_ceiling and credit_ceiling > 0):
        raise ValueError("scrape_websites_firecrawl needs a credit_ceiling: run it with dry_run=True for an "
                         "estimate first. Without one a run stops only when the account is out of credits.")
    prompt_str = summary_prompt or mc.COUNTING_PROMPT

    with get_session(session=session) as session:
        keys = [o.key for o in orgs]
        homes = {}
        for chunk in _chunks(keys):
            for row in session.query(*_HOME_COLUMNS).filter(WebPagesScrapedV2.cleaned_key.in_(chunk)):
                homes[row.cleaned_key] = row
        start = clock()
        has_org_row = set()
        for chunk in _chunks(keys):
            has_org_row.update(k for (k,) in session.query(WebPagesParsedV2.cleaned_home_key).filter(
                WebPagesParsedV2.cleaned_home_key.in_(chunk)))
        due = [o for o in orgs if o.key not in has_org_row
               or is_due(homes.get(o.key), start, max_age_days, retry_failed_after_days, max_errors)]
        logger.info("firecrawl: %s orgs, %s served from the cache, %s to fetch", len(orgs), len(orgs) - len(due), len(due))

        if dry_run:
            client = client or FirecrawlClient.from_config(credit_ceiling=credit_ceiling)
            report = await client.dry_run([o.home_url for o in due], credits_per_miss=credits_per_org)
            n_cached = len(orgs) - len(due)
            return replace(report, n_urls=report.n_urls + n_cached, n_cached=n_cached)

        # The frame, before anything is fetched: what the cache holds for every
        # org. A fetched org's row replaces its entry; an org cut off by a stop
        # keeps it, which is its last known good text.
        by_key = {o.key: o for o in orgs}
        frame = {o.key: _frame_row(o, None, None) for o in orgs}
        for chunk in _chunks(sorted(has_org_row)):
            for row in session.query(WebPagesParsedV2).filter(WebPagesParsedV2.cleaned_home_key.in_(chunk)):
                home = homes.get(row.cleaned_home_key)
                frame[row.cleaned_home_key] = _frame_row(by_key[row.cleaned_home_key], row.to_dict(),
                                                         home.failure_reason if home is not None else None)
            session.expunge_all()
        sites = {key: _site(home) for key, home in homes.items()}
        had_good_home = {key for key, home in homes.items() if has_good_text(home)}

        if due:
            run_id = (client.run_id if client is not None else None) or f"fc-{start:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
            if client is None:
                client = FirecrawlClient.from_config(credit_ceiling=credit_ceiling, run_id=run_id)
            else:
                client.run_id = client.stats.run_id = run_id
                client.credit_ceiling = client.credit_ceiling or credit_ceiling
            session.add(ScrapeRun(
                run_id=run_id, started_at=start, git_sha=_git_sha(), extractor_version=mc.EXTRACTOR,
                request_profile=client.request_profile, plan_concurrency=client.limits.concurrency,
                plan_rpm=int(client.limits.rpm) if client.limits.rpm else None, credit_ceiling=client.credit_ceiling,
            ))
            session.commit()
            try:
                await _run(session, client, due, frame, sites, had_good_home, n_per_commit=n_per_commit, clock=clock,
                           prompt_str=prompt_str, max_tokens=max_tokens, max_per_subpath=max_per_subpath,
                           single_page_websites=single_page_websites)
            finally:
                run = session.get(ScrapeRun, run_id)
                run.ended_at = clock()
                run.credits_before = client.remaining_credits(client.stats.balance_before)
                run.credits_after = client.remaining_credits(client.stats.balance_after)
                run.credits_used = client.stats.credits_used
                run.stop_reason = client.stats.stop_reason
                session.commit()
            logger.info("firecrawl: run %s used %g credits over %s pages%s", run_id, client.stats.credits_used,
                        client.stats.n_pages, f"; stopped: {client.stats.stop_reason}" if client.stats.stop_reason else "")

        if org_ids:
            n_ids = _write_org_ids(session, org_ids, sites, clock())
            session.commit()
            logger.info("firecrawl: %s org ids linked to their sites", n_ids)
        return pd.DataFrame([frame[o.key] for o in orgs])


async def _run(session, client: FirecrawlClient, due: list[_Org], frame: dict, sites: dict, had_good_home: set, *,
               n_per_commit: int, clock, prompt_str: str, max_tokens: int, max_per_subpath: int,
               single_page_websites) -> None:
    """Fetch the due orgs, a bounded number at a time, and write each as it lands.
    Rows are read when an org is written and dropped from the session at each
    commit, so memory holds one batch whatever the size of the run."""
    in_flight = asyncio.Semaphore(max(1, client.limits.concurrency))

    async def one(org: _Org):
        async with in_flight:
            try:
                return await _fetch_org(client, org, had_good_home=org.key in had_good_home,
                                        max_per_subpath=max_per_subpath, single_page_websites=single_page_websites)
            except RunStopped as stop:
                return stop
            except Exception as exc:  # noqa: BLE001 - one org must not end the run
                logger.exception("firecrawl: %s failed", org.home_url)
                return exc

    async with client:
        written = 0
        for next_done in asyncio.as_completed([asyncio.ensure_future(one(o)) for o in due]):
            fetched = await next_done
            if not isinstance(fetched, _OrgFetch):
                continue   # cut off by a stop, or failed: nothing is written and the org stays due
            org = fetched.org
            parsed, home = _write_org(session, fetched, session.get(WebPagesParsedV2, org.key),
                                      session.get(WebPagesScrapedV2, org.key), now=clock(),
                                      prompt_str=prompt_str, max_tokens=max_tokens)
            frame[org.key] = _frame_row(org, parsed.to_dict(), home.failure_reason)
            sites[org.key] = _site(home)
            written += 1
            if written % n_per_commit == 0:
                session.commit()
                session.expunge_all()
        session.commit()
        session.expunge_all()


def scrape_websites_firecrawl(urls: Iterable[str], **kwargs) -> pd.DataFrame | DryRunReport:
    """``scrape_websites_firecrawl_async`` for synchronous callers. Inside a
    running event loop (a notebook), await the async form instead."""
    return asyncio.run(scrape_websites_firecrawl_async(urls, **kwargs))


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Scrape home URLs through Firecrawl into the v2 tables.")
    parser.add_argument("--urls", required=True, help="a text file (one URL per line) or a parquet")
    parser.add_argument("--column", default="home_url", help="URL column when --urls is a parquet")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="report what is due and what it would cost; fetch nothing")
    parser.add_argument("--credit-ceiling", type=float, default=None, help="required unless --dry-run")
    parser.add_argument("--max-age-days", type=float, default=None, help="refetch text older than this")
    parser.add_argument("--retry-failed-after-days", type=float, default=RETRY_FAILED_AFTER_DAYS)
    parser.add_argument("--max-errors", type=int, default=MAX_ERRORS)
    args = parser.parse_args(argv)

    urls = _read_urls(args.urls, args.column)
    if args.limit:
        urls = urls[: args.limit]
    out = scrape_websites_firecrawl(
        urls, credit_ceiling=args.credit_ceiling, dry_run=args.dry_run, max_age_days=args.max_age_days,
        retry_failed_after_days=args.retry_failed_after_days, max_errors=args.max_errors)
    if args.dry_run:
        print(out.summary())
    else:
        print(out["text_quality"].value_counts(dropna=False).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
