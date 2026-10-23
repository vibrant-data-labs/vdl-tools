"""End-to-end org pipeline shared by the phase-2 runners.

For one organization, the same steps ``scrape_websites_psql`` takes for a
home URL, minus the cache:

1. fetch the home page;
2. choose subpages: ``filter_links`` with ``PATHS_TO_KEEP`` and
   ``max_per_subpath=6`` (``subpage_type='about'``) over the internal
   links, each link cleaned with ``scrape_websites.__clean_website_path``
   and joined to the home URL with ``urljoin``, exactly as production does;
   single-page platforms (``check_is_single_page_websites``) get none;
3. fetch each chosen subpage;
4. build the org's combined text with ``make_group_text`` (production's
   ordering and token budget, the default counting prompt) over the pages'
   text, and classify it with ``classify_text_quality``.

Where the internal links come from is the fetcher's call and is recorded
per org: the in-house fetcher runs ``extract_website_links`` over its own
HTML (production); the Firecrawl fetcher runs the same ``filter_anchors`` +
``filter_links`` over the vendor's links array, and also computes the
HTML-derived set from ``rawHtml`` for comparison.

Output per runner: ``<name>_e2e_orgs.jsonl`` (append-only, resumable, one
line per org with its pages' text), ``<name>_e2e_orgs.parquet`` (one row
per org), ``<name>_e2e_pages.parquet`` (one row per page) and
``<name>_e2e_run_meta.json`` (timing, 429/402 counts, balance, context).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd

from vdl_tools.scrape_enrich.scraper import scrape_websites as sw
from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.scraper.website_processor import filter_anchors
from vdl_tools.shared_tools.parquet_cache import write_dataframe
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import StopRun, git_sha, load_jsonl
from vdl_tools.scrape_enrich.scraper.vendor_pilot.compare import markdown_to_plain, vendor_links_as_hrefs

MAX_PER_SUBPATH = 6          # scrape_websites_psql default
SUBPAGE_TYPE = "about"
COUNTING_PROMPT = "test prompt " * 100   # scrape_websites_psql default when no summary prompt is given
ORGS_FILENAME = "sample_orgs.parquet"


@dataclass
class PageResult:
    url: str
    subpath: str                      # '/' for the home page, else the link path as production stores it
    page_type: str                    # 'index' | 'page'
    success: bool = False
    api_status: int | None = None
    http_status: int | None = None
    text: str = ""                    # what feeds make_group_text: vendor markdown or our parsed_html
    text_len: int = 0
    raw_html_len: int = 0
    n_links: int = 0
    credits: float | None = None
    wall_seconds: float = 0.0
    attempts: int = 1
    error: str | None = None
    mode: str | None = None           # http/browser/failed, basic/stealth
    verdict: str = ""                 # classify_text_quality on this page's text alone

    def finalize(self, plain_text: str | None = None) -> "PageResult":
        self.text = self.text or ""
        self.text_len = len(self.text)
        plain = plain_text if plain_text is not None else self.text
        self.verdict = classify_text_quality(plain, 0 if self.success else 1)
        return self

    @property
    def http_2xx(self) -> bool:
        return bool(self.success) and self.http_status is not None and 200 <= int(self.http_status) < 300


@dataclass
class OrgResult:
    runner: str
    org_id: str
    home_url: str
    cleaned_home_key: str
    sample_order: int = -1
    is_single_page: bool = False
    home: PageResult | None = None
    link_source: str = ""             # 'html' (our extraction) | 'array' (vendor links)
    picks_used: list = field(default_factory=list)
    picks_html: list = field(default_factory=list)     # extract_website_links over the page's HTML
    picks_array: list = field(default_factory=list)    # filter_links over the vendor links array
    subpages: list = field(default_factory=list)       # list[PageResult]
    combined_text: str = ""
    combined_len: int = 0
    combined_verdict: str = ""
    usable: bool = False              # home 2xx AND combined verdict ok
    credits_total: float = 0.0
    n_pages: int = 0
    n_fetched: int = 0
    n_subpages_ok: int = 0
    wall_seconds: float = 0.0
    error: str | None = None
    fetched_at: str = ""


class PageFetcher:
    """Implemented per runner. ``fetch_page`` returns a finalized PageResult;
    ``pick_subpages`` returns (picks_used, link_source, picks_html, picks_array)."""

    name = "fetcher"
    default_org_concurrency = 20

    async def start(self):
        pass

    async def stop(self):
        pass

    async def run_context(self) -> dict | None:
        return None

    async def balance(self) -> dict | None:
        return None

    async def fetch_page(self, url: str, subpath: str, page_type: str) -> PageResult:
        raise NotImplementedError

    def pick_subpages(self, home: PageResult, home_url: str) -> tuple[list, str, list, list]:
        raise NotImplementedError

    retry_counter: Counter = Counter()   # api_status -> retried calls (429s, 5xx)


# ---------------------------------------------------------------------------
# Link choice helpers (production logic, two inputs)
# ---------------------------------------------------------------------------

def picks_from_html(home_url: str, raw_html: str) -> list[str]:
    """What production chooses: ``extract_website_links(url, html, 'about', 6)``."""
    if not raw_html:
        return []
    from vdl_tools.scrape_enrich.scraper.website_processor import extract_website_links
    return extract_website_links(home_url, raw_html, SUBPAGE_TYPE, max_per_subpath=MAX_PER_SUBPATH) or []


def picks_from_array(home_url: str, links: list) -> list[str]:
    """The same ``filter_anchors`` + ``filter_links`` over a vendor's links
    array (same-host absolute URLs reduced to paths first)."""
    if not links:
        return []
    _, res_links = filter_anchors(home_url, vendor_links_as_hrefs(home_url, links))
    if not res_links:
        return []
    return filter_links(links=sorted(set(res_links)), keep_paths=PATHS_TO_KEEP, max_per_subpath=MAX_PER_SUBPATH)


def subpage_targets(home_url: str, picks: list[str]) -> list[tuple[str, str]]:
    """(full_url, subpath) per pick, as scrape_websites_psql builds them."""
    out, seen = [], set()
    for link in picks:
        clean_path = sw.__clean_website_path(link)
        if not clean_path or clean_path in seen:
            continue
        seen.add(clean_path)
        out.append((urljoin(home_url, link), link))
    return out


def combine(home_url: str, pages: list[PageResult]) -> str:
    """Production never holds text for a non-2xx page (the HTTP fetch returns
    no content), so only 2xx pages feed the combined text here too."""
    records = [
        {"type": "index" if p.page_type == "index" else "page", "source": home_url,
         "subpath": p.subpath, "text": p.text or ""}
        for p in pages if p.http_2xx
    ]
    if not records:
        return ""
    return make_group_text(COUNTING_PROMPT, records) or ""


# ---------------------------------------------------------------------------
# One org
# ---------------------------------------------------------------------------

async def process_org(fetcher: PageFetcher, org) -> OrgResult:
    t0 = time.monotonic()
    home_url = sw.ensure_url_scheme(org.home_url)
    result = OrgResult(runner=fetcher.name, org_id=str(org.org_id), home_url=home_url,
                       cleaned_home_key=org.cleaned_home_key, sample_order=int(org.sample_order),
                       is_single_page=sw.check_is_single_page_websites(home_url))
    try:
        home = await fetcher.fetch_page(home_url, "/", "index")
        result.home = home
        pages = [home]
        if home.success and not result.is_single_page:
            picks, source, picks_html, picks_array = fetcher.pick_subpages(home, home_url)
            result.picks_used, result.link_source = list(picks), source
            result.picks_html, result.picks_array = list(picks_html), list(picks_array)
            targets = subpage_targets(home_url, picks)
            subpages = await asyncio.gather(
                *(fetcher.fetch_page(url, subpath, "page") for url, subpath in targets))
            result.subpages = list(subpages)
            pages.extend(subpages)
        result.combined_text = combine(home_url, pages)
    except StopRun:
        raise
    except Exception as exc:  # noqa: BLE001 - one org must not kill the run
        logger.exception("%s: org %s failed", fetcher.name, home_url)
        result.error = f"{type(exc).__name__}: {exc}"[:300]
        pages = [p for p in [result.home] if p] + list(result.subpages)
    result.combined_len = len(result.combined_text)
    result.combined_verdict = classify_text_quality(result.combined_text, 0 if result.combined_text else 1)
    result.usable = bool(result.home and result.home.http_2xx and result.combined_verdict == "ok")
    result.credits_total = float(sum(p.credits or 0 for p in pages))
    result.n_pages = len(pages)
    result.n_fetched = sum(1 for p in pages if p.success)
    result.n_subpages_ok = sum(1 for p in result.subpages if p.verdict == "ok")
    result.wall_seconds = time.monotonic() - t0
    result.fetched_at = datetime.now(timezone.utc).isoformat()
    return result


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def e2e_paths(out_dir: Path, name: str) -> dict:
    return {
        "jsonl": out_dir / f"{name}_e2e_orgs.jsonl",
        "orgs": out_dir / f"{name}_e2e_orgs.parquet",
        "pages": out_dir / f"{name}_e2e_pages.parquet",
        "meta": out_dir / f"{name}_e2e_run_meta.json",
    }


def _org_to_row(d: dict) -> dict:
    row = {k: v for k, v in d.items() if k not in ("home", "subpages", "combined_text")}
    home = d.get("home") or {}
    row.update({
        "home_success": home.get("success", False),
        "home_http_status": home.get("http_status"),
        "home_http_2xx": bool(home.get("success")) and home.get("http_status") is not None and 200 <= int(home["http_status"]) < 300,
        "home_verdict": home.get("verdict", ""),
        "home_text_len": home.get("text_len", 0),
        "home_mode": home.get("mode"),
        "home_wall_seconds": home.get("wall_seconds", 0.0),
        "home_error": home.get("error"),
        "n_subpages_chosen": len(d.get("picks_used") or []),
        "n_subpages_fetched": sum(1 for p in d.get("subpages") or [] if p.get("success")),
        "subpage_credits": float(sum((p.get("credits") or 0) for p in d.get("subpages") or [])),
    })
    return row


def _pages_rows(d: dict) -> list[dict]:
    rows = []
    for p in [d.get("home")] + list(d.get("subpages") or []):
        if not p:
            continue
        r = {k: v for k, v in p.items() if k != "text"}
        r.update({"runner": d["runner"], "org_id": d["org_id"], "home_url": d["home_url"],
                  "cleaned_home_key": d["cleaned_home_key"], "text": p.get("text", "")})
        rows.append(r)
    return rows


async def run_e2e(fetcher: PageFetcher, orgs: pd.DataFrame, out_dir: Path, *,
                  concurrency: int | None = None, limit: int | None = None,
                  resume: bool = True) -> pd.DataFrame:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = e2e_paths(out_dir, fetcher.name)
    concurrency = concurrency or fetcher.default_org_concurrency

    todo = orgs.sort_values("sample_order")
    if limit:
        todo = todo.head(limit)
    done = {r["cleaned_home_key"] for r in load_jsonl(paths["jsonl"])} if resume else set()
    if not resume:
        for key in ("jsonl", "meta"):  # a fresh start discards the history too
            if paths[key].exists():
                paths[key].unlink()
    todo = todo[~todo["cleaned_home_key"].isin(done)]
    logger.info("%s e2e: %s orgs to run (%s already done), org concurrency=%s",
                fetcher.name, len(todo), len(done), concurrency)

    sem = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()
    stop = asyncio.Event()
    progress = {"n": 0, "usable": 0, "credits": 0.0, "pages": 0}
    stop_reason = None

    await fetcher.start()
    context = await fetcher.run_context()
    balance_before = await _safe(fetcher.balance)
    started = datetime.now(timezone.utc)
    t_start = time.monotonic()

    async def one(org):
        nonlocal stop_reason
        if stop.is_set():
            return
        async with sem:
            if stop.is_set():
                return
            try:
                res = await process_org(fetcher, org)
            except StopRun as exc:
                stop_reason = str(exc)
                stop.set()
                logger.error("%s e2e: stopping run: %s", fetcher.name, stop_reason)
                return
            d = asdict(res)
            async with lock:
                with paths["jsonl"].open("a") as fh:
                    fh.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
                progress["n"] += 1
                progress["usable"] += int(res.usable)
                progress["credits"] += res.credits_total
                progress["pages"] += res.n_pages
                if progress["n"] % 25 == 0 or progress["n"] == len(todo):
                    el = time.monotonic() - t_start
                    logger.info("%s e2e: %s/%s orgs, %s usable, %s pages, %.0f credits, %.1f min, %.2f pages/s",
                                fetcher.name, progress["n"], len(todo), progress["usable"], progress["pages"],
                                progress["credits"], el / 60, progress["pages"] / max(el, 1e-6))

    balance_after = None
    try:
        await asyncio.gather(*(one(org) for org in todo.itertuples(index=False)))
    finally:
        t_end = time.monotonic()
        ended = datetime.now(timezone.utc)
        balance_after = await _safe(fetcher.balance)   # before stop(): the balance call needs the client
        await fetcher.stop()

    records = load_jsonl(paths["jsonl"])
    orgs_df = pd.DataFrame([_org_to_row(d) for d in records])
    pages_df = pd.DataFrame([r for d in records for r in _pages_rows(d)])
    meta = {
        "runner": fetcher.name, "git_sha": git_sha(),
        "started_at": started.isoformat(), "ended_at": ended.isoformat(),
        "wall_seconds_this_run": round(t_end - t_start, 2),
        "n_orgs_this_run": progress["n"], "n_pages_this_run": progress["pages"],
        "credits_this_run": progress["credits"],
        "pages_per_second_this_run": round(progress["pages"] / max(t_end - t_start, 1e-6), 4),
        "org_concurrency": concurrency,
        "api_retries_by_status": dict(fetcher.retry_counter),
        "runner_context": context,
        "balance_before": balance_before, "balance_after": balance_after,
        "stop_reason": stop_reason,
        "n_orgs_total": len(records),
    }
    history = json.loads(paths["meta"].read_text()) if paths["meta"].exists() else []
    history.append(meta)
    paths["meta"].write_text(json.dumps(history, indent=2, default=str))
    lineage = {
        "created_by": f"vdl_tools.scrape_enrich.scraper.vendor_pilot (e2e, {fetcher.name})",
        "git_sha": git_sha(), "sample": str(out_dir / ORGS_FILENAME), "runs": history,
    }
    if not orgs_df.empty:
        write_dataframe(orgs_df, paths["orgs"], lineage=lineage)
        write_dataframe(pages_df, paths["pages"], lineage=lineage)
    logger.info("%s e2e: %s orgs, %s pages -> %s", fetcher.name, len(orgs_df), len(pages_df), paths["orgs"])
    return orgs_df


async def _safe(fn):
    try:
        return await fn()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)[:200]}


def e2e_cli(fetcher_factory, name: str, default_concurrency: int, extra_args=None, argv=None):
    """Shared CLI: ``fetcher_factory(args)`` builds the fetcher."""
    import argparse

    from vdl_tools.shared_tools.parquet_cache import read_dataframe

    from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR

    parser = argparse.ArgumentParser(description=f"{name} end-to-end run over sample_orgs.parquet")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--concurrency", type=int, default=default_concurrency, help="orgs in flight")
    parser.add_argument("--limit", type=int, default=None, help="first N orgs (smoke test)")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, fetch nothing")
    if extra_args:
        extra_args(parser)
    args = parser.parse_args(argv)
    orgs = read_dataframe(args.out_dir / ORGS_FILENAME)
    fetcher = fetcher_factory(args)
    paths = e2e_paths(args.out_dir, fetcher.name)
    if args.dry_run:
        done = {r["cleaned_home_key"] for r in load_jsonl(paths["jsonl"])} if not args.no_resume else set()
        todo = orgs.sort_values("sample_order")
        if args.limit:
            todo = todo.head(args.limit)
        todo = todo[~todo["cleaned_home_key"].isin(done)]
        print(f"{fetcher.name}: would run {len(todo)} orgs ({len(done)} already in {paths['jsonl'].name}); "
              f"org concurrency={args.concurrency}")
        return
    df = asyncio.run(run_e2e(fetcher, orgs, args.out_dir, concurrency=args.concurrency,
                             limit=args.limit, resume=not args.no_resume))
    if not df.empty:
        print(df[["usable", "credits_total", "n_subpages_chosen", "n_pages"]].describe().loc[["count", "mean", "50%"]].to_string())
