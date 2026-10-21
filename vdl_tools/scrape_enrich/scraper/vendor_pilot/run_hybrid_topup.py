"""Fetch, through the vendor, the pages the hybrid design would send it.

The hybrid Zein is considering: in-house first, the vendor only for pages
that fail, hit a wall, or come back thin or empty, at both home and
subpage level. Simulating it needs vendor results for exactly those pages:

- an org whose in-house *home* page is not usable goes to the vendor
  wholesale, and the vendor-only run already fetched its home and the
  subpages the vendor's links choose;
- an org whose in-house home is usable but some of its *subpages* are not
  needs vendor fetches of those subpage URLs, which the vendor-only run
  fetched only when its own link choice happened to pick the same path.

This script computes the second set from ``inhouse_<label>_e2e_pages``,
subtracts what ``<vendor>_e2e_pages`` already holds, fetches the rest with
the same settings, and writes ``<vendor>_hybrid_topup_pages.{jsonl,parquet}``.
``compare_e2e.py`` then assembles the hybrid per org from the three sets.
Resumable; ``--dry-run`` only counts.

Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_hybrid_topup --fake --inhouse inhouse_laptop
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.run_hybrid_topup --inhouse inhouse_laptop --dry-run
"""

import argparse
import asyncio
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.logger import logger

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR, StopRun, get_api_key, git_sha, load_jsonl
from vdl_tools.scrape_enrich.scraper.vendor_pilot._e2e import e2e_paths
from vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl_e2e import FakeFetcher, FirecrawlFetcher


def topup_targets(inhouse_orgs: pd.DataFrame, inhouse_pages: pd.DataFrame, vendor_pages: pd.DataFrame) -> pd.DataFrame:
    """Subpages of orgs whose in-house home is usable but whose in-house
    fetch of that subpage is not, minus URLs the vendor run already fetched."""
    home_ok = inhouse_orgs[inhouse_orgs.home_http_2xx & (inhouse_orgs.home_verdict == "ok")].cleaned_home_key
    sub = inhouse_pages[(inhouse_pages.page_type == "page") & inhouse_pages.cleaned_home_key.isin(home_ok)]
    failed = sub[~(sub.success & (sub.verdict == "ok"))]
    have = set(vendor_pages.url) if not vendor_pages.empty else set()
    return failed[~failed.url.isin(have)][["cleaned_home_key", "home_url", "url", "subpath"]].drop_duplicates("url")


async def run_topup(fetcher, targets: pd.DataFrame, out_dir: Path, concurrency: int, resume: bool = True) -> pd.DataFrame:
    jsonl = out_dir / f"{fetcher.name}_hybrid_topup_pages.jsonl"
    parquet = out_dir / f"{fetcher.name}_hybrid_topup_pages.parquet"
    meta_path = out_dir / f"{fetcher.name}_hybrid_topup_run_meta.json"
    done = {r["url"] for r in load_jsonl(jsonl)} if resume else set()
    if not resume and jsonl.exists():
        jsonl.unlink()
    todo = targets[~targets.url.isin(done)]
    logger.info("%s top-up: %s pages to fetch (%s already done)", fetcher.name, len(todo), len(done))
    sem, lock, stop = asyncio.Semaphore(concurrency), asyncio.Lock(), asyncio.Event()
    stop_reason = None
    await fetcher.start()
    balance_before = await fetcher.balance()
    t0 = time.monotonic()
    n = {"done": 0}

    async def one(row):
        nonlocal stop_reason
        if stop.is_set():
            return
        async with sem:
            if stop.is_set():
                return
            try:
                page = await fetcher.fetch_page(row.url, row.subpath, "page")
            except StopRun as exc:
                stop_reason, _ = str(exc), stop.set()
                logger.error("%s top-up: stopping: %s", fetcher.name, stop_reason)
                return
            d = asdict(page)
            d.update({"cleaned_home_key": row.cleaned_home_key, "home_url": row.home_url, "runner": fetcher.name,
                      "fetched_at": datetime.now(timezone.utc).isoformat()})
            async with lock:
                with jsonl.open("a") as fh:
                    fh.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
                n["done"] += 1
                if n["done"] % 50 == 0:
                    logger.info("%s top-up: %s/%s", fetcher.name, n["done"], len(todo))

    try:
        await asyncio.gather(*(one(r) for r in todo.itertuples(index=False)))
    finally:
        await fetcher.stop()
    balance_after = await fetcher.balance()
    rows = load_jsonl(jsonl)
    df = pd.DataFrame(rows)
    meta = {"runner": fetcher.name, "git_sha": git_sha(), "n_pages_this_run": n["done"], "n_pages_total": len(rows),
            "wall_seconds_this_run": round(time.monotonic() - t0, 2), "credits_this_run": float(df.credits.fillna(0).sum()) if not df.empty else 0,
            "api_retries_by_status": dict(fetcher.retry_counter), "balance_before": balance_before,
            "balance_after": balance_after, "stop_reason": stop_reason}
    history = json.loads(meta_path.read_text()) if meta_path.exists() else []
    history.append(meta)
    meta_path.write_text(json.dumps(history, indent=2, default=str))
    if not df.empty:
        write_dataframe(df, parquet, lineage={"created_by": "vendor_pilot.run_hybrid_topup", "git_sha": git_sha(), "runs": history})
    logger.info("%s top-up: %s pages -> %s", fetcher.name, len(df), parquet)
    return df


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--inhouse", default="inhouse_laptop", help="in-house e2e runner name")
    parser.add_argument("--fake", action="store_true", help="use the fake vendor (reads fake_e2e_pages)")
    parser.add_argument("--max-concurrent", type=int, default=25)
    parser.add_argument("--rpm", type=float, default=500)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    fetcher = FakeFetcher() if args.fake else FirecrawlFetcher(get_api_key("firecrawl"), args.max_concurrent, args.rpm or None)
    ih = e2e_paths(args.out_dir, args.inhouse)
    vp = e2e_paths(args.out_dir, fetcher.name)
    inhouse_orgs, inhouse_pages = read_dataframe(ih["orgs"]), read_dataframe(ih["pages"])
    vendor_pages = read_dataframe(vp["pages"]) if vp["pages"].exists() else pd.DataFrame(columns=["url"])
    targets = topup_targets(inhouse_orgs, inhouse_pages, vendor_pages)
    print(f"{fetcher.name} top-up: {len(targets)} subpages across {targets.cleaned_home_key.nunique()} orgs need a vendor fetch "
          f"({len(inhouse_pages[inhouse_pages.page_type == 'page'])} in-house subpages in all)")
    if args.dry_run:
        return
    df = asyncio.run(run_topup(fetcher, targets, args.out_dir, args.max_concurrent, resume=not args.no_resume))
    if not df.empty:
        print(f"fetched {int(df.success.sum())}/{len(df)}; credits {df.credits.fillna(0).sum():.0f}; ok verdicts {int((df.verdict == 'ok').sum())}")


if __name__ == "__main__":
    main()
