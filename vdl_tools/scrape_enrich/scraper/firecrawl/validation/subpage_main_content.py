"""D9: should subpages be fetched main-content-only?

Full-page markdown repeats the site's navigation, header and footer on every
subpage. Firecrawl's ``onlyMainContent=true`` strips them at the source, but
it may also strip real content, and the usable rate cannot tell. This script
measures what it drops, for the Q2 decision ("main-content-only if sentence
recall is at least 0.95 and thin flips stay under 2%").

Sample: 200 orgs, seeded, from the vendor pilot's phase 2 orgs that have at
least one subpage fetched with a 2xx by both Firecrawl and the in-house
scraper (in-house text of 200+ characters). Every such subpage of each org is
fetched today twice, so the comparison has no time drift:

``full``  ``onlyMainContent=false`` (the control, the pilot's setting)
``main``  ``onlyMainContent=true``

Firecrawl's browser blocks ads by default, which leaves Chrome's blocked-frame
error page inside 363 phase 2 pages; the text rules strip it
(``markdown_clean.strip_blocked_frames``). Turning the ad blocker off is not an
option: on 2026-11-03 every ``blockAds=false`` request failed with
``SCRAPE_ALL_ENGINES_FAILED`` ("engines tried: pdf, document, image"), even
for https://example.com, so the browser engines refuse to run without it.

Measures, with the production text rules (``markdown_clean``):

- sentence recall: the share of the in-house text's sentences (5+ words,
  from 2026-10-21) found in an arm's cleaned text; per org and pooled, and
  main relative to full;
- duplicated-line share across an org's subpages, before dedupe;
- thin flips: pages ok under ``full`` and thin or empty under ``main``, and
  orgs whose combined text (home page from the pilot plus these subpages)
  changes verdict;
- tokens per org after the rules.

Requests: ``formats=['markdown']``, ``maxAge=0``, ``proxy='auto'``. One
credit per page; failures are free. Results are JSONL per arm under
``<out-dir>/d9/`` and resume where they stopped.

    python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.subpage_main_content \\
        --out-dir .cache/vendor_pilot --limit 3          # smoke: 3 orgs
    python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.subpage_main_content \\
        --out-dir .cache/vendor_pilot
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    DEFAULT_OUT_DIR,
    RateLimiter,
    StopRun,
    _fetch_with_retry,
    get_api_key,
    git_sha,
    load_jsonl,
)
from vdl_tools.scrape_enrich.scraper.vendor_pilot.run_firecrawl import FirecrawlRunner
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.logger import logger

SEED = 7118
N_ORGS = 200
MIN_INHOUSE_CHARS = 200
MIN_SENTENCE_WORDS = 5
ARMS = {
    "full": {"onlyMainContent": False},
    "main": {"onlyMainContent": True},
}


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def arm_runner(arm: str) -> FirecrawlRunner:
    """Firecrawl scrape with one arm's options; markdown only."""
    runner = FirecrawlRunner(get_api_key("firecrawl"),
                             payload_overrides={"formats": ["markdown"], **ARMS[arm]})
    runner.name = f"d9_{arm}"
    return runner


async def fetch_arm(arm: str, urls: list[str], out: Path, concurrency: int, rpm: float) -> dict:
    """Fetch ``urls`` once each under ``arm``; append JSONL; return run meta."""
    path = out / f"{arm}.jsonl"
    done = {r["url"] for r in load_jsonl(path)} if path.exists() else set()
    todo = [u for u in urls if u not in done]
    runner = arm_runner(arm)
    sem, limiter, lock = asyncio.Semaphore(concurrency), RateLimiter(rpm), asyncio.Lock()
    stop = {"reason": None}
    t0 = time.monotonic()
    logger.info("d9 %s: %s to fetch, %s already done", arm, len(todo), len(done))
    async with httpx.AsyncClient(headers=runner.headers(), timeout=runner.timeout_seconds) as client:
        before = await _credits(runner, client)

        async def one(url):
            if stop["reason"]:
                return
            async with sem:
                if stop["reason"]:
                    return
                await limiter.acquire()
                try:
                    result = await _fetch_with_retry(runner, client, url)
                except StopRun as exc:
                    stop["reason"] = str(exc)
                    logger.error("d9 %s: stopping: %s", arm, exc)
                    return
                row = {k: v for k, v in asdict(result).items() if k not in ("raw_html", "links")}
                async with lock:
                    with path.open("a") as fh:
                        fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

        await asyncio.gather(*(one(u) for u in todo))
        after = await _credits(runner, client)
    return {"arm": arm, "options": ARMS[arm], "n_requested": len(todo), "stop_reason": stop["reason"],
            "seconds": round(time.monotonic() - t0, 1), "credits_before": before, "credits_after": after}


async def _credits(runner, client):
    try:
        return await runner.credits(client)
    except Exception as exc:  # noqa: BLE001 - balance is best effort
        return {"error": str(exc)[:200]}


# ---------------------------------------------------------------------------
# Sample
# ---------------------------------------------------------------------------

def _ok(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["success"] & df["http_status"].between(200, 299)]


def build_sample(pilot: Path, limit: int | None) -> pd.DataFrame:
    """The subpages to compare: every 2xx subpage of the sampled orgs."""
    fc = read_dataframe(pilot / "firecrawl_e2e_pages.parquet")
    ih = read_dataframe(pilot / "inhouse_laptop_e2e_pages.parquet")
    sub = _ok(fc)[lambda d: d["page_type"] == "page"]
    ref = _ok(ih)[lambda d: (d["page_type"] == "page") & (d["text"].fillna("").str.len() >= MIN_INHOUSE_CHARS)]
    pairs = sub.merge(ref[["cleaned_home_key", "subpath", "text"]].rename(columns={"text": "inhouse_text"}),
                      on=["cleaned_home_key", "subpath"])
    keys = sorted(pairs["cleaned_home_key"].unique())
    rng = random.Random(SEED)
    chosen = rng.sample(keys, min(N_ORGS, len(keys)))
    if limit:
        chosen = chosen[:limit]
    pages = pairs[pairs["cleaned_home_key"].isin(chosen)][
        ["org_id", "cleaned_home_key", "home_url", "subpath", "url", "inhouse_text", "text"]
    ].rename(columns={"text": "pilot_markdown"})
    return pages.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Measures
# ---------------------------------------------------------------------------

def _norm(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def sentences(text: str) -> list[str]:
    """Normalized sentences of 5+ words, in order, without duplicates."""
    out, seen = [], set()
    for chunk in re.split(r"(?<=[.!?])\s+|\n+", text or ""):
        s = _norm(chunk)
        if len(s.split()) >= MIN_SENTENCE_WORDS and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def recall(reference: list[str], text: str) -> tuple[int, int]:
    """(reference sentences found in ``text``, reference sentences)."""
    hay = " " + _norm(text) + " "
    return sum(1 for s in reference if f" {s} " in hay), len(reference)


def _dup_share(texts: list[str]) -> float | None:
    seen, dup, total = set(), 0, 0
    for t in texts:
        for line in (t or "").split("\n"):
            key = mc._line_key(line)
            if not key:
                continue
            total += len(line)
            dup += len(line) if key in seen else 0
            seen.add(key)
    return dup / total if total else None


def _arm_pages(out: Path, arm: str) -> pd.DataFrame:
    rows = load_jsonl(out / f"{arm}.jsonl") if (out / f"{arm}.jsonl").exists() else []
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ok2xx"] = df["success"].fillna(False) & df["http_status"].between(200, 299)
    df["page"] = [mc.page_text(m) if ok else None for m, ok in zip(df["markdown"], df["ok2xx"])]
    df["clean"] = [p.text if p else "" for p in df["page"]]
    df["verdict"] = [p.verdict if p else "failed" for p in df["page"]]
    return df.drop(columns=["page"]).drop_duplicates("url", keep="last").set_index("url")


def measure(pilot: Path, out: Path, pages: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    full, main = _arm_pages(out, "full"), _arm_pages(out, "main")
    pages = pages[pages["url"].isin(full.index) & pages["url"].isin(main.index)].copy()
    for arm, df in (("full", full), ("main", main)):
        pages[f"{arm}_ok2xx"] = pages["url"].map(df["ok2xx"])
        pages[f"{arm}_verdict"] = pages["url"].map(df["verdict"])
        pages[f"{arm}_clean"] = pages["url"].map(df["clean"]).fillna("")
        pages[f"{arm}_chars"] = pages[f"{arm}_clean"].str.len()
        pages[f"{arm}_credits"] = pages["url"].map(df["credits"])
    pages = pages[pages["full_ok2xx"] & pages["main_ok2xx"]].copy()
    ref = pages["inhouse_text"].map(sentences)
    for arm in ("full", "main"):
        hits = [recall(r, t) for r, t in zip(ref, pages[f"{arm}_clean"])]
        pages[f"{arm}_found"] = [h for h, _ in hits]
    pages["ref_sentences"] = ref.map(len)
    pages = pages[pages["ref_sentences"] > 0]
    pages["thin_flip"] = pages["full_verdict"].eq("ok") & pages["main_verdict"].isin(["thin", "empty"])

    # Org level: home page from the pilot (full page, as production fetches
    # it), subpages from each arm, combined by the production rules.
    fc = read_dataframe(pilot / "firecrawl_e2e_pages.parquet")
    homes = _ok(fc)[lambda d: d["page_type"] == "index"].set_index("cleaned_home_key")
    org_rows = []
    for key, g in pages.groupby("cleaned_home_key"):
        row = {"cleaned_home_key": key, "n_pages": len(g)}
        home = homes.loc[key] if key in homes.index else None
        if isinstance(home, pd.DataFrame):
            home = home.iloc[0]
        for arm in ("full", "main"):
            found, total = int(g[f"{arm}_found"].sum()), int(g["ref_sentences"].sum())
            row[f"{arm}_recall"] = found / total
            row[f"{arm}_dup_share"] = _dup_share(list(g[f"{arm}_clean"]))
            org_pages = [mc.OrgPage(sp, False, v, t) for sp, v, t in
                         zip(g["subpath"], g[f"{arm}_verdict"], g[f"{arm}_clean"])]
            if home is not None:
                hp = mc.page_text(home["text"])
                org_pages.append(mc.OrgPage("/", True, hp.verdict, hp.text))
            o = mc.org_text(g["home_url"].iloc[0], org_pages)
            row[f"{arm}_org_verdict"], row[f"{arm}_tokens"] = o.verdict, o.tokens
            row[f"{arm}_tokens_before_cap"] = o.tokens_before_cap
            row[f"{arm}_sub_chars"] = int(g[f"{arm}_chars"].sum())
        row["recall_ratio"] = row["main_recall"] / row["full_recall"] if row["full_recall"] else np.nan
        org_rows.append(row)
    orgs = pd.DataFrame(org_rows)

    return pages, orgs


def report(pages, orgs, meta: list) -> str:
    q = lambda s, p: float(np.nanquantile(s, p))  # noqa: E731
    pooled_full = pages["full_found"].sum() / pages["ref_sentences"].sum()
    pooled_main = pages["main_found"].sum() / pages["ref_sentences"].sum()
    org_ok = orgs["full_org_verdict"].eq("ok")
    org_flip = (org_ok & ~orgs["main_org_verdict"].eq("ok")).sum()
    requested = sum(int(r.get("n_requested") or 0) for r in meta)
    balances = [r[k].get("remainingCredits") for r in meta for k in ("credits_before", "credits_after")
                if isinstance(r.get(k), dict) and r[k].get("remainingCredits") is not None]
    used = f"{balances[0] - balances[-1]:,} credits" if len(balances) >= 2 else "credits not read"
    lines = [
        f"## D9: main-content-only subpages (run {datetime.now(timezone.utc).date().isoformat()})", "",
        f"{len(orgs)} orgs, {len(pages)} subpages fetched 2xx under both settings today; text rules "
        f"`{mc.EXTRACTOR}`; git {git_sha()[:10]}. {requested:,} pages requested across both settings, {used}.", "",
        "| measure | full page | main content only |",
        "|---|---|---|",
        f"| sentence recall of in-house text, pooled | {pooled_full:.3f} | {pooled_main:.3f} |",
        f"| recall per org, median | {orgs['full_recall'].median():.3f} | {orgs['main_recall'].median():.3f} |",
        f"| subpage characters after cleaning, median per page | {pages['full_chars'].median():,.0f} | {pages['main_chars'].median():,.0f} |",
        f"| duplicated-line share across an org's subpages | {orgs['full_dup_share'].mean():.1%} | {orgs['main_dup_share'].mean():.1%} |",
        f"| tokens per org after the rules, mean | {orgs['full_tokens'].mean():,.0f} | {orgs['main_tokens'].mean():,.0f} |",
        f"| tokens per org before the cap, mean | {orgs['full_tokens_before_cap'].mean():,.0f} | {orgs['main_tokens_before_cap'].mean():,.0f} |",
        f"| orgs over the 8k cap | {(orgs['full_tokens_before_cap'] > mc.ORG_TOKEN_CAP).sum()} | {(orgs['main_tokens_before_cap'] > mc.ORG_TOKEN_CAP).sum()} |",
        "",
        f"Recall of main relative to full, pooled: **{pooled_main / pooled_full:.3f}**; per org median "
        f"{orgs['recall_ratio'].median():.3f}, 10th percentile {q(orgs['recall_ratio'], 0.1):.3f}; orgs below 0.95: "
        f"{(orgs['recall_ratio'] < 0.95).sum()} of {len(orgs)}.",
        "",
        f"Thin flips: **{int(pages['thin_flip'].sum())} of {int(pages['full_verdict'].eq('ok').sum())} pages "
        f"({pages['thin_flip'].sum() / max(pages['full_verdict'].eq('ok').sum(), 1):.1%})** ok under full page and thin or "
        f"empty main-content-only; {int(org_flip)} of {int(org_ok.sum())} orgs lose an ok combined verdict.",
        "",
        "Page verdicts, full page (rows) against main content only (columns):", "",
        pd.crosstab(pages["full_verdict"], pages["main_verdict"]).to_markdown(), "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--limit", type=int, default=None, help="first N sampled orgs and ad pages (smoke)")
    parser.add_argument("--concurrency", type=int, default=20, help="pages in flight (Standard plan: 25 browsers)")
    parser.add_argument("--rpm", type=float, default=400, help="requests per minute (Standard plan: 500)")
    parser.add_argument("--report-only", action="store_true")
    args = parser.parse_args(argv)

    out = args.out_dir / "d9"
    out.mkdir(parents=True, exist_ok=True)
    pages = build_sample(args.out_dir, args.limit)
    logger.info("D9 sample: %s orgs, %s subpages", pages["cleaned_home_key"].nunique(), len(pages))
    meta_path = out / "run_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else []
    if not args.report_only:
        urls = sorted(pages["url"].unique())
        for arm in ARMS:
            m = asyncio.run(fetch_arm(arm, urls, out, args.concurrency, args.rpm))
            m["git_sha"], m["at"] = git_sha(), datetime.now(timezone.utc).isoformat()
            meta.append(m)
            meta_path.write_text(json.dumps(meta, indent=2, default=str))
            if m["stop_reason"]:
                logger.error("D9 stopped on %s: %s", arm, m["stop_reason"])
                return

    page_rows, orgs = measure(args.out_dir, out, pages)
    lineage = {"created_by": "vdl_tools.scrape_enrich.scraper.firecrawl.validation.subpage_main_content",
               "git_sha": git_sha(), "extractor": mc.EXTRACTOR, "arms": ARMS, "seed": SEED, "runs": meta}
    write_dataframe(page_rows.drop(columns=["pilot_markdown"]), out / "d9_pages.parquet", lineage=lineage)
    write_dataframe(orgs, out / "d9_orgs.parquet", lineage=lineage)
    text = report(page_rows, orgs, meta)
    (out / "d9_summary.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
