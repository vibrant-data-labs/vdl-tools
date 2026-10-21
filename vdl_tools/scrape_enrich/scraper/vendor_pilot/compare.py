"""Compare each vendor's fetch against our cache, per URL and per stratum.

Per URL and vendor:

* **text**: ``classify_text_quality`` on the vendor's markdown (link syntax
  stripped first so URLs don't pad the length) versus the verdict on our
  cached ``parsed_html`` from the sample.
* **links**: the internal paths ``filter_anchors`` keeps from (a) the
  ``<a href>`` tags BeautifulSoup finds in the vendor's raw HTML, i.e. what
  ``extract_website_links`` would do in production, and (b) the vendor's
  ``links`` array (same-host absolute URLs reduced to paths, which is the
  adapter production would need). ``internal_recall`` is the share of (a)
  found in (b). ``pick_recall`` is the same after ``filter_links`` with
  ``PATHS_TO_KEEP`` and ``max_per_subpath=6`` (the production subpage
  choice), which is noisier because ``filter_links`` caps per top-level
  path in set-iteration order.

Every ``<name>_results.parquet`` in the output directory is treated as a
vendor (``firecrawl``, ``firecrawl_mainonly``, ``spider``, ...).

Per vendor and stratum: fetch and 2xx rates, verdict shares, median markdown
length, median links, latency, pages/second (from the run meta), credits
and cost as the vendor reported them.

Writes ``comparison_results.parquet`` (long: one row per vendor x URL),
``comparison_summary.parquet`` and ``comparison_summary.md`` next to the
sample. Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.scraper.website_processor import filter_anchors
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import filter_links
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import (
    DEFAULT_OUT_DIR,
    SAMPLE_FILENAME,
    git_sha,
)

MAX_PER_SUBPATH = 6  # scrape_websites_psql default
VERDICTS = ("ok", "thin", "blocked", "parked", "garbled", "empty", "dead")

_MD_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_MD_LINK = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_BARE_URL = re.compile(r"https?://\S+")
_MD_MARKUP = re.compile(r"^[ \t]*[#>*\-|=]+[ \t]*", re.MULTILINE)


def markdown_to_plain(markdown: str) -> str:
    """Markdown with images, link targets, bare URLs and line markup removed,
    so the thin-text threshold sees prose, not URLs."""
    if not markdown:
        return ""
    text = _MD_IMAGE.sub(" ", markdown)
    text = _MD_LINK.sub(r"\1", text)
    text = _BARE_URL.sub(" ", text)
    text = _MD_MARKUP.sub("", text)
    return re.sub(r"[ \t]+", " ", text).strip()


def _host(url: str) -> str:
    netloc = urlparse(url if "://" in url else f"http://{url}").netloc.lower()
    return netloc[4:] if netloc.startswith("www.") else netloc


def vendor_links_as_hrefs(url: str, links: list) -> list[str]:
    """Same-host absolute links -> site-relative paths (what an <a href>
    would usually hold); everything else passed through for filter_anchors
    to drop."""
    home = _host(url)
    out = []
    for link in links or []:
        if not isinstance(link, str) or not link:
            continue
        if "://" not in link:
            out.append(link)
            continue
        parsed = urlparse(link)
        if _host(link) == home:
            path = parsed.path or "/"
            out.append(urlunparse(("", "", path, parsed.params, parsed.query, "")))
        else:
            out.append(link)
    return out


def hrefs_from_html(raw_html: str) -> list[str]:
    if not raw_html:
        return []
    try:
        soup = BeautifulSoup(raw_html, "lxml")
    except Exception:  # noqa: BLE001 - same tolerance as extract_website_links
        return []
    return [a.get("href") for a in soup.find_all("a") if a.get("href")]


def _recall(ours: set, theirs: set) -> float | None:
    return len(ours & theirs) / len(ours) if ours else None


def _jaccard(a: set, b: set) -> float | None:
    return len(a & b) / len(a | b) if (a | b) else None


def link_metrics(url: str, raw_html: str, vendor_links: list) -> dict:
    ours_cand, ours_res = filter_anchors(url, hrefs_from_html(raw_html))
    vend_cand, vend_res = filter_anchors(url, vendor_links_as_hrefs(url, vendor_links))
    ours_cand, vend_cand = set(ours_cand), set(vend_cand)
    ours_pick = set(filter_links(sorted(set(ours_res)), PATHS_TO_KEEP, MAX_PER_SUBPATH)) if ours_res else set()
    vend_pick = set(filter_links(sorted(set(vend_res)), PATHS_TO_KEEP, MAX_PER_SUBPATH)) if vend_res else set()
    return {
        "ours_n_internal": len(ours_cand),
        "vendor_n_internal": len(vend_cand),
        "internal_recall": _recall(ours_cand, vend_cand),
        "internal_jaccard": _jaccard(ours_cand, vend_cand),
        "ours_n_pick": len(ours_pick),
        "vendor_n_pick": len(vend_pick),
        "pick_recall": _recall(ours_pick, vend_pick),
        "pick_jaccard": _jaccard(ours_pick, vend_pick),
    }


def _balance(raw) -> float | None:
    """Pull a numeric balance out of whatever the credits endpoint returned."""
    if isinstance(raw, list) and raw:
        raw = raw[0]
    if not isinstance(raw, dict):
        return None
    for key in ("remainingCredits", "credits", "balance", "credits_remaining", "remaining"):
        if key in raw:
            try:
                return float(raw[key])
            except (TypeError, ValueError):
                return None
    return None


def run_meta_summary(out_dir: Path, vendor: str) -> dict:
    path = out_dir / f"{vendor}_run_meta.json"
    if not path.exists():
        return {}
    runs = json.loads(path.read_text())
    n = sum(r["n_fetched_this_run"] for r in runs)
    wall = sum(r["wall_seconds_this_run"] for r in runs if r["n_fetched_this_run"])
    billed = 0.0
    unknown = False
    note = None
    for r in runs:
        before, after = _balance(r.get("credits_before")), _balance(r.get("credits_after"))
        if before is None or after is None:
            unknown = True
        elif after > before:
            # A top-up landed mid-run: the balance no longer measures spend.
            unknown = True
            note = f"balance rose from {before:g} to {after:g} during a run (top-up); use the per-page cost/credits columns"
        else:
            billed += before - after
    return {
        "runs": len(runs),
        "n_fetched": n,
        "wall_seconds": round(wall, 1),
        "pages_per_second": round(n / wall, 3) if wall else None,
        "balance_delta": None if unknown else round(billed, 4),
        "balance_note": note,
        "balance_before_first_run": runs[0].get("credits_before"),
        "balance_after_last_run": runs[-1].get("credits_after"),
        "stop_reasons": [r["stop_reason"] for r in runs if r.get("stop_reason")],
    }


def per_url(sample: pd.DataFrame, vendor_df: pd.DataFrame, vendor: str) -> pd.DataFrame:
    cols = ["cleaned_key", "home_url", "stratum", "response_status_code", "num_errors",
            "parsed_html_len", "ours_verdict", "ours_combined_verdict", "parsed_html"]
    merged = sample[cols].merge(
        vendor_df.drop(columns=["stratum", "url"], errors="ignore"),
        on="cleaned_key", how="inner",
    )
    # The in-house runner has no links array: our extraction over its raw
    # HTML *is* the reference, so array-vs-HTML recall is not applicable.
    has_links_array = not vendor.startswith("inhouse")
    rows = []
    for r in merged.itertuples(index=False):
        plain = markdown_to_plain(r.markdown or "")
        verdict = classify_text_quality(plain, 0 if r.success else 1)
        meta = r.vendor_metadata if isinstance(r.vendor_metadata, dict) else {}
        row = {
            "vendor": vendor,
            "cleaned_key": r.cleaned_key,
            "home_url": r.home_url,
            "stratum": r.stratum,
            "ours_status": r.response_status_code,
            "ours_len": r.parsed_html_len,
            "ours_verdict": r.ours_verdict,
            "success": bool(r.success),
            "http_2xx": bool(r.success) and r.http_status is not None and 200 <= int(r.http_status) < 300,
            "api_status": r.api_status,
            "http_status": r.http_status,
            "final_url": r.final_url,
            "markdown_len": r.markdown_len,
            "plain_len": len(plain),
            "raw_html_len": r.raw_html_len,
            "n_links": r.n_links,
            "wall_seconds": r.wall_seconds,
            "attempts": r.attempts,
            "credits": r.credits,
            "cost_usd": r.cost_usd,
            "error": r.error,
            "vendor_verdict": verdict,
            "recovered": r.ours_verdict != "ok" and verdict == "ok",
            "lost": r.ours_verdict == "ok" and verdict != "ok",
            # how the page was fetched: http/browser/failed (in-house), basic/stealth proxy (Firecrawl)
            "mode": meta.get("method") or meta.get("proxyUsed") or ("failed" if not r.success else None),
        }
        links = link_metrics(r.home_url, r.raw_html or "", r.links or [])
        if not has_links_array:
            for key in ("vendor_n_internal", "internal_recall", "internal_jaccard",
                        "vendor_n_pick", "pick_recall", "pick_jaccard"):
                links[key] = None
        row.update(links)
        rows.append(row)
    return pd.DataFrame(rows)


def summarize(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    def agg(g: pd.DataFrame) -> dict:
        ok = g[g.success]
        out = {
            "n": len(g),
            "success_rate": g.success.mean(),
            "http_2xx_rate": g.http_2xx.mean(),
            # The decision metric: the site answered 2xx AND the text passes the
            # gate. "ok" alone lets a 404 page with a long nav/footer through.
            "usable_share": (g.http_2xx & (g.vendor_verdict == "ok")).mean(),
            "ok_share": (g.vendor_verdict == "ok").mean(),
            "thin_share": (g.vendor_verdict == "thin").mean(),
            "blocked_share": (g.vendor_verdict == "blocked").mean(),
            "parked_share": (g.vendor_verdict == "parked").mean(),
            "empty_or_dead_share": g.vendor_verdict.isin(["empty", "dead", "garbled"]).mean(),
            "ours_ok_share": (g.ours_verdict == "ok").mean(),
            "recovered_share": g.recovered.mean(),
            "lost_share": g.lost.mean(),
            "median_markdown_len": ok.markdown_len.median() if len(ok) else np.nan,
            "median_plain_len": ok.plain_len.median() if len(ok) else np.nan,
            "median_links": ok.n_links.median() if len(ok) else np.nan,
            "median_wall_s": g.wall_seconds.median(),
            "median_wall_s_fetched": ok.wall_seconds.median() if len(ok) else np.nan,
            "p95_wall_s": g.wall_seconds.quantile(0.95),
            # browser fallback (in-house) or stealth/enhanced proxy (Firecrawl): the expensive path
            "escalated_share": g["mode"].isin(["browser", "stealth", "enhanced"]).mean(),
            "mode_mix": g["mode"].fillna("none").value_counts().to_dict(),
            "internal_recall_median": g.internal_recall.median(),
            "internal_recall_ge_0_9": (g.internal_recall >= 0.9).sum() / g.internal_recall.notna().sum()
            if g.internal_recall.notna().any() else np.nan,
            "pick_recall_median": g.pick_recall.median(),
            "credits_reported": g.credits.sum(min_count=1),
            "cost_usd_reported": g.cost_usd.sum(min_count=1),
        }
        return out

    parts = []
    for stratum, g in df.groupby("stratum"):
        parts.append({"vendor": df.vendor.iloc[0], "stratum": stratum, **agg(g)})
    parts.append({"vendor": df.vendor.iloc[0], "stratum": "ALL", **agg(df),
                  "pages_per_second_run": meta.get("pages_per_second"),
                  "balance_delta_run": meta.get("balance_delta")})
    return pd.DataFrame(parts)


def to_markdown_table(summary: pd.DataFrame) -> str:
    cols = [
        ("stratum", "stratum", None),
        ("n", "n", "{:.0f}"),
        ("success_rate", "fetched", "{:.0%}"),
        ("http_2xx_rate", "2xx", "{:.0%}"),
        ("usable_share", "usable (2xx+ok)", "{:.0%}"),
        ("ok_share", "ok", "{:.0%}"),
        ("ours_ok_share", "ours ok", "{:.0%}"),
        ("recovered_share", "recovered", "{:.0%}"),
        ("lost_share", "lost", "{:.0%}"),
        ("thin_share", "thin", "{:.0%}"),
        ("blocked_share", "blocked", "{:.0%}"),
        ("parked_share", "parked", "{:.0%}"),
        ("empty_or_dead_share", "empty/dead", "{:.0%}"),
        ("median_markdown_len", "med md chars", "{:.0f}"),
        ("median_links", "med links", "{:.0f}"),
        ("internal_recall_median", "link recall (med)", "{:.2f}"),
        ("pick_recall_median", "pick recall (med)", "{:.2f}"),
        ("median_wall_s", "med s/page", "{:.1f}"),
        ("median_wall_s_fetched", "med s (fetched)", "{:.1f}"),
        ("p95_wall_s", "p95 s/page", "{:.1f}"),
        ("escalated_share", "browser/stealth", "{:.0%}"),
        ("credits_reported", "credits (api)", "{:.0f}"),
        ("cost_usd_reported", "cost $ (api)", "{:.4f}"),
    ]
    lines = ["| " + " | ".join(h for _, h, _ in cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in summary.iterrows():
        cells = []
        for key, _, fmt in cols:
            val = row.get(key)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                cells.append("—")
            elif fmt:
                cells.append(fmt.format(val))
            else:
                cells.append(str(val))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def headline_tables(summary_df: pd.DataFrame) -> str:
    """Stratum x vendor pivots of the numbers that decide the question."""
    strata = [s for s in ("ok_200", "empty_combined", "status_429", "status_0", "status_404", "ALL")
              if s in set(summary_df.stratum)]
    vendors = list(summary_df.vendor.unique())
    out = []
    for key, title, fmt in (
        ("usable_share", "Usable: site answered 2xx AND the text passes `classify_text_quality`", "{:.0%}"),
        ("http_2xx_rate", "Share fetched with a 2xx status", "{:.0%}"),
        ("ok_share", "Share whose text passes the gate regardless of status (a 404 page with a long nav can pass)", "{:.0%}"),
        ("escalated_share", "Share needing the expensive path (browser fallback / stealth proxy)", "{:.0%}"),
        ("median_wall_s_fetched", "Median seconds per fetched page", "{:.1f}"),
        ("p95_wall_s", "p95 seconds per page (all attempts)", "{:.1f}"),
        ("internal_recall_median", "Median recall of our HTML link extraction by the vendor links array", "{:.2f}"),
    ):
        pivot = summary_df.pivot(index="stratum", columns="vendor", values=key).reindex(strata)
        lines = [f"**{title}**", "", "| stratum | " + " | ".join(vendors) + " |",
                 "|---|" + "|".join("---" for _ in vendors) + "|"]
        for stratum in strata:
            cells = []
            for v in vendors:
                val = pivot.loc[stratum, v] if v in pivot.columns else np.nan
                cells.append("—" if val is None or (isinstance(val, float) and np.isnan(val)) else fmt.format(val))
            lines.append(f"| {stratum} | " + " | ".join(cells) + " |")
        out.append("\n".join(lines))
    return "\n\n".join(out)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)

    sample = read_dataframe(args.out_dir / SAMPLE_FILENAME)
    long_parts, summary_parts, md_sections = [], [], []
    for path in sorted(args.out_dir.glob("*_results.parquet")):
        vendor = path.name[: -len("_results.parquet")]
        if vendor.startswith("comparison"):
            continue
        vendor_df = read_dataframe(path)
        logger.info("%s: %s fetched rows", vendor, len(vendor_df))
        df = per_url(sample, vendor_df, vendor)
        meta = run_meta_summary(args.out_dir, vendor)
        summary = summarize(df, meta)
        long_parts.append(df)
        summary_parts.append(summary)
        crosstab = pd.crosstab(df.ours_verdict, df.vendor_verdict).reindex(
            index=[v for v in VERDICTS if v in set(df.ours_verdict)],
            columns=[v for v in VERDICTS if v in set(df.vendor_verdict)],
        )
        mode_mix = pd.crosstab(df.stratum, df["mode"].fillna("none"))
        md_sections.append(
            f"### {vendor}\n\n"
            f"Run meta: {json.dumps({k: v for k, v in meta.items() if k not in ('balance_before_first_run', 'balance_after_last_run')}, default=str)}\n\n"
            f"Balance before first run: `{json.dumps(meta.get('balance_before_first_run'), default=str)}`; "
            f"after last run: `{json.dumps(meta.get('balance_after_last_run'), default=str)}`\n\n"
            + to_markdown_table(summary)
            + "\n\nOur cached verdict (rows) vs vendor verdict (columns):\n\n"
            + crosstab.to_markdown()
            + "\n\nFetch mode by stratum:\n\n"
            + mode_mix.to_markdown()
            + "\n"
        )

    if not long_parts:
        raise SystemExit(f"no <vendor>_results.parquet under {args.out_dir}; run a vendor first")

    long_df = pd.concat(long_parts, ignore_index=True)
    summary_df = pd.concat(summary_parts, ignore_index=True)
    lineage = {
        "created_by": "vdl_tools.scrape_enrich.scraper.vendor_pilot.compare",
        "git_sha": git_sha(),
        "sample": str(args.out_dir / SAMPLE_FILENAME),
        "vendors": sorted(long_df.vendor.unique()),
        "max_per_subpath": MAX_PER_SUBPATH,
    }
    write_dataframe(long_df, args.out_dir / "comparison_results.parquet", lineage=lineage)
    write_dataframe(summary_df, args.out_dir / "comparison_summary.parquet", lineage=lineage)
    md = "## Headline\n\n" + headline_tables(summary_df) + "\n\n## Per vendor\n\n" + "\n\n".join(md_sections)
    (args.out_dir / "comparison_summary.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
