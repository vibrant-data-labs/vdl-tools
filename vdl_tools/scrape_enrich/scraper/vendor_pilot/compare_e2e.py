"""Phase 2 comparison: vendor end to end vs in-house end to end vs the cache,
per org and in aggregate, plus the hybrid simulation.

Inputs under the output directory: ``sample_orgs.parquet`` (with the cache
columns), ``<vendor>_e2e_{orgs,pages}.parquet`` and
``<inhouse>_e2e_{orgs,pages}.parquet`` from the runners, and optionally
``<vendor>_hybrid_topup_pages.parquet`` from ``run_hybrid_topup.py``.

Per org: usable (home 2xx and combined-text verdict ok) for the vendor,
the in-house run and the cache; credits spent (home plus subpages,
failures free); subpages chosen by each, with the overlap between the
vendor's array-derived picks, the vendor's HTML-derived picks, the in-house
picks and the cache's stored subpaths; wall time.

Hybrid: in-house first; the vendor wholesale when the in-house home is not
usable (its home and the subpages its links choose, as fetched in the
vendor-only run); otherwise the vendor only for the in-house subpages that
are not usable (from the vendor-only run when it fetched the same URL,
else from the top-up run). The combined text is rebuilt with
``make_group_text`` over the assembled pages; credits are the vendor pages
actually used. Pages the hybrid would need but no run fetched are counted
as ``missing`` (run the top-up to fill them).

Writes ``e2e_comparison_orgs.parquet``, ``e2e_summary.parquet`` and
``e2e_summary.md``. Run from the repo root::

    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare_e2e --vendor fake --inhouse inhouse_laptop
    python -m vdl_tools.scrape_enrich.scraper.vendor_pilot.compare_e2e
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe

from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR, git_sha
from vdl_tools.scrape_enrich.scraper.vendor_pilot._e2e import PageResult, combine, e2e_paths
from vdl_tools.scrape_enrich.scraper.vendor_pilot.sample_orgs import ORGS_FILENAME


def _norm(paths) -> set:
    return {str(p).strip("/") for p in (paths or []) if str(p).strip("/")}


def _jaccard(a: set, b: set) -> float | None:
    return len(a & b) / len(a | b) if (a | b) else None


def _recall(ref: set, other: set) -> float | None:
    return len(ref & other) / len(ref) if ref else None


def _page(row) -> PageResult:
    return PageResult(url=row.url, subpath=row.subpath, page_type=row.page_type, success=bool(row.success),
                      http_status=row.http_status, text=row.text or "", credits=row.credits,
                      wall_seconds=row.wall_seconds, verdict=row.verdict)


def hybrid_for_org(key: str, home_url: str, inhouse_pages: pd.DataFrame, vendor_org: pd.Series | None,
                   vendor_pages: pd.DataFrame, topup_pages: pd.DataFrame) -> dict:
    ih = inhouse_pages[inhouse_pages.cleaned_home_key == key]
    home_rows = ih[ih.page_type == "index"]
    if home_rows.empty:
        return {"hybrid_usable": False, "hybrid_credits": 0.0, "hybrid_level": "no_inhouse", "hybrid_missing": 0}
    home = _page(home_rows.iloc[0])
    if not (home.http_2xx and home.verdict == "ok"):
        # Home-level fallback: the vendor's whole org result.
        if vendor_org is None:
            return {"hybrid_usable": False, "hybrid_credits": 0.0, "hybrid_level": "home_missing", "hybrid_missing": 1}
        return {"hybrid_usable": bool(vendor_org.usable), "hybrid_credits": float(vendor_org.credits_total),
                "hybrid_level": "home", "hybrid_missing": 0}
    pages, credits, missing, replaced = [home], 0.0, 0, 0
    vp = vendor_pages[vendor_pages.cleaned_home_key == key] if not vendor_pages.empty else vendor_pages
    tp = topup_pages[topup_pages.cleaned_home_key == key] if not topup_pages.empty else topup_pages
    for row in ih[ih.page_type == "page"].itertuples(index=False):
        page = _page(row)
        if page.success and page.verdict == "ok":
            pages.append(page)
            continue
        cand = pd.concat([d[d.url == row.url] for d in (vp, tp) if not d.empty and "url" in d.columns]) if (not vp.empty or not tp.empty) else pd.DataFrame()
        if cand.empty:
            missing += 1
            pages.append(page)
            continue
        v = _page(cand.iloc[0])
        credits += float(v.credits or 0)
        replaced += 1
        pages.append(v)
    combined = combine(home_url, pages)
    verdict = classify_text_quality(combined, 0 if combined else 1)
    return {"hybrid_usable": bool(home.http_2xx and verdict == "ok"), "hybrid_credits": credits,
            "hybrid_level": "subpages" if replaced or missing else "inhouse_only", "hybrid_missing": missing,
            "hybrid_replaced": replaced}


def build(out_dir: Path, vendor: str, inhouse: str) -> tuple[pd.DataFrame, dict]:
    sample = read_dataframe(out_dir / ORGS_FILENAME)
    vp, ip = e2e_paths(out_dir, vendor), e2e_paths(out_dir, inhouse)
    v_orgs = read_dataframe(vp["orgs"]).set_index("cleaned_home_key") if vp["orgs"].exists() else pd.DataFrame()
    v_pages = read_dataframe(vp["pages"]) if vp["pages"].exists() else pd.DataFrame()
    i_orgs = read_dataframe(ip["orgs"]).set_index("cleaned_home_key") if ip["orgs"].exists() else pd.DataFrame()
    i_pages = read_dataframe(ip["pages"]) if ip["pages"].exists() else pd.DataFrame()
    topup_path = out_dir / f"{vendor}_hybrid_topup_pages.parquet"
    t_pages = read_dataframe(topup_path) if topup_path.exists() else pd.DataFrame()

    rows = []
    for org in sample.itertuples(index=False):
        key = org.cleaned_home_key
        r = {"cleaned_home_key": key, "home_url": org.home_url, "org_id": org.org_id,
             "in_cache": bool(org.in_cache), "cache_usable": org.cache_combined_verdict == "ok",
             "cache_combined_verdict": org.cache_combined_verdict, "cache_n_subpages": int(org.cache_n_subpages)}
        cache_sub = _norm(org.cache_subpaths)
        v = v_orgs.loc[key] if key in v_orgs.index else None
        i = i_orgs.loc[key] if key in i_orgs.index else None
        for tag, o in (("vendor", v), ("inhouse", i)):
            if o is None:
                r[f"{tag}_run"] = False
                continue
            r[f"{tag}_run"] = True
            r[f"{tag}_usable"] = bool(o.usable)
            r[f"{tag}_home_2xx"] = bool(o.home_http_2xx)
            r[f"{tag}_home_verdict"] = o.home_verdict
            r[f"{tag}_combined_verdict"] = o.combined_verdict
            r[f"{tag}_combined_len"] = int(o.combined_len)
            r[f"{tag}_credits"] = float(o.credits_total)
            r[f"{tag}_n_subpages_chosen"] = int(o.n_subpages_chosen)
            r[f"{tag}_n_subpages_fetched"] = int(o.n_subpages_fetched)
            r[f"{tag}_n_pages"] = int(o.n_pages)
            r[f"{tag}_wall_seconds"] = float(o.wall_seconds)
            r[f"{tag}_home_mode"] = o.home_mode
            r[f"{tag}_is_single_page"] = bool(o.is_single_page)
        if v is not None:
            va, vh = _norm(v.picks_array), _norm(v.picks_html)
            r["vendor_array_vs_html_jaccard"] = _jaccard(va, vh)
            r["vendor_array_recall_of_html"] = _recall(vh, va)
            r["vendor_vs_cache_jaccard"] = _jaccard(va, cache_sub) if org.in_cache else None
            if i is not None:
                ih = _norm(i.picks_used)
                r["vendor_vs_inhouse_jaccard"] = _jaccard(va, ih)
                r["vendor_recall_of_inhouse_picks"] = _recall(ih, va)
        if i is not None and org.in_cache:
            r["inhouse_vs_cache_jaccard"] = _jaccard(_norm(i.picks_used), cache_sub)
        if i is not None:
            r.update(hybrid_for_org(key, org.home_url, i_pages, v, v_pages, t_pages))
        rows.append(r)
    df = pd.DataFrame(rows)

    def meta(paths):
        if not paths["meta"].exists():
            return {}
        runs = json.loads(paths["meta"].read_text())
        n_pages = sum(m["n_pages_this_run"] for m in runs)
        wall = sum(m["wall_seconds_this_run"] for m in runs if m["n_pages_this_run"])
        retries = {}
        for m in runs:
            for k, c in (m.get("api_retries_by_status") or {}).items():
                retries[k] = retries.get(k, 0) + c
        return {"runs": len(runs), "n_pages": n_pages, "wall_seconds": round(wall, 1),
                "pages_per_second": round(n_pages / wall, 3) if wall else None,
                "api_retries_by_status": retries, "stop_reasons": [m["stop_reason"] for m in runs if m.get("stop_reason")],
                "balance_before": runs[0].get("balance_before"), "balance_after": runs[-1].get("balance_after"),
                "context": runs[-1].get("runner_context")}
    return df, {"vendor": meta(vp), "inhouse": meta(ip), "topup_pages": int(len(t_pages))}


def summarize(df: pd.DataFrame, metas: dict, vendor: str, inhouse: str) -> tuple[pd.DataFrame, str]:
    both = df[df.get("vendor_run", False) & df.get("inhouse_run", False)] if "vendor_run" in df and "inhouse_run" in df else df.iloc[0:0]
    lines = [f"## Phase 2: end to end, {len(df)} orgs sampled; vendor `{vendor}` ran {int(df.get('vendor_run', pd.Series(dtype=bool)).sum())}, "
             f"in-house `{inhouse}` ran {int(df.get('inhouse_run', pd.Series(dtype=bool)).sum())}, both {len(both)}, in cache {int(df.in_cache.sum())}", ""]
    rows = []

    def add(name, mask_df, usable_col, credits_col=None, sub_col=None, pages_col=None, wall_col=None):
        d = mask_df
        row = {"run": name, "n": len(d), "usable": d[usable_col].mean() if len(d) else np.nan}
        row["credits_total"] = d[credits_col].sum() if credits_col and credits_col in d else np.nan
        row["credits_per_org"] = d[credits_col].mean() if credits_col and credits_col in d else np.nan
        row["subpages_chosen_mean"] = d[sub_col].mean() if sub_col and sub_col in d else np.nan
        row["pages_per_org"] = d[pages_col].mean() if pages_col and pages_col in d else np.nan
        row["wall_s_per_org_median"] = d[wall_col].median() if wall_col and wall_col in d else np.nan
        rows.append(row)

    if len(both):
        add(f"{vendor} (vendor only)", both, "vendor_usable", "vendor_credits", "vendor_n_subpages_chosen", "vendor_n_pages", "vendor_wall_seconds")
        add(f"{inhouse} (in-house only)", both, "inhouse_usable", None, "inhouse_n_subpages_chosen", "inhouse_n_pages", "inhouse_wall_seconds")
        if "hybrid_usable" in both:
            add("hybrid (in-house first, vendor on failure)", both, "hybrid_usable", "hybrid_credits")
        cache = both[both.in_cache]
        add("cache (historical combined_text)", cache, "cache_usable", None, "cache_n_subpages")
    summary = pd.DataFrame(rows)
    if len(summary):
        lines.append("| run | n orgs | usable | credits total | credits / org | subpages chosen / org | pages / org | median s / org |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for _, r in summary.iterrows():
            f = lambda v, fmt: "—" if v is None or (isinstance(v, float) and np.isnan(v)) else fmt.format(v)
            lines.append(f"| {r.run} | {r.n} | {f(r.usable, '{:.1%}')} | {f(r.credits_total, '{:.0f}')} | {f(r.credits_per_org, '{:.2f}')} | "
                         f"{f(r.subpages_chosen_mean, '{:.1f}')} | {f(r.pages_per_org, '{:.1f}')} | {f(r.wall_s_per_org_median, '{:.1f}')} |")
        lines.append("")
    if len(both):
        v, i = both.vendor_usable, both.inhouse_usable
        lines.append(f"Paired (both ran): usable on both {int((v & i).sum())}, vendor only {int((v & ~i).sum())}, in-house only {int((~v & i).sum())}, neither {int((~v & ~i).sum())}.")
        if "hybrid_usable" in both:
            h = both.hybrid_usable
            vc, hc = both.vendor_credits.sum(), both.hybrid_credits.sum()
            lvl = both.hybrid_level.value_counts().to_dict()
            lines.append(f"Hybrid: usable {h.mean():.1%} (vendor only {v.mean():.1%}, in-house only {i.mean():.1%}); credits {hc:.0f} = "
                         f"{(hc / vc if vc else float('nan')):.0%} of vendor-only {vc:.0f}; fallback level {lvl}; "
                         f"pages the hybrid needed but no run fetched: {int(both.hybrid_missing.sum())}.")
        for col, label in (("vendor_array_recall_of_html", "vendor links array recall of the vendor's own HTML-derived picks"),
                           ("vendor_recall_of_inhouse_picks", "vendor array recall of the in-house picks"),
                           ("vendor_vs_inhouse_jaccard", "Jaccard vendor array picks vs in-house picks"),
                           ("vendor_vs_cache_jaccard", "Jaccard vendor array picks vs cache subpaths"),
                           ("inhouse_vs_cache_jaccard", "Jaccard in-house picks vs cache subpaths")):
            if col in both and both[col].notna().any():
                s = both[col].dropna()
                lines.append(f"- {label}: median {s.median():.2f}, mean {s.mean():.2f} (n={len(s)})")
        lines.append("")
        lines.append("Combined-text verdicts: vendor " + json.dumps(both.vendor_combined_verdict.value_counts().to_dict())
                     + "; in-house " + json.dumps(both.inhouse_combined_verdict.value_counts().to_dict())
                     + "; cache " + json.dumps(both[both.in_cache].cache_combined_verdict.value_counts().to_dict()))
    lines.append("")
    lines.append("Run meta: " + json.dumps({k: {kk: vv for kk, vv in m.items() if kk not in ("balance_before", "balance_after", "context")}
                                            if isinstance(m, dict) else m for k, m in metas.items()}, default=str))
    for k in ("vendor", "inhouse"):
        m = metas.get(k) or {}
        if m.get("balance_before") is not None:
            lines.append(f"{k} balance: before `{json.dumps(m['balance_before'], default=str)}` after `{json.dumps(m['balance_after'], default=str)}`")
    return summary, "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--vendor", default=None, help="vendor e2e runner name (default: firecrawl if present, else fake)")
    parser.add_argument("--inhouse", default="inhouse_laptop")
    args = parser.parse_args(argv)
    vendor = args.vendor or ("firecrawl" if e2e_paths(args.out_dir, "firecrawl")["orgs"].exists() else "fake")
    df, metas = build(args.out_dir, vendor, args.inhouse)
    summary, md = summarize(df, metas, vendor, args.inhouse)
    lineage = {"created_by": "vendor_pilot.compare_e2e", "git_sha": git_sha(), "vendor": vendor, "inhouse": args.inhouse}
    write_dataframe(df, args.out_dir / "e2e_comparison_orgs.parquet", lineage=lineage)
    if len(summary):
        write_dataframe(summary, args.out_dir / "e2e_summary.parquet", lineage=lineage)
    (args.out_dir / "e2e_summary.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
