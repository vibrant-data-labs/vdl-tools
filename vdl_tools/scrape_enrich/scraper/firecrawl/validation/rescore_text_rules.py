"""Phase 0 rescore: the production text rules over the saved pilot artifacts.

Zero credits. Re-derives every phase 2 org's combined text from the stored
Firecrawl markdown through ``markdown_clean`` (the module the production
pipeline will call), recomputes the usable rate, and measures what the rules
do to page size, tokens per org and duplicated lines. Then diffs subpage picks
on the phase 1 links arrays: the pilot's rule (``filter_anchors`` against the
URL we asked for) against ``same_site_paths`` (exact host compare against the
URL the page was served from).

Inputs, all in ``--out-dir``: ``firecrawl_e2e_pages.parquet``,
``firecrawl_e2e_orgs.parquet``, ``inhouse_laptop_e2e_pages.parquet``,
``e2e_comparison_orgs.parquet`` (phase 2) and ``firecrawl_results.parquet``
(phase 1).

Outputs, in ``--out-dir``:

- ``rescore_pages.parquet``: one row per 2xx Firecrawl page with its pilot
  verdict, new verdict and cleaned text.
- ``rescore_orgs.parquet``: one row per org with the Firecrawl combined text
  under the production rules and the in-house combined text from the same
  day. This is the input to the paired LLM test (D3).
- ``rescore_picks.parquet``: one row per fetched phase 1 home page with the
  old and new subpage picks.
- ``rescore_summary.md``: the tables quoted in the README.

Every parquet carries ``CLEANER_VERSION`` and the git sha in its lineage
footer, so a D3 result can name the text it was measured on.

    python -m vdl_tools.scrape_enrich.scraper.firecrawl.validation.rescore_text_rules \\
        --out-dir /path/to/.cache/vendor_pilot
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vdl_tools.scrape_enrich.scraper.firecrawl import markdown_clean as mc
from vdl_tools.scrape_enrich.scraper.firecrawl.site_links import normalize_host, same_site_paths
from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.scrape_enrich.scraper.vendor_pilot._common import DEFAULT_OUT_DIR, git_sha
from vdl_tools.scrape_enrich.scraper.vendor_pilot._e2e import MAX_PER_SUBPATH, picks_from_array
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.parquet_cache import read_dataframe, write_dataframe
from vdl_tools.shared_tools.web_summarization.make_page_text import MODEL, make_group_text
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import deterministic_len_key, filter_links
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP

ORGS_PER_REFRESH = 30_000   # the CFT org count the plan's cost figures use
INPUT_PRICE_PER_TOKEN = MODEL_DATA[MODEL]["input_cost_per_token"]


def _is_2xx(status) -> bool:
    return status is not None and status == status and 200 <= int(status) < 300


# ---------------------------------------------------------------------------
# Phase 2: pages and orgs
# ---------------------------------------------------------------------------

def rescore_pages(pages: pd.DataFrame) -> pd.DataFrame:
    """Every 2xx Firecrawl page through ``page_text``."""
    ok = pages[pages["success"] & pages["http_status"].map(_is_2xx)].copy()
    results = [mc.page_text(t) for t in ok["text"]]
    ok["pilot_verdict"] = ok["verdict"]
    ok["verdict"] = [r.verdict for r in results]
    ok["raw_len"] = ok["text"].str.len()
    ok["clean_text"] = [r.text for r in results]
    ok["clean_len"] = ok["clean_text"].str.len()
    return ok[["org_id", "cleaned_home_key", "home_url", "subpath", "page_type", "http_status",
               "raw_len", "clean_len", "pilot_verdict", "verdict", "text", "clean_text"]]


def _ordered(rows: pd.DataFrame) -> pd.DataFrame:
    """Combine order: home page first, then subpages by deterministic_len_key."""
    order = [(r.page_type != "index", deterministic_len_key(r.subpath)) for r in rows.itertuples(index=False)]
    return rows.iloc[sorted(range(len(rows)), key=order.__getitem__)]


def _dup_share(texts) -> tuple[int, int]:
    """(characters in lines already seen earlier in the org, characters in all
    non-blank lines), with dedupe's line normalization. On cleaned pages the
    first number is exactly what dedupe removes."""
    seen, dup, total = set(), 0, 0
    for text in texts:
        for line in (text or "").split("\n"):
            key = mc._line_key(line)
            if not key:
                continue
            total += len(line)
            if key in seen:
                dup += len(line)
            else:
                seen.add(key)
    return dup, total


def _raw_combined(source: str, rows: pd.DataFrame, text_col: str) -> str:
    """The pilot's combine: every 2xx page's text through make_group_text."""
    records = [{"type": "index" if r.page_type == "index" else "page", "source": source,
                "subpath": r.subpath, "text": getattr(r, text_col) or ""}
               for r in rows.itertuples(index=False)]
    if not records:
        return ""
    return make_group_text(mc.COUNTING_PROMPT, records) or ""


def rescore_orgs(fc_orgs: pd.DataFrame, fc_pages: pd.DataFrame, ih_pages: pd.DataFrame,
                 comparison: pd.DataFrame) -> pd.DataFrame:
    by_org = {k: g for k, g in fc_pages.groupby("cleaned_home_key")}
    ih_ok = ih_pages[ih_pages["success"] & ih_pages["http_status"].map(_is_2xx)]
    ih_by_org = {k: g for k, g in ih_ok.groupby("cleaned_home_key")}
    cmp = comparison.set_index("cleaned_home_key")
    rows = []
    for org in fc_orgs.itertuples(index=False):
        key = org.cleaned_home_key
        pages = by_org.get(key, fc_pages.iloc[0:0])
        org_pages = [mc.OrgPage(subpath=r.subpath, is_home=r.page_type == "index",
                                verdict=r.verdict, text=r.clean_text)
                     for r in pages.itertuples(index=False)]
        result = mc.org_text(org.home_url, org_pages)
        ordered = sorted((p for p in org_pages if p.verdict in mc.COMBINABLE_VERDICTS and p.text),
                         key=mc._page_order)
        dup_clean, total_clean = _dup_share([p.text for p in ordered])
        dup_raw, total_raw = _dup_share(_ordered(pages)["text"])
        raw_combined = _raw_combined(org.home_url, pages, "text")
        ih_rows = ih_by_org.get(key)
        ih_combined = _raw_combined(org.home_url, ih_rows, "text") if ih_rows is not None else ""
        dup_ih, total_ih = _dup_share(_ordered(ih_rows)["text"]) if ih_rows is not None else (0, 0)
        c = cmp.loc[key] if key in cmp.index else None
        rows.append({
            "org_id": org.org_id,
            "cleaned_home_key": key,
            "home_url": org.home_url,
            "home_2xx": bool(org.home_http_2xx),
            "home_verdict": next((p.verdict for p in org_pages if p.is_home), "no_2xx_home"),
            "pilot_verdict": org.combined_verdict,
            "pilot_usable": bool(org.usable),
            "verdict": result.verdict,
            "usable": bool(org.home_http_2xx) and result.verdict == "ok",
            # The pilot's org definition on the new text, to show what
            # running the markers once, per page, changes.
            "usable_if_reclassified": bool(org.home_http_2xx) and classify_text_quality(result.text) == "ok",
            "combined_text": result.text,
            "combined_len": len(result.text),
            "subpaths": list(result.subpaths),
            "n_pages_2xx": len(org_pages),
            "n_pages_used": len(result.subpaths),
            "tokens": result.tokens,
            "tokens_before_cap": result.tokens_before_cap,
            "truncated": result.truncated,
            "raw_tokens": mc.count_tokens(raw_combined),
            "dup_chars_raw": dup_raw, "chars_raw": total_raw,
            "dup_chars_clean": dup_clean, "chars_clean": total_clean,
            "inhouse_usable": bool(c["inhouse_usable"]) if c is not None else False,
            "inhouse_text": ih_combined,
            "inhouse_tokens": mc.count_tokens(ih_combined),
            "dup_chars_inhouse": dup_ih, "chars_inhouse": total_ih,
            "extractor": result.extractor,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Phase 1: subpage picks, old rule against new
# ---------------------------------------------------------------------------

def _jaccard(a: set, b: set) -> float | None:
    return len(a & b) / len(a | b) if (a | b) else None


def rescore_picks(phase1: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for r in phase1.itertuples(index=False):
        if not (r.success and _is_2xx(r.http_status)):
            continue
        links = json.loads(r.links) if isinstance(r.links, str) else list(r.links or [])
        final_url = r.final_url if isinstance(r.final_url, str) and r.final_url else r.url
        old = {p.strip("/") for p in picks_from_array(r.url, links)}
        candidates = same_site_paths(links, final_url)
        new = {p.strip("/") for p in filter_links(candidates, keep_paths=PATHS_TO_KEEP,
                                                  max_per_subpath=MAX_PER_SUBPATH)}
        old.discard("")
        new.discard("")
        rows.append({
            "url": r.url, "final_url": final_url, "stratum": r.stratum,
            "host_changed": normalize_host(r.url) != normalize_host(final_url),
            "n_links": len(links), "n_candidates": len(candidates),
            "picks_old": sorted(old), "picks_new": sorted(new),
            "n_old": len(old), "n_new": len(new),
            "jaccard": _jaccard(old, new),
            "identical": old == new,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _pct(x: float) -> str:
    return f"{100 * x:.1f}%"


def _dist(s: pd.Series) -> str:
    s = s.dropna()
    return (f"mean {s.mean():,.0f}, median {s.median():,.0f}, p90 {s.quantile(0.9):,.0f}, "
            f"max {s.max():,.0f}")


def summarize(pages: pd.DataFrame, orgs: pd.DataFrame, picks: pd.DataFrame,
              ih_pages: pd.DataFrame) -> str:
    n = len(orgs)
    ih_ok = ih_pages[ih_pages["success"] & ih_pages["http_status"].map(_is_2xx)]
    paired = pd.crosstab(orgs["usable"], orgs["inhouse_usable"])
    both = int(paired.loc[True, True]) if True in paired.index and True in paired.columns else 0
    vendor_only = int(((orgs["usable"]) & (~orgs["inhouse_usable"])).sum())
    ih_only = int(((~orgs["usable"]) & (orgs["inhouse_usable"])).sum())
    lost = orgs[orgs["pilot_usable"] & ~orgs["usable"]]
    gained = orgs[~orgs["pilot_usable"] & orgs["usable"]]
    with_text = orgs[orgs["tokens"] > 0]
    ih_text = orgs[orgs["inhouse_tokens"] > 0]

    def cost(tokens: pd.Series) -> str:
        return f"${tokens.mean() * ORGS_PER_REFRESH * INPUT_PRICE_PER_TOKEN:,.0f}"

    def share(dup: str, tot: str, df: pd.DataFrame) -> str:
        return _pct(df[dup].sum() / max(df[tot].sum(), 1))

    lines = [
        "## Phase 0 rescore: production text rules over the pilot artifacts",
        "",
        f"`markdown_clean` CLEANER_VERSION {mc.CLEANER_VERSION} (`{mc.EXTRACTOR}`), "
        f"cap {mc.ORG_TOKEN_CAP:,} tokens, git {git_sha()[:10]}, "
        f"{datetime.now(timezone.utc).date().isoformat()}. Zero credits.",
        "",
        "### Usable orgs (home 2xx and combined verdict ok)",
        "",
        "| text | usable | n |",
        "|---|---|---|",
        f"| pilot: raw markdown, markers on the combined text | {_pct(orgs['pilot_usable'].mean())} | {n} |",
        f"| production rules: markers per page, length check on the combined text | {_pct(orgs['usable'].mean())} | {n} |",
        f"| production text, reclassified with `classify_text_quality` | {_pct(orgs['usable_if_reclassified'].mean())} | {n} |",
        f"| in-house, same day | {_pct(orgs['inhouse_usable'].mean())} | {n} |",
        "",
        f"Paired with in-house on the production rules: both {both}, Firecrawl only {vendor_only}, "
        f"in-house only {ih_only}, neither {n - both - vendor_only - ih_only}.",
        "",
        f"Pilot usable but not under the production rules: {len(lost)}, by new verdict "
        f"{lost['verdict'].value_counts().to_dict()}. Not usable in the pilot but usable now: {len(gained)}.",
        "",
        "Org verdicts, pilot (rows) against production rules (columns):",
        "",
        pd.crosstab(orgs["pilot_verdict"], orgs["verdict"]).to_markdown(),
        "",
        "### Pages (2xx Firecrawl pages)",
        "",
        "| median characters | home | subpages | all |",
        "|---|---|---|---|",
        f"| raw markdown | {pages.loc[pages.page_type == 'index', 'raw_len'].median():,.0f} | "
        f"{pages.loc[pages.page_type == 'page', 'raw_len'].median():,.0f} | {pages['raw_len'].median():,.0f} |",
        f"| cleaned | {pages.loc[pages.page_type == 'index', 'clean_len'].median():,.0f} | "
        f"{pages.loc[pages.page_type == 'page', 'clean_len'].median():,.0f} | {pages['clean_len'].median():,.0f} |",
        f"| in-house text, same day | | | {ih_ok['text'].fillna('').str.len().median():,.0f} |",
        "",
        "Page verdicts, pilot (rows) against production rules (columns):",
        "",
        pd.crosstab(pages["pilot_verdict"], pages["verdict"]).to_markdown(),
        "",
        "### Tokens per org (orgs with any text) and summarizer input cost",
        "",
        f"Cost is mean tokens per org times {ORGS_PER_REFRESH:,} orgs at {MODEL}'s input price "
        f"(${INPUT_PRICE_PER_TOKEN * 1e6:.2f} per million), combined text only, prompt excluded.",
        "",
        "| text | tokens per org | over 8k | over 32k | cost per 30k orgs |",
        "|---|---|---|---|---|",
        f"| raw markdown (pilot combine) | {_dist(orgs.loc[orgs.raw_tokens > 0, 'raw_tokens'])} | "
        f"{(orgs.raw_tokens > 8000).sum()} | {(orgs.raw_tokens > 32000).sum()} | {cost(orgs.loc[orgs.raw_tokens > 0, 'raw_tokens'])} |",
        f"| cleaned and deduped, before the cap | {_dist(with_text['tokens_before_cap'])} | "
        f"{(with_text.tokens_before_cap > 8000).sum()} | {(with_text.tokens_before_cap > 32000).sum()} | {cost(with_text['tokens_before_cap'])} |",
        f"| production rules (cap applied) | {_dist(with_text['tokens'])} | "
        f"{(with_text.tokens > 8000).sum()} | 0 | {cost(with_text['tokens'])} |",
        f"| in-house, same day | {_dist(ih_text['inhouse_tokens'])} | "
        f"{(ih_text.inhouse_tokens > 8000).sum()} | {(ih_text.inhouse_tokens > 32000).sum()} | {cost(ih_text['inhouse_tokens'])} |",
        "",
        f"Orgs cut by the cap: {int(orgs['truncated'].sum())}.",
        "",
        "### Lines repeated across an org's pages (share of characters dedupe removes)",
        "",
        f"Raw markdown {share('dup_chars_raw', 'chars_raw', orgs)}; cleaned pages before dedupe "
        f"{share('dup_chars_clean', 'chars_clean', orgs)}; in-house text {share('dup_chars_inhouse', 'chars_inhouse', orgs)}. "
        "Dedupe removes all of the cleaned share.",
        "",
        "### Subpage picks on the phase 1 links arrays",
        "",
        "Old rule: `filter_anchors` over the links array against the URL we asked for (the pilot). "
        "New rule: `same_site_paths` against the URL the page was served from. Both then go "
        f"through `filter_links` with `max_per_subpath={MAX_PER_SUBPATH}`.",
        "",
        f"- Fetched 2xx home pages compared: {len(picks)}; identical picks {_pct(picks['identical'].mean())}; "
        f"Jaccard median {picks['jaccard'].median():.2f}, mean {picks['jaccard'].mean():.2f} "
        f"(n={picks['jaccard'].notna().sum()} with any pick).",
        f"- Home pages served from another host: {int(picks['host_changed'].sum())}; picks old rule "
        f"{int(picks.loc[picks.host_changed, 'n_old'].sum())}, new rule {int(picks.loc[picks.host_changed, 'n_new'].sum())}.",
        f"- Same host: old {int(picks.loc[~picks.host_changed, 'n_old'].sum())} picks, new "
        f"{int(picks.loc[~picks.host_changed, 'n_new'].sum())}; pages where they differ "
        f"{int((~picks.loc[~picks.host_changed, 'identical']).sum())}.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)
    d = args.out_dir

    fc_pages = rescore_pages(read_dataframe(d / "firecrawl_e2e_pages.parquet"))
    ih_pages = read_dataframe(d / "inhouse_laptop_e2e_pages.parquet")
    orgs = rescore_orgs(read_dataframe(d / "firecrawl_e2e_orgs.parquet"), fc_pages, ih_pages,
                        read_dataframe(d / "e2e_comparison_orgs.parquet"))
    picks = rescore_picks(read_dataframe(d / "firecrawl_results.parquet"))

    lineage = {
        "created_by": "vdl_tools.scrape_enrich.scraper.firecrawl.validation.rescore_text_rules",
        "git_sha": git_sha(),
        "cleaner_version": mc.CLEANER_VERSION,
        "extractor": mc.EXTRACTOR,
        "org_token_cap": mc.ORG_TOKEN_CAP,
        "inputs": [str(d / f) for f in ("firecrawl_e2e_pages.parquet", "firecrawl_e2e_orgs.parquet",
                                        "inhouse_laptop_e2e_pages.parquet", "e2e_comparison_orgs.parquet",
                                        "firecrawl_results.parquet")],
    }
    write_dataframe(fc_pages.drop(columns=["text"]), d / "rescore_pages.parquet", lineage=lineage)
    write_dataframe(orgs, d / "rescore_orgs.parquet", lineage=lineage)
    write_dataframe(picks, d / "rescore_picks.parquet", lineage=lineage)
    summary = summarize(fc_pages, orgs, picks, ih_pages)
    (d / "rescore_summary.md").write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()
