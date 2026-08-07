"""Phase 2, stages 4-5 — per-text summaries and the general org summary.

Stage 4: each scraped site gets one LLM summary (shared Postgres prompt
cache, gpt-4.1-mini). Stage 5: per row, every text we hold — customer
description, source descriptions, grant purposes, website summaries — is
synthesized into one ``Summary`` paragraph; ``text_for_taxonomy`` is the
longer of Summary and the best website summary (mirrors climate-landscape's
pre-OE step). Rows with exactly one text skip the LLM: the summary IS that
text. (spec: phase2-enrichment.md §4-5)
"""

from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger

SUMMARIES_BASENAME = "org_summaries"

# Texts feeding the general summary, in preference order.
DESCRIPTION_COLUMNS = [
    "customer_description",
    "nzi_description", "nzi_pitchline",
    "cb_description", "cb_short_description",
    "gt_unique_text", "gt_grant_purposes",
]
MIN_TEXT_CHARS = 60


def _default_website_summarizer(frame: pd.DataFrame) -> dict:
    from vdl_tools.shared_tools.web_summarization.website_summarization_psql import (
        summarize_scraped_df,
    )

    return summarize_scraped_df(frame, is_combined=True)


def _default_sos(ids_text_lists: list) -> dict:
    from vdl_tools.shared_tools.all_source_organization_summarization import (
        generate_summary_of_summaries,
    )

    return generate_summary_of_summaries(ids_text_lists)


def summarize_websites(scraped: pd.DataFrame, summarizer=_default_website_summarizer) -> dict:
    """One summary per distinct scraped domain (only usable texts)."""
    texts: dict[str, str] = {}
    for _, row in scraped.iterrows():
        for dom_col, text_col in (("customer_domain", "scraped_text_customer_url"),
                                  ("source_domain", "scraped_text_source_url")):
            domain, text = row.get(dom_col), row.get(text_col)
            if (pd.notna(domain) and domain not in texts
                    and pd.notna(text) and len(str(text).strip()) >= MIN_TEXT_CHARS):
                texts[domain] = str(text)
    if not texts:
        return {}
    frame = pd.DataFrame(
        [{"home_url": f"https://{d}", "combined_text": t} for d, t in sorted(texts.items())]
    )
    logger.info("website summaries: %d distinct domains", len(frame))
    by_url = summarizer(frame) or {}
    return {url.removeprefix("https://"): summary for url, summary in by_url.items()}


def _clean(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if len(text) >= MIN_TEXT_CHARS else None


def build_general_summaries(
    acquired: pd.DataFrame,
    scraped: pd.DataFrame,
    results_dir: str | Path,
    summarizer=_default_website_summarizer,
    sos=_default_sos,
) -> pd.DataFrame:
    """Stages 4+5 in one pass; writes ``org_summaries.parquet``/``.csv``."""
    site_summaries = summarize_websites(scraped, summarizer=summarizer)

    df = acquired.merge(
        scraped[["customer_row_id", "customer_domain", "source_domain",
                 "text_quality"]],
        on="customer_row_id", how="left",
    )

    def _site_summary(domain):
        return site_summaries.get(domain) if pd.notna(domain) else None

    rows, needs_llm = [], []
    for _, row in df.iterrows():
        cust_sum = _site_summary(row.get("customer_domain"))
        src_sum = _site_summary(row.get("source_domain"))
        texts = [t for t in (_clean(row.get(c)) for c in DESCRIPTION_COLUMNS) if t]
        texts += [t for t in (cust_sum, src_sum) if t]
        # Same text from two sources adds nothing.
        texts = list(dict.fromkeys(texts))
        rows.append({
            "customer_row_id": row["customer_row_id"],
            "customer_name": row["customer_name"],
            "website_summary_customer": cust_sum,
            "website_summary_source": src_sum,
            "n_texts": len(texts),
            "_texts": texts,
        })
        if len(texts) >= 2:
            needs_llm.append((row["customer_row_id"], texts))

    logger.info("general summaries: %d rows need synthesis, %d single-text, %d textless",
                len(needs_llm),
                sum(1 for r in rows if r["n_texts"] == 1),
                sum(1 for r in rows if r["n_texts"] == 0))
    synthesized = sos(needs_llm) if needs_llm else {}

    def _summary_text(entry):
        v = synthesized.get(entry["customer_row_id"])
        if isinstance(v, dict):
            v = v.get("response_text")
        if v:
            return v
        return entry["_texts"][0] if entry["_texts"] else None

    out = pd.DataFrame([{
        **{k: r[k] for k in ("customer_row_id", "customer_name",
                             "website_summary_customer", "website_summary_source",
                             "n_texts")},
        "Summary": _summary_text(r),
    } for r in rows])

    def _longer(a, b):
        a, b = (a if pd.notna(a) else ""), (b if pd.notna(b) else "")
        best = a if len(str(a)) >= len(str(b)) else b
        return best or pd.NA

    site_best = out.apply(
        lambda r: _longer(r["website_summary_customer"], r["website_summary_source"]),
        axis=1,
    )
    out["text_for_taxonomy"] = [
        _longer(s, w) for s, w in zip(out["Summary"], site_best)
    ]
    out = out.astype(object).where(pd.notna(out), pd.NA)

    results_dir = Path(results_dir)
    out.to_parquet(results_dir / f"{SUMMARIES_BASENAME}.parquet", index=False)
    out.to_csv(results_dir / f"{SUMMARIES_BASENAME}.csv", index=False)
    n_ready = int(out["text_for_taxonomy"].notna().sum())
    logger.info("summaries: %d of %d rows carry text_for_taxonomy", n_ready, len(out))
    return out
