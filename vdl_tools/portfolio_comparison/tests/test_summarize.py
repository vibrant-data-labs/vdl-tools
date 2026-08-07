"""Stages 4-5: website summaries + general summary synthesis."""

import pandas as pd

from vdl_tools.portfolio_comparison.enrichment.summarize import (
    build_general_summaries,
    summarize_websites,
)

LONG = "Detailed climate org text. " * 10  # > MIN_TEXT_CHARS


def scraped_frame():
    return pd.DataFrame([
        {"customer_row_id": "r1", "customer_domain": "aquila.space",
         "source_domain": "aquila.earth",
         "scraped_text_customer_url": LONG + "space site",
         "scraped_text_source_url": LONG + "earth site", "text_quality": "ok"},
        {"customer_row_id": "r2", "customer_domain": "solo.com",
         "source_domain": pd.NA,
         "scraped_text_customer_url": pd.NA,
         "scraped_text_source_url": pd.NA, "text_quality": "dead"},
        {"customer_row_id": "r3", "customer_domain": pd.NA, "source_domain": pd.NA,
         "scraped_text_customer_url": pd.NA, "scraped_text_source_url": pd.NA,
         "text_quality": "no_url"},
    ])


def acquired_frame():
    base = {c: pd.NA for c in (
        "customer_description", "nzi_description", "nzi_pitchline",
        "cb_description", "cb_short_description", "gt_unique_text",
        "gt_grant_purposes")}
    return pd.DataFrame([
        {"customer_row_id": "r1", "customer_name": "Aquila", **base,
         "nzi_description": "NZI describes Aquila's light-based energy work in detail here."},
        {"customer_row_id": "r2", "customer_name": "Solo Co", **base,
         "cb_description": "CB-only description of Solo Co, long enough to count as text."},
        {"customer_row_id": "r3", "customer_name": "Nothing Org", **base},
    ])


def fake_summarizer(frame):
    return {url: f"SUMMARY[{url.removeprefix('https://')}]"
            for url in frame["home_url"]}


def fake_sos(ids_text_lists):
    return {rid: f"SYNTH({len(texts)} texts)" for rid, texts in ids_text_lists}


def test_website_summaries_dedupe_domains():
    got = summarize_websites(scraped_frame(), summarizer=fake_summarizer)
    assert set(got) == {"aquila.space", "aquila.earth"}


def test_general_summary_synthesis_and_fallbacks(tmp_path):
    out = build_general_summaries(
        acquired_frame(), scraped_frame(), tmp_path,
        summarizer=fake_summarizer, sos=fake_sos,
    ).set_index("customer_row_id")

    # r1: description + two site summaries -> synthesized.
    assert out.at["r1", "Summary"] == "SYNTH(3 texts)"
    assert out.at["r1", "website_summary_customer"] == "SUMMARY[aquila.space]"
    # text_for_taxonomy = longer of Summary and best site summary.
    assert out.at["r1", "text_for_taxonomy"] == "SUMMARY[aquila.space]"

    # r2: single text -> no LLM call, Summary IS the text.
    assert out.at["r2", "Summary"].startswith("CB-only description")
    assert out.at["r2", "n_texts"] == 1

    # r3: nothing anywhere.
    assert pd.isna(out.at["r3", "Summary"])
    assert pd.isna(out.at["r3", "text_for_taxonomy"])
    assert (tmp_path / "org_summaries.parquet").exists()


def test_sos_only_called_for_multi_text_rows(tmp_path):
    calls = []

    def counting_sos(ids_text_lists):
        calls.extend(rid for rid, _ in ids_text_lists)
        return {}

    build_general_summaries(acquired_frame(), scraped_frame(), tmp_path,
                            summarizer=fake_summarizer, sos=counting_sos)
    assert calls == ["r1"]  # r2 single-text, r3 textless
