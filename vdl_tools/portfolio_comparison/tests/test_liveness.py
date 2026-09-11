"""Website liveness gate: a URL nobody ever fetched is not a text source."""

import pandas as pd

from vdl_tools.portfolio_comparison.review_apps.customer_export import _ask
from vdl_tools.portfolio_comparison.run import verify_website_readiness
from vdl_tools.portfolio_comparison.schema import ID_MAPPING_COLUMNS


def make_mapping(rows):
    df = pd.DataFrame(rows)
    for col in ID_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[ID_MAPPING_COLUMNS]


def base_row(rid, url, **kw):
    return {
        "customer_row_id": rid, "customer_name": rid, "customer_url": url,
        "entity_type": "for_profit", "text_sources": "website",
        "enrichment_ready": True, **kw,
    }


def test_dead_url_demotes_readiness(tmp_path):
    m = make_mapping([
        base_row("dead-co", "https://gone.example"),
        base_row("live-co", "https://alive.example"),
    ])
    alive = {"gone.example": False, "alive.example": True}
    m = verify_website_readiness(
        m, tmp_path / "cache.json",
        checker=lambda url: alive[url.split("//")[1].rstrip("/")],
    )
    dead, live = m.iloc[0], m.iloc[1]
    assert dead["enrichment_ready"] == False  # noqa: E712
    assert dead["text_sources"] == "website_dead"
    assert live["enrichment_ready"] == True  # noqa: E712
    assert live["text_sources"] == "website"


def test_rows_with_other_sources_never_checked(tmp_path):
    # A matched row or a customer-described row is ready regardless of its
    # URL — no network call spent on it.
    calls = []

    def checker(url):
        calls.append(url)
        return False

    m = make_mapping([
        base_row("matched-co", "https://gone.example", matched_id="abc",
                 text_sources="website,source_record"),
        base_row("described-co", "https://gone.example",
                 customer_description="They make widgets."),
    ])
    m = verify_website_readiness(m, tmp_path / "cache.json", checker=checker)
    assert calls == []
    assert m["enrichment_ready"].all()


def test_liveness_cache_prevents_rechecks(tmp_path):
    calls = []

    def checker(url):
        calls.append(url)
        return False

    rows = [base_row("co-a", "https://gone.example"),
            base_row("co-b", "https://gone.example/other-page")]
    m = make_mapping(rows)
    m = verify_website_readiness(m, tmp_path / "cache.json", checker=checker)
    assert len(calls) == 1  # same domain, one check
    m2 = make_mapping(rows)
    m2 = verify_website_readiness(m2, tmp_path / "cache.json", checker=checker)
    assert len(calls) == 1  # second run: cache hit, no network


def test_export_ask_mentions_dead_website():
    row = pd.Series({
        "status": pd.NA, "matched_name": pd.NA, "entity_type": "for_profit",
        "customer_url": "https://gone.example",
        "text_sources": "website_dead",
    })
    ask = _ask(row, objective="text")
    assert "doesn't respond" in ask
    assert "gone.example" in ask
