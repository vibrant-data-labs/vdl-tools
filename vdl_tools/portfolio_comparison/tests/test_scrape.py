"""Stages 2-3: domain-deduped scraping, text columns, junk gate."""

import pandas as pd

from vdl_tools.portfolio_comparison.enrichment.scrape import (
    classify_text_quality,
    scrape_texts,
    select_scrape_targets,
)

LONG = "Real org text about climate work. " * 20


def acquired_frame():
    return pd.DataFrame([
        # Customer + NZI site on different domains: both scraped.
        {"customer_row_id": "r1", "customer_url": "https://aquila.space",
         "nzi_website": "https://www.aquila.earth", "cb_website": pd.NA,
         "gt_website": pd.NA},
        # Source domain same as customer's: one scrape only.
        {"customer_row_id": "r2", "customer_url": "https://same.com",
         "nzi_website": "https://same.com/about", "cb_website": pd.NA,
         "gt_website": pd.NA},
        # LinkedIn as customer URL: platform domain, never a target.
        {"customer_row_id": "r3",
         "customer_url": "https://linkedin.com/company/replant",
         "nzi_website": pd.NA, "cb_website": "https://replant.example",
         "gt_website": pd.NA},
        # No URLs anywhere.
        {"customer_row_id": "r4", "customer_url": pd.NA, "nzi_website": pd.NA,
         "cb_website": pd.NA, "gt_website": pd.NA},
    ])


def test_target_selection_dedupes_and_skips_platforms():
    t = select_scrape_targets(acquired_frame()).set_index("customer_row_id")
    assert t.at["r1", "customer_domain"] == "aquila.space"
    assert t.at["r1", "source_domain"] == "aquila.earth"
    assert t.at["r2", "source_domain"] is None          # same registrable domain
    assert t.at["r3", "customer_domain"] is None        # platform never a target
    assert t.at["r3", "source_domain"] == "replant.example"
    assert t.at["r4", "customer_domain"] is None


def test_scrape_texts_fans_out_and_gates(tmp_path):
    def fake_scraper(urls):
        from vdl_tools.scrape_enrich.scraper.scrape_websites import (
            extract_website_name,
        )

        texts = {
            "aquila.space": LONG,
            "aquila.earth": LONG + " earth variant",
            "same.com": "Buy this domain today!",     # parked
            "replant.example": "short",               # thin
        }
        rows = []
        for url in urls:
            key = extract_website_name(url)
            match = next((t for d, t in texts.items() if d in url), None)
            rows.append({"cleaned_home_key": key, "home_url": url,
                         "combined_text": match, "num_errors": 0})
        return pd.DataFrame(rows)

    out = scrape_texts(acquired_frame(), tmp_path, scraper=fake_scraper)
    o = out.set_index("customer_row_id")
    assert o.at["r1", "text_quality"] == "ok"
    assert o.at["r1", "scraped_text_customer_url"].startswith("Real org")
    assert o.at["r1", "scraped_text_source_url"].endswith("earth variant")
    assert o.at["r2", "customer_url_quality"] == "parked"
    assert o.at["r2", "text_quality"] == "parked"       # no fallback source
    assert o.at["r3", "source_url_quality"] == "thin"
    assert o.at["r4", "text_quality"] == "no_url"
    assert (tmp_path / "scraped_texts.parquet").exists()


def test_quality_classifier():
    assert classify_text_quality(LONG) == "ok"
    assert classify_text_quality("tiny") == "thin"
    assert classify_text_quality("THIS DOMAIN IS FOR SALE — act now " * 20) == "parked"
    assert classify_text_quality(None, num_errors=3) == "dead"
    assert classify_text_quality("", num_errors=0) == "empty"
