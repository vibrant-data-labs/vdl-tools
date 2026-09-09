"""Stages 6-7: taxonomy mapping wrapper + geocoding wrapper."""

import pandas as pd

from vdl_tools.portfolio_comparison.enrichment.taxonomy_geo import (
    geocode_rows,
    map_taxonomy,
)


def test_map_taxonomy_only_maps_rows_with_text(tmp_path):
    summaries = pd.DataFrame([
        {"customer_row_id": "r1", "customer_name": "A", "text_for_taxonomy": "solar tech"},
        {"customer_row_id": "r2", "customer_name": "B", "text_for_taxonomy": pd.NA},
    ])
    seen = {}

    def fake_mapper(df, taxonomy_path, results_dir, recovery=False, **kw):
        seen["ids"] = list(df["customer_row_id"])
        mapped = df.copy()
        mapped["one_earth_category"] = "Solar Photovoltaic"
        mapped["level0_one_earth_category"] = "Energy Transition"
        return mapped, pd.DataFrame()

    out = map_taxonomy(summaries, tmp_path, "tax.xlsx", mapper=fake_mapper)
    assert seen["ids"] == ["r1"]
    o = out.set_index("customer_row_id")
    assert o.at["r1", "one_earth_category"] == "Solar Photovoltaic"
    assert pd.isna(o.at["r2", "one_earth_category"])
    assert (tmp_path / "taxonomy_mapping.parquet").exists()


def test_geocode_uses_location_precedence(tmp_path):
    acquired = pd.DataFrame([
        {"customer_row_id": "r1", "cb_location": "Boulder, United States",
         "nzi_location": "Boulder, US", "gt_location": pd.NA},
        {"customer_row_id": "r2", "cb_location": pd.NA,
         "nzi_location": pd.NA, "gt_location": "Oakland, CA, 94601"},
        {"customer_row_id": "r3", "cb_location": pd.NA,
         "nzi_location": pd.NA, "gt_location": pd.NA},
    ])

    def fake_geocoder(df):
        out = df.copy()
        out["Latitude"] = 40.0
        out["Longitude"] = -105.0
        return out

    out = geocode_rows(acquired, tmp_path, geocoder=fake_geocoder).set_index("customer_row_id")
    assert out.at["r1", "Location"] == "Boulder, United States"  # cb first
    assert out.at["r2", "Location"] == "Oakland, CA, 94601"
    assert pd.isna(out.at["r3", "Location"])
    assert out.at["r1", "Latitude"] == 40.0
    assert pd.isna(out.at["r3", "Latitude"])
