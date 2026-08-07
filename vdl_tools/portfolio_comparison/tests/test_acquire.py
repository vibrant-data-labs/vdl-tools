"""Stage 1 acquisition: source records land as side-by-side columns."""

from dataclasses import dataclass

import pandas as pd

from vdl_tools.portfolio_comparison.enrichment.acquire import (
    ACQUIRED_BASENAME,
    acquire_records,
)

CB_UUID = "151d5f17-1348-4f5d-91ee-e28da195b1ec"


def base_final():
    return pd.DataFrame([
        {"customer_row_id": "r1", "customer_name": "Aquila",
         "customer_url": "https://aquila.space", "customer_ein": pd.NA,
         "customer_description": pd.NA, "entity_type": "for_profit",
         "cb_id": CB_UUID, "nzi_id": "108278",
         "matched_id": "108278", "matched_source": "nzi"},
        {"customer_row_id": "r2", "customer_name": "Green Fund",
         "customer_url": pd.NA, "customer_ein": "12-3456789",
         "customer_description": pd.NA, "entity_type": "nonprofit",
         "cb_id": pd.NA, "nzi_id": pd.NA,
         "matched_id": "12-3456789", "matched_source": "givingtuesday"},
        {"customer_row_id": "r3", "customer_name": "No Source Co",
         "customer_url": "https://nosource.example", "customer_ein": pd.NA,
         "customer_description": "They make widgets.", "entity_type": "for_profit",
         "cb_id": pd.NA, "nzi_id": pd.NA, "matched_id": pd.NA,
         "matched_source": pd.NA},
    ])


def fake_cb(ids):
    assert ids == [CB_UUID]
    return pd.DataFrame([{
        "uuid": CB_UUID, "name": "Aquila",
        "description": "Long light-based energy network description.",
        "short_description": "Light-based energy network.",
        "website_url": "https://www.aquila.earth",
        "linkedin": {"value": "https://www.linkedin.com/company/aquila-earth"},
        "status": "operating",
        "location_identifiers": [
            {"value": "Boulder", "location_type": "city"},
            {"value": "United States", "location_type": "country"},
        ],
    }])


def fake_nzi(ids):
    assert ids == [108278]
    return pd.DataFrame([{
        "clientID": 108278, "name": "Aquila",
        "description": "NZI long description.", "pitchLine": "Flexible energy.",
        "website": "https://www.aquila.earth",
        "linkedinURL": "https://www.linkedin.com/company/aquila-earth",
        "city": "Boulder", "country": "United States",
    }])


@dataclass
class FakeNonprofit:
    ein: str
    name: str = "Green Fund Inc"
    website: str = "https://greenfund.org"
    unique_text: str = "990 narrative about tree planting."
    city: str = "Oakland"
    state: str = "CA"
    zip: str = "94601"


@dataclass
class FakeGrant:
    grantee_ein: str
    grant_purpose: str
    taxyear: int
    granter_ein: str = "99-9999999"
    granter_name: str = "Big Foundation"


class FakeGT:
    def get_nonprofit(self, ein):
        return FakeNonprofit(ein=ein) if ein == "12-3456789" else None

    def get_grants(self, eins, role="grantee"):
        assert role == "grantee"
        return [
            FakeGrant("12-3456789", "Reforestation program", 2024),
            FakeGrant("12-3456789", "General support", 2023),
            FakeGrant("12-3456789", "Reforestation program", 2022),  # dupe text
        ]


def test_acquisition_attaches_all_sources(tmp_path):
    out = acquire_records(base_final(), tmp_path,
                          cb_fetch=fake_cb, nzi_fetch=fake_nzi, gt_client=FakeGT())
    r1 = out[out["customer_row_id"] == "r1"].iloc[0]
    assert r1["cb_description"].startswith("Long light-based")
    assert r1["cb_linkedin"] == "https://www.linkedin.com/company/aquila-earth"
    assert r1["cb_location"] == "Boulder, United States"
    assert r1["nzi_pitchline"] == "Flexible energy."
    assert r1["nzi_website"] == "https://www.aquila.earth"

    r2 = out[out["customer_row_id"] == "r2"].iloc[0]
    assert r2["gt_unique_text"].startswith("990 narrative")
    assert r2["gt_website"] == "https://greenfund.org"
    # Newest first, deduped.
    assert r2["gt_grant_purposes"] == "Reforestation program | General support"
    assert r2["gt_location"] == "Oakland, CA, 94601"

    r3 = out[out["customer_row_id"] == "r3"].iloc[0]
    assert pd.isna(r3["cb_description"]) and pd.isna(r3["gt_unique_text"])

    assert (tmp_path / f"{ACQUIRED_BASENAME}.parquet").exists()
    assert (tmp_path / f"{ACQUIRED_BASENAME}.csv").exists()


def test_source_failure_never_blocks_others(tmp_path):
    def broken_cb(ids):
        raise RuntimeError("CB API down")

    out = acquire_records(base_final(), tmp_path,
                          cb_fetch=broken_cb, nzi_fetch=fake_nzi, gt_client=FakeGT())
    r1 = out[out["customer_row_id"] == "r1"].iloc[0]
    assert pd.isna(r1["cb_description"])          # CB failed
    assert r1["nzi_pitchline"] == "Flexible energy."   # NZI still landed
    r2 = out[out["customer_row_id"] == "r2"].iloc[0]
    assert pd.notna(r2["gt_unique_text"])         # GT still landed
