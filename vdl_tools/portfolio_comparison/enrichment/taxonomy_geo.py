"""Phase 2, stages 6-7 — OE hierarchical taxonomy mapping and geocoding.

Taxonomy: the hierarchical LLM walk over ``text_for_taxonomy``, pinned to
the engagement's taxonomy vintage (``enrichment.taxonomy_path`` in
engagement.yaml — same file the baseline run used, so portfolio and
ecosystem taxonomies stay comparable). Geocoding: ``geocode_v2`` over the
best source-provided location string.
"""

from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger

TAXONOMY_BASENAME = "taxonomy_mapping"
GEOCODED_BASENAME = "geocoded"
LOCATION_PRECEDENCE = ["cb_location", "nzi_location", "gt_location"]


def _default_taxonomy_mapper(df, taxonomy_path, results_dir, recovery=False):
    from vdl_tools.shared_tools.taxonomy_mapping.oe_hierarchical_taxonomy_mapping import (
        add_one_earth_hierarchical_taxonomy,
    )

    return add_one_earth_hierarchical_taxonomy(
        df,
        id_col="customer_row_id",
        text_col="text_for_taxonomy",
        name_col="customer_name",
        taxonomy_path=Path(taxonomy_path),
        results_path=Path(results_dir) / f"{TAXONOMY_BASENAME}_results.json",
        distributed_funding_results_path=(
            Path(results_dir) / f"{TAXONOMY_BASENAME}_distributed_funding.json"
        ),
        # enrichment.recovery in engagement.yaml: a second-chance scope pass
        # for walk-refused orgs; in-scope ones re-walk and land at least at
        # pillar depth (OSP: recovered 40 of 91).
        recover_unmatched=recovery,
        walk_recovered=recovery,
    )


def map_taxonomy(
    summaries: pd.DataFrame,
    results_dir: str | Path,
    taxonomy_path: str | Path,
    mapper=_default_taxonomy_mapper,
    recovery: bool = False,
) -> pd.DataFrame:
    """Map every row with taxonomy text; write ``taxonomy_mapping.parquet``."""
    ready = summaries[summaries["text_for_taxonomy"].notna()].copy()
    logger.info("taxonomy: mapping %d of %d rows (recovery=%s)",
                len(ready), len(summaries), recovery)
    mapped, _distributed = mapper(ready, taxonomy_path, results_dir,
                                  recovery=recovery)

    tax_cols = [c for c in mapped.columns if c not in summaries.columns]
    out = summaries[["customer_row_id"]].merge(
        mapped[["customer_row_id"] + tax_cols], on="customer_row_id", how="left",
    )
    out = out.astype(object).where(pd.notna(out), pd.NA)
    results_dir = Path(results_dir)
    out.to_parquet(results_dir / f"{TAXONOMY_BASENAME}.parquet", index=False)
    out.to_csv(results_dir / f"{TAXONOMY_BASENAME}.csv", index=False)

    matched = out["one_earth_category"].notna() & (out["one_earth_category"] != "NoMatch")
    logger.info("taxonomy: %d of %d mapped rows matched", int(matched.sum()), len(ready))
    return out


def _default_geocoder(df):
    from vdl_tools.scrape_enrich import geocode
    from vdl_tools.scrape_enrich.geocode_v2 import add_geo_lat_long

    geo = add_geo_lat_long(df, idCol="customer_row_id", address="Location")
    return geocode.clean_geo(geo, summarize_new_geo=False)


def geocode_rows(
    acquired: pd.DataFrame,
    results_dir: str | Path,
    geocoder=_default_geocoder,
) -> pd.DataFrame:
    """Geocode the best source-provided location per row."""
    df = acquired[["customer_row_id"]].copy()
    location = pd.Series(pd.NA, index=acquired.index, dtype=object)
    for col in LOCATION_PRECEDENCE:
        if col in acquired.columns:
            location = location.where(location.notna(), acquired[col])
    df["Location"] = location.values

    with_loc = df[df["Location"].notna()].copy()
    logger.info("geocode: %d of %d rows carry a location string",
                len(with_loc), len(df))
    geo = geocoder(with_loc) if len(with_loc) else with_loc

    out = df.merge(
        geo.drop(columns=["Location"], errors="ignore"),
        on="customer_row_id", how="left",
    )
    out = out.astype(object).where(pd.notna(out), pd.NA)
    results_dir = Path(results_dir)
    out.to_parquet(results_dir / f"{GEOCODED_BASENAME}.parquet", index=False)
    out.to_csv(results_dir / f"{GEOCODED_BASENAME}.csv", index=False)
    if "Latitude" in out.columns:
        logger.info("geocode: %d rows resolved to coordinates",
                    int(out["Latitude"].notna().sum()))
    return out
