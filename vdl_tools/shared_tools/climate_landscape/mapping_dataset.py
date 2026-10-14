"""
The organization dataset that taxonomy mapping runs over.
=========================================================

One place that says where the entities for taxonomy mapping come from and which of them are
kept, shared by every project that maps organizations (oneearth, drawdown, hopper_dean).

Source
------
``cb_cd_li_meta_mapped`` — the enriched Crunchbase + Candid + LinkedIn organization table with
One Earth and Drawdown category columns, written by climate-landscape's
``process_map_taxonomies.py``. Its S3 location is read from the ``[s3_paths]`` section of
``climate-landscape/paths.ini``, so moving to a newer vintage is a one-line change there and
every project follows.

Filters
-------
1. Country is the United States.
2. For-profit companies must also be operating (``operating_status == "active"``) and venture
   backed (``venture_backed`` is True). Non-profits and organizations of unknown type are kept
   on country alone.

The values are the constants below; change them here rather than in a project.

Main functions
--------------
mapping_dataset_uri   resolve the source URI from climate-landscape's paths.ini
filter_entities       apply the filters, logging how many rows each one removes
load_mapping_dataset  read the source (through the local parquet cache) and filter it
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.parquet_cache import read_dataframe
from vdl_tools.shared_tools.project_config import get_s3_paths
from vdl_tools.shared_tools.tools.logger import logger

# vdl_tools/shared_tools/climate_landscape/ -> vdl-tools repo -> the directory holding the repos
CLIMATE_LANDSCAPE_DIR = Path(__file__).resolve().parents[3].parent / "climate-landscape"
SOURCE_KEY = "cb_cd_li_meta_mapped"      # key in climate-landscape paths.ini [s3_paths]

# filter values
COUNTRY = "United States"
FOR_PROFIT = "For Profit"
FOR_PROFIT_OPERATING_STATUS = "active"
FOR_PROFIT_REQUIRES_VENTURE_BACKED = True


def mapping_dataset_uri(climate_landscape_dir: str | Path | None = None) -> str:
    """Return the S3 URI of the source table, as pinned in climate-landscape's paths.ini."""
    root = Path(climate_landscape_dir) if climate_landscape_dir else CLIMATE_LANDSCAPE_DIR
    s3_paths = get_s3_paths(root / "paths.ini")
    if SOURCE_KEY not in s3_paths:
        raise KeyError(f"{SOURCE_KEY!r} missing from [s3_paths] in {root / 'paths.ini'}")
    return s3_paths[SOURCE_KEY]


def filter_entities(df: pd.DataFrame) -> pd.DataFrame:
    """Keep US organizations, and among for-profits only active, venture-backed ones.

    Returns a new frame with a reset index; ``attrs["filter_counts"]`` records the row count
    before and after each filter.
    """
    counts = [("source", len(df))]

    # 1. United States only
    out = df[df["Country"].eq(COUNTRY)]
    counts.append((f"Country == {COUNTRY!r}", len(out)))

    # 2. for-profits must be active and venture backed; everything else passes
    for_profit = out["Org Type"].eq(FOR_PROFIT)
    qualifies = out["operating_status"].eq(FOR_PROFIT_OPERATING_STATUS)
    if FOR_PROFIT_REQUIRES_VENTURE_BACKED:
        qualifies &= out["venture_backed"].eq(True)
    out = out[~for_profit | qualifies]
    counts.append(("for-profits active and venture backed", len(out)))

    for step, n in counts:
        logger.info("mapping dataset: %-40s %7d rows", step, n)
    out = out.reset_index(drop=True)
    out.attrs["filter_counts"] = counts
    return out


def load_mapping_dataset(
    climate_landscape_dir: str | Path | None = None,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Read the source table and apply the filters.

    ``use_cache`` passes through to ``parquet_cache.read_dataframe``, which keeps a local copy
    and re-downloads only when the S3 object has changed. ``attrs["source_uri"]`` records where
    the rows came from.
    """
    uri = mapping_dataset_uri(climate_landscape_dir)
    logger.info("mapping dataset: reading %s", uri)
    df = filter_entities(read_dataframe(uri, use_cache=use_cache))
    df.attrs["source_uri"] = uri
    return df
