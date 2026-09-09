"""Reviewed taxonomy overrides — corrections are data, not chat.

``data/results/taxonomy_overrides.json`` holds human/second-reader-verified
taxonomy placements for rows the walk got wrong. Applied as the last step of
the taxonomy stage, so they survive every rerun; each entry records who
decided and why. NB: overrides update the per-org level columns in the
enriched deliverable; the walk's long-format artifacts (distributed funding,
results JSON) keep the machine's original output — overridden rows carry
their correction only in the collapsed columns.
"""

import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger

OVERRIDES_FILENAME = "taxonomy_overrides.json"
LEVEL_COLS = [f"level{i}_one_earth_category" for i in range(4)]


def apply_taxonomy_overrides(out: pd.DataFrame, results_dir: str | Path) -> pd.DataFrame:
    path = Path(results_dir) / OVERRIDES_FILENAME
    if not path.exists():
        return out
    overrides = json.loads(path.read_text())
    n = 0
    for entry in overrides:
        mask = out["customer_row_id"] == entry["customer_row_id"]
        if not mask.any():
            continue
        idxs = out.index[mask]
        deepest = None
        for i, col in enumerate(LEVEL_COLS):
            value = entry.get(f"level{i}")
            out.loc[idxs, col] = value if value else pd.NA
            all_col = col.replace("level", "all_level")
            if all_col in out.columns:
                # List-valued cells need per-cell .at — .loc with a mask
                # reads a nested list as a 2D array and length-checks it.
                for ix in idxs:
                    out.at[ix, all_col] = [value] if value else []
            if value:
                deepest = value
        out.loc[idxs, "one_earth_category"] = deepest
        if "cat_level_one_earth_category" in out.columns:
            out.loc[idxs, "cat_level_one_earth_category"] = entry.get("cat_level")
        n += 1
    logger.info("taxonomy overrides: applied %d of %d entries (%s)",
                n, len(overrides), path.name)
    return out
