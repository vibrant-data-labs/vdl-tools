"""
Apply per-org attribute overrides from a checked-in patch file.

The problem this solves: an upstream fix (a corrected org type, a missing
flag) is known TODAY, but the pipeline stage that computes it won't re-run
for weeks. Instead of hand-editing artifacts or burying one-off fixes in
code, each repo keeps a small patch JSON under version control and applies
it wherever the artifact is (re)built. A patch entry is data: reviewable in
a diff, easy to add, easy to delete once the upstream fix lands.

Patch file format - a JSON list, one override per entry. JSON (not CSV) so
values keep their real types: lists for list-valued columns like
'Funding Types', booleans, numbers, null.

    [
      {"uid": "00ed2089-...",
       "column": "For-Profit vs Non-Profit",
       "value": "For Profit",
       "reason": "seed-funded company; CFT predates the venture-round fix"}
    ]

The id key name is configurable (`id_col`).
"""

import json
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger


def apply_org_patches(df: pd.DataFrame, patch_file, id_col: str = "uid") -> pd.DataFrame:
    """Apply the overrides in ``patch_file`` to ``df`` and return a copy.

    Every patch entry sets ``df.loc[df[id_col] == uid, column] = value``.
    A missing or empty patch file is fine (returns ``df`` unchanged, with a
    log line) so callers can wire the hook up before any patch exists.
    Entries whose id or column is not in ``df`` are reported and skipped --
    a patch that no longer applies usually means the upstream fix landed
    and the entry should be deleted from the file.

    Parameters
    ----------
    df : pandas.DataFrame
        Frame to patch; may hold one row per org or many (all rows of a
        matching id are patched).
    patch_file : str or pathlib.Path
        JSON list of {<id_col>, column, value, reason} objects. Values are
        used as-is, so lists / booleans / numbers / null survive intact.
    id_col : str
        Name of the id key, in both ``df`` and the patch entries.

    Returns
    -------
    pandas.DataFrame
        A patched copy of ``df`` (the input is not modified).
    """
    patch_file = Path(patch_file)
    if not patch_file.exists():
        logger.info("No org patch file at %s - nothing to apply", patch_file)
        return df

    with open(patch_file) as f:
        patches = json.load(f)
    if not patches:
        logger.info("Org patch file %s has no entries - nothing to apply", patch_file)
        return df

    needed = {id_col, "column", "value", "reason"}
    for p in patches:
        missing = needed - set(p)
        if missing:
            raise ValueError(f"Patch entry {p} is missing keys {sorted(missing)}")

    out = df.copy()
    ids = set(out[id_col])
    for p in patches:
        if p[id_col] not in ids:
            logger.warning(
                "Org patch skipped (id not in frame - upstream fix landed?): "
                "%s | %s | %s", p[id_col], p["column"], p["reason"],
            )
            continue
        if p["column"] not in out.columns:
            logger.warning(
                "Org patch skipped (no column %r in frame): %s | %s",
                p["column"], p[id_col], p["reason"],
            )
            continue
        idx = out.index[out[id_col] == p[id_col]]
        if isinstance(p["value"], list):
            # a list value must be placed cell by cell -- a plain .loc
            # assignment would try to broadcast its elements across rows
            for i in idx:
                out.at[i, p["column"]] = list(p["value"])
        else:
            out.loc[idx, p["column"]] = p["value"]
        logger.info(
            "Org patch applied: %s | %s = %r (%d rows) | %s",
            p[id_col], p["column"], p["value"], len(idx), p["reason"],
        )
    return out
