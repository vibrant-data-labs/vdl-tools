"""Preserve saved mapping explanations on serving assignments (no I/O or inference).

The output is JSON text so existing pandas/SQL writers can store it without
changing their table-writing code. The dashboard accepts JSON text or JSONB.
An explanation belongs to the original full path, even when serving paths
collapse deeper taxonomy levels. Funding fractions are never recomputed.
"""
import json
import re
from collections import defaultdict
from collections.abc import Sequence

import pandas as pd


def _text(value):
    if isinstance(value, str):
        return value.strip() or None
    return None


def merge_explanations(values) -> str:
    """Union explanation payloads when funding paths collapse, without fan-out."""
    entries = {}
    for value in values:
        if not isinstance(value, str):
            continue
        for entry in json.loads(value):
            entries[json.dumps(entry, sort_keys=True, ensure_ascii=False)] = entry
    return json.dumps(list(entries.values()), ensure_ascii=False)


def _path(values):
    result = []
    for value in values:
        value = _text(value)
        if value is None or value == "No Match" or re.match(r"^No_Level_\d", value):
            break
        result.append(value)
    return result


def assignment_explanations(
    assignments: pd.DataFrame,
    mappings: pd.DataFrame,
    *,
    id_column: str = "id",
    level_columns: Sequence[str] = ("level0", "level1", "level2"),
    source: str,
    serving_depth: int = 3,
) -> pd.Series:
    """Return aligned JSON strings, joining on org + the complete serving path.

    Call separately per taxonomy dimension. Callers apply the SAME category
    normalization to mapping and serving paths, and exclude superseded model
    rows for manually overridden orgs. Missing evidence yields ``[]``. Multiple
    original paths collapsed to one serving path keep distinct explanations.
    A shallow model match never explains a deeper assignment (or vice versa).
    """
    by_path = defaultdict(dict)
    if not mappings.empty:
        required = {id_column, *level_columns}
        missing = required - set(mappings.columns)
        if missing:
            raise ValueError(f"Mapping evidence missing columns: {sorted(missing)}")
        for row in mappings.to_dict("records"):
            path = _path(row[column] for column in level_columns)
            evidence, reason = _text(row.get("evidence")), _text(row.get("reason"))
            if not path or not (evidence or reason):
                continue
            entry = {"path": path, "evidence": evidence, "reason": reason, "source": source}
            encoded = json.dumps(entry, ensure_ascii=False, sort_keys=True)
            key = (str(row[id_column]), tuple(path[:serving_depth]))
            by_path[key][encoded] = entry

    result = []
    for row in assignments.to_dict("records"):
        path = _path(row.get(f"level{i}") for i in range(serving_depth))
        entries = by_path.get((str(row["org_uid"]), tuple(path)), {}) if path else {}
        result.append(json.dumps(list(entries.values()), ensure_ascii=False))
    return pd.Series(result, index=assignments.index, dtype=object)
