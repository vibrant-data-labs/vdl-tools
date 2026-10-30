"""Prepare extracted activities as subjects for JEV taxonomy scoring."""

import pandas as pd

from ...tools.unique_ids import create_deterministic_sha256

STATE_FIELDS = {
    "activity": "Activity",
    "what_happens": "What happens",
    "who_carries_it_out": "Who carries it out",
    "aim": "Aim",
    "sector": "Sector",
    "methods": "Methods",
}


def build_activity_subjects(activities):
    """Build JEV input text and stable mapping-cache identities from activity records.

    Parameters
    ----------
    activities : pandas.DataFrame
        ``activity_id``, organization ``uid``, and the six fields in ``STATE_FIELDS``.
        Evidence, prominence, recurrence and extraction metadata are not scored.

    Returns
    -------
    pandas.DataFrame
        ``subject_id`` (the activity ID), ``state`` (six labelled fields), and
        ``cache_id`` (hash of organization UID and state). Cache identity is stable
        across extraction run labels and preserves previously cached JEV responses.
    """
    if missing := {"activity_id", "uid", *STATE_FIELDS} - set(activities):
        raise ValueError(f"JEV activity subjects require columns: {sorted(missing)}")
    rows = []
    for row in activities.to_dict("records"):
        state = "\n".join(
            f"{label}: {str(row[field]).strip()}" for field, label in STATE_FIELDS.items()
        )
        rows.append({
            "subject_id": row["activity_id"],
            "state": state,
            "cache_id": create_deterministic_sha256([str(row["uid"]), state]),
        })
    return pd.DataFrame(rows, columns=["subject_id", "state", "cache_id"])
