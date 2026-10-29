"""Activity evidence compatibility; optional for generic subject mapping."""

import pandas as pd
from .engine import drop_implied
from .taxonomies import Taxonomy


def rollup_paths(activity_paths, activities, taxonomy: Taxonomy):
    """All activities contribute equally to membership; importance is metadata only.

    Keep the prototype's max probability, best tier, and activity support count,
    adding actual activity IDs and verbatim evidence for serving explanations.
    No confidence score or activity count affects the funding split.
    """
    columns = [
        "uid",
        *taxonomy.columns,
        "prob",
        "tier",
        "n_activities",
        "supporting_activity_ids",
        "activity_evidence",
        "evidence",
        "reason",
    ]
    if activity_paths.empty:
        return pd.DataFrame(columns=columns)
    joined = activity_paths.merge(activities, on="activity_id", validate="many_to_one")
    if joined.uid.isna().any():
        raise ValueError("A matched activity has no organization")
    for c in taxonomy.columns:
        joined[c] = joined[c].fillna("")
    rows = []
    for key, group in joined.groupby(["uid", *taxonomy.columns], sort=True):
        evidence = [
            {
                "activity_id": r["activity_id"],
                "activity": r["activity"],
                "evidence": r["evidence"],
                "source_sha256": r["source_sha256"],
                "extraction_run_id": r["extraction_run_id"],
                "prominence": r.get("prominence"),
                "recurrence": r.get("recurrence"),
                "prob": float(r["prob"]),
                "match_origin": r["match_origin"],
                "ancestor_matches": r["ancestor_matches"],
            }
            for r in group.drop_duplicates("activity_id").to_dict("records")
        ]
        rows.append(
            {
                "uid": key[0],
                **{c: v or None for c, v in zip(taxonomy.columns, key[1:], strict=True)},
                "prob": float(group.prob.max()),
                "tier": int(group.tier.min()),
                "n_activities": group.activity_id.nunique(),
                "supporting_activity_ids": sorted(group.activity_id.unique()),
                "activity_evidence": evidence,
                "evidence": "\n".join(
                    dict.fromkeys(e["evidence"] for e in evidence if e["evidence"])
                ),
                "reason": "Union of supporting activities selected by the recorded Jev match policy.",
            }
        )
    return drop_implied(
        pd.DataFrame(rows, columns=columns), taxonomy.columns, id_col="uid"
    ).reset_index(drop=True)
