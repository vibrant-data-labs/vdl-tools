"""Union activity-level matches to their organization, keeping per-activity evidence.

This is the activity-specific counterpart of ``pipeline.group_matches``: it knows the
columns ``extract_frame`` puts on an activity and emits the evidence records the
legacy exporters and serving explanations consume.
"""

import pandas as pd

from .engine import drop_implied
from .taxonomies import Taxonomy


def rollup_paths(activity_paths, activities, taxonomy: Taxonomy):
    """Union every matched activity path to its organization.

    All activities count equally toward membership; ``prominence`` (core / secondary /
    minor) is carried as metadata only. No probability or activity count influences
    the funding split performed later by the exporter.

    Parameters
    ----------
    activity_paths : pandas.DataFrame
        ``engine.apply_policy`` output (or ``select_matches`` output with
        ``subject_id`` renamed to ``activity_id``): one row per (activity, matched
        node) with ``activity_id``, ``node_id``, the taxonomy path columns (for
        example ``Pillar``, ``Sub-Pillar``, ``Solution``), ``depth``, ``prob``,
        ``tier``, ``match_origin`` and ``ancestor_matches``.
    activities : pandas.DataFrame
        ``ExtractionResult.activities`` from ``extract_frame``: one row per extracted
        activity with ``activity_id``, ``uid`` (organization), ``activity``,
        ``evidence``, ``prominence``, ``recurrence``, ``state``, ``cache_id``,
        ``source_sha256`` and ``extraction_run_id``.
    taxonomy : Taxonomy
        Supplies the path columns to group on.

    Returns
    -------
    pandas.DataFrame
        One row per (``uid``, path): ``uid``, the taxonomy path columns, ``prob``
        (max over supporting activities), ``tier`` (best, i.e. lowest),
        ``n_activities``, ``supporting_activity_ids`` (sorted), ``activity_evidence``
        (one dict per supporting activity: ``activity_id``, ``activity``,
        ``evidence``, ``source_sha256``, ``extraction_run_id``, ``prominence``,
        ``recurrence``, ``prob``, ``match_origin``, ``ancestor_matches``),
        ``evidence`` (the distinct evidence quotes joined by newlines) and
        ``reason``. A path implied by a deeper selected path of the same
        organization is dropped.

    Raises
    ------
    ValueError
        A matched activity has no organization in ``activities``.
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
