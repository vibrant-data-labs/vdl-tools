"""Tracker-shaped export of activity matches: organization rollup, legacy columns, statuses, funding.

Generic inference never imports this module. The caller names the tracker's mapping column
and may reshape the result rows; four level columns and level-2 funding are the conventions
every VDL tracker loader reads.
"""

from dataclasses import dataclass

import pandas as pd

from ..hierarchical_taxonomy_mapping import attach_mapping_columns, distribute_funding_from_matches
from .engine import drop_implied

ID_COL = "id"
NAME_COL = "Organization"
PAD_LEVELS = 4  # legacy consumers expect level0..level3 columns even for three-level taxonomies
FUNDING_LEVEL = 2  # the existing allocation grain (Q4)


@dataclass
class MappingResult:
    """Consumer-facing output of ``export_extraction`` for one taxonomy.

    Attributes
    ----------
    organizations : pandas.DataFrame
        The caller's frame (IDs as strings) plus the legacy mapping columns
        (``level{i}_<name>``, ``all_level{i}_<name>``, ``<name>``, ``cat_level_<name>``)
        and ``<taxonomy>_mapping_status``.
    paths : pandas.DataFrame
        One row per (organization, path) from ``rollup_paths`` plus one no-match row
        per unmatched organization, with ``id``, ``Organization``, ``deepest_match``,
        ``taxonomy_sha256``, ``policy_sha256`` and ``mapping_status`` (``matched``,
        ``unmatched``, ``partial_failure``, ``scoring_failed``, or the extraction
        status when extraction did not succeed).
    activity_paths : pandas.DataFrame
        The per-activity matches (``select_matches`` output keyed by ``activity_id``).
    scores, usage : pandas.DataFrame
        Raw probabilities and request statuses keyed by ``activity_id``.
    funding : pandas.DataFrame
        ``distribute_funding_from_matches`` output.
    lineage : dict
        Mapping lineage plus ``export_columns``, ``max_funding_level`` and ``status_counts``.
    """

    organizations: pd.DataFrame
    paths: pd.DataFrame
    activity_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    funding: pd.DataFrame
    lineage: dict


def rollup_paths(activity_paths, activities, taxonomy):
    """Union every matched activity path to its organization, keeping per-activity evidence.

    All activities count equally toward membership; ``prominence`` is carried as metadata
    only. No probability or activity count influences the funding split performed later.

    Parameters
    ----------
    activity_paths : pandas.DataFrame
        ``select_matches`` output with ``subject_id`` renamed to ``activity_id``: one row
        per (activity, matched node) with ``activity_id``, ``node_id``, the taxonomy
        path columns, ``depth``, ``prob``, ``tier``, ``match_origin``, ``ancestor_matches``.
    activities : pandas.DataFrame
        ``ExtractionResult.activities``: one row per extracted activity with
        ``activity_id``, ``uid``, ``activity``, ``evidence``, ``prominence``,
        ``recurrence``, ``source_sha256``, ``extraction_run_id`` among others.
    taxonomy : Taxonomy
        Supplies the path columns to group on.

    Returns
    -------
    pandas.DataFrame
        One row per (``uid``, path): ``uid``, the path columns, ``prob`` (max over
        supporting activities), ``tier`` (best, i.e. lowest), ``n_activities``,
        ``supporting_activity_ids`` (sorted), ``activity_evidence`` (one dict per
        supporting activity: ``activity_id``, ``activity``, ``evidence``,
        ``source_sha256``, ``extraction_run_id``, ``prominence``, ``recurrence``,
        ``prob``, ``match_origin``, ``ancestor_matches``), ``evidence`` (the distinct
        quotes joined by newlines) and ``reason``. A path implied by a deeper selected
        path of the same organization is dropped.
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


def export_extraction(organizations, extraction, taxonomy, mapped, *, mapping_name, format_paths=None):
    """Roll activity matches up to organizations and attach tracker-shaped columns.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization with ``id`` and ``Organization``; ``id`` unique and
        non-null. Returned with IDs as strings and columns added, never rows removed.
    extraction : ExtractionResult
        The extraction whose ``activities`` were mapped. Its organization cohort must
        equal ``organizations.id``; its ``extraction_status`` feeds the mapping status
        of organizations without matches.
    taxonomy : Taxonomy
        The taxonomy the activities were scored against.
    mapped : SubjectMappingResult
        ``map_subjects`` output for ``extraction.activities`` with ``activity_id``
        renamed to ``subject_id``.
    mapping_name : str
        Suffix of the legacy mapping columns, e.g. ``one_earth_category``.
    format_paths : callable, optional
        ``format_paths(paths) -> paths`` applied to the result rows before statuses are
        attached, for a tracker's display conventions.

    Returns
    -------
    MappingResult
    """
    ids = organizations[ID_COL].astype(str)
    if organizations[ID_COL].isna().any() or ids.duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    if set(ids) != set(extraction.organizations.uid):
        raise ValueError("Extraction and mapping organization cohorts differ")
    activity_paths = mapped.subject_paths.rename(columns={"subject_id": "activity_id"})
    union = rollup_paths(activity_paths, extraction.activities, taxonomy)
    paths = union.rename(columns={"uid": ID_COL})
    names = extraction.organizations.set_index("uid")[NAME_COL]
    paths[NAME_COL] = paths[ID_COL].map(names)
    # One no-match row per org is required by the legacy attach helpers.
    matched = set(union.uid)
    missing_rows = [
        {
            ID_COL: uid,
            NAME_COL: names[uid],
            **dict.fromkeys(taxonomy.columns),
            "prob": None,
            "tier": None,
            "n_activities": 0,
            "supporting_activity_ids": [],
            "activity_evidence": [],
            "evidence": None,
            "reason": "No taxonomy match; see mapping_status and extraction_status.",
        }
        for uid in extraction.organizations.uid
        if uid not in matched
    ]
    paths = pd.DataFrame(paths.to_dict("records") + missing_rows, columns=[*paths.columns])
    if format_paths:
        paths = format_paths(paths)
    paths["deepest_match"] = [
        next((c for c in reversed(taxonomy.columns) if pd.notna(row[c])), "NoMatch")
        for row in paths.to_dict("records")
    ]
    paths["taxonomy_sha256"] = taxonomy.sha256
    paths["policy_sha256"] = mapped.lineage["policy_sha256"]
    failure_ids = set(mapped.usage.loc[mapped.usage.error.notna(), "subject_id"])
    affected_orgs = set(
        extraction.activities.loc[extraction.activities.activity_id.isin(failure_ids), "uid"]
    )
    statuses = {}
    for org in extraction.organizations.to_dict("records"):
        uid = org["uid"]
        if org["extraction_status"] != "success":
            statuses[uid] = org["extraction_status"]
        elif uid in affected_orgs:
            statuses[uid] = "partial_failure" if uid in matched else "scoring_failed"
        else:
            statuses[uid] = "matched" if uid in matched else "unmatched"
    paths["mapping_status"] = paths[ID_COL].map(statuses)

    out = organizations.assign(**{ID_COL: ids})
    out = attach_mapping_columns(
        out, paths, taxonomy.levels, ID_COL, mapping_name=mapping_name, pad_to_n_levels=PAD_LEVELS
    )
    out[f"{taxonomy.name}_mapping_status"] = out[ID_COL].map(statuses)
    funding = distribute_funding_from_matches(
        paths, taxonomy.levels, ID_COL, NAME_COL, max_level=FUNDING_LEVEL
    )
    export_columns = [
        *[
            f"{prefix}level{i}_{mapping_name}"
            for i in range(PAD_LEVELS)
            for prefix in ("", "all_")
        ],
        mapping_name,
        f"cat_level_{mapping_name}",
        f"{taxonomy.name}_mapping_status",
    ]
    lineage = {
        **mapped.lineage,
        "export_columns": export_columns,
        "max_funding_level": FUNDING_LEVEL,
        "status_counts": pd.Series(statuses).value_counts().to_dict(),
    }
    return MappingResult(
        out,
        paths,
        activity_paths,
        mapped.scores.rename(columns={"subject_id": "activity_id"}),
        mapped.usage.rename(columns={"subject_id": "activity_id"}),
        funding,
        lineage,
    )
