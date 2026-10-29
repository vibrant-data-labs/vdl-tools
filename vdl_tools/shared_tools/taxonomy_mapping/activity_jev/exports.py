"""Tracker-shaped export of activity matches: legacy mapping columns, statuses, funding.

Generic inference never imports this module. The per-taxonomy packages call
``export_extraction`` with their own column names, path formatting and funding depth.
"""

from dataclasses import dataclass

import pandas as pd

from .activity_rollup import rollup_paths


@dataclass
class MappingResult:
    """Consumer-facing output of ``export_extraction`` for one taxonomy.

    Attributes
    ----------
    organizations : pandas.DataFrame
        The caller's frame plus the legacy mapping columns (``level{i}_<name>``,
        ``all_level{i}_<name>``, ``<name>``, ``cat_level_<name>``) and
        ``<taxonomy>_mapping_status``.
    paths : pandas.DataFrame
        One row per (organization, path) from ``rollup_paths`` plus one no-match row
        per unmatched organization, with the caller's ID and name columns,
        ``deepest_match``, ``taxonomy_sha256``, ``policy_sha256`` and
        ``mapping_status`` (``matched``, ``unmatched``, ``partial_failure``,
        ``scoring_failed``, or the extraction status when extraction did not succeed).
    activity_paths : pandas.DataFrame
        The per-activity matches (``select_matches`` output with ``activity_id``).
    scores, usage : pandas.DataFrame
        Raw probabilities and request statuses with ``activity_id``.
    funding : pandas.DataFrame
        ``distribute_funding_from_matches`` output, or empty when no funding depth
        was requested.
    nodes : pandas.DataFrame
        Copy of the taxonomy nodes.
    lineage : dict
        Mapping lineage plus ``export_columns``, ``max_funding_level``,
        ``n_activities``, ``max_mapping_level``, ``rollup`` and ``status_counts``.
    """

    organizations: pd.DataFrame
    paths: pd.DataFrame
    activity_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    funding: pd.DataFrame
    nodes: pd.DataFrame
    lineage: dict


def export_extraction(
    organizations,
    extraction,
    taxonomy,
    mapped,
    *,
    id_col="id",
    name_col="Organization",
    mapping_name,
    path_formatter=None,
    pad_to_n_levels=None,
    max_funding_level=None,
):
    """Roll activity matches up to organizations and attach tracker-shaped columns.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization; ``id_col`` unique and non-null. Returned with
        columns added, never with rows removed.
    extraction : ExtractionResult
        The extraction whose ``activities`` were mapped. ``extraction.organizations``
        must cover exactly ``organizations[id_col]``; its ``extraction_status`` feeds
        the mapping status of organizations without matches.
    taxonomy : Taxonomy
        The taxonomy the activities were scored against.
    mapped : SubjectMappingResult
        ``map_subjects`` output for ``extraction.activities`` (``activity_id`` renamed
        to ``subject_id``).
    id_col, name_col : str
        Organization ID and display-name columns in ``organizations``.
    mapping_name : str
        Suffix of the legacy mapping columns, e.g. ``one_earth_category``.
    path_formatter : callable, optional
        ``path_formatter(paths) -> paths`` applied before statuses are attached, for
        taxonomy-specific display conventions.
    pad_to_n_levels : int, optional
        Emit mapping columns for this many levels even if the taxonomy is shallower
        (legacy consumers expect four).
    max_funding_level : int, optional
        Deepest level funding is allocated to; None skips allocation.

    Returns
    -------
    MappingResult
    """
    if organizations[id_col].isna().any() or organizations[id_col].astype(str).duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    if set(organizations[id_col].astype(str)) != set(extraction.organizations.uid):
        raise ValueError("Extraction and mapping organization cohorts differ")
    activity_paths = mapped.subject_paths.rename(columns={"subject_id": "activity_id"})
    union = rollup_paths(activity_paths, extraction.activities, taxonomy)
    # Use only normalized internal IDs; translate back to the caller's exact dtype
    # at the export boundary so numeric org IDs cannot silently lose their mappings.
    id_values = dict(zip(organizations[id_col].astype(str), organizations[id_col], strict=True))
    names = dict(zip(organizations[id_col].astype(str), organizations[name_col], strict=True))
    paths = union.copy()
    if id_col != "uid":
        paths = paths.rename(columns={"uid": id_col})
    paths[id_col] = paths[id_col].map(id_values)
    paths[name_col] = [names[str(uid)] for uid in paths[id_col]]
    # One no-match row per org is required by the legacy attach helpers, including
    # all-empty cohorts; per-activity match artifacts remain match rows only.
    matched = set(union.uid)
    missing_rows = []
    for org in extraction.organizations.to_dict("records"):
        if org["uid"] not in matched:
            missing_rows.append(
                {
                    id_col: id_values[org["uid"]],
                    name_col: org["Organization"],
                    **dict.fromkeys(taxonomy.columns),
                    "prob": None,
                    "tier": None,
                    "n_activities": 0,
                    "supporting_activity_ids": [],
                    "activity_evidence": [],
                    "evidence": None,
                    "reason": "No taxonomy match; see mapping_status and extraction_status.",
                }
            )
    paths = pd.DataFrame(paths.to_dict("records") + missing_rows, columns=[*paths.columns])
    paths = path_formatter(paths) if path_formatter else paths
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
    paths["mapping_status"] = paths[id_col].astype(str).map(statuses)

    # Output conventions and optional allocation are selected by the caller.
    from ..hierarchical_taxonomy_mapping import (
        attach_mapping_columns,
        distribute_funding_from_matches,
    )

    out = organizations.copy()
    if len(out):
        out = attach_mapping_columns(
            out,
            paths,
            taxonomy.levels,
            id_col,
            mapping_name=mapping_name,
            pad_to_n_levels=pad_to_n_levels,
        )
        funding = (
            distribute_funding_from_matches(
                paths, taxonomy.levels, id_col, name_col, max_level=max_funding_level
            )
            if max_funding_level is not None
            else pd.DataFrame()
        )
    else:
        # A bounded run over an empty input still has stable output schemas.
        for i in range(pad_to_n_levels or len(taxonomy.columns)):
            out[f"level{i}_{mapping_name}"] = pd.Series(dtype=object)
            out[f"all_level{i}_{mapping_name}"] = pd.Series(dtype=object)
        out[mapping_name] = pd.Series(dtype=object)
        out[f"cat_level_{mapping_name}"] = pd.Series(dtype=float)
        funding = (
            pd.DataFrame(
                columns=[
                    id_col,
                    "cat_level",
                    *[
                        f"level{i}"
                        for i in range(min(max_funding_level, len(taxonomy.columns) - 1) + 1)
                    ],
                    name_col,
                    "FundingFrac",
                ]
            )
            if max_funding_level is not None
            else pd.DataFrame()
        )
    out[f"{taxonomy.name}_mapping_status"] = out[id_col].astype(str).map(statuses)
    export_columns = [
        *[
            f"{prefix}level{i}_{mapping_name}"
            for i in range(pad_to_n_levels or len(taxonomy.columns))
            for prefix in ("", "all_")
        ],
        mapping_name,
        f"cat_level_{mapping_name}",
        f"{taxonomy.name}_mapping_status",
    ]
    lineage = {
        **mapped.lineage,
        "export_columns": export_columns,
        "max_funding_level": max_funding_level,
        "n_activities": mapped.lineage["n_subjects"],
        "max_mapping_level": len(taxonomy.columns) - 1,
        "rollup": "all_activity_union",
        "status_counts": pd.Series(statuses).value_counts().to_dict(),
    }
    return MappingResult(
        out,
        paths,
        activity_paths,
        mapped.scores.rename(columns={"subject_id": "activity_id"}),
        mapped.usage.rename(columns={"subject_id": "activity_id"}),
        funding,
        taxonomy.nodes.copy(),
        lineage,
    )
