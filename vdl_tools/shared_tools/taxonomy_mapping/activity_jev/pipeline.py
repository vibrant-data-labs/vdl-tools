"""Both taxonomy adapters, union rollup, and existing funding/export contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from ...activity_extraction.pipeline import ExtractionResult
from .engine import MatchPolicy, apply_policy, drop_implied, score_activities
from .taxonomies import SOLUTION_PROPERTIES, ActivityTaxonomy


@dataclass
class MappingResult:
    organizations: pd.DataFrame
    paths: pd.DataFrame
    activity_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    funding: pd.DataFrame
    nodes: pd.DataFrame
    lineage: dict


def rollup_paths(activity_paths, activities, taxonomy: ActivityTaxonomy):
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


def _legacy_columns(paths, taxonomy):
    """Keep display cluster prefixes compatible with the existing tracker loaders."""
    out = paths.copy()
    if taxonomy.name == "drawdown":
        out["Cluster"] = [
            f"{sector}: {cluster}"
            if pd.notna(cluster) and not str(cluster).startswith(f"{sector}: ")
            else cluster
            for sector, cluster in zip(out.Sector, out.Cluster, strict=True)
        ]
        out["Activity"] = None
    else:
        out["Sub-Term"] = None
    return out


def map_extraction(
    organizations: pd.DataFrame,
    extraction: ExtractionResult,
    taxonomy: ActivityTaxonomy,
    *,
    id_col="id",
    name_col="Organization",
    policy: MatchPolicy | None = None,
    request_runner=None,
    session=None,
    max_workers=4,
    max_question_tokens=48_000,
) -> MappingResult:
    """Map through Solution, preserving all organization rows and existing allocation.

    Unclassified rows have explicit status; missing/failed extraction does not remove
    an org or trigger organization-description inference. No publishing occurs here.
    """
    if organizations[id_col].isna().any() or organizations[id_col].astype(str).duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    if set(organizations[id_col].astype(str)) != set(extraction.organizations.uid):
        raise ValueError("Extraction and mapping organization cohorts differ")
    policy = policy or MatchPolicy(descend=0.6 if taxonomy.name == "drawdown" else None)
    scored = score_activities(
        extraction.activities,
        taxonomy,
        request_runner=request_runner,
        session=session,
        max_workers=max_workers,
        max_question_tokens=max_question_tokens,
    )
    activity_paths = apply_policy(scored, taxonomy, policy)
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
    paths = _legacy_columns(paths, taxonomy)
    paths["deepest_match"] = [
        next((c for c in reversed(taxonomy.columns) if pd.notna(row[c])), "NoMatch")
        for row in paths.to_dict("records")
    ]
    paths["taxonomy_sha256"] = taxonomy.sha256
    paths["policy_sha256"] = policy.sha256
    failure_ids = set(scored.usage.loc[scored.usage.error.notna(), "activity_id"])
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

    # These are the existing shared helpers, with the agreed funding max_level=2.
    from ..hierarchical_taxonomy_mapping import (
        attach_mapping_columns,
        distribute_funding_from_matches,
    )

    out = organizations.copy()
    mapping_name = "one_earth_category" if taxonomy.name == "one_earth" else "drawdown_category"
    if len(out):
        out = attach_mapping_columns(
            out, paths, taxonomy.levels, id_col, mapping_name=mapping_name, pad_to_n_levels=4
        )
        funding = distribute_funding_from_matches(
            paths, taxonomy.levels, id_col, name_col, max_level=2
        )
    else:
        # A bounded run over an empty input still has stable output schemas.
        for i in range(4):
            out[f"level{i}_{mapping_name}"] = pd.Series(dtype=object)
            out[f"all_level{i}_{mapping_name}"] = pd.Series(dtype=object)
        out[mapping_name] = pd.Series(dtype=object)
        out[f"cat_level_{mapping_name}"] = pd.Series(dtype=float)
        funding = pd.DataFrame(
            columns=[id_col, "cat_level", "level0", "level1", "level2", name_col, "FundingFrac"]
        )
    out[f"{taxonomy.name}_mapping_status"] = out[id_col].astype(str).map(statuses)
    if taxonomy.name == "drawdown":
        solutions = taxonomy.nodes[taxonomy.nodes.depth == 2]
        if solutions.Solution.duplicated().any():
            raise ValueError("Legacy Drawdown property columns require unique Solution labels")
        for source, target in zip(
            SOLUTION_PROPERTIES,
            (
                "drawdown_solution_category",
                "drawdown_climate_pollutants",
                "drawdown_speed_of_action",
            ),
            strict=True,
        ):
            lookup = solutions.set_index("Solution")[source].to_dict()
            out[target] = out[f"all_level2_{mapping_name}"].map(
                lambda values, lookup=lookup: [lookup.get(v) for v in values]
            )
            paths[source] = paths.Solution.map(lookup)
    lineage = {
        **scored.lineage,
        "policy": asdict(policy),
        "policy_sha256": policy.sha256,
        "max_funding_level": 2,
        "max_mapping_level": 2,
        "rollup": "all_activity_union",
        "status_counts": pd.Series(statuses).value_counts().to_dict(),
    }
    return MappingResult(
        out,
        paths,
        activity_paths,
        scored.scores,
        scored.usage,
        funding,
        taxonomy.nodes.copy(),
        lineage,
    )
