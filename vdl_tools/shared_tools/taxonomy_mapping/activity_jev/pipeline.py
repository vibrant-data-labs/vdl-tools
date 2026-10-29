"""JEV mapping of supplied subjects; no extraction, funding or tracker dependency."""

from dataclasses import asdict, dataclass
from importlib import import_module

import pandas as pd

from .engine import MatchPolicy, drop_implied, score_subjects, select_matches


@dataclass
class SubjectMappingResult:
    subject_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    nodes: pd.DataFrame
    lineage: dict


def map_subjects(subjects, taxonomy, *, policy: MatchPolicy, **scoring_options):
    """Map supplied text against one taxonomy with an explicit offline policy."""
    scored = score_subjects(subjects, taxonomy, **scoring_options)
    paths = select_matches(scored, taxonomy, policy)
    return SubjectMappingResult(
        paths,
        scored.scores,
        scored.usage,
        taxonomy.nodes.copy(),
        {
            **scored.lineage,
            "policy": asdict(policy),
            "policy_sha256": policy.sha256,
            "max_mapping_level": len(taxonomy.columns) - 1,
        },
    )


def group_matches(matches, subjects, taxonomy, *, entity_col="entity_id"):
    """Optional entity union retaining supporting subject records."""
    if subjects.subject_id.isna().any() or subjects.subject_id.duplicated().any():
        raise ValueError("Subject IDs must be non-null and unique")
    if subjects[entity_col].isna().any():
        raise ValueError("Entity links cannot be null")
    columns = [
        entity_col,
        *taxonomy.columns,
        "prob",
        "tier",
        "supporting_subject_ids",
        "subject_evidence",
    ]
    if matches.empty:
        return pd.DataFrame(columns=columns)
    if not set(matches.subject_id).issubset(set(subjects.subject_id)):
        raise ValueError("A match has no supporting subject")
    joined = matches.merge(
        subjects[["subject_id", entity_col]], on="subject_id", validate="many_to_one"
    )
    source = subjects.set_index("subject_id").to_dict("index")
    for c in taxonomy.columns:
        joined[c] = joined[c].fillna("")
    records = []
    for key, group in joined.groupby([entity_col, *taxonomy.columns], sort=False):
        ids = list(dict.fromkeys(group.subject_id))
        records.append(
            {
                entity_col: key[0],
                **{c: v or None for c, v in zip(taxonomy.columns, key[1:], strict=True)},
                "prob": float(group.prob.max()),
                "tier": int(group.tier.min()),
                "supporting_subject_ids": ids,
                "subject_evidence": [{"subject_id": sid, **source[sid]} for sid in ids],
            }
        )
    return drop_implied(
        pd.DataFrame(records, columns=columns), taxonomy.columns, id_col=entity_col
    ).reset_index(drop=True)


def rollup_paths(activity_paths, activities, taxonomy):
    """Compatibility wrapper preserving the original activity-evidence export."""
    from .activity_rollup import rollup_paths as rollup

    return rollup(activity_paths, activities, taxonomy)


def map_extraction(
    organizations,
    extraction,
    taxonomy,
    *,
    policy=None,
    id_col="id",
    name_col="Organization",
    **kwargs,
):
    """Legacy exporter; generic callers use map_subjects without extraction."""
    if organizations[id_col].isna().any() or organizations[id_col].astype(str).duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    if set(organizations[id_col].astype(str)) != set(extraction.organizations.uid):
        raise ValueError("Extraction and mapping organization cohorts differ")
    if taxonomy.name not in ("one_earth", "drawdown"):
        raise ValueError(
            "map_extraction is a legacy exporter; use map_subjects for another taxonomy"
        )
    adapter = import_module(f"{__package__}.{taxonomy.name}")
    mapped = map_subjects(
        extraction.activities.rename(columns={"activity_id": "subject_id"}),
        taxonomy,
        policy=policy or adapter.default_policy(),
        **kwargs,
    )
    return adapter.export_extraction(
        organizations, extraction, taxonomy, mapped, id_col=id_col, name_col=name_col
    )
