"""Map supplied subjects to one taxonomy and, optionally, group the matches by entity.

``map_subjects`` scores every subject against every node of one ``Taxonomy`` through
the cached JEV gateway and selects matches with an explicit offline ``MatchPolicy``.
``group_matches`` unions those subject-level matches to their entity (for example,
activities to their organization) while keeping the supporting subjects. Nothing
here extracts text, names a taxonomy, allocates funding or writes tracker columns;
``exports`` and the per-taxonomy packages (``taxonomy_mapping.one_earth``,
``taxonomy_mapping.drawdown``) do that.
"""

from dataclasses import asdict, dataclass

import pandas as pd

from .engine import MatchPolicy, drop_implied, score_subjects, select_matches


@dataclass
class SubjectMappingResult:
    """Everything ``map_subjects`` produces for one taxonomy.

    Attributes
    ----------
    subject_paths : pandas.DataFrame
        Selected matches, one row per (subject, node): ``subject_id``, ``node_id``,
        the taxonomy path columns, ``depth``, ``prob``, ``tier``, ``match_origin``
        (``own`` or ``descend``), ``ancestor_matches`` (list of dicts with
        ``node_id``, ``level``, ``assignment``, ``prob``) and ``policy_sha256``.
    scores : pandas.DataFrame
        Every returned probability: ``subject_id``, ``node_id``, ``prob``,
        ``request_group``, ``question_sha256``.
    usage : pandas.DataFrame
        One row per subject and request group: ``subject_id``, ``request_group``,
        ``question_sha256``, ``n_questions``, ``model``, ``error`` (None on success),
        ``input_tokens``, ``output_tokens``.
    nodes : pandas.DataFrame
        Copy of ``taxonomy.nodes`` the scores refer to.
    lineage : dict
        Scoring identity from ``score_subjects`` (taxonomy hash and path, question
        hash, variant, model, request-group hashes, counts) plus ``policy``,
        ``policy_sha256`` and ``max_mapping_level``.
    """

    subject_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    nodes: pd.DataFrame
    lineage: dict


def map_subjects(subjects, taxonomy, *, policy: MatchPolicy, **scoring_options):
    """Score supplied subjects against one taxonomy and select matches offline.

    Parameters
    ----------
    subjects : pandas.DataFrame
        One row per subject with ``subject_id`` (unique, non-null) and ``state``, the
        text JEV judges. Optional ``cache_id`` overrides the derived cache key;
        optional ``kind`` must equal ``taxonomy.subject_kind`` when both are set.
    taxonomy : Taxonomy
        The taxonomy whose built questions are asked of every subject.
    policy : MatchPolicy
        Thresholds and optional descent that turn probabilities into matches.
    **scoring_options
        Forwarded to ``engine.score_subjects``: ``request_runner``, ``session``,
        ``max_workers``, ``read_from_cache``, ``write_to_cache``,
        ``max_question_tokens`` and ``namespace``.

    Returns
    -------
    SubjectMappingResult
    """
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
    """Union subject-level matches to their entity, keeping the supporting subjects.

    Parameters
    ----------
    matches : pandas.DataFrame
        ``subject_paths`` from ``map_subjects``.
    subjects : pandas.DataFrame
        The subjects that were mapped; needs ``subject_id`` and ``entity_col``. Every
        other column is carried into ``subject_evidence``.
    taxonomy : Taxonomy
        Supplies the path columns to group on.
    entity_col : str
        Column linking a subject to its entity (default ``entity_id``).

    Returns
    -------
    pandas.DataFrame
        One row per (entity, path): ``entity_col``, the taxonomy path columns,
        ``prob`` (max over supporting subjects), ``tier`` (best, i.e. lowest),
        ``supporting_subject_ids`` and ``subject_evidence`` (one dict per supporting
        subject with its ``subject_id`` and every subject column). A path implied by
        a deeper selected path of the same entity is dropped.

    Raises
    ------
    ValueError
        Null or duplicate subject IDs, null entity links, or a match whose subject is
        not in ``subjects``.
    """
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
