"""Score subjects against taxonomy nodes through the cached Jev gateway; select matches offline.

Scoring (``score_subjects``) is the only step that talks to the gateway, through the
existing SQL prompt cache. Selection (``select_matches``) is deterministic and needs
no credentials, so thresholds can be changed and re-applied to saved scores.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass

import pandas as pd
import tiktoken

from ...tools.unique_ids import create_deterministic_sha256
from .taxonomies import Taxonomy

JEV_MODEL = "typesafe-ai/jev"


@dataclass(frozen=True)
class MatchPolicy:
    """How probabilities become matches; deterministic and versioned by ``sha256``.

    Attributes
    ----------
    thresholds : tuple[float, ...]
        Strictly descending tiers. A subject is matched to every node at or above
        ``thresholds[0]``; only a subject with no such node falls through to
        ``thresholds[1]``, and so on. Default ``(0.7, 0.5)``.
    descend : float, optional
        When set, a child of a matched node is also matched if its own probability is
        at least this value, recursively down the hierarchy (``match_origin ==
        "descend"``). None disables descent.
    """

    thresholds: tuple[float, ...] = (0.7, 0.5)
    descend: float | None = None

    def __post_init__(self):
        if not self.thresholds or any(
            not math.isfinite(t) or not 0 <= t <= 1 for t in self.thresholds
        ):
            raise ValueError("Thresholds must be finite probabilities")
        if any(a <= b for a, b in zip(self.thresholds, self.thresholds[1:], strict=False)):
            raise ValueError("Fallback thresholds must be strictly descending")
        if self.descend is not None and (
            not math.isfinite(self.descend) or not 0 <= self.descend <= 1
        ):
            raise ValueError("Descent threshold must be a finite probability")

    @property
    def sha256(self):
        """Fingerprint of the policy fields; recorded on every selected path."""
        return create_deterministic_sha256(asdict(self))


@dataclass
class ScoringResult:
    """Raw Jev output for one taxonomy, before any policy is applied.

    Attributes
    ----------
    scores : pandas.DataFrame
        One row per successful (subject, node): ``subject_id``, ``node_id``, ``prob``
        (float in [0, 1]), ``request_group``, ``question_sha256``.
    usage : pandas.DataFrame
        One row per subject and request group: ``subject_id``, ``request_group``,
        ``question_sha256``, ``n_questions``, ``model``, ``error`` (None on success,
        otherwise why the group has no scores), ``input_tokens``, ``output_tokens``.
    lineage : dict
        ``taxonomy``, ``questions_sha256``, ``max_question_tokens``, ``subject_kind``,
        ``taxonomy_path``, ``taxonomy_sha256``, ``question_variant``, ``model``,
        ``question_sets`` (group -> question hash), ``n_nodes``, ``n_groups``,
        ``n_subjects``, ``n_failed_requests``.
    """

    scores: pd.DataFrame
    usage: pd.DataFrame
    lineage: dict


def _noul_values(raw, questions):
    answers = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise ValueError("Missing or unexpected Jev answers")
    values = {}
    for node, answer in answers.items():
        probability = answer.get("noul") if isinstance(answer, dict) else None
        if isinstance(probability, bool) or not isinstance(probability, (int, float)):
            raise ValueError("Jev answer is not a numeric Noul probability")
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("Jev probability is outside [0, 1]")
        values[node] = float(probability)
    return values


def question_sets(taxonomy: Taxonomy, *, max_tokens: int = 48_000) -> dict[str, dict]:
    """Pack the taxonomy's questions into deterministic request groups under a token budget.

    Parameters
    ----------
    taxonomy : Taxonomy
        Its ``nodes`` are taken in order; each contributes ``node_id -> question``.
    max_tokens : int
        cl100k estimate allowed per group, at most 48 000. That reserves at least
        16k of the gateway's 64k context for subject state, request framing and
        tokenizer differences.

    Returns
    -------
    dict[str, dict]
        ``{"g000": {node_id: question, ...}, "g001": ...}``. Every node appears in
        exactly one group; groups are filled in node order and never split a node.

    Raises
    ------
    ValueError
        A budget outside 1..48000, or a single question that alone exceeds it.
        Definitions are never truncated to make a request fit.
    """
    if max_tokens <= 0 or max_tokens > 48_000:
        raise ValueError("Question token budget must be in 1..48000")
    encoding = tiktoken.get_encoding("cl100k_base")
    groups, current = {}, {}
    for row in taxonomy.nodes.to_dict("records"):
        candidate = {**current, row["node_id"]: row["question"]}
        tokens = len(encoding.encode(json.dumps(candidate, sort_keys=True), disallowed_special=()))
        if tokens > max_tokens:
            if not current:
                raise ValueError(f"Taxonomy question exceeds request budget: {row['node_id']}")
            groups[f"g{len(groups):03}"] = current
            current = {row["node_id"]: row["question"]}
            if (
                len(encoding.encode(json.dumps(current, sort_keys=True), disallowed_special=()))
                > max_tokens
            ):
                raise ValueError(f"Taxonomy question exceeds request budget: {row['node_id']}")
        else:
            current = candidate
    if current:
        groups[f"g{len(groups):03}"] = current
    return groups


def sql_request(
    questions,
    ids_texts,
    *,
    prompt_name,
    max_workers=4,
    read_from_cache=True,
    write_to_cache=True,
    session=None,
):
    """Send one question group for many subjects through the cached SQL Jev adapter.

    This is the default ``request_runner`` of ``score_subjects``; tests inject a fake
    with the same signature. Imports are local so offline selection needs no
    database configuration.

    Parameters
    ----------
    questions : dict[str, dict]
        One request group from ``question_sets``: ``node_id -> question``.
    ids_texts : list[tuple[str, str]]
        ``(cache_id, state)`` pairs; ``cache_id`` keys the cache row.
    prompt_name : str
        Human-readable cache prompt name (taxonomy, subject kind, variant, group).
    max_workers : int
        Parallel gateway calls.
    read_from_cache, write_to_cache : bool
        Cache behaviour, as for ``bulk_get_cache_or_run``.
    session : sqlalchemy.orm.Session, optional
        Reuse an open session; otherwise one is opened for the call.

    Returns
    -------
    dict[str, dict]
        ``cache_id -> row`` with ``response_text`` (JSON ``{node_id: {"noul": p}}``)
        and ``response_full``. Requests that failed after bounded retries are absent.
        A cached row whose answers are incomplete or invalid is re-requested once
        without reading the cache, so malformed rows written by older callers heal.
    """
    from ...database_cache.database_utils import get_session
    from ...openai.prompt_response_cache_jev import JevPromptResponseCacheSQL

    class CompleteNoulCache(JevPromptResponseCacheSQL):
        def get_completion(self, prompt_str, text, return_all=False, **kwargs):
            response = super().get_completion(prompt_str, text, return_all=True, **kwargs)
            # Invalid gateway responses must be stored as errors so later runs
            # can retry, rather than caching incomplete answers as a success.
            _noul_values(response.output_text, self.questions)
            return response if return_all else response.payload["answers"]

    with get_session(session=session) as active:
        cache = CompleteNoulCache(session=active, questions=questions, prompt_name=prompt_name)
        rows = (
            cache.bulk_get_cache_or_run(
                ids_texts,
                max_workers=max_workers,
                max_errors=3,
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
            )
            or {}
        )
        invalid = set()
        for uid, row in rows.items():
            try:
                _noul_values(row.get("response_text"), questions)
            except (ValueError, TypeError):
                invalid.add(uid)
        if invalid:
            # Repair malformed rows written by older callers of the base cache.
            rows = {uid: row for uid, row in rows.items() if uid not in invalid}
            rows.update(
                cache.bulk_get_cache_or_run(
                    [(uid, state) for uid, state in ids_texts if uid in invalid],
                    max_workers=max_workers,
                    max_errors=3,
                    read_from_cache=False,
                    write_to_cache=write_to_cache,
                )
                or {}
            )
        return rows


def score_subjects(
    subjects,
    taxonomy,
    *,
    request_runner=None,
    session=None,
    max_workers=4,
    read_from_cache=True,
    write_to_cache=True,
    max_question_tokens=48_000,
    namespace="",
) -> ScoringResult:
    """Ask every node's question of every subject and keep every probability.

    Parameters
    ----------
    subjects : pandas.DataFrame
        One row per subject with ``subject_id`` (unique, non-null) and ``state`` (the
        text Jev judges, at most 8 000 cl100k tokens). Optional ``cache_id`` replaces
        the derived key ``create_deterministic_sha256([namespace, subject_id, state])``; optional
        ``kind`` must equal ``taxonomy.subject_kind`` when both are set.
    taxonomy : Taxonomy
        Supplies the questions and the identity recorded in ``lineage``.
    request_runner : callable, optional
        ``runner(questions, ids_texts, *, prompt_name, max_workers, read_from_cache,
        write_to_cache, session) -> {cache_id: row}``; defaults to ``sql_request``.
    session : sqlalchemy.orm.Session, optional
        Passed to the runner.
    max_workers : int
        Passed to the runner.
    read_from_cache, write_to_cache : bool
        Passed to the runner.
    max_question_tokens : int
        Budget per request group; see ``question_sets``.
    namespace : str
        Caller namespace mixed into derived cache IDs so two projects never share a
        subject's cached answers by accident.

    Returns
    -------
    ScoringResult
        A request group whose answers are missing, incomplete or invalid yields no
        scores for that subject and a non-null ``error`` in ``usage``; selection later
        refuses such a subject rather than matching on partial evidence.

    Raises
    ------
    ValueError
        Missing columns, null or duplicate subject IDs, a ``kind`` that contradicts
        the taxonomy, an empty or oversized ``state``, or one ``cache_id`` used for
        two different states.
    """
    subjects = subjects.copy()
    required = {"subject_id", "state"}
    if missing := required - set(subjects):
        raise ValueError(f"Missing subject columns: {sorted(missing)}")
    if subjects.subject_id.isna().any() or subjects.subject_id.duplicated().any():
        raise ValueError("Subject IDs must be non-null and unique")
    if "cache_id" not in subjects:
        subjects["cache_id"] = [
            create_deterministic_sha256([namespace, str(r.subject_id), r.state]) for r in subjects.itertuples()
        ]
    if (
        taxonomy.subject_kind
        and "kind" in subjects
        and not subjects.kind.eq(taxonomy.subject_kind).all()
    ):
        raise ValueError("Subject kind does not match taxonomy question wording")
    groups = question_sets(taxonomy, max_tokens=max_question_tokens)
    encoding = tiktoken.get_encoding("cl100k_base")
    for row in subjects.to_dict("records"):
        if not isinstance(row["state"], str) or not row["state"].strip():
            raise ValueError("Subject state cannot be empty")
        if len(encoding.encode(row["state"], disallowed_special=())) > 8_000:
            raise ValueError(f"Subject state exceeds token budget: {row['subject_id']}")
    if subjects.groupby("cache_id").state.nunique().gt(1).any():
        raise ValueError("A cache ID cannot refer to multiple subject states")
    pairs = list(
        subjects[["cache_id", "state"]].drop_duplicates().itertuples(index=False, name=None)
    )
    runner = request_runner or sql_request
    scores, usage = [], []
    for group, questions in groups.items():
        rows = (
            runner(
                questions,
                pairs,
                prompt_name=f"{taxonomy.name} Jev {'activities' if taxonomy.subject_kind == 'activity' else 'subjects'} ({taxonomy.variant}) {group}",
                max_workers=max_workers,
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
                session=session,
            )
            if pairs
            else {}
        )
        question_hash = create_deterministic_sha256(questions)
        for subject in subjects.to_dict("records"):
            row = rows.get(subject["cache_id"]) or {}
            error, values = None, {}
            try:
                values = _noul_values(row.get("response_text"), questions)
            except (ValueError, TypeError):
                error = "request failed or returned incomplete/invalid Jev probabilities"
            full = row.get("response_full") or {}
            if isinstance(full, str):
                try:
                    full = json.loads(full)
                except ValueError:
                    full = {}
            full = full if isinstance(full, dict) else {}
            token_usage = full.get("usage") or {}
            token_usage = token_usage if isinstance(token_usage, dict) else {}
            usage.append(
                {
                    "subject_id": subject["subject_id"],
                    "request_group": group,
                    "question_sha256": question_hash,
                    "n_questions": len(questions),
                    "model": full.get("model", JEV_MODEL),
                    "error": error,
                    "input_tokens": token_usage.get("input_tokens"),
                    "output_tokens": token_usage.get("output_tokens"),
                }
            )
            if error is None:
                scores.extend(
                    {
                        "subject_id": subject["subject_id"],
                        "node_id": node,
                        "prob": p,
                        "request_group": group,
                        "question_sha256": question_hash,
                    }
                    for node, p in values.items()
                )
    score_frame = pd.DataFrame(
        scores, columns=["subject_id", "node_id", "prob", "request_group", "question_sha256"]
    )
    usage_frame = pd.DataFrame(
        usage,
        columns=[
            "subject_id",
            "request_group",
            "question_sha256",
            "n_questions",
            "model",
            "error",
            "input_tokens",
            "output_tokens",
        ],
    )
    return ScoringResult(
        score_frame,
        usage_frame,
        {
            "taxonomy": taxonomy.name,
            "questions_sha256": taxonomy.questions_sha256,
            "max_question_tokens": max_question_tokens,
            "subject_kind": taxonomy.subject_kind,
            "taxonomy_path": taxonomy.path,
            "taxonomy_sha256": taxonomy.sha256,
            "question_variant": taxonomy.variant,
            "model": JEV_MODEL,
            "question_sets": {g: create_deterministic_sha256(q) for g, q in groups.items()},
            "n_nodes": len(taxonomy.nodes),
            "n_groups": len(groups),
            "n_subjects": len(subjects),
            "n_failed_requests": int(usage_frame.error.notna().sum()),
        },
    )


def _path(row, columns):
    return tuple(row[c] for c in columns if pd.notna(row[c]) and row[c] != "")


def drop_implied(paths, columns, *, id_col="subject_id"):
    """Drop rows whose path is a strict prefix of another selected path for the same ID.

    Parameters
    ----------
    paths : pandas.DataFrame
        Rows with ``id_col`` and the taxonomy path columns.
    columns : sequence[str]
        The taxonomy path columns, shallowest first.
    id_col : str
        Column identifying the subject or entity the paths belong to.

    Returns
    -------
    pandas.DataFrame
        A copy without the implied ancestors; the deeper rows carry them.
    """
    keys = [(row[id_col], _path(row, columns)) for row in paths.to_dict("records")]
    implied = {(uid, key[:depth]) for uid, key in keys for depth in range(1, len(key))}
    return paths.loc[[key not in implied for key in keys]].copy()


def select_matches(result: ScoringResult, taxonomy: Taxonomy, policy: MatchPolicy) -> pd.DataFrame:
    """Turn saved probabilities into matched paths with an explicit policy; no API calls.

    Parameters
    ----------
    result : ScoringResult
        Scores and usage for the subjects. Must have been produced for ``taxonomy``
        (same payload hash, variant and question hash) and the same subject kind.
    taxonomy : Taxonomy
        Supplies node paths and the hierarchy used for descent and implication.
    policy : MatchPolicy
        Tier thresholds and optional descent.

    Returns
    -------
    pandas.DataFrame
        One row per selected (subject, node): ``subject_id``, ``node_id``, the path
        columns, ``depth``, ``prob``, ``tier`` (1-based tier index; descent rows use
        ``len(thresholds) + 1``), ``match_origin`` (``own`` or ``descend``),
        ``ancestor_matches`` (one dict per level of the path with ``node_id``,
        ``level``, ``assignment`` of ``own`` / ``descend`` / ``implied`` and
        ``prob``) and ``policy_sha256``. Ancestors of a selected node are implied
        regardless of their own probabilities; a path implied by a deeper selected
        path is not repeated as its own row.

    Raises
    ------
    ValueError
        Scores from a different taxonomy, variant, question set or subject kind;
        a subject with scores for only some nodes; a probability outside [0, 1]; or
        duplicate (subject, node) scores. Subjects with a failed request group are
        skipped, not partially matched.
    """
    if (
        result.lineage.get("taxonomy_sha256") != taxonomy.sha256
        or result.lineage.get("question_variant") != taxonomy.variant
    ):
        raise ValueError("Stored scores belong to a different taxonomy or question variant")
    questions_match = result.lineage.get("questions_sha256") == taxonomy.questions_sha256
    if "questions_sha256" not in result.lineage:
        # Older saved activity runs recorded hashes per request group instead.
        groups = question_sets(
            taxonomy, max_tokens=result.lineage.get("max_question_tokens", 48_000)
        )
        questions_match = result.lineage.get("question_sets") == {
            group: create_deterministic_sha256(questions) for group, questions in groups.items()
        }
    if not questions_match:
        raise ValueError("Stored scores belong to different taxonomy questions; re-run scoring")
    if result.lineage.get("subject_kind", taxonomy.subject_kind) != taxonomy.subject_kind:
        raise ValueError("Stored scores belong to a different subject kind")
    failed = set(result.usage.loc[result.usage.error.notna(), "subject_id"])
    rows = result.scores[~result.scores.subject_id.isin(failed)]
    expected_subjects = set(result.usage.subject_id) - failed
    if set(rows.subject_id) != expected_subjects:
        raise ValueError("Incomplete subject scores cannot be matched; re-run scoring")
    if any(
        isinstance(p, bool)
        or not isinstance(p, (int, float))
        or not math.isfinite(p)
        or not 0 <= p <= 1
        for p in rows.prob
    ):
        raise ValueError("Stored probabilities must be finite values in [0, 1]")
    if rows.duplicated(["subject_id", "node_id"]).any():
        raise ValueError("Duplicate subject/node scores")
    expected = set(taxonomy.nodes.node_id)
    for _, group in rows.groupby("subject_id"):
        if set(group.node_id) != expected:
            raise ValueError("Incomplete node scores cannot be matched; re-run scoring")
    scores = rows.merge(taxonomy.nodes, on="node_id", validate="many_to_one")
    matched, chosen = set(), []
    for tier, minimum in enumerate(policy.thresholds, start=1):
        hits = scores[(~scores.subject_id.isin(matched)) & (scores.prob >= minimum)]
        chosen.extend(hits.assign(tier=tier, match_origin="own").to_dict("records"))
        matched.update(hits.subject_id)
    if policy.descend is not None:
        have = {(r["subject_id"], _path(r, taxonomy.columns)) for r in chosen}
        for depth in range(1, len(taxonomy.columns)):
            parents = {(aid, path[:depth]) for aid, path in have if len(path) >= depth}
            candidates = scores[(scores.depth == depth) & (scores.prob >= policy.descend)]
            for r in candidates.to_dict("records"):
                branch = _path(r, taxonomy.columns)
                key = (r["subject_id"], branch)
                if (r["subject_id"], branch[:-1]) in parents and key not in have:
                    chosen.append(
                        {**r, "tier": len(policy.thresholds) + 1, "match_origin": "descend"}
                    )
                    have.add(key)
    columns = ["subject_id", "node_id", *taxonomy.columns, "depth", "prob", "tier", "match_origin"]
    paths = pd.DataFrame(chosen, columns=columns)
    paths = drop_implied(paths, taxonomy.columns).drop_duplicates(["subject_id", "node_id"])
    origins = {(r["subject_id"], r["node_id"]): r["match_origin"] for r in chosen}
    node_by_path = {_path(r, taxonomy.columns): r for r in taxonomy.nodes.to_dict("records")}
    probabilities = {(r["subject_id"], r["node_id"]): r["prob"] for r in scores.to_dict("records")}
    ancestor_records = []
    for row in paths.to_dict("records"):
        branch = _path(row, taxonomy.columns)
        ancestors = []
        for depth in range(1, len(branch) + 1):
            node = node_by_path[branch[:depth]]
            key = (row["subject_id"], node["node_id"])
            ancestors.append(
                {
                    "node_id": node["node_id"],
                    "level": node["level"],
                    "assignment": origins.get(key, "implied"),
                    "prob": probabilities[key],
                }
            )
        ancestor_records.append(ancestors)
    paths["ancestor_matches"] = ancestor_records
    paths["policy_sha256"] = policy.sha256
    return paths.sort_values(["subject_id", *taxonomy.columns], na_position="first").reset_index(
        drop=True
    )


def score_activities(activities, taxonomy, **kwargs):
    """``score_subjects`` for a frame keyed by ``activity_id`` instead of ``subject_id``.

    Parameters
    ----------
    activities : pandas.DataFrame
        ``ExtractionResult.activities`` or any frame with ``activity_id`` and ``state``.
    taxonomy : Taxonomy
    **kwargs
        Forwarded to ``score_subjects``.

    Returns
    -------
    ScoringResult
        ``scores`` and ``usage`` carry ``activity_id`` rather than ``subject_id``.
    """
    scored = score_subjects(
        activities.rename(columns={"activity_id": "subject_id"}), taxonomy, **kwargs
    )
    return ScoringResult(
        scored.scores.rename(columns={"subject_id": "activity_id"}),
        scored.usage.rename(columns={"subject_id": "activity_id"}),
        scored.lineage,
    )


def apply_policy(result, taxonomy, policy):
    """``select_matches`` for a ``score_activities`` result; returns ``activity_id`` rows.

    Parameters
    ----------
    result : ScoringResult
        From ``score_activities``.
    taxonomy : Taxonomy
    policy : MatchPolicy

    Returns
    -------
    pandas.DataFrame
        As ``select_matches``, with ``subject_id`` renamed to ``activity_id``.
    """
    scored = ScoringResult(
        result.scores.rename(columns={"activity_id": "subject_id"}),
        result.usage.rename(columns={"activity_id": "subject_id"}),
        result.lineage,
    )
    return select_matches(scored, taxonomy, policy).rename(columns={"subject_id": "activity_id"})
