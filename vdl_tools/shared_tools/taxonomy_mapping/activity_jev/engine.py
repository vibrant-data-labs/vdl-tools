"""Cached scoring and offline selection, generalized from both Jev prototypes."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass

import pandas as pd
import tiktoken

from ...activity_extraction.pipeline import fingerprint
from .taxonomies import ActivityTaxonomy

JEV_MODEL = "typesafe-ai/jev"


@dataclass(frozen=True)
class MatchPolicy:
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
        return fingerprint(asdict(self))


@dataclass
class ScoringResult:
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


def question_sets(taxonomy: ActivityTaxonomy, *, max_tokens: int = 48_000) -> dict[str, dict]:
    """Deterministic bounded requests, unlike source Drawdown's fixed sector groups.

    cl100k estimates reserve at least 16k of the gateway's 64k context for state,
    request framing, and tokenizer differences. Reject an individually oversized
    question; never truncate taxonomy definitions to make a request fit.
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
    """Use the existing SQL Jev adapter; importing offline tools needs no credentials."""
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


def score_activities(
    activities,
    taxonomy,
    *,
    request_runner=None,
    session=None,
    max_workers=4,
    read_from_cache=True,
    write_to_cache=True,
    max_question_tokens=48_000,
) -> ScoringResult:
    """Keep every probability and one status per activity/request, including failures.

    Cache IDs derive from organization + state, independently of extraction run IDs.
    A missing/invalid question makes its entire request incomplete. Selection later
    refuses an activity with any incomplete group, preventing biased partial matches.
    """
    required = {"activity_id", "cache_id", "state"}
    if missing := required - set(activities):
        raise ValueError(f"Missing activity columns: {sorted(missing)}")
    if activities.activity_id.isna().any() or activities.activity_id.duplicated().any():
        raise ValueError("Activity IDs must be non-null and unique")
    groups = question_sets(taxonomy, max_tokens=max_question_tokens)
    encoding = tiktoken.get_encoding("cl100k_base")
    for row in activities.to_dict("records"):
        if not isinstance(row["state"], str) or not row["state"].strip():
            raise ValueError("Activity state cannot be empty")
        if len(encoding.encode(row["state"], disallowed_special=())) > 8_000:
            raise ValueError(f"Activity state exceeds token budget: {row['activity_id']}")
    if activities.groupby("cache_id").state.nunique().gt(1).any():
        raise ValueError("A cache ID cannot refer to multiple activity states")
    pairs = list(
        activities[["cache_id", "state"]].drop_duplicates().itertuples(index=False, name=None)
    )
    runner = request_runner or sql_request
    scores, usage = [], []
    for group, questions in groups.items():
        rows = (
            runner(
                questions,
                pairs,
                prompt_name=f"{taxonomy.name} Jev activities ({taxonomy.variant}) {group}",
                max_workers=max_workers,
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
                session=session,
            )
            if pairs
            else {}
        )
        question_hash = fingerprint(questions)
        for activity in activities.to_dict("records"):
            row = rows.get(activity["cache_id"]) or {}
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
                    "activity_id": activity["activity_id"],
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
                        "activity_id": activity["activity_id"],
                        "node_id": node,
                        "prob": p,
                        "request_group": group,
                        "question_sha256": question_hash,
                    }
                    for node, p in values.items()
                )
    score_frame = pd.DataFrame(
        scores, columns=["activity_id", "node_id", "prob", "request_group", "question_sha256"]
    )
    usage_frame = pd.DataFrame(
        usage,
        columns=[
            "activity_id",
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
            "taxonomy_path": taxonomy.path,
            "taxonomy_sha256": taxonomy.sha256,
            "question_variant": taxonomy.variant,
            "model": JEV_MODEL,
            "question_sets": {g: fingerprint(q) for g, q in groups.items()},
            "n_nodes": len(taxonomy.nodes),
            "n_groups": len(groups),
            "n_activities": len(activities),
            "n_failed_requests": int(usage_frame.error.notna().sum()),
        },
    )


def _path(row, columns):
    return tuple(row[c] for c in columns if pd.notna(row[c]) and row[c] != "")


def drop_implied(paths, columns, *, id_col="activity_id"):
    """The source Drawdown prefix rule, preserving full parent identity."""
    keys = [(row[id_col], _path(row, columns)) for row in paths.to_dict("records")]
    implied = {(uid, key[:depth]) for uid, key in keys for depth in range(1, len(key))}
    return paths.loc[[key not in implied for key in keys]].copy()


def apply_policy(
    result: ScoringResult, taxonomy: ActivityTaxonomy, policy: MatchPolicy
) -> pd.DataFrame:
    """Two-tier own-node selection and optional source Drawdown descendant recovery.

    Never issues API calls. Scores of every node are required for an activity before
    applying fallback thresholds. Ancestors of accepted nodes are implied, regardless
    of the ancestors' own scores, as in both source prototypes.
    """
    if (
        result.lineage.get("taxonomy_sha256") != taxonomy.sha256
        or result.lineage.get("question_variant") != taxonomy.variant
    ):
        raise ValueError("Stored scores belong to a different taxonomy or question variant")
    failed = set(result.usage.loc[result.usage.error.notna(), "activity_id"])
    rows = result.scores[~result.scores.activity_id.isin(failed)]
    if rows.duplicated(["activity_id", "node_id"]).any():
        raise ValueError("Duplicate activity/node scores")
    expected = set(taxonomy.nodes.node_id)
    for _, group in rows.groupby("activity_id"):
        if set(group.node_id) != expected:
            raise ValueError("Incomplete node scores cannot be matched; re-run scoring")
    scores = rows.merge(taxonomy.nodes, on="node_id", validate="many_to_one")
    matched, chosen = set(), []
    for tier, minimum in enumerate(policy.thresholds, start=1):
        hits = scores[(~scores.activity_id.isin(matched)) & (scores.prob >= minimum)]
        chosen.extend(hits.assign(tier=tier, match_origin="own").to_dict("records"))
        matched.update(hits.activity_id)
    if policy.descend is not None:
        have = {(r["activity_id"], _path(r, taxonomy.columns)) for r in chosen}
        for depth in (1, 2):
            parents = {(aid, path[:depth]) for aid, path in have if len(path) >= depth}
            candidates = scores[(scores.depth == depth) & (scores.prob >= policy.descend)]
            for r in candidates.to_dict("records"):
                branch = _path(r, taxonomy.columns)
                key = (r["activity_id"], branch)
                if (r["activity_id"], branch[:-1]) in parents and key not in have:
                    chosen.append(
                        {**r, "tier": len(policy.thresholds) + 1, "match_origin": "descend"}
                    )
                    have.add(key)
    columns = ["activity_id", "node_id", *taxonomy.columns, "depth", "prob", "tier", "match_origin"]
    paths = pd.DataFrame(chosen, columns=columns)
    paths = drop_implied(paths, taxonomy.columns).drop_duplicates(["activity_id", "node_id"])
    origins = {(r["activity_id"], r["node_id"]): r["match_origin"] for r in chosen}
    node_by_path = {_path(r, taxonomy.columns): r for r in taxonomy.nodes.to_dict("records")}
    probabilities = {(r["activity_id"], r["node_id"]): r["prob"] for r in scores.to_dict("records")}
    ancestor_records = []
    for row in paths.to_dict("records"):
        branch = _path(row, taxonomy.columns)
        ancestors = []
        for depth in range(1, len(branch) + 1):
            node = node_by_path[branch[:depth]]
            key = (row["activity_id"], node["node_id"])
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
    return paths.sort_values(["activity_id", *taxonomy.columns], na_position="first").reset_index(
        drop=True
    )
