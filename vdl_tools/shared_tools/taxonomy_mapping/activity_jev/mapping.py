"""Map any text onto a taxonomy with Jev: the taxonomy structure, scoring and match selection.

Nothing here knows about activities, organizations or trackers. A subject is one piece of
text with an ID, for example an extracted activity or an organization's own description.
A ``Taxonomy`` holds the hierarchy and one Jev question per node.

- ``Taxonomy`` and ``load_workbook`` build a taxonomy from a workbook.
- ``score_subjects`` is the only step that talks to the gateway, through the existing SQL
  prompt cache (``jev_gateway_request``).
- ``select_matches`` is deterministic and needs no credentials, so a ``MatchPolicy`` can be
  changed and re-applied to the same scores.
- ``map_subjects`` runs both.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from ...openai.openai_api_utils import get_num_tokens
from ...tools.logger import logger
from ...tools.unique_ids import create_deterministic_sha256


@dataclass
class Taxonomy:
    """One taxonomy with its hierarchy, node definitions and built Jev questions.

    Attributes
    ----------
    name : str
        Short identifier, e.g. ``one_earth``; prefixes the exported status column.
    columns : tuple[str, ...]
        One path column per level, shallowest first, e.g.
        ``("Pillar", "Sub-Pillar", "Solution")``.
    nodes : pandas.DataFrame
        One row per node: ``node_id`` (hash of taxonomy name and full ancestor path,
        so same-named children do not collide), ``level`` (the path column name),
        ``depth`` (0-based), ``name``, ``Definition``, ``question`` (dict with ``type``,
        ``instructions`` and optional ``criteria``), the path columns (None below the
        node's depth) and any loader-supplied attributes.
    sha256 : str
        Hash of the source payload (the workbook bytes); part of every score's lineage.
    path : str
        Where the payload was read from.
    """

    name: str
    columns: tuple[str, ...]
    nodes: pd.DataFrame
    sha256: str
    path: str

    def __post_init__(self):
        self.columns = tuple(self.columns)
        missing = [
            r["node_id"]
            for r in self.nodes.to_dict("records")
            if not isinstance(r["question"], dict) or not r["question"].get("instructions")
        ]
        if missing:
            raise ValueError(f"Taxonomy nodes without a question: {missing[:5]}")

    @property
    def levels(self):
        """Level specs in the shape ``attach_mapping_columns`` and funding allocation read."""
        return [{"idx": i, "key_col": c, "output_col": c} for i, c in enumerate(self.columns)]

    @property
    def questions_sha256(self):
        """Hash of every node's question, keyed by node ID; changes when wording changes."""
        return create_deterministic_sha256(
            {str(r.node_id): r.question for r in self.nodes.itertuples()}
        )


def load_workbook(name, path, *, columns, sheets, question_builder, attributes=()):
    """Read one sheet per level from an Excel workbook and build a ``Taxonomy``.

    Parameters
    ----------
    name : str
        Taxonomy identifier stored on the result and mixed into every ``node_id``.
    path : str or pathlib.Path
        Workbook to read. An unresolved Git LFS pointer is rejected.
    columns : sequence[str]
        Path column per level, shallowest first. Sheet ``i`` must contain
        ``columns[:i + 1]`` and ``Definition``; it may contain ``Exclude``.
    sheets : sequence[str]
        Sheet name per level, aligned with ``columns``.
    question_builder : callable
        ``question_builder(nodes, columns) -> nodes`` adds the ``question`` column (a
        dict with ``type``, ``instructions`` and optional ``criteria``) to the node
        frame described under ``Taxonomy.nodes``.
    attributes : sequence[str]
        Extra sheet columns copied onto nodes where present (None elsewhere).

    Returns
    -------
    Taxonomy
        A row with ``Exclude == 1`` and all of its descendants are omitted. Every
        other row becomes a node whose ``node_id`` is the first 24 hex characters of
        the SHA-256 of ``[name, full path]``.

    Raises
    ------
    ValueError
        Mismatched ``columns`` / ``sheets`` lengths, a missing required column, a
        blank path or definition, a duplicate or orphan path, or a workbook with no
        included nodes.
    """
    columns, sheets = tuple(columns), tuple(sheets)
    if len(columns) != len(sheets):
        raise ValueError("Configure one sheet per path column")
    path = Path(path)
    payload = path.read_bytes()
    if payload.startswith(b"version https://git-lfs.github.com/spec/"):
        raise ValueError(f"Taxonomy is an unresolved Git LFS pointer: {path}")
    rows, included, known = [], set(), set()
    with pd.ExcelFile(path) as book:
        for depth, sheet in enumerate(sheets):
            table = pd.read_excel(book, sheet_name=sheet)
            required = [*columns[: depth + 1], "Definition"]
            if missing := set(required) - set(table):
                raise ValueError(f"{sheet} is missing columns: {sorted(missing)}")
            for raw in table.to_dict("records"):
                if any(pd.isna(raw[c]) or not str(raw[c]).strip() for c in columns[: depth + 1]):
                    raise ValueError(f"Blank path in {sheet}")
                branch = tuple(str(raw[c]).strip() for c in columns[: depth + 1])
                if branch in known:
                    raise ValueError(f"Duplicate taxonomy path: {branch}")
                known.add(branch)
                if depth and branch[:-1] not in known:
                    raise ValueError(f"Orphan taxonomy path: {branch}")
                if pd.to_numeric(raw.get("Exclude", 0), errors="coerce") == 1:
                    continue
                if depth and branch[:-1] not in included:
                    continue  # descendant of an excluded node
                if pd.isna(raw["Definition"]) or not str(raw["Definition"]).strip():
                    raise ValueError(f"Blank definition in {sheet}")
                included.add(branch)
                rows.append(
                    {
                        "node_id": create_deterministic_sha256([name, branch])[:24],
                        "level": columns[depth],
                        "depth": depth,
                        "name": branch[-1],
                        "Definition": str(raw["Definition"]).strip(),
                        **{c: branch[i] if i <= depth else None for i, c in enumerate(columns)},
                        **{c: raw.get(c) if pd.notna(raw.get(c)) else None for c in attributes},
                    }
                )
    if not rows:
        raise ValueError("Taxonomy has no included nodes")
    return Taxonomy(
        name,
        columns,
        question_builder(pd.DataFrame(rows), columns),
        hashlib.sha256(payload).hexdigest(),
        str(path),
    )


JEV_MODEL = "typesafe-ai/jev"
# cl100k estimate per request; leaves at least 16k of the gateway's 64k context for the
# subject state, request framing and tokenizer differences.
MAX_QUESTION_TOKENS = 48_000


@dataclass(frozen=True)
class MatchPolicy:
    """How probabilities become matches; deterministic and versioned by ``sha256``.

    Attributes
    ----------
    thresholds : tuple[float, ...]
        Descending tiers. A subject is matched to every node at or above
        ``thresholds[0]``; only a subject with no such node falls through to
        ``thresholds[1]``, and so on. Default ``(0.7, 0.5)``.
    descend : float, optional
        When set, a child of a matched node is also matched if its own probability is
        at least this value, recursively down the hierarchy (``match_origin ==
        "descend"``). None disables descent.
    """

    thresholds: tuple[float, ...] = (0.7, 0.5)
    descend: float | None = None

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
        ``taxonomy``, ``taxonomy_path``, ``taxonomy_sha256``, ``questions_sha256``,
        ``model``, ``n_nodes``, ``n_groups``, ``n_subjects``, ``n_failed_requests``.
    """

    scores: pd.DataFrame
    usage: pd.DataFrame
    lineage: dict


@dataclass
class SubjectMappingResult:
    """Everything ``map_subjects`` produces for one taxonomy.

    Attributes
    ----------
    subject_paths : pandas.DataFrame
        Selected matches from ``select_matches``.
    scores, usage : pandas.DataFrame
        As on ``ScoringResult``.
    lineage : dict
        The scoring lineage plus ``policy``, ``policy_sha256`` and ``max_mapping_level``.
    """

    subject_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    lineage: dict


def _noul_values(raw, questions):
    """Parse a Jev answer payload into ``{node_id: probability}``; raise if incomplete or invalid."""
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


def question_sets(taxonomy: Taxonomy, *, max_tokens: int = MAX_QUESTION_TOKENS) -> dict[str, dict]:
    """Pack the taxonomy's questions into deterministic request groups under a token budget.

    Parameters
    ----------
    taxonomy : Taxonomy
        Its ``nodes`` are taken in order; each contributes ``node_id -> question``.
    max_tokens : int
        cl100k estimate allowed per group.

    Returns
    -------
    dict[str, dict]
        ``{"g000": {node_id: question, ...}, "g001": ...}``. Every node appears in
        exactly one group; groups are filled in node order and never split a node.

    Raises
    ------
    ValueError
        A single question that alone exceeds the budget. Definitions are never
        truncated to make a request fit.
    """
    def size(questions):
        return get_num_tokens(
            json.dumps(questions, sort_keys=True), "gpt-4", disallowed_special=()
        )

    groups, current = {}, {}
    for row in taxonomy.nodes.to_dict("records"):
        single = {row["node_id"]: row["question"]}
        if size(single) > max_tokens:
            raise ValueError(f"Taxonomy question exceeds request budget: {row['node_id']}")
        if current and size({**current, **single}) > max_tokens:
            groups[f"g{len(groups):03}"] = current
            current = {}
        current.update(single)
    if current:
        groups[f"g{len(groups):03}"] = current
    return groups


def jev_gateway_request(
    questions,
    ids_texts,
    *,
    prompt_name,
    max_workers=20,
    read_from_cache=True,
    write_to_cache=True,
):
    """Send one question group for many subjects to the Jev gateway, through the prompt cache.

    ``score_subjects`` sends every request group through this function. Imports are
    local so offline selection needs no database configuration.

    Parameters
    ----------
    questions : dict[str, dict]
        One request group from ``question_sets``: ``node_id -> question``.
    ids_texts : list[tuple[str, str]]
        ``(cache_id, state)`` pairs; ``cache_id`` keys the cache row.
    prompt_name : str
        Human-readable cache prompt name.
    max_workers : int
        Parallel gateway calls.
    read_from_cache, write_to_cache : bool
        Cache behaviour, as for ``bulk_get_cache_or_run``.

    Returns
    -------
    dict[str, dict]
        ``cache_id -> row`` with ``response_text`` (JSON ``{node_id: {"noul": p}}``)
        and ``response_full``. Requests that failed after bounded retries are absent.
    """
    from ...database_cache.database_utils import get_session
    from ...openai.prompt_response_cache_jev import JevPromptResponseCacheSQL

    class CompleteNoulCache(JevPromptResponseCacheSQL):
        """Store an incomplete or invalid gateway answer as an error so a later run retries it."""

        def get_completion(self, prompt_str, text, return_all=False, **kwargs):
            response = super().get_completion(prompt_str, text, return_all=True, **kwargs)
            _noul_values(response.output_text, self.questions)
            return response if return_all else response.payload["answers"]

    with get_session() as session:
        cache = CompleteNoulCache(
            session=session, questions=questions, prompt_name=prompt_name,
            model_provider="typesafe-ai",
        )
        return cache.bulk_get_cache_or_run(
            ids_texts,
            max_workers=max_workers,
            max_errors=3,
            extra_body={"providerOptions": {"gateway": {"only": ["typesafe-ai"]}}},
            read_from_cache=read_from_cache,
            write_to_cache=write_to_cache,
        )


def score_subjects(
    subjects,
    taxonomy,
    *,
    max_workers=20,
    read_from_cache=True,
    write_to_cache=True,
) -> ScoringResult:
    """Ask every node's question of every subject and keep every probability.

    Parameters
    ----------
    subjects : pandas.DataFrame
        One row per subject with ``subject_id`` (unique, non-null) and ``state`` (the
        non-empty text Jev judges), for example an extracted activity such as
        installing rooftop solar, or an organization's supplied description.
        Optional ``cache_id`` keys the cache row; when
        absent it is the SHA-256 of ``[subject_id, state]``.
    taxonomy : Taxonomy
        Supplies the questions and the identity recorded in ``lineage``.
    max_workers : int
        Parallel gateway requests, passed to ``jev_gateway_request``.
    read_from_cache, write_to_cache : bool
        Cache behaviour, passed to ``jev_gateway_request``.

    Returns
    -------
    ScoringResult
        A request group whose answers are missing, incomplete or invalid yields no
        scores for that subject and a non-null ``error`` in ``usage``; selection later
        skips such a subject rather than matching on partial evidence.

    Raises
    ------
    ValueError
        Missing columns, null or duplicate subject IDs, or an empty ``state``.
    """
    subjects = subjects.copy()
    if missing := {"subject_id", "state"} - set(subjects):
        raise ValueError(f"Missing subject columns: {sorted(missing)}")
    if subjects.subject_id.isna().any() or subjects.subject_id.duplicated().any():
        raise ValueError("Subject IDs must be non-null and unique")
    if not all(isinstance(s, str) and s.strip() for s in subjects.state):
        raise ValueError("Subject state cannot be empty")
    if "cache_id" not in subjects:
        subjects["cache_id"] = [
            create_deterministic_sha256([str(r.subject_id), r.state]) for r in subjects.itertuples()
        ]
    logger.info("JEV %s: packing %d node questions for %d subjects", taxonomy.name, len(taxonomy.nodes), len(subjects))
    groups = question_sets(taxonomy)
    pairs = list(
        subjects[["cache_id", "state"]].drop_duplicates().itertuples(index=False, name=None)
    )
    scores, usage = [], []
    for group_index, (group, questions) in enumerate(groups.items(), start=1):
        started = perf_counter()
        logger.info("JEV %s: group %d/%d (%s), %d questions for %d unique inputs; reading cache and requesting misses", taxonomy.name, group_index, len(groups), group, len(questions), len(pairs))
        rows = (
            jev_gateway_request(
                questions,
                pairs,
                prompt_name=f"{taxonomy.name} Jev {group}",
                max_workers=max_workers,
                read_from_cache=read_from_cache,
                write_to_cache=write_to_cache,
            )
            if pairs
            else {}
        )
        logger.info("JEV %s: group %s returned %d/%d responses in %.1fs", taxonomy.name, group, len(rows), len(pairs), perf_counter() - started)
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
            "taxonomy_path": taxonomy.path,
            "taxonomy_sha256": taxonomy.sha256,
            "questions_sha256": taxonomy.questions_sha256,
            "model": JEV_MODEL,
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
    """Turn probabilities into matched paths with an explicit policy; no API calls.

    Parameters
    ----------
    result : ScoringResult
        Scores and usage produced for ``taxonomy`` (same payload hash and question hash).
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
        path is not repeated as its own row. A subject with a failed request group is
        skipped, never partially matched.

    Raises
    ------
    ValueError
        Scores from a different taxonomy payload or question set.
    """
    if (result.lineage["taxonomy_sha256"], result.lineage["questions_sha256"]) != (
        taxonomy.sha256,
        taxonomy.questions_sha256,
    ):
        raise ValueError("Scores belong to a different taxonomy or question set; re-run scoring")
    failed = set(result.usage.loc[result.usage.error.notna(), "subject_id"])
    scores = result.scores[~result.scores.subject_id.isin(failed)].merge(
        taxonomy.nodes, on="node_id", validate="many_to_one"
    )
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


def map_subjects(subjects, taxonomy, *, policy: MatchPolicy, **scoring_options):
    """Score supplied subjects against one taxonomy and select matches offline.

    Parameters
    ----------
    subjects : pandas.DataFrame
        As for ``score_subjects``.
    taxonomy : Taxonomy
    policy : MatchPolicy
        Thresholds and optional descent that turn probabilities into matches.
    **scoring_options
        Forwarded to ``score_subjects``: ``max_workers``, ``read_from_cache``,
        ``write_to_cache``.

    Returns
    -------
    SubjectMappingResult
    """
    scored = score_subjects(subjects, taxonomy, **scoring_options)
    return SubjectMappingResult(
        select_matches(scored, taxonomy, policy),
        scored.scores,
        scored.usage,
        {
            **scored.lineage,
            "policy": asdict(policy),
            "policy_sha256": policy.sha256,
            "max_mapping_level": len(taxonomy.columns) - 1,
        },
    )
