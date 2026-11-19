"""Classify organizations from their extracted website activities, and shape the results for a tracker.

``run_activity_mapping`` is what a project calls. It extracts each organization's website
activities once (``activity_extraction``), turns them into subjects
(``build_activity_subjects``), maps them onto each ``MappingJob``'s taxonomy with
``mapping.map_subjects``, rolls the matches up to organizations, and writes every table
through ``parquet_cache`` with its lineage in the footer. Each taxonomy's files are written
as soon as it finishes. ``export_extraction`` is the roll-up and tracker export on its own,
and ``assemble_mapping_runs`` copies selected taxonomies' columns from finished runs onto a
base frame by ID.

The caller names its organization columns and owns every tracker convention: mapping
column names, display formatting, padding, funding depth, roll-up and file names. Nothing
here names a taxonomy or a project.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter

import pandas as pd

from ...activity_extraction.pipeline import ExtractionResult, extract_frame
from ...parquet_cache import get_lineage, read_dataframe, write_dataframe, write_dataframes
from ...tools.logger import logger
from ...tools.unique_ids import create_deterministic_sha256
from .mapping import MatchPolicy, Taxonomy, drop_implied, map_subjects

STATE_FIELDS = {
    "activity": "Activity",
    "what_happens": "What happens",
    "who_carries_it_out": "Who carries it out",
    "aim": "Aim",
    "sector": "Sector",
    "methods": "Methods",
}


def build_activity_subjects(activities):
    """Build JEV input text and stable mapping-cache identities from activity records.

    Parameters
    ----------
    activities : pandas.DataFrame
        ``activity_id``, organization ``uid``, and the six fields in ``STATE_FIELDS``.
        Evidence, prominence, recurrence and extraction metadata are not scored.

    Returns
    -------
    pandas.DataFrame
        ``subject_id`` (the activity ID), ``state`` (six labelled fields), and
        ``cache_id`` (hash of organization UID and state). Cache identity is stable
        across extraction run labels and preserves previously cached JEV responses.
    """
    if missing := {"activity_id", "uid", *STATE_FIELDS} - set(activities):
        raise ValueError(f"JEV activity subjects require columns: {sorted(missing)}")
    rows = []
    for row in activities.to_dict("records"):
        state = "\n".join(
            f"{label}: {str(row[field]).strip()}" for field, label in STATE_FIELDS.items()
        )
        rows.append({
            "subject_id": row["activity_id"],
            "state": state,
            "cache_id": create_deterministic_sha256([str(row["uid"]), state]),
        })
    return pd.DataFrame(rows, columns=["subject_id", "state", "cache_id"])


# Which activities' matches make up an organization's result: "union" takes every
# activity's matches; "core_first" takes the core activities' matches, falling back to
# core and secondary when no core activity matched, and to all activities after that.
ROLLUPS = ("union", "core_first")
PROMINENCE_ORDER = ("core", "secondary", "minor")

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
        One row per (organization, path) from ``_rollup_paths`` plus one no-match row
        per unmatched organization, with the caller's ID and name columns, any listed
        node attributes, ``deepest_match``,
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
        Mapping lineage plus ``export_columns``, ``rollup``, ``max_funding_level`` and
        ``status_counts``.
    """

    organizations: pd.DataFrame
    paths: pd.DataFrame
    activity_paths: pd.DataFrame
    scores: pd.DataFrame
    usage: pd.DataFrame
    funding: pd.DataFrame
    lineage: dict


def _core_first_paths(activity_paths, activities):
    """Keep each organization's matches from its most prominent activities that matched.

    The Drawdown prototype's adopted rule (drawdown ``jev/DECISIONS.md``, 2026-10-26): an
    organization's result uses its core activities' matches; when no core activity
    matched anything, core and secondary; when that still matched nothing, every
    activity. The dropped activities' matches stay in the per-activity results.

    Parameters
    ----------
    activity_paths : pandas.DataFrame
        ``select_matches`` output keyed by ``activity_id``, one row per matched node.
    activities : pandas.DataFrame
        ``ExtractionResult.activities`` with ``activity_id``, ``uid`` and ``prominence``
        (``core``, ``secondary`` or ``minor``).

    Returns
    -------
    pandas.DataFrame
        The kept rows of ``activity_paths``, unchanged.
    """
    info = activities.set_index("activity_id")
    rank = activity_paths.activity_id.map(info.prominence).map(
        {p: i for i, p in enumerate(PROMINENCE_ORDER)}
    )
    best = rank.groupby(activity_paths.activity_id.map(info.uid)).transform("min")
    return activity_paths[rank <= best]


def _rollup_paths(activity_paths, activities, taxonomy):
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


def export_extraction(
    organizations,
    extraction,
    taxonomy,
    mapped,
    *,
    id_col,
    name_col,
    mapping_name,
    format_paths=None,
    attribute_columns=None,
    pad_levels=None,
    funding_level=None,
    rollup="union",
):
    """Roll activity matches up to organizations and attach tracker-shaped columns.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization with ``id_col`` unique and non-null. Returned with IDs
        as strings and columns added, never rows removed.
    extraction : ExtractionResult
        The extraction whose ``activities`` were mapped. Its organization cohort must
        equal ``organizations[id_col]``; its ``extraction_status`` feeds the mapping
        status of organizations without matches.
    taxonomy : Taxonomy
        The taxonomy the activities were scored against.
    mapped : SubjectMappingResult
        ``map_subjects`` output for ``extraction.activities`` with ``activity_id``
        renamed to ``subject_id``.
    id_col : str
        Organization ID column, used in ``organizations`` and written to the result rows
        and funding.
    name_col : str
        Column the result rows and funding carry the extracted organization name in.
    mapping_name : str
        Suffix of the legacy mapping columns, e.g. ``one_earth_category``.
    format_paths : callable, optional
        ``format_paths(paths) -> paths`` applied to the result rows before statuses are
        attached, for a tracker's display conventions.
    attribute_columns : dict[str, str], optional
        ``node attribute -> output column`` for attributes the loader kept on the
        deepest level's nodes (e.g. Drawdown solution properties). Each output column
        lists the attribute per organization, aligned with
        ``all_level{deepest}_<mapping_name>``; each result row gets the attribute
        under its own name.
    pad_levels : int, optional
        Emit ``level{i}`` and ``all_level{i}`` columns up to this many levels even
        when the taxonomy is shallower. None emits the taxonomy's own levels.
    funding_level : int, optional
        Deepest level funding is allocated to. None allocates to the taxonomy's
        deepest level.
    rollup : str
        One of ``ROLLUPS``. ``"union"`` rolls up every activity's matches;
        ``"core_first"`` applies ``_core_first_paths`` first. The result rows, statuses
        and funding follow the kept activities; ``activity_paths`` keeps every match.

    Returns
    -------
    MappingResult
    """
    # Imported here: the legacy helpers' module pulls in the SQL prompt cache, which reads
    # the database configuration at import time; offline callers need none of that.
    from ..hierarchical_taxonomy_mapping import (
        attach_mapping_columns,
        distribute_funding_from_matches,
    )

    ids = organizations[id_col].astype(str)
    if organizations[id_col].isna().any() or ids.duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    if set(ids) != set(extraction.organizations.uid):
        raise ValueError("Extraction and mapping organization cohorts differ")
    if rollup not in ROLLUPS:
        raise ValueError(f"Unknown rollup {rollup!r}; expected one of {ROLLUPS}")
    activity_paths = mapped.subject_paths.rename(columns={"subject_id": "activity_id"})
    kept = activity_paths if rollup == "union" else _core_first_paths(activity_paths, extraction.activities)
    union = _rollup_paths(kept, extraction.activities, taxonomy)
    paths = union.rename(columns={"uid": id_col})
    names = extraction.organizations.set_index("uid")["name"]
    paths[name_col] = paths[id_col].map(names)
    # One no-match row per org is required by the legacy attach helpers.
    matched = set(union.uid)
    missing_rows = [
        {
            id_col: uid,
            name_col: names[uid],
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
    paths["mapping_status"] = paths[id_col].map(statuses)

    deepest = len(taxonomy.columns) - 1
    funding_level = deepest if funding_level is None else funding_level
    out = organizations.assign(**{id_col: ids})
    out = attach_mapping_columns(
        out, paths, taxonomy.levels, id_col, mapping_name=mapping_name, pad_to_n_levels=pad_levels
    )
    out[f"{taxonomy.name}_mapping_status"] = out[id_col].map(statuses)
    funding = distribute_funding_from_matches(
        paths, taxonomy.levels, id_col, name_col, max_level=funding_level
    )
    export_columns = [
        *[
            f"{prefix}level{i}_{mapping_name}"
            for i in range(max(pad_levels or 0, deepest + 1))
            for prefix in ("", "all_")
        ],
        mapping_name,
        f"cat_level_{mapping_name}",
        f"{taxonomy.name}_mapping_status",
    ]
    leaves = taxonomy.nodes[taxonomy.nodes.depth == deepest].set_index(taxonomy.columns[-1])
    for attribute, column in (attribute_columns or {}).items():
        lookup = leaves[attribute].to_dict()
        out[column] = out[f"all_level{deepest}_{mapping_name}"].map(
            lambda values, lookup=lookup: [lookup.get(v) for v in values]
        )
        paths[attribute] = paths[taxonomy.columns[-1]].map(lookup)
        export_columns.append(column)
    lineage = {
        **mapped.lineage,
        "export_columns": export_columns,
        "rollup": rollup,
        "max_funding_level": funding_level,
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


# Extraction outcomes copied onto the mapped organizations, prefixed ``activity_``.
EXTRACTION_COLUMNS = ("extraction_status", "source_sha256", "n_activities", "extraction_run_id")


@dataclass(frozen=True)
class MappingJob:
    """One taxonomy to map and the shape of its tracker export.

    Attributes
    ----------
    taxonomy : Taxonomy
        Loaded by the taxonomy's own package, e.g. ``one_earth.load_taxonomy(path)``.
    policy : MatchPolicy
        Match selection, e.g. the package's ``DEFAULT_POLICY``.
    mapping_name : str
        Suffix of the exported mapping columns, e.g. ``one_earth_category``.
    format_paths, attribute_columns, pad_levels, funding_level, rollup
        Passed to ``export_extraction``; see there. All optional; ``rollup`` defaults to
        ``"union"``, and each taxonomy package names its own as ``DEFAULT_ROLLUP``.
    """

    taxonomy: Taxonomy
    policy: MatchPolicy
    mapping_name: str
    format_paths: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    attribute_columns: dict[str, str] | None = None
    pad_levels: int | None = None
    funding_level: int | None = None
    rollup: str = "union"


@dataclass
class ActivityMappingRun:
    """What ``run_activity_mapping`` produced and wrote.

    Attributes
    ----------
    organizations : pandas.DataFrame
        The input organizations plus every job's export columns and the
        ``activity_<extraction column>`` columns; written as the ``organizations`` file.
    extraction : ExtractionResult
        The single extraction every job mapped.
    results : dict[str, MappingResult]
        Job name -> its export (result rows, funding, scores, request statuses,
        per-activity paths and lineage).
    lineage : dict
        Footer of the ``organizations`` file: ``run_id``, ``n_orgs``, ``columns``,
        ``input``, ``extraction`` and ``taxonomies`` (job name -> mapping lineage).
    """

    organizations: pd.DataFrame
    extraction: ExtractionResult
    results: dict[str, MappingResult]
    lineage: dict


def _attach(target, source, columns, id_col):
    """Copy ``columns`` from ``source`` onto ``target`` by organization ID."""
    source = source.set_index(source[id_col].astype(str))
    for column in columns:
        target[column] = target[id_col].astype(str).map(source[column])


def run_activity_mapping(
    organizations,
    jobs,
    *,
    run_id,
    directory,
    id_col,
    name_col,
    url_col=None,
    website_texts=None,
    file_names=None,
    max_workers=20,
    input_lineage=None,
):
    """Extract activities once, map them onto every job's taxonomy and write the run.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization with ``id_col`` (unique, non-null), ``name_col`` and,
        unless ``website_texts`` is given, ``url_col``. Every other column is carried
        through untouched.
    jobs : dict[str, MappingJob]
        Job name (e.g. ``one_earth``) -> taxonomy and export settings, mapped in order.
    run_id : str
        Label of the run; becomes the extraction run ID. Reruns with unchanged inputs
        reuse the cached extraction and Jev responses.
    directory : str
        Local directory or ``s3://`` prefix the run is written to.
    id_col, name_col : str
        The caller's organization ID and name columns.
    url_col : str, optional
        The caller's website column; required when ``website_texts`` is None.
    website_texts : dict, optional
        ``organization ID -> text`` already loaded; None reads each website through the
        shared scraper.
    file_names : dict[str, str], optional
        Overrides for the file names of ``<job>_paths``, ``<job>_funding`` and
        ``organizations``, which default to ``<key>.parquet``. Scores, request statuses
        and per-activity paths are always ``<job>_scores``, ``<job>_usage`` and
        ``<job>_activity_paths``.
    max_workers : int
        Parallel extraction and gateway calls.
    input_lineage : dict, optional
        Provenance of ``organizations``, recorded in the ``organizations`` file.

    Returns
    -------
    ActivityMappingRun
        Every file it names is already written. A taxonomy whose mapping raises stops
        the run; the files of taxonomies already finished stay on disk, and the
        ``organizations`` file is not written.
    """
    started = perf_counter()
    directory = str(directory).rstrip("/")
    names = {
        "organizations": "organizations.parquet",
        **{f"{name}_{table}": f"{name}_{table}.parquet" for name in jobs for table in ("paths", "funding")},
        **(file_names or {}),
    }
    logger.info("Run %s: %d organizations, taxonomies=%s, workers=%d, output=%s",
                run_id, len(organizations), ", ".join(jobs), max_workers, directory)
    extraction = extract_frame(
        organizations,
        run_id=run_id,
        id_col=id_col,
        name_col=name_col,
        url_col=url_col,
        website_texts=website_texts,
        max_workers=max_workers,
    )
    subjects = build_activity_subjects(extraction.activities)
    mapped, results = organizations.copy(), {}
    for name, job in jobs.items():
        job_started = perf_counter()
        logger.info("[%s] Mapping %d activities against %d nodes", name, len(subjects), len(job.taxonomy.nodes))
        scored = map_subjects(subjects, job.taxonomy, policy=job.policy, max_workers=max_workers)
        logger.info("[%s] Scoring finished: %d score rows, %d failed requests; exporting matches",
                    name, len(scored.scores), int(scored.usage.error.notna().sum()))
        result = export_extraction(
            organizations,
            extraction,
            job.taxonomy,
            scored,
            id_col=id_col,
            name_col=name_col,
            mapping_name=job.mapping_name,
            format_paths=job.format_paths,
            attribute_columns=job.attribute_columns,
            pad_levels=job.pad_levels,
            funding_level=job.funding_level,
            rollup=job.rollup,
        )
        write_dataframes(
            {
                f"{name}_scores": result.scores,
                f"{name}_usage": result.usage,
                f"{name}_activity_paths": result.activity_paths,
            },
            directory,
            lineage=result.lineage,
        )
        for table, frame in (("paths", result.paths), ("funding", result.funding)):
            uri = f"{directory}/{names[f'{name}_{table}']}"
            logger.info("[%s] Writing %d rows to %s", name, len(frame), uri)
            write_dataframe(frame, uri, lineage=result.lineage)
        _attach(mapped, result.organizations, result.lineage["export_columns"], id_col)
        results[name] = result
        logger.info("[%s] Mapping complete in %.1fs: %d organization paths, %d funding rows",
                    name, perf_counter() - job_started, len(result.paths), len(result.funding))
    outcomes = extraction.organizations.set_index("uid")
    for column in EXTRACTION_COLUMNS:
        mapped[f"activity_{column}"] = mapped[id_col].astype(str).map(outcomes[column])
    lineage = {
        "run_id": run_id,
        "n_orgs": len(organizations),
        "columns": {"id": id_col, "name": name_col, "url": url_col},
        "input": input_lineage,
        "extraction": extraction.lineage,
        "taxonomies": {name: result.lineage for name, result in results.items()},
    }
    uri = f"{directory}/{names['organizations']}"
    logger.info("Run %s: writing %d mapped organizations to %s", run_id, len(mapped), uri)
    write_dataframe(mapped, uri, lineage=lineage)
    logger.info("Run %s complete in %.1fs", run_id, perf_counter() - started)
    return ActivityMappingRun(mapped, extraction, results, lineage)


def assemble_mapping_runs(frame, runs, *, id_col):
    """Copy one taxonomy's export columns from each selected run onto ``frame`` by ID.

    Parameters
    ----------
    frame : pandas.DataFrame
        Base organizations. Columns it already carries for a taxonomy not in ``runs``
        (for example an approved legacy output) are left untouched.
    runs : dict[str, str]
        Job name -> the ``organizations`` file of the run to take it from.
    id_col : str
        Organization ID column in ``frame`` and in the runs.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
        A run covers a different set of organization IDs than ``frame``.
    """
    out = frame.copy()
    for name, uri in runs.items():
        columns = get_lineage(uri)["taxonomies"][name]["export_columns"]
        mapped = read_dataframe(uri)
        if set(mapped[id_col].astype(str)) != set(frame[id_col].astype(str)):
            raise ValueError(f"{name} run {uri} covers a different organization cohort")
        _attach(out, mapped, columns, id_col)
    return out
