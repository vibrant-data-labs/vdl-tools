"""Extract website activities once, map them onto one or more taxonomies, and write the run.

``run_activity_mapping`` is what a project calls to classify its organizations: it runs the
shared extraction, maps every activity onto each ``MappingJob``'s taxonomy, rolls the matches
up to organizations, and writes every table through ``parquet_cache`` with its lineage in the
footer. Each taxonomy's files are written as soon as it finishes. ``assemble_mapping_runs``
copies selected taxonomies' columns from finished runs onto a base frame by ID.

The caller names its organization columns and owns every tracker convention (mapping column
names, display formatting, padding, funding depth, file names). Nothing here names a
taxonomy or a project.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter

import pandas as pd

from ...activity_extraction.pipeline import ExtractionResult, extract_frame
from ...parquet_cache import get_lineage, read_dataframe, write_dataframe, write_dataframes
from ...tools.logger import logger
from .engine import MatchPolicy, map_subjects
from .exports import MappingResult, export_extraction
from .subjects import build_activity_subjects
from .taxonomies import Taxonomy

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
    format_paths, attribute_columns, pad_levels, funding_level
        Passed to ``export_extraction``; see there. All optional.
    """

    taxonomy: Taxonomy
    policy: MatchPolicy
    mapping_name: str
    format_paths: Callable[[pd.DataFrame], pd.DataFrame] | None = None
    attribute_columns: dict[str, str] | None = None
    pad_levels: int | None = None
    funding_level: int | None = None


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
    extraction_runner=None,
    request_runner=None,
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
    extraction_runner, request_runner : callable, optional
        Offline replacements for the extraction model and the Jev gateway.
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
        runner=extraction_runner,
        max_workers=max_workers,
    )
    subjects = build_activity_subjects(extraction.activities)
    mapped, results = organizations.copy(), {}
    for name, job in jobs.items():
        job_started = perf_counter()
        logger.info("[%s] Mapping %d activities against %d nodes", name, len(subjects), len(job.taxonomy.nodes))
        scored = map_subjects(
            subjects, job.taxonomy, policy=job.policy, request_runner=request_runner, max_workers=max_workers
        )
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
