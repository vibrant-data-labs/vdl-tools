"""Accounted, website-only activity extraction over an organization frame."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

import pandas as pd

from ..parquet_cache import get_lineage, read_dataframes, write_dataframes
from ..tools.logger import logger
from ..tools.unique_ids import create_deterministic_sha256
from .extraction import (
    OrgInput,
    build_inputs,
    load_website_texts,
    render_profile,
    run_extraction_prompt,
)
from .general import GENERAL_EXTRACTION_PROMPT

SNAPSHOT_TABLES = ("activity_extraction_orgs", "activity_extraction_activities")


@dataclass
class ExtractionResult:
    """One extraction run: a row per organization, a row per activity, and the lineage.

    Attributes
    ----------
    organizations : pandas.DataFrame
        One row per input organization: ``uid``, ``name``, ``url`` (the caller's
        ID, name and website values; ``url`` is None without a URL column),
        ``extraction_run_id``, ``source_sha256``, ``text_chars``, ``Description``, ``summary``, ``coverage``, ``n_activities``,
        ``extraction_status`` (``success``, ``empty``, ``missing_text`` or
        ``request_failed``) and the prompt's organization-level fields.
    activities : pandas.DataFrame
        One row per extracted activity: ``activity_id``, ``uid``, ``name``,
        ``activity_index``, ``extraction_run_id``, ``source_sha256``, every activity
        schema field. Mapping-specific input text and cache keys are built by consumers.
    lineage : dict
        Run, prompt, schema and model identity plus ``status_counts``.
    """

    organizations: pd.DataFrame
    activities: pd.DataFrame
    lineage: dict

    def save(self, directory):
        """Write both tables as Parquet into ``directory`` with the lineage in their footers."""
        write_dataframes(
            dict(zip(SNAPSHOT_TABLES, (self.organizations, self.activities), strict=True)),
            directory,
            lineage=self.lineage,
        )

    @classmethod
    def load(cls, directory):
        """Read a snapshot written by ``save``; needs no model, website cache or taxonomy."""
        tables = read_dataframes(directory, list(SNAPSHOT_TABLES))
        lineage = get_lineage(f"{str(directory).rstrip('/')}/{SNAPSHOT_TABLES[0]}.parquet")
        lineage.pop("table_name", None)
        return cls(*(tables[name] for name in SNAPSHOT_TABLES), lineage)


def extract_frame(
    organizations: pd.DataFrame,
    *,
    run_id: str,
    id_col: str,
    name_col: str,
    url_col: str | None = None,
    website_texts: dict[str, str] | None = None,
    prompt=GENERAL_EXTRACTION_PROMPT,
    runner=None,
    max_workers: int = 20,
) -> ExtractionResult:
    """Extract once per organization, keeping every organization and recording every outcome.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization, carrying the columns named below.
    run_id : str
        Label of this extraction run; mixed into every ``activity_id``.
    id_col : str
        Column with each organization's ID; unique and non-null. IDs are compared as
        strings.
    name_col : str
        Column with the organization's name, which heads its prompt text.
    url_col : str, optional
        Column with the organization's website. Required when ``website_texts`` is
        None, because the text is then read by URL; otherwise only recorded.
    website_texts : dict, optional
        ``organization ID -> text`` already loaded. None reads each organization's
        website through `load_website_texts`, which serves the SQL scrape cache through
        the shared scraper. No descriptions or Form 990 programs are ever used as a
        fallback.
    prompt : ExtractionPrompt
        What to extract; defaults to the general prompt.
    runner : callable, optional
        ``runner(given_ids_texts, prompt, max_workers=...) -> {cache key: profile}``
        replaces `run_extraction_prompt` for offline fixtures.
    max_workers : int
        Passed to the runner.

    Returns
    -------
    ExtractionResult
    """
    started = perf_counter()
    logger.info("Extracting activities for %d organizations (run=%s, workers=%d)", len(organizations), run_id, max_workers)
    if website_texts is None and url_col is None:
        raise ValueError("Pass url_col to read website text, or supply website_texts")
    columns = [c for c in (id_col, name_col, url_col) if c is not None]
    if missing := set(columns) - set(organizations):
        raise ValueError(f"Missing organization columns: {sorted(missing)}")
    ids = organizations[id_col].astype(str)
    if organizations[id_col].isna().any() or ids.duplicated().any():
        raise ValueError("Organization IDs must be non-null and unique")
    orgs = [
        OrgInput(
            uid=str(row[id_col]),
            name=str(row[name_col]),
            url=str(row[url_col]).strip() if url_col and pd.notna(row[url_col]) and row[url_col] else None,
        )
        for row in organizations.to_dict("records")
    ]
    if website_texts is None:
        by_url = load_website_texts([o.url for o in orgs if o.url])
        website_texts = {o.uid: by_url[o.url] for o in orgs if o.url in by_url}
    inputs, meta = build_inputs(orgs, prompt, website_texts)
    texts = dict(inputs)
    logger.info("Extracting %d usable organization profiles through %s", len(inputs), "the shared prompt cache" if runner is None else "the supplied runner")
    profiles = (runner or run_extraction_prompt)(inputs, prompt, max_workers=max_workers)
    org_rows, activity_rows = [], []
    for org in orgs:
        profile = profiles.get(org.uid)
        source_hash = create_deterministic_sha256(texts[org.uid]) if org.uid in texts else None
        record = {
            "uid": org.uid,
            "name": org.name,
            "url": org.url,
            "extraction_run_id": run_id,
            "source_sha256": source_hash,
            "text_chars": meta.get(org.uid, {}).get("text_chars", 0),
            "Description": None,
            "summary": None,
            "coverage": None,
            "n_activities": 0,
            "extraction_status": "missing_text" if org.uid not in texts else "request_failed",
        }
        if profile is not None:
            record.update(
                Description=render_profile(profile, prompt),
                summary=profile.summary,
                coverage=profile.coverage,
                n_activities=len(profile.activities),
                extraction_status="success" if profile.activities else "empty",
                **{field: getattr(profile, field) for field in prompt.org_fields()},
            )
            for index, activity in enumerate(profile.activities, start=1):
                row = {
                    "activity_id": create_deterministic_sha256([run_id, org.uid, index]),
                    "uid": org.uid,
                    "name": org.name,
                    "activity_index": index,
                    "extraction_run_id": run_id,
                    "source_sha256": source_hash,
                    **activity.model_dump(),
                }
                activity_rows.append(row)
        org_rows.append(record)
    org_columns = [
        "uid",
        "name",
        "url",
        "extraction_run_id",
        "source_sha256",
        "text_chars",
        "Description",
        "summary",
        "coverage",
        "n_activities",
        "extraction_status",
        *prompt.org_fields(),
    ]
    activity_columns = [
        "activity_id",
        "uid",
        "name",
        "activity_index",
        "extraction_run_id",
        "source_sha256",
        *prompt.activity_model.model_fields,
    ]
    org_frame = pd.DataFrame(org_rows, columns=org_columns)
    activities = pd.DataFrame(activity_rows, columns=activity_columns)
    lineage = {
        "extraction_run_id": run_id,
        "prompt_name": prompt.prompt_name,
        "prompt_sha256": create_deterministic_sha256(prompt.text),
        "schema_sha256": create_deterministic_sha256(prompt.response_model.model_json_schema()),
        "model": prompt.model,
        "reasoning": prompt.reasoning,
        "n_input_orgs": len(orgs),
        "n_activities": len(activities),
        "status_counts": org_frame.extraction_status.value_counts().to_dict(),
    }
    logger.info("Activity extraction complete in %.1fs: %d activities; statuses=%s", perf_counter() - started, len(activities), lineage["status_counts"])
    return ExtractionResult(org_frame, activities, lineage)
