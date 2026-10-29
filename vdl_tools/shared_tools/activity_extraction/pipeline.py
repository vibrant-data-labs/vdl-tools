"""Accounted, website-only extraction for callers outside the One Earth project."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..parquet_cache import get_lineage, read_dataframes, write_dataframes
from ..tools.unique_ids import create_deterministic_sha256
from .extraction import OrgInput, build_inputs, render_profile, run_extraction_prompt
from .general import GENERAL_EXTRACTION_PROMPT

STATE_FIELDS = {
    "activity": "Activity",
    "what_happens": "What happens",
    "who_carries_it_out": "Who carries it out",
    "aim": "Aim",
    "sector": "Sector",
    "methods": "Methods",
}


def activity_state(row) -> str:
    """The six activity fields used in both source repos; no summary or importance."""
    return "\n".join(f"{label}: {str(row[field]).strip()}" for field, label in STATE_FIELDS.items())


SNAPSHOT_TABLES = ("activity_extraction_orgs", "activity_extraction_activities")


@dataclass
class ExtractionResult:
    """One extraction run: a row per organization, a row per activity, and the lineage.

    Attributes
    ----------
    organizations : pandas.DataFrame
        One row per input organization: ``uid``, ``Organization``, ``Website``,
        ``extraction_run_id``, ``source_sha256``, ``text_source``, ``text_chars``,
        ``Description``, ``summary``, ``coverage``, ``n_activities``,
        ``extraction_status`` (``success``, ``empty``, ``missing_text`` or
        ``request_failed``) and the prompt's organization-level fields.
    activities : pandas.DataFrame
        One row per extracted activity: ``activity_id``, ``uid``, ``Organization``,
        ``activity_index``, ``extraction_run_id``, ``source_sha256``, every activity
        schema field, ``state`` (the six fields Jev judges) and ``cache_id``.
    lineage : dict
        Run, prompt, schema and model identity plus ``status_counts``.
    """

    organizations: pd.DataFrame
    activities: pd.DataFrame
    lineage: dict

    def save(self, directory):
        """Write both tables as Parquet into ``directory`` with the lineage in their footers.

        Raises
        ------
        FileExistsError
            A snapshot is already there; snapshots are immutable.
        """
        directory = Path(directory)
        if any((directory / f"{name}.parquet").exists() for name in SNAPSHOT_TABLES):
            raise FileExistsError(f"Extraction snapshot already exists: {directory}")
        write_dataframes(
            dict(zip(SNAPSHOT_TABLES, (self.organizations, self.activities), strict=True)),
            directory,
            lineage=self.lineage,
        )

    @classmethod
    def load(cls, directory):
        """Read a snapshot written by ``save``; needs no model, website cache or taxonomy."""
        tables = read_dataframes(directory, list(SNAPSHOT_TABLES))
        lineage = get_lineage(Path(directory) / f"{SNAPSHOT_TABLES[0]}.parquet")
        lineage.pop("table_name", None)
        return cls(*(tables[name] for name in SNAPSHOT_TABLES), lineage)


def extract_frame(
    organizations: pd.DataFrame,
    *,
    run_id: str,
    id_col: str = "id",
    name_col: str = "Organization",
    url_col: str = "Website",
    site_texts: dict | None = None,
    prompt=GENERAL_EXTRACTION_PROMPT,
    runner=None,
    session=None,
    max_workers: int = 16,
) -> ExtractionResult:
    """Extract once per org, preserving all orgs and recording every missing result.

    Parameters
    ----------
    organizations : pandas.DataFrame
        One row per organization with ``id_col`` (unique and non-null after string
        normalization), ``name_col`` and ``url_col``.
    run_id : str
        Label of this extraction run; mixed into every ``activity_id``.
    id_col, name_col, url_col : str
        Column names in ``organizations``.
    site_texts : dict, optional
        ``url -> SiteText`` already-loaded pages, as the cached website loader
        returns; when None the SQL website cache is read. No descriptions, Form 990
        programs or repo-local overlays are ever used as a fallback.
    prompt : ExtractionPrompt
        What to extract; defaults to the general prompt.
    runner : callable, optional
        ``runner(given_ids_texts, prompt, max_workers=..., session=...) ->
        {cache key: profile}`` replaces `run_extraction_prompt` for offline fixtures.
        A profile may be a ``prompt.response_model`` instance or a dict; both are
        validated against the schema.
    session : sqlalchemy.orm.Session, optional
        Passed to the website loader and the runner.
    max_workers : int
        Passed to the runner.

    Returns
    -------
    ExtractionResult
        ``organizations`` has one row per input with ``extraction_status`` of
        ``success``, ``empty``, ``missing_text`` or ``request_failed``; ``activities``
        has one row per extracted activity (``activity_id``, ``uid``, the schema
        fields, ``state``, ``cache_id``, ``source_sha256``, ``extraction_run_id``);
        ``lineage`` records prompt, schema and model identity and status counts.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a nonempty string")
    required = {id_col, name_col, url_col}
    if missing := required - set(organizations):
        raise ValueError(f"Missing organization columns: {sorted(missing)}")
    if organizations[id_col].isna().any():
        raise ValueError("Organization IDs cannot be null")
    ids = organizations[id_col].astype(str)
    if ids.duplicated().any():
        raise ValueError("Organization IDs must be unique after string normalization")
    orgs = [
        OrgInput(
            uid=str(row[id_col]),
            name=str(row[name_col]),
            url=str(row[url_col]).strip() if pd.notna(row[url_col]) and row[url_col] else None,
        )
        for row in organizations.to_dict("records")
    ]
    if orgs:
        inputs, meta = build_inputs(orgs, prompt, session=session, site_texts=site_texts)
    else:
        inputs, meta = [], {}
    texts = dict(inputs)
    profiles = (
        (runner or run_extraction_prompt)(inputs, prompt, max_workers=max_workers, session=session)
        if inputs
        else {}
    )
    org_rows, activity_rows = [], []
    for org in orgs:
        profile = profiles.get(org.uid)
        source_hash = create_deterministic_sha256(texts[org.uid]) if org.uid in texts else None
        status = "missing_text" if org.uid not in texts else "request_failed"
        record = {
            "uid": org.uid,
            "Organization": org.name,
            "Website": org.url,
            "extraction_run_id": run_id,
            "source_sha256": source_hash,
            "text_source": meta.get(org.uid, {}).get("text_source"),
            "text_chars": meta.get(org.uid, {}).get("text_chars", 0),
            "Description": None,
            "summary": None,
            "coverage": None,
            "n_activities": 0,
            "extraction_status": status,
        }
        if profile is not None:
            # Validate injected/cached output against the same pinned response schema.
            profile = prompt.response_model.model_validate(profile)
            record.update(
                Description=render_profile(profile, prompt),
                summary=profile.summary,
                coverage=profile.coverage,
                n_activities=len(profile.activities),
                extraction_status="success" if profile.activities else "empty",
                **{field: getattr(profile, field) for field in prompt.org_fields()},
            )
            for index, activity in enumerate(profile.activities, start=1):
                identity = create_deterministic_sha256([run_id, org.uid, index])
                row = {
                    "activity_id": identity,
                    "uid": org.uid,
                    "Organization": org.name,
                    "activity_index": index,
                    "extraction_run_id": run_id,
                    "source_sha256": source_hash,
                    **activity.model_dump(),
                }
                row["state"] = activity_state(row)
                # Stable across run IDs, while text hashes in the shared cache prevent
                # old answers being reused after source activities change or reorder.
                row["cache_id"] = create_deterministic_sha256([org.uid, row["state"]])
                activity_rows.append(row)
        org_rows.append(record)
    org_columns = [
        "uid",
        "Organization",
        "Website",
        "extraction_run_id",
        "source_sha256",
        "text_source",
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
        "Organization",
        "activity_index",
        "extraction_run_id",
        "source_sha256",
        *prompt.activity_model.model_fields,
        "state",
        "cache_id",
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
        "source_policy": "cached_website_only",
        "n_input_orgs": len(orgs),
        "n_activities": len(activities),
        "status_counts": org_frame.extraction_status.value_counts().to_dict(),
    }
    return ExtractionResult(org_frame, activities, lineage)
