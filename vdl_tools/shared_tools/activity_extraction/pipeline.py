"""Accounted, website-only extraction for callers outside the One Earth project."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pandas as pd

from .extraction import OrgInput, build_inputs, render_profile
from .general import GENERAL_EXTRACTION_PROMPT
from .structured_cache import run_structured

STATE_FIELDS = {
    "activity": "Activity",
    "what_happens": "What happens",
    "who_carries_it_out": "Who carries it out",
    "aim": "Aim",
    "sector": "Sector",
    "methods": "Methods",
}


def fingerprint(value) -> str:
    """Stable JSON fingerprint; also used for run and request provenance."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()


def activity_state(row) -> str:
    """The six activity fields used in both source repos; no summary or importance."""
    return "\n".join(f"{label}: {str(row[field]).strip()}" for field, label in STATE_FIELDS.items())


@dataclass
class ExtractionResult:
    organizations: pd.DataFrame
    activities: pd.DataFrame
    lineage: dict


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

    ``site_texts`` accepts the same SiteText objects as the cached website loader,
    so an enrichment caller can provide its already-loaded pages. No descriptions,
    Form 990 programs, or repo-local overlays are used as a fallback.
    ``runner`` is injectable for offline fixtures; production uses the SQL cache.
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
        (runner or run_structured)(
            inputs,
            prompt_str=prompt.text,
            prompt_name=prompt.prompt_name,
            response_model=prompt.response_model,
            model=prompt.model,
            reasoning=prompt.reasoning,
            max_workers=max_workers,
            session=session,
        )
        if inputs
        else {}
    )
    org_rows, activity_rows = [], []
    for org in orgs:
        profile = profiles.get(org.uid)
        source_hash = fingerprint(texts[org.uid]) if org.uid in texts else None
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
                identity = fingerprint([run_id, org.uid, index])
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
                row["cache_id"] = fingerprint([org.uid, row["state"]])
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
        "prompt_sha256": fingerprint(prompt.text),
        "schema_sha256": fingerprint(prompt.response_model.model_json_schema()),
        "model": prompt.model,
        "reasoning": prompt.reasoning,
        "source_policy": "cached_website_only",
        "n_input_orgs": len(orgs),
        "n_activities": len(activities),
        "status_counts": org_frame.extraction_status.value_counts().to_dict(),
    }
    return ExtractionResult(org_frame, activities, lineage)
