"""Intake profiling: data dictionary, entity typing, disposition validation.

Produces the intake profile that a human confirms before matching runs — the
formalization of "make sure you understand everything the customer sent."
"""

import hashlib
import json
from pathlib import Path

import pandas as pd

from vdl_tools.portfolio_comparison.intake.normalize import (
    normalize_domain,
    normalize_ein,
    normalize_name,
)

# Header aliases → canonical column. Matching is case/punctuation-insensitive.
CANONICAL_HEADERS = {
    "name": {"name", "organization", "organization name", "org", "org name",
             "company", "company name", "portfolio company", "grantee"},
    "url": {"url", "website", "web site", "homepage", "domain", "site",
            "company website", "web address"},
    "ein": {"ein", "tax id", "taxid", "irs ein", "employer identification number"},
    "disposition": {"disposition", "status", "invested", "decision", "outcome",
                    "invested or passed", "invest decision"},
}

DISPOSITION_VALUE_MAP = {
    "invested": "invested", "invest": "invested", "yes": "invested",
    "portfolio": "invested", "active": "invested",
    "passed": "passed", "pass": "passed", "no": "passed", "declined": "passed",
}
# Per-engagement overrides may additionally map values to "exclude"
# (e.g. spreadsheet TOTAL rows) — excluded rows are counted, never silent.
DISPOSITION_TARGETS = {"invested", "passed", "exclude"}


def map_dispositions(series, overrides: dict[str, str] | None = None):
    """Normalize raw disposition values via the base map + engagement
    overrides (overrides win; the "" key covers blank cells)."""
    overrides = {k.lower(): v for k, v in (overrides or {}).items()}
    bad_targets = set(overrides.values()) - DISPOSITION_TARGETS
    if bad_targets:
        raise ValueError(
            f"disposition_value_overrides targets must be {sorted(DISPOSITION_TARGETS)}, "
            f"got {sorted(bad_targets)}"
        )
    mapping = {**DISPOSITION_VALUE_MAP, **overrides}
    raw = series.fillna("").astype(str).str.strip().str.lower()
    return raw.map(mapping)


import re as _re


def _norm_header(header: str) -> str:
    cleaned = _re.sub(r"[^\w\s]", " ", str(header).lower().replace("_", " "))
    return " ".join(cleaned.split())


def propose_column_mapping(columns) -> dict[str, str]:
    """Map each customer column to a canonical field or 'passthrough'.

    Two passes: exact alias match first, then a token-superset fallback for
    multi-token aliases only ("Organization / Project Name" ⊇ "organization
    name"). Single-token aliases never fall back — "name" would otherwise
    claim headers like "Fiscal Sponsor Name".
    """
    mapping = {col: "passthrough" for col in columns}
    claimed = set()
    for col in columns:
        normed = _norm_header(col)
        for canonical, aliases in CANONICAL_HEADERS.items():
            if normed in aliases and canonical not in claimed:
                mapping[col] = canonical
                claimed.add(canonical)
                break
    for col in columns:
        if mapping[col] != "passthrough":
            continue
        header_tokens = set(_norm_header(col).split())
        for canonical, aliases in CANONICAL_HEADERS.items():
            if canonical in claimed:
                continue
            if any(
                len(alias.split()) > 1 and set(alias.split()) <= header_tokens
                for alias in aliases
            ):
                mapping[col] = canonical
                claimed.add(canonical)
                break
    return mapping


def make_row_id(file_label: str, name: str, url: str, index: int) -> str:
    """Stable per-row key back to the customer's file."""
    basis = f"{file_label}|{normalize_name(name)}|{normalize_domain(url)}|{index}"
    return hashlib.sha1(basis.encode()).hexdigest()[:16]


def _validate_disposition(series: pd.Series, overrides: dict | None = None) -> dict:
    raw = series.fillna("").astype(str).str.strip().str.lower()
    mapped = map_dispositions(series, overrides)
    blank_covered = "" in {k.lower() for k in (overrides or {})}
    n_blank = int((raw == "").sum())
    unrecognized = sorted(set(raw[(raw != "") & mapped.isna()]))
    result = {
        "present": True,
        "n_invested": int((mapped == "invested").sum()),
        "n_passed": int((mapped == "passed").sum()),
        "n_excluded": int((mapped == "exclude").sum()),
        "n_blank": n_blank,
        "unrecognized_values": unrecognized,
        "blocking": bool(unrecognized)
        or (0 < n_blank < len(series) and not blank_covered),
    }
    return result


def profile_file(
    df: pd.DataFrame,
    file_label: str,
    default_entity_type: str,
    column_mapping: dict[str, str] | None = None,
    disposition_overrides: dict[str, str] | None = None,
) -> dict:
    """Profile one customer file. ``default_entity_type`` comes from which
    input slot the file was provided in (companies vs nonprofits)."""
    df = df.rename(columns=str)
    mapping = column_mapping or propose_column_mapping(df.columns)
    inverse = {v: k for k, v in mapping.items() if v != "passthrough"}

    profile = {
        "file": file_label,
        "n_rows": len(df),
        "column_mapping": mapping,
        "coverage": {
            str(col): round(float(df[col].notna().mean()), 3) for col in df.columns
        },
        "default_entity_type": default_entity_type,
    }

    if "name" not in inverse:
        profile["blocking"] = f"no column maps to 'name' in {file_label}"
        return profile

    name_col = inverse["name"]
    url_col = inverse.get("url")
    ein_col = inverse.get("ein")

    normed_names = df[name_col].map(normalize_name)
    normed_domains = df[url_col].map(normalize_domain) if url_col else pd.Series("", index=df.index)
    dupe_mask = pd.Series(
        list(zip(normed_names, normed_domains)), index=df.index
    ).duplicated(keep=False) & (normed_names != "")
    profile["n_duplicate_rows"] = int(dupe_mask.sum())

    if ein_col is not None:
        eins = df[ein_col].map(normalize_ein)
        profile["n_valid_ein"] = int((eins != "").sum())
        entity_type = pd.Series(default_entity_type, index=df.index).where(eins == "", "nonprofit")
    else:
        entity_type = pd.Series(default_entity_type, index=df.index)
    profile["entity_type_counts"] = entity_type.value_counts().to_dict()

    if "disposition" in inverse:
        profile["disposition"] = _validate_disposition(
            df[inverse["disposition"]], disposition_overrides
        )
    else:
        profile["disposition"] = {"present": False, "note": "all rows default to invested"}

    return profile


def preflight(profiles: list[dict]) -> dict:
    """Volume & cost estimate, printed before anything runs."""
    n_rows = sum(p["n_rows"] for p in profiles)
    n_passed = sum(p["disposition"].get("n_passed", 0) for p in profiles)
    n_nonprofit = sum(
        p["entity_type_counts"].get("nonprofit", 0) for p in profiles if "entity_type_counts" in p
    )
    return {
        "n_rows_total": n_rows,
        "n_passed_rows": n_passed,
        "n_nonprofit_rows": n_nonprofit,
        # Tier 1 is local and free; worst case every row misses it.
        "max_source_api_calls": n_rows - n_nonprofit,
        "max_gt_queries": n_nonprofit,
        "note": "matching phase is cheap; real costs arrive with enrichment",
    }


def write_intake_profile(profiles: list[dict], results_dir: str | Path) -> Path:
    payload = {"files": profiles, "preflight": preflight(profiles)}
    out = Path(results_dir) / "intake_profile.json"
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return out
