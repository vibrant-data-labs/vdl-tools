"""Review queues and decision merge-back.

Humans review by exception: only rows without a confident auto-match reach a
queue. Every decision is appended to ``decisions.jsonl`` (the lineage record
and, over time, threshold-tuning data) and written back into the ID Mapping
File, which always holds current state.
"""

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from vdl_tools.portfolio_comparison.matching.source_adapter import Candidate
from vdl_tools.portfolio_comparison.schema import validate_id_mapping

DECISIONS_FILENAME = "decisions.jsonl"
ID_MAPPING_BASENAME = "id_mapping"


def load_id_mapping(results_dir: str | Path) -> pd.DataFrame:
    return pd.read_parquet(Path(results_dir) / f"{ID_MAPPING_BASENAME}.parquet")


def save_id_mapping(df: pd.DataFrame, results_dir: str | Path):
    results_dir = Path(results_dir)
    # Normalize missing values to a single representation (np.nan and pd.NA
    # both appear after .loc writes and read-backs, splitting value_counts).
    df = df.astype(object).where(pd.notna(df), pd.NA)
    df.to_parquet(results_dir / f"{ID_MAPPING_BASENAME}.parquet", index=False)
    df.to_csv(results_dir / f"{ID_MAPPING_BASENAME}.csv", index=False)


def replay_decisions(id_mapping: pd.DataFrame, results_dir: str | Path) -> pd.DataFrame:
    """Re-apply recorded human decisions onto a freshly rebuilt ID Mapping File.

    match reruns recompute every row from scratch; humans must never lose
    work to a rerun. Last decision per row wins. Decisions for rows that no
    longer exist (customer file changed) are skipped.
    """
    log_path = Path(results_dir) / DECISIONS_FILENAME
    if not log_path.exists():
        return id_mapping
    latest: dict[str, dict] = {}
    with open(log_path) as f:
        for line in f:
            entry = json.loads(line)
            latest[entry["customer_row_id"]] = entry
    # Legacy entries serialized missing values as strings; normalize both
    # them and proper nulls back to pd.NA.
    _null_strings = {"<NA>", "nan", "NaT", "None"}
    for row_id, entry in latest.items():
        mask = id_mapping["customer_row_id"] == row_id
        if not mask.any():
            continue
        for col, val in entry["after"].items():
            if col in id_mapping.columns:
                if val is None or (isinstance(val, str) and val in _null_strings):
                    val = pd.NA
                id_mapping.loc[mask, col] = val
    return id_mapping


def apply_decision(results_dir: str | Path, customer_row_id: str, **kwargs) -> pd.DataFrame:
    """Load → record one decision → persist. The review app's entry point."""
    lock = Path(results_dir) / ".match_running"
    if lock.exists():
        raise RuntimeError(
            "A match run is in progress — wait for it to finish before "
            "recording decisions (your work would race its save)."
        )
    df = load_id_mapping(results_dir)
    df = record_decision(df, results_dir, customer_row_id, **kwargs)
    save_id_mapping(df, results_dir)
    return df


RESEARCH_FILENAME = "research_annotations.json"


def build_review_queue(
    id_mapping: pd.DataFrame,
    candidates_by_row: dict[str, list[Candidate]],
    results_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Rows needing a human, with their candidate lists serialized alongside.

    Research annotations (agent pre-research verdicts + any candidates they
    discovered) persist in ``research_annotations.json`` and re-attach on
    every rebuild — queue regeneration must never eat research, same rule
    as decisions.
    """
    pending = id_mapping[id_mapping["status"] == "needs_review"].copy()
    pending["candidates"] = pending["customer_row_id"].map(
        lambda rid: [asdict(c) for c in candidates_by_row.get(rid, [])]
    )
    pending["research"] = None

    research_path = Path(results_dir) / RESEARCH_FILENAME if results_dir else None
    if research_path is not None and research_path.exists():
        annotations = json.loads(research_path.read_text())
        for idx, row in pending.iterrows():
            ann = annotations.get(row["customer_row_id"])
            if not ann:
                continue
            pending.at[idx, "research"] = ann.get("research")
            extra = ann.get("extra_candidates") or []
            seen = {c["matched_id"] for c in row["candidates"]}
            pending.at[idx, "candidates"] = row["candidates"] + [
                c for c in extra if c["matched_id"] not in seen
            ]
    return pending


def record_decision(
    id_mapping: pd.DataFrame,
    results_dir: str | Path,
    customer_row_id: str,
    decided_by: str,
    status: str,
    reason: str = "",
    gate: str = "match_review",
    **fields,
) -> pd.DataFrame:
    """Apply one human decision: update the row, append to the decisions log.

    ``fields`` may set matched_id/matched_name/matched_url/match_method/
    confidence/in_universe/out_of_universe_reason/notes.
    """
    mask = id_mapping["customer_row_id"] == customer_row_id
    if not mask.any():
        raise KeyError(f"no row with customer_row_id={customer_row_id!r}")

    before = id_mapping.loc[mask].iloc[0].to_dict()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    updates = {**fields, "status": status, "decided_by": decided_by, "decided_at": now}
    for col, val in updates.items():
        id_mapping.loc[mask, col] = val
    validate_id_mapping(id_mapping)

    def _jsonable(value):
        # pd.NA/nan must serialize as JSON null — default=str would write
        # the literal string "<NA>", which replay then applies as a real
        # status value (and Tier 2 skips the row as "already decided").
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return value

    entry = {
        "customer_row_id": customer_row_id,
        "gate": gate,
        "decided_by": decided_by,
        "decided_at": now,
        "reason": reason,
        "before": {k: _jsonable(before.get(k)) for k in updates},
        "after": {k: _jsonable(v) for k, v in updates.items()},
    }
    log_path = Path(results_dir) / DECISIONS_FILENAME
    with open(log_path, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")
    return id_mapping


def match_rate_report(id_mapping: pd.DataFrame) -> pd.DataFrame:
    """Match-rate summary by entity type and disposition — the Phase-1 exit report."""
    df = id_mapping.copy()
    df["matched"] = df["matched_id"].notna()
    grouped = (
        df.groupby(["entity_type", "disposition"], dropna=False)
        .agg(
            n_rows=("customer_row_id", "count"),
            n_matched=("matched", "sum"),
            n_in_universe=("in_universe", lambda s: int(s.eq(True).sum())),
            n_needs_review=("status", lambda s: int((s == "needs_review").sum())),
        )
        .reset_index()
    )
    grouped["match_rate"] = (grouped["n_matched"] / grouped["n_rows"]).round(3)
    return grouped
