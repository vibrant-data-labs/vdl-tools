"""Finalize the ID Mapping File + manual source-id entry.

``set-id``: a VDL employee who finds a Crunchbase/NZI/Coresignal id out of
process records it as a decision (gate ``manual_id``) — decisions are data,
so the id survives every rebuild. Ids are live-verified against the source
API by default; on an unresolved row a verified id also becomes the primary
match (a human found the org — that IS the resolution).

``finalize``: validates and emits the canonical ID Mapping File artifact
(``id_mapping_final.parquet`` + ``.csv``, content-hashed) that the Phase-2
enrichment pipeline consumes. Re-runnable by design: import customer
responses or record new decisions, rerun match, finalize again.
"""

import hashlib
import re
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.matching.queue import (
    load_id_mapping,
    match_lock_active,
    record_decision,
    replay_decisions,
    save_id_mapping,
)
from vdl_tools.portfolio_comparison.state import PipelineState

# Zein's contract columns first (2026-08-07), context columns after.
FINAL_COLUMNS = [
    "customer_row_id",
    "customer_ein",
    "customer_name",
    "customer_url",
    "customer_description",
    "cb_id",
    "nzi_id",
    "coresignal_id",
    "entity_type",
    "disposition",
    "status",
    "in_universe",
    "out_of_universe_reason",
    "matched_source",
    "matched_id",
    "matched_name",
    "matched_url",
    "linkedin_url",
    "text_sources",
    "enrichment_ready",
    "match_method",
    "confidence",
    "decided_by",
    "decided_at",
    "notes",
]

ID_SHAPES = {
    "cb_id": re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
                        r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
    "nzi_id": re.compile(r"\d+"),
    "coresignal_id": re.compile(r"\d+"),
}

FINAL_BASENAME = "id_mapping_final"


def find_row(id_mapping: pd.DataFrame, row_id: str | None = None,
             name: str | None = None) -> pd.Series:
    """Locate exactly one row by id or by (case-insensitive, then unique
    substring) customer name. Ambiguity is an error listing the matches."""
    if row_id:
        hit = id_mapping[id_mapping["customer_row_id"] == row_id]
        if hit.empty:
            raise KeyError(f"no row with customer_row_id={row_id!r}")
        return hit.iloc[0]
    if not name:
        raise ValueError("pass --row <customer_row_id> or --name <customer name>")
    names = id_mapping["customer_name"].astype(str)
    hit = id_mapping[names.str.lower() == name.strip().lower()]
    if hit.empty:
        hit = id_mapping[names.str.contains(name.strip(), case=False, regex=False)]
    if len(hit) != 1:
        shown = ", ".join(hit["customer_name"].head(8)) or "none"
        raise ValueError(
            f"--name {name!r} matches {len(hit)} rows ({shown}) — "
            "use --row <customer_row_id> from id_mapping_review.xlsx"
        )
    return hit.iloc[0]


def verify_cb_id(cb_id: str) -> str:
    """Fetch the org from CB; returns 'Name (website)'. Raises if not found."""
    import vdl_tools.scrape_enrich.crunchbase.api as api
    from vdl_tools.portfolio_comparison.matching.source_adapter import CrunchbaseClient

    hits = CrunchbaseClient()._query([api.includes("uuid", [cb_id])])
    if not hits:
        raise ValueError(f"Crunchbase has no organization with uuid {cb_id}")
    hit = hits[0]
    name = (hit.get("identifier") or {}).get("value") or "?"
    return f"{name} ({hit.get('website_url') or 'no website'})"


def verify_nzi_id(nzi_id: str) -> str:
    """Fetch the org from NZI; returns 'Name (website)'. Raises if not found."""
    from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import (
        get_full_details_from_company_ids,
    )

    details = get_full_details_from_company_ids(
        [int(nzi_id)], return_investor_details=False, return_funding_rounds=False,
    )
    if details is None or len(details) == 0:
        raise ValueError(f"NZI has no company with id {nzi_id}")
    rec = details[0] if isinstance(details, list) else details.iloc[0].to_dict()
    name = rec.get("name") or rec.get("clientName") or "?"
    return f"{name} ({rec.get('website') or rec.get('domain') or 'no website'})"


VERIFIERS = {"cb_id": verify_cb_id, "nzi_id": verify_nzi_id}


def set_manual_id(
    engagement_root: str | Path,
    decided_by: str,
    row_id: str | None = None,
    name: str | None = None,
    cb_id: str | None = None,
    nzi_id: str | None = None,
    coresignal_id: str | None = None,
    note: str = "",
    verify: bool = True,
    verifiers: dict | None = None,
) -> pd.DataFrame:
    """Record an out-of-process source id as a replay-safe decision."""
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    if match_lock_active(results_dir):
        raise RuntimeError("a match run is in progress — try again when it finishes")

    ids = {k: v for k, v in
           {"cb_id": cb_id, "nzi_id": nzi_id, "coresignal_id": coresignal_id}.items()
           if v}
    if not ids:
        raise ValueError("pass at least one of --cb-id / --nzi-id / --coresignal-id")
    for col, value in ids.items():
        if not ID_SHAPES[col].fullmatch(str(value).strip()):
            raise ValueError(
                f"{col}={value!r} doesn't look like a valid id "
                f"(expected {ID_SHAPES[col].pattern})"
            )

    id_mapping = load_id_mapping(results_dir)
    row = find_row(id_mapping, row_id=row_id, name=name)

    verified = {}
    if verify:
        for col, value in ids.items():
            fn = (verifiers or VERIFIERS).get(col)
            if fn is None:
                continue
            verified[col] = fn(str(value).strip())
            logger.info("verified %s=%s -> %s", col, value, verified[col])

    reason = note or "manual source id"
    if verified:
        reason += " | verified: " + "; ".join(
            f"{col}={desc}" for col, desc in verified.items()
        )

    fields = {col: str(v).strip() for col, v in ids.items()}
    # An unresolved row plus a human-found id IS the resolution: promote the
    # id to the primary match. Rows that already carry a match only gain the
    # supplementary column — the primary is never silently replaced.
    unresolved = pd.isna(row["matched_id"]) or str(row["matched_id"]) == ""
    if unresolved and len(ids) == 1:
        (col, value), = ids.items()
        desc = verified.get(col, "")
        m = re.match(r"(.*) \((.*)\)$", desc)
        fields.update(
            matched_id=str(value).strip(),
            matched_name=m.group(1) if m else row["customer_name"],
            matched_url=(m.group(2) if m and m.group(2) != "no website" else pd.NA),
            match_method="manual",
            confidence=0.99,
            in_universe=False,
        )
        status = "vdl_reviewed"
    else:
        status = row["status"] if pd.notna(row["status"]) else pd.NA

    id_mapping = record_decision(
        id_mapping, results_dir, row["customer_row_id"],
        decided_by=decided_by, status=status, reason=reason,
        gate="manual_id", **fields,
    )
    save_id_mapping(id_mapping, results_dir)
    logger.info(
        "set-id: %s (%s) <- %s", row["customer_name"], row["customer_row_id"],
        ", ".join(f"{k}={v}" for k, v in ids.items()),
    )
    return id_mapping


def run_finalize(engagement_root: str | Path) -> Path:
    """Validate and emit the canonical ID Mapping File artifact."""
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    if match_lock_active(results_dir):
        raise RuntimeError("a match run is in progress — finalize when it finishes")

    id_mapping = load_id_mapping(results_dir)
    id_mapping = replay_decisions(id_mapping, results_dir)

    pending = id_mapping[id_mapping["status"] == "needs_review"]
    if len(pending):
        names = ", ".join(pending["customer_name"].astype(str).head(10))
        raise RuntimeError(
            f"{len(pending)} rows still await VDL review ({names}) — "
            "finish the review or record decisions before finalizing"
        )

    problems = []
    for col, shape in ID_SHAPES.items():
        vals = id_mapping[col].dropna().astype(str)
        bad = vals[~vals.str.fullmatch(shape.pattern)]
        if len(bad):
            problems.append(f"{col}: malformed values {sorted(set(bad))[:5]}")
    if problems:
        raise ValueError("finalize blocked — " + "; ".join(problems))

    for col in ("cb_id", "nzi_id", "matched_id"):
        dupes = id_mapping[id_mapping[col].notna() & id_mapping[col].duplicated(keep=False)]
        if len(dupes):
            for value, group in dupes.groupby(col):
                logger.warning(
                    "finalize: %s=%s appears on %d rows (%s) — same org in the "
                    "portfolio more than once?", col, value, len(group),
                    ", ".join(group["customer_name"].astype(str)),
                )

    final = id_mapping[[c for c in FINAL_COLUMNS if c in id_mapping.columns]]
    out = Path(results_dir) / f"{FINAL_BASENAME}.parquet"
    final.to_parquet(out, index=False)
    final.to_csv(Path(results_dir) / f"{FINAL_BASENAME}.csv", index=False)
    digest = hashlib.sha256(out.read_bytes()).hexdigest()

    state = PipelineState(config.root)
    state.record_artifact("id_mapping_final", out, sha256=digest)
    state.record_stage(
        "finalize", status="finalized",
        n_rows=len(final),
        n_matched=int(final["matched_id"].notna().sum()),
        n_enrichment_ready=int(final["enrichment_ready"].fillna(False).astype(bool).sum()),
        sha256=digest,
    )
    logger.info(
        "finalized ID Mapping File: %d rows (%d matched, %d enrichment-ready) "
        "-> %s (sha256 %s)",
        len(final), int(final["matched_id"].notna().sum()),
        int(final["enrichment_ready"].fillna(False).astype(bool).sum()),
        out, digest[:12],
    )
    return out
