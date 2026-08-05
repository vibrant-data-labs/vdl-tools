"""Customer round-trip: annotated Excel export and re-import.

Export: every row no automated source could resolve (plus rows a VDL
reviewer marked ``customer_review``) becomes one spreadsheet row with our
best guess and a plain-English ask. Customers get spreadsheets, not apps.

Import: responses become recorded decisions (``decided_by=customer``,
gate ``customer_roundtrip``) — replay-safe like every other decision.
A customer-supplied EIN or website is an identity *signal*, so the import
re-resolves it (universe domain → source API domain → EIN) rather than
trusting free text as a match.
"""

from datetime import date
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.intake.normalize import normalize_domain, normalize_ein
from vdl_tools.portfolio_comparison.matching.queue import (
    load_id_mapping,
    record_decision,
    save_id_mapping,
)
from vdl_tools.portfolio_comparison.state import PipelineState

ID_COL = "ID (do not edit)"
RESPONSE_COLUMNS = [
    "Correct Website",
    "Correct Legal Name",
    "EIN (nonprofits)",
    "Your Notes",
]


def _ask(row) -> str:
    if row["status"] == "customer_review" and pd.notna(row["matched_name"]):
        return (
            f"Our best guess: {row['matched_name']} ({row['matched_url']}). "
            "Correct? If not, please fill in the columns to the right."
        )
    base = "We couldn't find this organization in our data sources. "
    if row["entity_type"] == "nonprofit":
        return base + "Please confirm its website and legal name, and its EIN if it has one (or its fiscal sponsor's, marked as such in Notes)."
    return base + "Please confirm its website and legal name."


def build_export_frame(id_mapping: pd.DataFrame) -> pd.DataFrame:
    rows = id_mapping[
        id_mapping["status"].isna() | (id_mapping["status"] == "customer_review")
    ]
    return pd.DataFrame({
        ID_COL: rows["customer_row_id"],
        "Organization": rows["customer_name"],
        "Website (as provided)": rows["customer_url"],
        "EIN (as provided)": rows["customer_ein"],
        "Type": rows["entity_type"].map({"for_profit": "Company", "nonprofit": "Nonprofit"}),
        "What we need": rows.apply(_ask, axis=1),
        **{col: "" for col in RESPONSE_COLUMNS},
    })


def export_customer_roundtrip(engagement_root: str | Path) -> Path:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    frame = build_export_frame(load_id_mapping(results_dir))

    out = results_dir / f"customer_review_{config.customer}_{date.today().isoformat()}.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="Organizations to confirm")
        ws = writer.sheets["Organizations to confirm"]
        ws.freeze_panes = "A2"
        widths = [18, 32, 34, 16, 12, 60, 30, 30, 16, 40]
        for i, width in enumerate(widths[: len(frame.columns)], start=1):
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = width

    state = PipelineState(config.root)
    state.record_artifact("customer_roundtrip_export", out)
    state.record_stage("review_customer", status="exported", n_rows=len(frame))
    logger.info("Customer round-trip export: %d rows -> %s", len(frame), out)
    return out


def _resolve_response(name, domain, ein, universe_domains, universe_ids, cb_client):
    """Turn customer-supplied signals into match fields, same standards as
    the automated tiers: domain/EIN evidence auto-accepts, names don't."""
    if ein:
        return {
            "status": "auto_matched", "matched_id": ein, "matched_name": name,
            "match_method": "customer_provided", "confidence": 0.99,
            "in_universe": ein in (universe_ids or set()),
        }
    if domain and universe_domains and domain in universe_domains:
        rec = universe_domains[domain]
        return {
            "status": "auto_matched", "matched_id": rec["id"],
            "matched_name": rec["name"], "matched_url": rec["url"],
            "match_method": "customer_provided", "confidence": 0.99,
            "in_universe": rec["in_universe"],
        }
    if domain and cb_client is not None:
        cands = cb_client.search(name or "", domain)
        sole_domain = len(cands) == 1 and cands[0].evidence.get("signal") == "domain"
        if sole_domain:
            top = cands[0]
            return {
                "status": "auto_matched", "matched_id": top.matched_id,
                "matched_name": top.matched_name, "matched_url": top.matched_url,
                "match_method": "customer_provided", "confidence": top.score,
                "in_universe": False,
            }
    if domain or name:
        # New signals but no confident resolution — back to the VDL queue.
        return {"status": "needs_review"}
    return {"status": "unmatched_final"}


def import_customer_responses(
    engagement_root: str | Path,
    response_file: str | Path,
    cb_client=None,
    universe_domains: dict | None = None,
    universe_ids: set | None = None,
) -> pd.DataFrame:
    """Merge a filled-in round-trip spreadsheet. Every responded row becomes
    a recorded decision; rows left blank become ``unmatched_final``."""
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    id_mapping = load_id_mapping(results_dir)
    responses = pd.read_excel(response_file).fillna("")

    if cb_client is None:
        try:
            from vdl_tools.portfolio_comparison.matching.source_adapter import (
                get_source_client,
            )

            cb_client = get_source_client(
                config.baseline_run.source,
                cache_path=results_dir / "source_search_cache.json",
            )
        except Exception as exc:
            logger.warning("source client unavailable for re-import: %s", exc)

    n_resolved = n_queued = n_final = 0
    for _, resp in responses.iterrows():
        row_id = str(resp.get(ID_COL, "")).strip()
        if not row_id or row_id not in set(id_mapping["customer_row_id"]):
            continue
        name = str(resp.get("Correct Legal Name", "")).strip()
        domain = normalize_domain(str(resp.get("Correct Website", "")).strip())
        ein = normalize_ein(resp.get("EIN (nonprofits)", ""))
        notes = str(resp.get("Your Notes", "")).strip()

        fields = _resolve_response(name, domain, ein, universe_domains, universe_ids, cb_client)
        status = fields.pop("status")
        n_resolved += status == "auto_matched"
        n_queued += status == "needs_review"
        n_final += status == "unmatched_final"
        id_mapping = record_decision(
            id_mapping, results_dir, row_id,
            decided_by="customer", status=status, gate="customer_roundtrip",
            reason=notes or "customer round-trip response",
            notes=notes, **fields,
        )

    save_id_mapping(id_mapping, results_dir)
    state = PipelineState(config.root)
    state.record_stage(
        "review_customer", status="completed",
        n_resolved=n_resolved, n_requeued=n_queued, n_unmatched_final=n_final,
    )
    logger.info(
        "Customer round-trip import: %d resolved, %d back to VDL queue, %d unmatched_final",
        n_resolved, n_queued, n_final,
    )
    return id_mapping
