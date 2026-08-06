"""Human-browsable workbook of the ID Mapping File, refreshed every match run.

Read-only snapshot for VDL eyes (`data/results/id_mapping_review.xlsx`) —
changes belong in the review app or the decisions log, never in this file.
"""

from pathlib import Path

import pandas as pd

REVIEW_COLUMNS = [
    "customer_name", "customer_url", "entity_type", "disposition", "status",
    "matched_name", "matched_url", "matched_id", "nzi_id", "match_method",
    "confidence", "in_universe", "text_sources", "enrichment_ready",
    "decided_by", "notes",
]


def write_review_workbook(id_mapping: pd.DataFrame, results_dir: str | Path) -> Path:
    m = id_mapping[[c for c in REVIEW_COLUMNS if c in id_mapping.columns]]
    m = m.sort_values(["entity_type", "disposition", "customer_name"])
    sheets = {
        "All rows": m,
        "Auto-matched": m[m["status"] == "auto_matched"],
        "VDL review queue": m[m["status"] == "needs_review"],
        "Customer round-trip": m[m["status"].isna() | (m["status"] == "customer_review")],
        "Decided by human": m[
            m["decided_by"].astype(str).str.startswith(("vdl:", "customer"))
        ],
    }
    out = Path(results_dir) / "id_mapping_review.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
            ws = writer.sheets[name[:31]]
            ws.freeze_panes = "A2"
            for i, col in enumerate(df.columns, start=1):
                q90 = df[col].astype(str).str.len().quantile(0.9) if len(df) else 14
                width = min(42, max(14, int(q90) + 2))
                ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = width
    return out
