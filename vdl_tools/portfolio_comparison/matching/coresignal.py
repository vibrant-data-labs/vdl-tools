"""Coresignal last-resort lane (opt-in: ``use_coresignal: true``).

For text-objective rows with NO usable text source and no LinkedIn, search
Coresignal (via ``vdl_tools.linkedin.org_loader.get_organizations_by_search``)
by name to find the org's LinkedIn identity — the text source of last
resort. Costs search credits; that's why it's a flag, not a default.

Results only ever fill ``linkedin_url`` (plus a note) — a Coresignal name
hit is never treated as canonical identity.
"""

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger


def _linkedin_url_from_record(rec: dict) -> str:
    for key in ("url", "canonical_url", "linkedin_url"):
        if rec.get(key) and "linkedin.com" in str(rec[key]):
            return str(rec[key])
    shorthand = rec.get("canonical_shorthand_name") or rec.get("shorthand_name")
    return f"https://www.linkedin.com/company/{shorthand}" if shorthand else ""


def coresignal_last_resort(
    id_mapping: pd.DataFrame, api_key: str, max_rows: int = 50
) -> pd.DataFrame:
    """Fill ``linkedin_url`` for textless rows via a batched name search."""
    from vdl_tools.linkedin.org_loader import get_organizations_by_search

    targets = id_mapping[
        ~id_mapping["enrichment_ready"].fillna(False).astype(bool)
        & ~id_mapping["text_sources"].astype(str).str.contains("linkedin")
        & id_mapping["customer_name"].notna()
    ].head(max_rows)
    if targets.empty:
        return id_mapping

    names = targets["customer_name"].astype(str).str.strip().tolist()
    logger.info("Coresignal last resort: searching %d names (costs credits)", len(names))
    results = get_organizations_by_search(
        api_key=api_key, name_filters=names, attempt_original_matches=True,
    )
    if results is None or len(results) == 0:
        logger.info("Coresignal last resort: no hits")
        return id_mapping

    match_col = next(
        (c for c in ("original_name_match", "name_match") if c in results.columns), None
    )
    n_filled = 0
    for _, row in targets.iterrows():
        name = str(row["customer_name"]).strip()
        if match_col is not None:
            hits = results[results[match_col].astype(str).str.lower() == name.lower()]
        else:
            hits = results[results.get("name", pd.Series(dtype=str)).astype(str).str.lower() == name.lower()]
        if len(hits) != 1:  # ambiguous or absent — last resort stays empty
            continue
        rec = hits.iloc[0].to_dict()
        li = _linkedin_url_from_record(rec)
        if li:
            mask = id_mapping["customer_row_id"] == row["customer_row_id"]
            id_mapping.loc[mask, "linkedin_url"] = li
            if rec.get("id") and str(rec["id"]).isdigit():
                id_mapping.loc[mask, "coresignal_id"] = str(rec["id"])
            n_filled += 1
    logger.info("Coresignal last resort: filled linkedin_url for %d rows", n_filled)
    return id_mapping
