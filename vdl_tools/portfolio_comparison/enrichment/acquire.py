"""Phase 2, stage 1 — source-record acquisition.

For every finalized row, fetch the organization record from each source we
hold an id for (CB by uuid, NZI by id, GT by EIN) and record the important
fields as columns — customer-provided and source-provided side by side.
Duplicate values are lineage, not redundancy (spec: phase2-enrichment.md §1).

Sources fail independently: one source being down never blocks the others.
All source clients cache (CB/NZI in their own caches, GT is plain SQL), so
reruns are cheap.
"""

from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger

ACQUIRED_BASENAME = "acquired_records"

# The spec's per-source column contract. Every column appears even when a
# fetch fails, so downstream stages never need existence checks.
CB_COLUMNS = ["cb_name", "cb_description", "cb_short_description",
              "cb_website", "cb_linkedin", "cb_status", "cb_location"]
NZI_COLUMNS = ["nzi_name", "nzi_description", "nzi_pitchline", "nzi_website",
               "nzi_linkedin", "nzi_location"]
GT_COLUMNS = ["gt_name", "gt_website", "gt_unique_text", "gt_grant_purposes",
              "gt_location"]


def _fetch_cb(cb_ids: list[str]) -> pd.DataFrame:
    from vdl_tools.scrape_enrich.crunchbase.organizations_api_db import (
        companies_id_query,
    )

    return companies_id_query(cb_ids)


def _fetch_nzi(nzi_ids: list[int]) -> pd.DataFrame:
    from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import (
        get_companies_details,
    )

    res = get_companies_details(nzi_ids)
    return pd.DataFrame(res) if isinstance(res, list) else res


def _jsonish(value, key="value"):
    """CB JSONB fields arrive as dicts (or lists of dicts)."""
    if isinstance(value, dict):
        return value.get(key)
    return value


def _cb_location(value) -> str:
    if not isinstance(value, (list, tuple)):
        return ""
    parts = [v.get("value") for v in value if isinstance(v, dict) and v.get("value")]
    return ", ".join(parts)


def _attach_cb(out: pd.DataFrame, orgs: pd.DataFrame) -> pd.DataFrame:
    orgs = orgs.drop_duplicates(subset="uuid").set_index("uuid")

    def _get(uid, col, fn=lambda v: v):
        if pd.isna(uid) or uid not in orgs.index:
            return pd.NA
        v = orgs.at[uid, col] if col in orgs.columns else None
        return fn(v) if v is not None else pd.NA

    ids = out["cb_id"]
    out["cb_name"] = ids.map(lambda u: _get(u, "name"))
    out["cb_description"] = ids.map(lambda u: _get(u, "description"))
    out["cb_short_description"] = ids.map(lambda u: _get(u, "short_description"))
    out["cb_website"] = ids.map(lambda u: _get(u, "website_url"))
    out["cb_linkedin"] = ids.map(lambda u: _get(u, "linkedin", _jsonish))
    out["cb_status"] = ids.map(lambda u: _get(u, "status"))
    out["cb_location"] = ids.map(lambda u: _get(u, "location_identifiers", _cb_location))
    return out


def _attach_nzi(out: pd.DataFrame, companies: pd.DataFrame) -> pd.DataFrame:
    id_col = next((c for c in ("clientID", "id") if c in companies.columns), None)
    if id_col is None:
        logger.warning("NZI companies frame has no id column; columns=%s",
                       list(companies.columns)[:12])
        return out
    companies = companies.drop_duplicates(subset=id_col).copy()
    companies[id_col] = companies[id_col].astype(str)
    companies = companies.set_index(id_col)

    def _get(nid, col):
        nid = str(nid) if pd.notna(nid) else nid
        if pd.isna(nid) or nid not in companies.index:
            return pd.NA
        v = companies.at[nid, col] if col in companies.columns else None
        return v if v is not None and v == v else pd.NA

    def _loc(nid):
        parts = [_get(nid, c) for c in ("city", "country")]
        parts = [str(p) for p in parts if pd.notna(p) and str(p).strip()]
        return ", ".join(parts) or pd.NA

    def _website(nid):
        v = _get(nid, "website")
        return v if pd.notna(v) else _get(nid, "domain")

    ids = out["nzi_id"]
    out["nzi_name"] = ids.map(lambda i: _get(i, "name"))
    out["nzi_description"] = ids.map(lambda i: _get(i, "description"))
    out["nzi_pitchline"] = ids.map(lambda i: _get(i, "pitchLine"))
    out["nzi_website"] = ids.map(_website)
    out["nzi_linkedin"] = ids.map(lambda i: _get(i, "linkedinURL"))
    out["nzi_location"] = ids.map(_loc)
    return out


def _gt_ein(row) -> str | None:
    """The EIN we matched on: GT primary matches carry it as matched_id
    (NN-NNNNNNN); otherwise fall back to the customer-provided EIN."""
    import re

    for value in (row.get("matched_id"), row.get("customer_ein")):
        if pd.notna(value) and re.fullmatch(r"\d{2}-\d{7}", str(value)):
            return str(value)
    return None


def _attach_gt(out: pd.DataFrame, gt_client) -> pd.DataFrame:
    # The datamart stores EINs digits-only; our canonical form is dashed.
    # Query and key everything by digits.
    def _digits(ein):
        return str(ein).replace("-", "")

    eins = sorted({_digits(e) for e in out["_gt_ein"].dropna()})
    if not eins:
        return out

    nonprofits = {}
    for ein in eins:
        try:
            np_row = gt_client.get_nonprofit(ein)
        except Exception as exc:
            logger.warning("GT get_nonprofit(%s) failed: %s", ein, exc)
            continue
        if np_row is not None:
            nonprofits[ein] = np_row

    purposes: dict[str, list] = {}
    try:
        grants = gt_client.get_grants(eins, role="grantee")
        for g in grants:
            if g.grantee_ein and g.grant_purpose and str(g.grant_purpose).strip():
                purposes.setdefault(_digits(g.grantee_ein), []).append(
                    (g.taxyear or 0, str(g.grant_purpose).strip())
                )
    except Exception as exc:
        logger.warning("GT get_grants failed: %s", exc)

    def _np(ein, attr):
        rec = nonprofits.get(ein)
        v = getattr(rec, attr, None) if rec else None
        return v if v else pd.NA

    def _loc(ein):
        rec = nonprofits.get(ein)
        if not rec:
            return pd.NA
        parts = [getattr(rec, a, None) for a in ("city", "state", "zip")]
        return ", ".join(str(p) for p in parts if p) or pd.NA

    def _purposes(ein):
        rows = purposes.get(ein) or []
        seen, ordered = set(), []
        for _, text in sorted(rows, key=lambda t: -t[0]):  # newest first
            if text not in seen:
                seen.add(text)
                ordered.append(text)
        return " | ".join(ordered) if ordered else pd.NA

    keys = out["_gt_ein"].map(lambda e: _digits(e) if pd.notna(e) else e)
    out["gt_name"] = keys.map(lambda e: _np(e, "name") if pd.notna(e) else pd.NA)
    out["gt_website"] = keys.map(lambda e: _np(e, "website") if pd.notna(e) else pd.NA)
    out["gt_unique_text"] = keys.map(lambda e: _np(e, "unique_text") if pd.notna(e) else pd.NA)
    out["gt_location"] = keys.map(lambda e: _loc(e) if pd.notna(e) else pd.NA)
    out["gt_grant_purposes"] = keys.map(lambda e: _purposes(e) if pd.notna(e) else pd.NA)
    logger.info("GT: %d nonprofits fetched, %d with grant purposes",
                len(nonprofits), len(purposes))
    return out


def acquire_records(
    final: pd.DataFrame,
    results_dir: str | Path,
    cb_fetch=_fetch_cb,
    nzi_fetch=_fetch_nzi,
    gt_client=None,
) -> pd.DataFrame:
    """Fetch source records for every row holding a source id; write
    ``acquired_records.parquet``/``.csv`` keyed by ``customer_row_id``."""
    out = final[[
        "customer_row_id", "customer_name", "customer_url", "customer_ein",
        "customer_description", "entity_type", "cb_id", "nzi_id",
        "matched_id", "matched_source",
    ]].copy()
    for col in CB_COLUMNS + NZI_COLUMNS + GT_COLUMNS:
        out[col] = pd.NA

    cb_ids = sorted({str(i) for i in out["cb_id"].dropna() if str(i).strip()})
    if cb_ids:
        try:
            orgs = cb_fetch(cb_ids)
            out = _attach_cb(out, orgs)
            logger.info("CB: fetched %d of %d ids", len(orgs), len(cb_ids))
        except Exception as exc:
            logger.warning("CB acquisition failed (continuing): %s", exc)

    nzi_ids = sorted({int(str(i)) for i in out["nzi_id"].dropna()
                      if str(i).strip().isdigit()})
    if nzi_ids:
        try:
            companies = nzi_fetch(nzi_ids)
            out = _attach_nzi(out, companies)
            logger.info("NZI: fetched %d of %d ids", len(companies), len(nzi_ids))
        except Exception as exc:
            logger.warning("NZI acquisition failed (continuing): %s", exc)

    out["_gt_ein"] = out.apply(_gt_ein, axis=1)
    if out["_gt_ein"].notna().any():
        if gt_client is None:
            from givingtuesday_datamart.client.client import GtDatamartClient

            gt_client = GtDatamartClient()
        try:
            out = _attach_gt(out, gt_client)
        except Exception as exc:
            logger.warning("GT acquisition failed (continuing): %s", exc)
    out = out.drop(columns=["_gt_ein"])

    results_dir = Path(results_dir)
    out = out.astype(object).where(pd.notna(out), pd.NA)
    out.to_parquet(results_dir / f"{ACQUIRED_BASENAME}.parquet", index=False)
    out.to_csv(results_dir / f"{ACQUIRED_BASENAME}.csv", index=False)

    n_any = int(out[CB_COLUMNS + NZI_COLUMNS + GT_COLUMNS].notna().any(axis=1).sum())
    logger.info("acquisition: %d of %d rows carry at least one source record",
                n_any, len(out))
    return out
