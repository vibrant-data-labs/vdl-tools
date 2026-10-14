"""Live lookups for an organization the pipeline never pulled.

Three sources, each optional and each failing soft (a dict with ``errors``):

* Crunchbase: the ``cb_organizations`` cache in the vdl Postgres first (every
  org we ever fetched, no API cost), then the CB API by name (one metered
  call). Each hit gets the pull's own eligibility replayed: search terms in
  the description, HQ in the United States, a funding round since the pull's
  start year, and (when the spec says so) an active status.
* Netzero Insights: company search by name.
* Giving Tuesday datamart: identity search (EIN > domain > name), the org's
  deduplicated 990 text tested against the nonprofit search terms, and its
  recent cash contributions against the floors the spec names.

``classify_relevance`` runs the pipeline's own relevance classifier on a
description through the shared prompt cache. ``live_db_check`` reads the
serving schema for ground truth on what the app has now.
"""
from __future__ import annotations

import re
import time

import pandas as pd

from vdl_tools.shared_tools.why_not.spec import ClassifySpec, ProbeSpec, ServingSpec


def match_terms(text, terms: list[str]) -> list[str]:
    """Search terms found in ``text``.

    The pulls ask the sources for ``description contains <term>`` with the
    term exactly as listed (many carry surrounding spaces, e.g. ' ev ', so
    they behave like whole words). This mirrors that with a substring test on
    a space-padded, punctuation-stripped lower-case copy of the text. It is a
    proxy for the sources' own matching, not a replay of it.
    """
    if not text:
        return []
    padded = " " + re.sub(r"[^a-z0-9]+", " ", str(text).lower()) + " "
    hits = []
    for term in terms:
        t = str(term).lower()
        if t.strip() and t in padded:
            hits.append(t.strip())
    return sorted(set(hits))


def _timed(fn):
    t0 = time.time()
    out = fn()
    out["seconds"] = round(time.time() - t0, 1)
    return out


def _jsonish(v):
    if isinstance(v, str) and v[:1] in "[{":
        try:
            import json
            return json.loads(v)
        except Exception:  # noqa: BLE001
            return v
    return v


# ---------------------------------------------------------------------------
# Crunchbase
# ---------------------------------------------------------------------------

def cb_record(raw: dict, probe: ProbeSpec, terms: list[str]) -> dict:
    ident = _jsonish(raw.get("identifier")) or {}
    locs = _jsonish(raw.get("location_identifiers")) or []
    loc_names = [loc.get("value") if isinstance(loc, dict) else str(loc) for loc in locs]
    loc_links = [loc.get("permalink") if isinstance(loc, dict) else str(loc) for loc in locs]
    funding = _jsonish(raw.get("funding_total"))
    funding_usd = funding.get("value_usd") if isinstance(funding, dict) else funding
    website = _jsonish(raw.get("website_url"))
    website = website.get("value") if isinstance(website, dict) else website
    cats = _jsonish(raw.get("categories")) or []
    description = raw.get("description") or raw.get("short_description") or ""
    last = str(raw.get("last_funding_at") or "")
    rec = {
        "uuid": raw.get("uuid") or (ident.get("uuid") if isinstance(ident, dict) else None),
        "name": raw.get("name") or (ident.get("value") if isinstance(ident, dict) else None),
        "permalink": raw.get("permalink") or (ident.get("permalink") if isinstance(ident, dict) else None),
        "website": website, "location": ", ".join(x for x in loc_names if x),
        "operating_status": raw.get("operating_status"), "last_funding_at": last or None,
        "num_funding_rounds": raw.get("num_funding_rounds"), "funding_total_usd": funding_usd,
        "categories": [c.get("value") if isinstance(c, dict) else str(c) for c in cats][:10],
        "category_uuids": [c.get("uuid") for c in cats if isinstance(c, dict) and c.get("uuid")],
        "description": str(description)[:600],
        "search_terms_matched": match_terms(description, terms),
        "is_us": any(x in ("united-states", "United States") for x in loc_links + loc_names),
        "funded_since_pull_year": bool(last) and last[:4].isdigit() and int(last[:4]) >= probe.pull_min_funding_year,
        "is_active": raw.get("operating_status") == "active",
    }
    if probe.cb_eligibility is not None:
        ok, why = probe.cb_eligibility(rec)
    else:
        why = []
        if not rec["search_terms_matched"]:
            why.append("no search term appears in its Crunchbase description")
        if not rec["is_us"]:
            why.append("HQ is not in the United States")
        if not rec["funded_since_pull_year"]:
            why.append(f"no funding round on or after {probe.pull_min_funding_year}" if last
                       else "no funding date on record")
        if probe.cb_requires_active and not rec["is_active"]:
            why.append(f"operating status is '{rec['operating_status']}', not active")
        ok = not why
    rec["would_be_pulled"], rec["not_pulled_because"] = bool(ok), why
    return rec


def probe_crunchbase(probe: ProbeSpec, name, domain, uuid, terms: list[str], use_api: bool = True) -> dict:
    def run():
        out: dict = {"source": "Crunchbase", "cache_hits": [], "api_hits": [], "errors": []}
        try:
            from sqlalchemy import text

            from vdl_tools.shared_tools.database_cache.database_utils import get_session
            conds, params = [], {}
            if uuid:
                conds.append("uuid = :uuid")
                params["uuid"] = uuid
            if name:
                conds.append("name ILIKE :name")
                params.update(name=f"%{name}%", exact=name.lower())
            if domain:
                conds.append("website_url ILIKE :dom")
                params["dom"] = f"%{domain}%"
            if conds:
                order = "ORDER BY (lower(name) = :exact) DESC, num_funding_rounds DESC NULLS LAST" if name else ""
                sql = ("SELECT uuid, name, permalink, website_url, description, short_description, "
                       "location_identifiers, operating_status, last_funding_at, num_funding_rounds, "
                       f"funding_total, categories FROM cb_organizations WHERE {' OR '.join(conds)} {order} LIMIT 10")
                with get_session() as session:
                    rows = session.execute(text(sql), params).mappings().all()
                out["cache_hits"] = [cb_record(dict(r), probe, terms) for r in rows]
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"cb_organizations cache: {type(exc).__name__}: {exc}"[:300])
        if use_api and name and not out["cache_hits"]:
            try:
                import vdl_tools.scrape_enrich.crunchbase.organizations_api_db as orgs_api
                df = orgs_api.companies_query_names([name], extra_filters=[])
                if df is not None and len(df):
                    out["api_hits"] = [cb_record(r, probe, terms) for r in df.head(10).to_dict("records")]
            except Exception as exc:  # noqa: BLE001
                out["errors"].append(f"Crunchbase API: {type(exc).__name__}: {exc}"[:300])
        out["hits"] = out["cache_hits"] + out["api_hits"]
        return out
    return _timed(run)


# ---------------------------------------------------------------------------
# Netzero Insights
# ---------------------------------------------------------------------------

def probe_nzi(name, terms: list[str]) -> dict:
    def run():
        out: dict = {"source": "Netzero Insights", "hits": [], "errors": []}
        if not name:
            return out
        try:
            from vdl_tools.scrape_enrich.netzero_insights.search_netzero_api import search_companies
            res = search_companies(limit=10, name=name, checkpoint_dir=None)
            results = res.get("results", []) if isinstance(res, dict) else list(res or [])
            for c in results[:10]:
                if not isinstance(c, dict):
                    continue
                desc = c.get("description") or c.get("pitchLine") or ""
                tags = [t.get("label") or t.get("tagType") or t.get("name") for t in (c.get("tags") or [])
                        if isinstance(t, dict)]
                out["hits"].append({
                    "id": c.get("clientID") or c.get("id"), "name": c.get("name"),
                    "website": c.get("website") or c.get("domain"), "country": c.get("country"),
                    "active": c.get("active"), "funding_usd": c.get("fundingAmountUSD"),
                    "founded": c.get("foundedDate"), "tags": [t for t in tags if t][:12],
                    "description": str(desc)[:600], "search_terms_matched": match_terms(desc, terms),
                })
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{type(exc).__name__}: {exc}"[:300])
        return out
    return _timed(run)


# ---------------------------------------------------------------------------
# Giving Tuesday datamart
# ---------------------------------------------------------------------------

def fmt_ein(ein) -> str | None:
    d = re.sub(r"\D", "", str(ein or ""))
    return f"{d[:2]}-{d[2:]}" if len(d) == 9 else None


def probe_gt(probe: ProbeSpec, name, domain, ein, terms: list[str], detail_top: int = 2) -> dict:
    def run():
        out: dict = {"source": "Giving Tuesday datamart", "hits": [], "errors": []}
        if not (name or domain or ein):
            return out
        try:
            from vdl_tools.portfolio_comparison.gt_client import make_gt_client
            client = make_gt_client()
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"client: {type(exc).__name__}: {exc}"[:300])
            return out
        hits = []
        for kwargs in ({"name": name, "url": domain or None, "ein": ein}, {"name": name}):
            kwargs = {k: v for k, v in kwargs.items() if v}
            if not kwargs:
                continue
            try:
                hits = client.search_identity(limit=5, **kwargs)
                if hits:
                    break
            except ValueError as exc:
                out["errors"].append(f"search_identity{tuple(kwargs)}: {exc}"[:200])
            except Exception as exc:  # noqa: BLE001
                out["errors"].append(f"search_identity: {type(exc).__name__}: {exc}"[:300])
                return out
        for i, h in enumerate(hits):
            fe = fmt_ein(h.ein)
            rec = {"ein": fe, "name": h.name, "signal": h.signal, "org_type": h.org_type,
                   "location": ", ".join(x for x in (h.city, h.state) if x), "latest_taxyear": h.latest_taxyear}
            if i < detail_top and fe:
                try:
                    np_ = client.get_nonprofit(h.ein)
                    text = (np_.unique_text if np_ else None) or ""
                    rec["website"] = np_.website if np_ else None
                    rec["text_990"], rec["text_990_len"] = text[:600], len(text)
                    rec["search_terms_matched"] = match_terms(text, terms)
                except Exception as exc:  # noqa: BLE001
                    out["errors"].append(f"get_nonprofit {fe}: {type(exc).__name__}: {exc}"[:200])
                try:
                    rows = client.get_basic_fields([h.ein], min_taxyear=2017)
                    by_year: dict[int, int] = {}
                    for r in rows:
                        if r.taxyear is not None and r.total_cash_contributions is not None:
                            by_year[r.taxyear] = by_year.get(r.taxyear, 0) + int(r.total_cash_contributions)
                    if by_year:
                        rec["cash_contributions_by_year"] = dict(sorted(by_year.items()))
                        rec["avg_cash_contributions"] = int(sum(by_year.values()) / len(by_year))
                        rec["floors"] = {label: max(by_year.values()) >= amount
                                         for label, amount in probe.nonprofit_floors.items()}
                except Exception as exc:  # noqa: BLE001
                    out["errors"].append(f"get_basic_fields {fe}: {type(exc).__name__}: {exc}"[:200])
            out["hits"].append(rec)
        return out
    return _timed(run)


# ---------------------------------------------------------------------------
# Optional extras
# ---------------------------------------------------------------------------

def classify_relevance(classify: ClassifySpec, uid: str, text: str) -> dict:
    """Run the pipeline's relevance classifier on ``text`` through the shared prompt cache."""
    def run():
        out: dict = {"uid": uid, "prediction": None, "confidence": None, "model": classify.model, "errors": []}
        try:
            from vdl_tools.shared_tools.model_caches.climate_relevance_cache import (
                generate_climate_relevance_predictions,
            )
            kwargs = {"prompt_name": classify.prompt_name}
            if classify.system_prompt:
                kwargs["system_prompt"] = classify.system_prompt
            if classify.prompt_format:
                kwargs["prompt_format"] = classify.prompt_format
            df = pd.DataFrame([{"id": uid, "text": text}])
            res = generate_climate_relevance_predictions(
                df, column_text="text", idn="id", model=classify.model, max_workers=1, n_per_commit=1, **kwargs)
            # the helper's docstring promises {id: (prediction, probability)}; it returns a DataFrame
            if isinstance(res, pd.DataFrame):
                hit = res[res["id"].astype(str) == str(uid)]
                pred = hit["prediction"].iloc[0] if len(hit) else None
                prob = hit["probability"].iloc[0] if len(hit) else None
            else:
                pred, prob = res.get(uid, (None, None))
            out["prediction"] = None if pred is None or pd.isna(pred) else int(pred)
            out["confidence"] = None if prob is None or pd.isna(prob) else float(prob)
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{type(exc).__name__}: {exc}"[:300])
        return out
    return _timed(run)


def live_db_check(serving: ServingSpec, schema: str, uid: str | None = None, name: str | None = None) -> dict:
    """What the serving schema says right now (read-only)."""
    def run():
        out: dict = {"schema": schema, "rows": [], "errors": []}
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema or ""):
            out["errors"].append("invalid schema identifier")
            return out
        try:
            from sqlalchemy import text

            from vdl_tools.shared_tools.database_cache.database_utils import get_session
            cols = ", ".join(serving.columns)
            if uid:
                sql, params = f"SELECT {cols} FROM {schema}.{serving.table} WHERE uid = :uid", {"uid": uid}
            else:
                sql, params = (f"SELECT {cols} FROM {schema}.{serving.table} WHERE name ILIKE :n LIMIT 5",
                               {"n": f"%{name}%"})
            with get_session() as session:
                rows = session.execute(text(sql), params).mappings().all()
            out["rows"] = [dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"{type(exc).__name__}: {exc}"[:300])
        return out
    return _timed(run)


def run_probe(probe: ProbeSpec, name, domain=None, ein=None, uuid=None, sources=None, use_cb_api=True) -> dict:
    sources = tuple(sources or probe.sources)
    company_terms = probe.company_terms() if probe.company_terms else []
    nonprofit_terms = probe.nonprofit_terms() if probe.nonprofit_terms else []
    out = {"name": name, "domain": domain, "ein": ein, "uuid": uuid,
           "search_terms": {"company": len(company_terms), "nonprofit": len(nonprofit_terms)}, "results": {}}
    if "crunchbase" in sources:
        out["results"]["crunchbase"] = probe_crunchbase(probe, name, domain, uuid, company_terms, use_api=use_cb_api)
    if "nzi" in sources:
        out["results"]["nzi"] = probe_nzi(name, company_terms)
    if "gt" in sources:
        out["results"]["gt"] = probe_gt(probe, name, domain, ein, nonprofit_terms)
    return out
