"""Resolve a query to an organization in the index and walk the spec's stages."""
from __future__ import annotations

import re
from dataclasses import asdict

import pandas as pd

from vdl_tools.shared_tools.why_not.loaders import linkedin_slug, normalize_domain, normalize_name
from vdl_tools.shared_tools.why_not.spec import (
    FAIL,
    HIDDEN,
    PASS,
    UNKNOWN,
    Ctx,
    PipelineSpec,
    Stage,
)

EIN_RE = re.compile(r"^\d{2}-?\d{7}$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

AUTO_SCORE = 90      # top name score needed to trace without asking
AUTO_GAP = 5         # ... and lead over the runner-up
FOUND_SCORE = 85     # below this the org counts as "not in the pipeline" (triggers the live probe)


def classify_query(query: str) -> tuple[str, str]:
    """-> (kind, value) with kind in ein | uuid | linkedin | url | name. EINs come back as NN-NNNNNNN."""
    s = query.strip()
    if EIN_RE.match(s):
        d = s.replace("-", "")
        return "ein", f"{d[:2]}-{d[2:]}"
    if UUID_RE.match(s):
        return "uuid", s.lower()
    if "linkedin.com/" in s.lower():
        return "linkedin", linkedin_slug(s)
    if normalize_domain(s):
        return "url", normalize_domain(s)
    return "name", s


_CHOICES: dict[int, tuple[list[str], list[int]]] = {}


def _choices(index: pd.DataFrame):
    key = id(index)
    if key not in _CHOICES:
        parts = []
        for col in ("name_norm", "alt_norm"):
            if col in index:
                s = index[col].fillna("")
                parts.append(pd.DataFrame({"pos": s.index.values, "norm": s.values}))
        if "network_name" in index:
            s = index["network_name"].map(normalize_name).fillna("")
            parts.append(pd.DataFrame({"pos": s.index.values, "norm": s.values}))
        ch = pd.concat(parts, ignore_index=True)
        ch = ch[ch["norm"] != ""].drop_duplicates()
        _CHOICES[key] = (ch["norm"].tolist(), ch["pos"].tolist())
    return _CHOICES[key]


def _candidate(index, pos, score, how, spec, meta, loose=None) -> dict:
    row = index.iloc[pos]
    tr = trace_uid(index, row["uid"], spec, meta)
    org = tr["org"]
    return {
        "uid": row["uid"], "name": org.get("name"), "profile_name": org.get("profile_name"),
        "source": org.get("source"), "hq": org.get("hq"),
        "score": round(float(score), 1), "loose_score": None if loose is None else round(float(loose), 1),
        "matched_on": how, "exit": tr["exit"]["summary"],
    }


def resolve(index: pd.DataFrame, query: str, spec: PipelineSpec, meta: dict | None = None, limit: int = 5) -> dict:
    """Rank candidate orgs for a query.

    Names are scored two ways: ``score`` is a length-sensitive token-sort
    ratio (what decides ``found`` and ``auto``), ``loose_score`` is
    rapidfuzz's WRatio, which rewards partial overlaps. A query whose words
    are all contained in a longer org name (``Audubon`` for ``Maine Audubon
    Society``) is a *partial* match: listed for the user to pick from, never
    traced or probed automatically. Weaker lookalikes are listed as near
    matches while the live lookup proceeds for the name as typed.
    """
    kind, value = classify_query(query)
    cands: list[dict] = []
    exact = index.index[index["uid"] == query.strip()]
    if len(exact):
        kind, value = "uid", query.strip()
        cands = [_candidate(index, p, 100, "uid", spec, meta) for p in exact]
    elif kind in ("ein", "uuid", "linkedin", "url"):
        if kind == "ein":
            uids = {u.lower() for u in spec.uid_from_ein(value)}
            hits = index.index[index["uid"].str.lower().isin(uids)]
        elif kind == "uuid":
            uids = {u.lower() for u in spec.uid_from_uuid(value)}
            hits = index.index[index["uid"].str.lower().isin(uids)]
        elif kind == "linkedin":
            hits = index.index[index["linkedin_slug"] == value] if value else []
        else:
            hits = index.index[index["domain"] == value]
        how = {"ein": "ein", "uuid": "uuid", "linkedin": "linkedin", "url": "website domain"}[kind]
        cands = [_candidate(index, p, 100, how, spec, meta) for p in hits]
    else:
        from rapidfuzz import fuzz, process
        qn = normalize_name(value)
        if qn:
            norms, positions = _choices(index)
            pool = process.extract(qn, norms, scorer=fuzz.WRatio, limit=limit * 20, score_cutoff=55)
            qtokens = set(qn.split())
            best: dict[int, tuple[float, float, bool]] = {}
            for choice, loose, i in pool:
                strict = 100.0 if choice == qn else fuzz.token_sort_ratio(qn, choice)
                is_partial = strict < AUTO_SCORE and qtokens < set(choice.split())
                pos = positions[i]
                prev = best.get(pos)
                if prev is None or (strict, loose) > prev[:2]:
                    best[pos] = (strict, loose, is_partial)
            ranked = sorted(best.items(), key=lambda kv: (-kv[1][0], -kv[1][1]))
            seen = set()
            for pos, (strict, loose, is_partial) in ranked:
                uid = index.iloc[pos]["uid"]
                if uid in seen:
                    continue
                seen.add(uid)
                cands.append(_candidate(index, pos, strict, "name (partial)" if is_partial else "name",
                                        spec, meta, loose))
                if len(cands) >= limit:
                    break
    auto = False
    if cands:
        top = cands[0]["score"]
        second = cands[1]["score"] if len(cands) > 1 else 0
        auto = (kind != "name" and len(cands) == 1) or (top >= AUTO_SCORE and top - second >= AUTO_GAP)
    found = bool(cands) and cands[0]["score"] >= FOUND_SCORE and cands[0]["matched_on"] != "name (partial)"
    partial = bool(cands) and not found and cands[0]["matched_on"] == "name (partial)"
    return {"kind": kind, "value": value, "candidates": cands, "auto": auto and found,
            "found": found, "partial": partial}


def default_summary(ctx: Ctx) -> dict:
    v = ctx.v
    return {
        "uid": ctx.row["uid"], "name": v("name"), "profile_name": v("profile_name"),
        "network_name": v("network_name"), "source": v("source"),
        "website": v("website_enriched") or v("website"), "linkedin": v("linkedin_enriched") or v("linkedin"),
        "hq": v("location") or v("hq"), "country": v("country"), "operating_status": v("operating_status"),
        "year_last_funded": None if v("year_last_funded") is None else int(v("year_last_funded")),
        "total_funding": v("total_funding"), "org_type": v("org_type_pred"),
        "crunchbase_url": f"https://www.crunchbase.com/organization/{v('cb_permalink')}" if v("cb_permalink") else None,
        "in_pull": ctx.flag("in_pull"), "in_meta": ctx.flag("in_meta"), "in_network": ctx.flag("in_network"),
    }


def trace_uid(index: pd.DataFrame, uid: str, spec: PipelineSpec, meta: dict | None = None) -> dict:
    hits = index.index[index["uid"] == uid]
    if len(hits) == 0:
        raise KeyError(uid)
    ctx = Ctx(row=index.iloc[hits[0]], lineage=(meta or {}).get("lineage", {}), stages=[], spec=spec)
    for rule in spec.stages:
        ctx.stages.append(rule(ctx))
    stages: list[Stage] = ctx.stages
    exit_stage = next((st for st in stages if st.status == FAIL), None)
    tracker = ctx.prev(spec.in_tracker_stage)
    if exit_stage is not None:
        exit_ = {"stage": exit_stage.key, "label": exit_stage.label, "status": FAIL,
                 "summary": f"dropped at '{exit_stage.key}': {exit_stage.detail[0] if exit_stage.detail else ''}"}
    elif any(st.status == HIDDEN for st in stages):
        exit_ = {"stage": "visible", "label": stages[-1].label, "status": HIDDEN,
                 "summary": "in the tracker but hidden by the default filters"}
    elif tracker is not None and tracker.status == PASS:
        exit_ = {"stage": None, "label": None, "status": PASS, "summary": "in the tracker"}
    else:
        exit_ = {"stage": None, "label": None, "status": UNKNOWN,
                 "summary": "could not determine (see unknown stages)"}
    summary = (spec.summary or default_summary)(ctx)
    return {"org": summary, "stages": [asdict(st) for st in stages], "exit": exit_}
