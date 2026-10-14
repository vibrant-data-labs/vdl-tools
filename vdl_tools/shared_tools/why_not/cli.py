"""Command line and markdown report shared by every pipeline's ``why_not`` entry point.

A pipeline's ``__main__`` does ``sys.exit(main(SPEC))``. Flags are identical
across pipelines; wording comes from the spec (title, hints, header notes).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from vdl_tools.shared_tools.why_not import probe as live
from vdl_tools.shared_tools.why_not.index import load_index
from vdl_tools.shared_tools.why_not.spec import STATUS_WORD, PipelineSpec
from vdl_tools.shared_tools.why_not.stages import money
from vdl_tools.shared_tools.why_not.trace import resolve, trace_uid


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _artifact_line(meta: dict) -> str:
    parts = []
    for a in meta.get("artifacts", []):
        if a["status"] == "ok":
            rows = f", {a['rows']:,} rows" if a.get("rows") else ""
            parts.append(f"{a['key']} ({a.get('mtime') or 'n/a'}{rows})")
        else:
            parts.append(f"{a['key']} ({a['status'].upper()})")
    return " · ".join(parts)


def default_header_notes(meta: dict) -> list[str]:
    lin = meta.get("lineage") or {}
    if lin.get("enrichment_behind_pull"):
        n = (meta.get("counts") or {}).get("pull_without_verdict")
        return [f"**Note:** the candidate pull is dated {str(lin.get('pull_created_at'))[:10]} but the enrichment "
                f"stage last ran {str(lin.get('meta_created_at'))[:10]} on an earlier pull"
                + (f"; {n:,} pulled orgs have no relevance verdict yet." if n else ".")]
    return []


def render_trace(tr: dict) -> list[str]:
    org = tr["org"]
    out = [f"## Pipeline trace: {org.get('name') or org['uid']}", ""]
    facts = [f"uid `{org['uid']}`", org.get("source"), org.get("hq"), org.get("website"), org.get("crunchbase_url")]
    out.append(" · ".join(str(f) for f in facts if f))
    if tr.get("resolved_via"):
        out.append(f"_resolved via {tr['resolved_via']}_")
    out.append("")
    out.append("| # | Stage | Result | Evidence |")
    out.append("|---|---|---|---|")
    for i, st in enumerate(tr["stages"], 1):
        detail = "<br>".join(d.replace("|", "\\|") for d in st["detail"]) if st["detail"] else ""
        out.append(f"| {i} | {st['label']} | **{STATUS_WORD[st['status']]}** | {detail} |")
    out.append("")
    out.append(f"**Outcome:** {tr['exit']['summary']}")
    out.append("")
    return out


def render_probe(pr: dict, classification: dict | None, floors: dict) -> list[str]:
    out = ["## Live lookup (the org is not in the candidate pull)", ""]
    terms = pr.get("search_terms", {})
    out.append(f"Search terms tested: {terms.get('company', 0)} company, {terms.get('nonprofit', 0)} nonprofit. "
               "Term matching here is a substring proxy for the sources' own search, not a replay of it.")
    out.append("")
    cb = pr["results"].get("crunchbase")
    if cb:
        out.append(f"### Crunchbase ({cb['seconds']}s)")
        if not cb["hits"]:
            out.append("- no match in the cb_organizations cache or the API")
        for h in cb["hits"][:5]:
            where = "cache" if h in cb["cache_hits"] else "API"
            out.append(f"- **{h['name']}** ({where}; `{h['permalink'] or h['uuid']}`) · {h['location'] or 'location unknown'} · "
                       f"status {h['operating_status'] or 'unknown'} · last funded {h['last_funding_at'] or 'never'} · "
                       f"total {money(h['funding_total_usd'])}")
            out.append(f"  - search terms in description: {', '.join(h['search_terms_matched']) or 'none'}")
            if h.get("categories"):
                out.append(f"  - categories: {', '.join(h['categories'][:8])}")
            verdict = "YES" if h["would_be_pulled"] else "NO: " + "; ".join(h["not_pulled_because"])
            out.append(f"  - would the pull have taken it? {verdict}")
            if h.get("description"):
                out.append(f"  - description: \"{h['description'][:250].strip()}{'...' if len(h['description']) > 250 else ''}\"")
        for e in cb.get("errors", []):
            out.append(f"- error: {e}")
        if cb["cache_hits"]:
            out.append("- a cache hit means we fetched this org from Crunchbase at some point (any project, any "
                       "role, e.g. as an investor); it does not by itself mean it was in this pull")
        out.append("")
    nz = pr["results"].get("nzi")
    if nz:
        out.append(f"### Netzero Insights ({nz['seconds']}s)")
        if not nz["hits"]:
            out.append("- no company with that name")
        for h in nz["hits"][:5]:
            out.append(f"- **{h['name']}** (id {h['id']}) · {h['country'] or 'country unknown'} · "
                       f"{'active' if h['active'] else 'inactive/unknown'} · funding {money(h['funding_usd'])} · "
                       f"tags: {', '.join(h['tags'][:6]) or 'none'}")
            out.append(f"  - search terms in description: {', '.join(h['search_terms_matched']) or 'none'}")
        for e in nz.get("errors", []):
            out.append(f"- error: {e}")
        out.append("- NZI is a coverage check, not a pipeline source")
        out.append("")
    gt = pr["results"].get("gt")
    if gt:
        out.append(f"### Giving Tuesday datamart ({gt['seconds']}s)")
        if not gt["hits"]:
            out.append("- no nonprofit matched by EIN, domain or name")
        for h in gt["hits"][:5]:
            out.append(f"- **{h['name']}** (EIN {h['ein']}, matched on {h['signal']}) · {h['location'] or ''} · "
                       f"latest 990 {h['latest_taxyear'] or 'unknown'}")
            if "search_terms_matched" in h:
                out.append(f"  - search terms in 990 text ({h.get('text_990_len', 0)} chars): "
                           f"{', '.join(h['search_terms_matched']) or 'none'}")
            if "cash_contributions_by_year" in h:
                yrs = ", ".join(f"{y}: {money(v)}" for y, v in h["cash_contributions_by_year"].items())
                out.append(f"  - cash contributions: {yrs}")
                if h.get("floors"):
                    out.append("  - " + "; ".join(f"clears the {label} floor ({money(floors.get(label))}): "
                                                 f"{'yes' if ok else 'no'}" for label, ok in h["floors"].items()))
            if h.get("text_990"):
                out.append(f"  - 990 text: \"{h['text_990'][:250].strip()}{'...' if len(h['text_990']) > 250 else ''}\"")
        for e in gt.get("errors", []):
            out.append(f"- error: {e}")
        out.append("")
    if classification:
        out.append("## Relevance classifier on the live description")
        if classification.get("prediction") is None:
            out.append(f"- no verdict ({'; '.join(classification.get('errors') or ['unknown'])})")
        else:
            word = "relevant" if classification["prediction"] == 1 else "NOT relevant"
            conf = f", confidence {classification['confidence']:.2f}" if classification.get("confidence") is not None else ""
            out.append(f"- `{classification['uid']}`: {word}{conf} (model {classification.get('model')}) · "
                       "cached in the shared prompt cache")
        out.append("")
    return out


def render(report: dict, spec: PipelineSpec) -> str:
    out = [f"# why not ({spec.title}): {report['query']}", ""]
    idx = report["index"]
    out.append(f"_Index built {idx['built_at'][:10]} from: {_artifact_line(idx)}_")
    notes = (spec.header_notes or default_header_notes)(idx)
    if notes:
        out.append("")
        out.extend(notes)
    out.append("")
    res = report["resolution"]
    cands = res["candidates"]
    if cands:
        if res["found"]:
            out.append("## Resolved to")
        elif res.get("partial"):
            out.append("## Partial name matches in the pipeline")
        else:
            out.append("## Near matches in the pipeline (not traced)")
        for i, c in enumerate(cands):
            mark = "**" if i == 0 and res["found"] else ""
            label = c["name"] or c["profile_name"] or c["uid"]
            out.append(f"- {mark}{label}{mark} · {c['source']} · `{c['uid']}` · match {c['score']} ({c['matched_on']})"
                       f"{' · ' + str(c['hq']) if c.get('hq') else ''} → {c['exit']}")
        if res.get("partial"):
            out.append("")
            out.append("No organization matches the whole name. These contain the query as part of their "
                       "name: ask the user which one they mean (re-run with its uid), or re-run with "
                       "`--live always` to look the name up outside the pipeline.")
        elif not res["found"]:
            out.append("")
            out.append("None of these matches the name closely enough to trace. If one is the intended "
                       "organization, re-run with its uid; otherwise the live lookup below covers the name as typed.")
        elif not res["auto"] and len(cands) > 1:
            out.append("")
            out.append("The top match was traced below; it is not a clear winner, so confirm with the user "
                       "or re-run with the exact uid.")
        out.append("")
    else:
        out.append("No organization in the pipeline index matched this query.")
        out.append("")
    for tr in report["traces"]:
        out.extend(render_trace(tr))
    if report.get("probe"):
        out.extend(render_probe(report["probe"], report.get("classification"), spec.probe.nonprofit_floors))
    if report.get("live_db"):
        db = report["live_db"]
        out.append(f"## Serving database: {db['schema']} ({db['seconds']}s)")
        if db["rows"]:
            for r in db["rows"]:
                out.append("- " + " · ".join(f"{k}={r.get(k)}" for k in r))
        else:
            out.append("- not in the serving table" + (f" ({'; '.join(db['errors'])})" if db["errors"] else ""))
        out.append("")
    hints = [h for tr in report["traces"] for h in (spec.hints(tr) if spec.hints else [])]
    if report.get("probe"):
        pr = report["probe"]["results"]
        cbh = (pr.get("crunchbase") or {}).get("hits") or []
        if any(h["would_be_pulled"] for h in cbh):
            hints.append("Crunchbase has it and it passes the pull's own filters, so it was most likely added or "
                         "funded after the pull date; it should arrive with the next pull, or can be force-included.")
        elif cbh and all(not h["search_terms_matched"] for h in cbh):
            hints.append("Crunchbase has it but none of the search terms appear in its description. Either the "
                         "description is the problem or the term list needs a term that covers it.")
        gth = (pr.get("gt") or {}).get("hits") or []
        if gth and gth[0].get("search_terms_matched"):
            hints.append("The datamart has the nonprofit and its 990 text matches the search terms, so the pull "
                         "should have included it; check the pull date and the funding floors above.")
    if hints:
        out.append("## What to do next")
        out.extend(f"- {h}" for h in hints)
        out.append("")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_parser(spec: PipelineSpec) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog=f"python -m <repo>.why_not",  # noqa: F541
        description=f"Explain why an organization is, or is not, in the {spec.title}.")
    p.add_argument("query", help="organization name, pipeline uid, EIN, website or LinkedIn URL")
    p.add_argument("--top", type=int, default=5, help="candidates to list for a name query (default 5)")
    p.add_argument("--live", choices=["auto", "always", "never"], default="auto",
                   help="look the org up in the live sources: auto = only when it is not in the pull (default)")
    p.add_argument("--sources", default=",".join(spec.probe.sources),
                   help=f"live sources to query (default {','.join(spec.probe.sources)})")
    p.add_argument("--no-cb-api", action="store_true", help="never call the metered Crunchbase API; cache only")
    p.add_argument("--classify", action="store_true",
                   help="run the pipeline's relevance classifier on the best live description (one model call)")
    p.add_argument("--schema", nargs="?", const=spec.serving.default_schema or "",
                   help=f"serving schema to read live (default when given bare: {spec.serving.default_schema})")
    p.add_argument("--rebuild-index", action="store_true", help="re-read every source")
    p.add_argument("--json", action="store_true", help="emit the full report as JSON instead of markdown")
    return p


def run(spec: PipelineSpec, argv=None) -> dict:
    args = build_parser(spec).parse_args(argv)
    os.chdir(spec.repo_root)
    idx, meta = load_index(spec, rebuild=args.rebuild_index, log=log)
    res = resolve(idx, args.query, spec, meta, limit=args.top)
    report: dict = {
        "pipeline": spec.name, "query": args.query, "kind": res["kind"],
        "index": {"built_at": meta["built_at"], "counts": meta["counts"], "lineage": meta.get("lineage", {}),
                  "artifacts": meta["artifacts"]},
        "resolution": res, "traces": [], "probe": None, "classification": None, "live_db": None,
    }
    if res["found"]:
        report["traces"].append(trace_uid(idx, res["candidates"][0]["uid"], spec, meta))

    not_in_pull = not res["found"] or report["traces"][0]["exit"]["stage"] == "pull"
    if args.live == "always" or (args.live == "auto" and not_in_pull and not res["partial"]):
        kind, value = res["kind"], res["value"]
        name = args.query if kind == "name" else (report["traces"][0]["org"].get("name") if report["traces"] else None)
        sources = tuple(s.strip() for s in args.sources.split(",") if s.strip())
        log(f"Looking the organization up live ({', '.join(sources)})...")
        report["probe"] = live.run_probe(
            spec.probe, name=name, domain=value if kind == "url" else None, ein=value if kind == "ein" else None,
            uuid=value if kind == "uuid" else None, sources=sources, use_cb_api=not args.no_cb_api)
        traced = {tr["org"]["uid"] for tr in report["traces"]}
        found_ids = [h.get("uuid") for h in (report["probe"]["results"].get("crunchbase") or {}).get("hits", [])]
        for h in (report["probe"]["results"].get("gt") or {}).get("hits", []):
            found_ids.extend(spec.uid_from_ein(h["ein"]) if h.get("ein") else [])
        for fid in [f for f in found_ids if f]:
            if fid in traced or not (idx["uid"] == fid).any():
                continue
            tr = trace_uid(idx, fid, spec, meta)
            tr["resolved_via"] = "a live source id"
            report["traces"].append(tr)
            traced.add(fid)
        if args.classify:
            if spec.probe.classify is None:
                report["classification"] = {"prediction": None, "errors": ["this pipeline declares no classifier"]}
            else:
                cbh = (report["probe"]["results"].get("crunchbase") or {}).get("hits") or []
                gth = (report["probe"]["results"].get("gt") or {}).get("hits") or []
                if cbh and cbh[0].get("description") and cbh[0].get("uuid"):
                    report["classification"] = live.classify_relevance(spec.probe.classify, cbh[0]["uuid"], cbh[0]["description"])
                elif gth and gth[0].get("text_990") and gth[0].get("ein"):
                    uid = (spec.uid_from_ein(gth[0]["ein"]) or [gth[0]["ein"]])[0]
                    report["classification"] = live.classify_relevance(spec.probe.classify, uid, gth[0]["text_990"])
                else:
                    report["classification"] = {"prediction": None, "errors": ["no live description with a real id to classify"]}

    if args.schema:
        uid = report["traces"][0]["org"]["uid"] if report["traces"] else None
        report["live_db"] = live.live_db_check(spec.serving, args.schema, uid=uid, name=None if uid else args.query)

    print(json.dumps(report, indent=1, default=str) if args.json else render(report, spec))
    return report


def main(spec: PipelineSpec, argv=None) -> int:
    run(spec, argv)
    return 0
