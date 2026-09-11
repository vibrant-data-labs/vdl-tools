"""Compare `process_nzi` outputs built from v1 records vs. v2 records for the
same companies.

Why this shape: the v1 pipeline has only ever run off the Postgres cache
(`startups_nzi`, `company_funding_rounds_nzi`, `investors_nzi`), so "what we
have done" *is* the cache. The v2 side is fetched live through
``NetZeroAPI(api_version="v2")`` with the cache disabled, so nothing is
written. Both sides go through the identical `process_nzi` chain, the
identical stage-bucket split and the identical survival classification.

Every bucket / survival difference is then attributed to one of:

* **drift** — the company's round set, a per-round value (type, financing,
  date, amount, investor IDs) or one of its investors' types changed since the
  cache was written. Expected; not a pipeline property.
* **tie** — the company has several rounds on one date. `divide_funding_rows`
  orders rounds by date only, so tie order is input order, which differs
  between the cache and the live API. A pipeline determinism property.
* **mapping** — none of the above: same inputs, different output. The thing
  this comparison exists to catch.

Columns that merely echo the input payload (``*_all_funding_activity``,
``*_investors``) are counted separately and never drive attribution.

Run::

    VDL_GLOBAL_CONFIG_PATH=/path/to/config.ini \\
    python -m vdl_tools.scrape_enrich.netzero_insights.scripts.compare_v1_cache_v2_live \\
        --n 40 --out /tmp/nzi_v1_v2 [--reuse-raw]

``--reuse-raw`` reloads the raw records saved by a previous run from ``--out``
instead of hitting the database and the API again. Writes ``report.md`` plus
CSVs of every processed frame under ``--out``.
"""

import argparse
import asyncio
import json
import math
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

import pandas as pd
from sqlalchemy import func

from vdl_tools.shared_tools.database_cache.database_models import (
    CompanyFundingRounds, Investor, Startup,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.scrape_enrich.netzero_insights.netzero_api import NetZeroAPI
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.company import process_nzi_companies_details
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.funding_round import process_nzi_funding_rounds
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.investor import process_nzi_investors
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.nzi_survival_rates import (
    precompute_survival_classifications,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.split_early_late_funding_rounds import (
    divided_funding_rows_and_flatten,
)
from vdl_tools.scrape_enrich.netzero_insights.process_nzi.stage_constants import NZI_SURVIVAL_STAGES


STATUS_COL = "ensemble_operating_status_classification"
# The survival classifier needs an operating-status column that comes from a
# separate pipeline. A constant on both sides makes the classification depend
# only on the funding rounds, which is what is being compared.
CONSTANT_STATUS = "Operating"
KEY_ROUND, KEY_COMPANY, KEY_INVESTOR = "co_funding_round_id_nzi", "client_id_nzi", "investor_id_nzi"
ROUND_INPUT_FIELDS = ["round_type_nzi", "financing_type_nzi", "round_date_nzi", "round_amount_usd_nzi", "round_investor_ids_nzi"]
INVESTOR_INPUT_FIELDS = ["primary_type_nzi", "secondary_types_nzi", "strategic_nzi", "growth_investor_nzi"]
ECHO_SUFFIXES = ("_all_funding_activity", "_investors")


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------

def load_cohort_ids(session, n: int, min_rounds: int, max_rounds: int) -> List[int]:
    """Companies present in both cache tables with a venture-sized round history."""
    n_rounds = func.jsonb_array_length(CompanyFundingRounds.fullData)
    rows = (
        session.query(CompanyFundingRounds.clientID)
        .join(Startup, Startup.clientID == CompanyFundingRounds.clientID)
        .filter(n_rounds.between(min_rounds, max_rounds))
        .order_by(CompanyFundingRounds.clientID)
        .limit(n)
        .all()
    )
    return [int(r[0]) for r in rows]


def load_v1_from_cache(session, ids: List[int]) -> Dict[str, Any]:
    companies = [s.fullData for s in session.query(Startup).filter(Startup.clientID.in_(ids))]
    rounds: List[Dict] = []
    for row in session.query(CompanyFundingRounds).filter(CompanyFundingRounds.clientID.in_(ids)):
        for r in row.fullData or []:
            r = dict(r)
            r.setdefault("clientId", row.clientID)
            rounds.append(r)
    investor_ids = sorted({i for r in rounds for i in (r.get("roundInvestorIDs") or [])})
    investors = [i.fullData for i in session.query(Investor).filter(Investor.investorID.in_(investor_ids))]
    found = {i.get("investorID") for i in investors}
    return {"companies": companies, "rounds": rounds, "investors": investors,
            "missing_investors": [i for i in investor_ids if i not in found]}


def load_v2_live(api: NetZeroAPI, ids: List[int]) -> Dict[str, Any]:
    kw = {"read_from_cache": False, "write_to_cache": False}
    companies = asyncio.run(api.get_startup_details(ids, **kw))
    rounds = asyncio.run(api.get_company_funding_rounds(ids, flatten=True, **kw))
    investor_ids = sorted({i for r in rounds for i in (r.get("roundInvestorIDs") or [])})
    investors = asyncio.run(api.get_investor_details(investor_ids, **kw))
    return {"companies": companies, "rounds": rounds, "investors": investors, "missing_investors": []}


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

def _to_naive_utc(series: pd.Series) -> pd.Series:
    # v1 cache dates carry "+00:00"; v2 dates are naive. process_nzi does not
    # parse dates, so both sides are normalised the same way here.
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_localize(None)


def run_pipeline(raw: Dict[str, Any], label: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"label": label, "errors": {}}
    companies_df, rounds_df, investors_df = (pd.DataFrame(raw[k]) for k in ("companies", "rounds", "investors"))
    if "roundDate" in rounds_df:
        rounds_df["roundDate"] = _to_naive_utc(rounds_df["roundDate"])

    def stage(name, fn):
        try:
            out[name] = fn()
        except Exception:
            out["errors"][name] = traceback.format_exc()
            out[name] = None

    stage("investors", lambda: process_nzi_investors(investors_df))
    stage("rounds", lambda: process_nzi_funding_rounds(rounds_df, out["investors"]) if out["investors"] is not None else None)
    stage("companies", lambda: process_nzi_companies_details(companies_df, out["rounds"]) if out["rounds"] is not None else None)
    stage("buckets", lambda: divided_funding_rows_and_flatten(out["rounds"], processed_investor_df=out["investors"]) if out["rounds"] is not None else None)

    def survival():
        cdf = out["companies"][[KEY_COMPANY]].copy()
        cdf[STATUS_COL] = CONSTANT_STATUS
        return precompute_survival_classifications(out["rounds"], cdf, stages=NZI_SURVIVAL_STAGES)
    stage("survival", lambda: survival() if out["companies"] is not None and out["rounds"] is not None else None)
    return out


# --------------------------------------------------------------------------
# Diffing
# --------------------------------------------------------------------------

def _norm(v: Any) -> Any:
    """Make values comparable across the two sides."""
    if isinstance(v, float) and math.isnan(v):
        return None
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, (list, tuple, set)):
        try:
            return tuple(sorted(_norm(x) for x in v))
        except TypeError:
            return tuple(_norm(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _norm(x)) for k, x in v.items()))
    if isinstance(v, (pd.Timestamp, datetime)):
        return pd.Timestamp(v).date().isoformat()
    if hasattr(v, "item"):  # numpy scalar
        v = v.item()
    return v


def _equal(a: Any, b: Any) -> bool:
    a, b = _norm(a), _norm(b)
    if (isinstance(a, (int, float)) and isinstance(b, (int, float))
            and not isinstance(a, bool) and not isinstance(b, bool)):
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-9)
    return a == b


def diff_frames(v1: pd.DataFrame, v2: pd.DataFrame, key: str, fields: Iterable[str]) -> Dict[str, Any]:
    """Row-aligned field comparison: counts, full mismatch key lists, examples."""
    v1 = v1.drop_duplicates(subset=[key]).set_index(key)
    v2 = v2.drop_duplicates(subset=[key]).set_index(key)
    common = sorted(set(v1.index) & set(v2.index))
    res: Dict[str, Any] = {
        "n_v1": len(v1), "n_v2": len(v2), "n_common": len(common), "common": common,
        "only_v1": sorted(set(v1.index) - set(v2.index)), "only_v2": sorted(set(v2.index) - set(v1.index)),
        "mismatches": {}, "mismatch_keys": {}, "examples": {}, "missing_fields": {"v1": [], "v2": []},
    }
    for f in fields:
        if f not in v1.columns:
            res["missing_fields"]["v1"].append(f); continue
        if f not in v2.columns:
            res["missing_fields"]["v2"].append(f); continue
        bad = [k for k in common if not _equal(v1.at[k, f], v2.at[k, f])]
        res["mismatches"][f] = len(bad)
        res["mismatch_keys"][f] = bad
        res["examples"][f] = [(k, _norm(v1.at[k, f]), _norm(v2.at[k, f])) for k in bad[:5]]
    return res


def all_na_columns(df: pd.DataFrame) -> List[str]:
    return sorted(c for c in df.columns if df[c].isna().all())


def attribute_companies(v1: Dict[str, Any], v2: Dict[str, Any], rounds_diff: Dict[str, Any],
                        investors_diff: Dict[str, Any]) -> Tuple[Dict[int, str], Dict[int, int]]:
    """Return ``(drift, ties)``: drift reasons per company, and per company the
    number of same-date round pairs (0 = none)."""
    drift: Dict[int, str] = {}
    r1 = v1["rounds"].drop_duplicates(KEY_ROUND).set_index(KEY_ROUND)
    r2 = v2["rounds"].drop_duplicates(KEY_ROUND).set_index(KEY_ROUND)
    for rid in rounds_diff["only_v1"]:
        drift[int(r1.at[rid, KEY_COMPANY])] = "round only in v1 cache"
    for rid in rounds_diff["only_v2"]:
        drift.setdefault(int(r2.at[rid, KEY_COMPANY]), "round only in v2 live")
    for f in ROUND_INPUT_FIELDS:
        for rid in rounds_diff["mismatch_keys"].get(f, []):
            drift.setdefault(int(r1.at[rid, KEY_COMPANY]), f"round {f} differs")
    drifted_investors: Set[int] = set()
    for f in INVESTOR_INPUT_FIELDS + [c for c in investors_diff["mismatch_keys"] if c.startswith("is_")]:
        drifted_investors.update(int(k) for k in investors_diff["mismatch_keys"].get(f, []))
    if drifted_investors:
        for cid, grp in r2.groupby(KEY_COMPANY):
            ids = {int(i) for ids in grp["round_investor_ids_nzi"] for i in (ids if isinstance(ids, (list, tuple)) else [])}
            if ids & drifted_investors:
                drift.setdefault(int(cid), "an investor's type changed")
    ties: Dict[int, int] = {}
    for cid, grp in r2.groupby(KEY_COMPANY):
        dates = grp["round_date_nzi"].dropna()
        ties[int(cid)] = int(dates.duplicated().sum())
    return drift, ties


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def _md_table(rows: List[List[Any]], header: List[str]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def _mismatch_table(d: Dict[str, Any]) -> str:
    rows = [[f, n, f"{100 * n / max(d['n_common'], 1):.0f}%"] for f, n in d["mismatches"].items() if n]
    return _md_table(sorted(rows, key=lambda r: -r[1]) or [["(none)", 0, ""]], ["field", "mismatches", "of common"])


def _examples(d: Dict[str, Any], limit: int = 25) -> List[str]:
    ex = [(f, k, a, b) for f, exs in d["examples"].items() for (k, a, b) in exs][:limit]
    if not ex:
        return []
    return ["<details><summary>Examples</summary>", "",
            _md_table([[f, k, str(a)[:80], str(b)[:80]] for f, k, a, b in ex], ["field", "key", "v1", "v2"]), "", "</details>", ""]


def informational(v1: Dict[str, Any], v2: Dict[str, Any], rounds_diff: Dict[str, Any]) -> List[str]:
    """Known v1→v2 encoding differences, counted so they are not mistaken for drift."""
    r1 = v1["rounds"].drop_duplicates(KEY_ROUND).set_index(KEY_ROUND)
    r2 = v2["rounds"].drop_duplicates(KEY_ROUND).set_index(KEY_ROUND)
    common = rounds_diff["common"]
    zero_to_null = sum(1 for k in common if _norm(r1.at[k, "round_amount_usd_nzi"]) == 0 and _norm(r2.at[k, "round_amount_usd_nzi"]) is None)
    null_to_no = sum(1 for k in common if _norm(r1.at[k, "connected_to_infrastructure_deal_nzi"]) is None and _norm(r2.at[k, "connected_to_infrastructure_deal_nzi"]) == "NO")
    case_only = [(k, r1.at[k, "round_type_nzi"], r2.at[k, "round_type_nzi"]) for k in common
                 if str(r1.at[k, "round_type_nzi"]) != str(r2.at[k, "round_type_nzi"])
                 and str(r1.at[k, "round_type_nzi"]).lower() == str(r2.at[k, "round_type_nzi"]).lower()]
    return ["## Known v1 → v2 encoding differences (informational)", "",
            f"- Undisclosed amounts: v1 stored `0.0`, v2 returns `null` — {zero_to_null} of {len(common)} common rounds.",
            f"- `connectedToInfrastructureDeal`: v1 stored `null`, v2 returns `\"NO\"` — {null_to_no} of {len(common)} common rounds.",
            f"- Round-type labels differing only by case (v2 matches `stage_constants`): {len(case_only)} — {case_only[:5]}", ""]


def build_report(ids, v1, v2, missing_investors, diffs, drift, ties) -> str:
    L = [f"# process_nzi: v1 cache vs v2 live — {len(ids)} companies", "",
         f"Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')}. Cohort: companies present in the "
         f"v1 cache with a venture-sized round history, ordered by client ID. Investors missing from the v1 cache: {len(missing_investors)}.", ""]
    for side in (v1, v2):
        for st, tb in side["errors"].items():
            L += [f"## {side['label']}: `{st}` FAILED", "```", tb.strip()[-1500:], "```", ""]
    L += ["## Pipeline stages", "", _md_table(
        [[st, "ok" if v1.get(st) is not None else "FAILED", "ok" if v2.get(st) is not None else "FAILED"]
         for st in ("investors", "rounds", "companies", "buckets", "survival")], ["stage", "v1 cache", "v2 live"]), ""]

    L += ["## Column coverage (processed frames)", ""]
    for name in ("companies", "rounds", "investors"):
        a, b = v1.get(name), v2.get(name)
        if a is None or b is None:
            continue
        na1, na2 = set(all_na_columns(a)), set(all_na_columns(b))
        L += [f"**{name}** — columns v1: {len(a.columns)}, v2: {len(b.columns)}",
              f"- all-NA on v2 but populated on v1: `{sorted(na2 - na1)}`",
              f"- all-NA on v1 but populated on v2: `{sorted(na1 - na2)}`",
              f"- only in v1: `{sorted(set(a.columns) - set(b.columns))}`; only in v2: `{sorted(set(b.columns) - set(a.columns))}`", ""]

    for name, title in (("rounds", "Funding rounds (joined on coFundingRoundID)"), ("investors", "Investors (joined on investorID)"),
                        ("companies", "Companies (joined on clientID)")):
        d = diffs.get(name)
        if not d:
            continue
        L += [f"## {title}", "", f"v1: {d['n_v1']} · v2: {d['n_v2']} · common: {d['n_common']} · only-v1: {len(d['only_v1'])} · only-v2: {len(d['only_v2'])}", ""]
        if d["missing_fields"]["v1"] or d["missing_fields"]["v2"]:
            L += [f"Fields absent — v1: `{d['missing_fields']['v1']}` · v2: `{d['missing_fields']['v2']}`", ""]
        L += [_mismatch_table(d), ""] + _examples(d)

    if "rounds" in diffs:
        L += informational(v1, v2, diffs["rounds"])

    b = diffs.get("buckets")
    if b:
        L += ["## Stage buckets — `divided_funding_rows_and_flatten` (joined on clientID)", "",
              f"common: {b['n_common']} · companies with drifted inputs: {len(drift)} · companies with same-date rounds: {sum(1 for t in ties.values() if t)}", "",
              "Derived fields (amounts, counts, dates, investor-type counts) — all mismatches:", "", _mismatch_table(b), "",
              f"Payload-echo columns (`*_all_funding_activity`, `*_investors`) mismatching: {b['echo_mismatches']} column-companies (not attributed).", ""]
        L += ["### Attribution of derived-field mismatches", "",
              _md_table([[k, n] for k, n in b["attribution"].items()], ["cause", "companies with ≥1 mismatch"]), ""]
        if b["mapping"]:
            L += ["**Mapping-attributable (same inputs, no same-date ties, different output):**", "",
                  _md_table([[f, len(v), v[:5]] for f, v in sorted(b["mapping"].items(), key=lambda x: -len(x[1]))], ["bucket field", "companies", "example clientIDs"]), ""]
        else:
            L += ["**No derived-field mismatch is attributable to the v2 mapping.**", ""]
        if b["tie"]:
            L += ["Tie-attributable (same inputs, same-date rounds ordered differently by source):", "",
                  _md_table([[f, len(v), v[:5]] for f, v in sorted(b["tie"].items(), key=lambda x: -len(x[1]))], ["bucket field", "companies", "example clientIDs"]), ""]
        L += _examples(b, 15)

    s = diffs.get("survival")
    if s:
        L += ["## Survival classification (constant status; depends only on rounds)", "",
              f"current-stage mismatches: {s['current_stage_mismatch']} of {s['n_common']}", "",
              _md_table([[st, n] for st, n in s["per_stage_mismatch"].items()], ["stage", "classification mismatches"]), "",
              f"Companies with any survival mismatch: {len(s['bad'])} — drift: {s['drift']}, tie: {s['tie']}, mapping: {s['mapping']}"
              + (f" `{s['mapping']}`" if s['mapping'] else ""), ""]
    return "\n".join(L)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=40)
    ap.add_argument("--min-rounds", type=int, default=5)
    ap.add_argument("--max-rounds", type=int, default=40)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--reuse-raw", action="store_true", help="reload raw_v1.json / raw_v2.json from --out instead of fetching")
    args = ap.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    raw1_path, raw2_path = args.out / "raw_v1.json", args.out / "raw_v2.json"

    if args.reuse_raw and raw1_path.exists() and raw2_path.exists():
        raw1, raw2 = json.loads(raw1_path.read_text()), json.loads(raw2_path.read_text())
        ids = raw1["ids"]
        print(f"reusing raw records from {args.out}")
    else:
        with get_session() as session:
            ids = load_cohort_ids(session, args.n, args.min_rounds, args.max_rounds)
            raw1 = load_v1_from_cache(session, ids)
        config = get_configuration()
        api = NetZeroAPI(config.get("netzero_insights", "username"), config.get("netzero_insights", "password"),
                         api_version="v2", read_from_cache=False, write_to_cache=False)
        raw2 = load_v2_live(api, ids)
        raw1["ids"] = raw2["ids"] = ids
        raw1_path.write_text(json.dumps(raw1, default=str)); raw2_path.write_text(json.dumps(raw2, default=str))
    for lab, raw in (("v1 cache", raw1), ("v2 live", raw2)):
        print(f"{lab}: {len(raw['companies'])} companies, {len(raw['rounds'])} rounds, {len(raw['investors'])} investors")

    v1 = run_pipeline(raw1, "v1 cache")
    v2 = run_pipeline(raw2, "v2 live")

    diffs: Dict[str, Any] = {}
    if v1["rounds"] is not None and v2["rounds"] is not None:
        has_cols = sorted(c for c in v1["rounds"].columns if c.startswith("has_") and c.endswith("_investor_calced_nzi"))
        diffs["rounds"] = diff_frames(v1["rounds"], v2["rounds"], KEY_ROUND,
                                      [KEY_COMPANY] + ROUND_INPUT_FIELDS + ["connected_to_infrastructure_deal_nzi"] + has_cols)
    if v1["investors"] is not None and v2["investors"] is not None:
        is_cols = sorted(c for c in v1["investors"].columns if c.startswith("is_") and c.endswith("_investor_calced_nzi"))
        diffs["investors"] = diff_frames(v1["investors"], v2["investors"], KEY_INVESTOR,
                                         INVESTOR_INPUT_FIELDS + ["number_of_deals_nzi"] + is_cols)
    if v1["companies"] is not None and v2["companies"] is not None:
        has_cols = sorted(c for c in v1["companies"].columns if c.startswith("has_") and c.endswith("_investor_calced_nzi"))
        diffs["companies"] = diff_frames(v1["companies"], v2["companies"], KEY_COMPANY,
                                         ["name_nzi", "stage_nzi", "size_nzi", "active_nzi", "last_round_type_nzi", "trl_parsed_nzi",
                                          "founded_date_nzi", "funding_amount_usd_nzi", "was_acquired_merged_calced_nzi",
                                          "has_project_finance_calced_nzi", "flat_tags_nzi"] + has_cols)

    drift: Dict[int, str] = {}
    ties: Dict[int, int] = {}
    if "rounds" in diffs and "investors" in diffs:
        drift, ties = attribute_companies(v1, v2, diffs["rounds"], diffs["investors"])

    if v1["buckets"] is not None and v2["buckets"] is not None:
        shared = set(v1["buckets"].columns) & set(v2["buckets"].columns) - {KEY_COMPANY}
        derived = sorted(c for c in shared if not c.endswith(ECHO_SUFFIXES))
        echo = sorted(c for c in shared if c.endswith(ECHO_SUFFIXES))
        b = diff_frames(v1["buckets"], v2["buckets"], KEY_COMPANY, derived)
        e = diff_frames(v1["buckets"], v2["buckets"], KEY_COMPANY, echo)
        b["echo_mismatches"] = sum(e["mismatches"].values())
        mapping, tie = defaultdict(list), defaultdict(list)
        cause: Dict[int, str] = {}
        for f, keys in b["mismatch_keys"].items():
            for k in keys:
                k = int(k)
                if k in drift:
                    cause[k] = "drift"
                elif ties.get(k):
                    tie[f].append(k); cause.setdefault(k, "tie")
                else:
                    mapping[f].append(k); cause[k] = "mapping"
        b["attribution"] = {c: sum(1 for v in cause.values() if v == c) for c in ("drift", "tie", "mapping")}
        b["mapping"], b["tie"] = dict(mapping), dict(tie)
        diffs["buckets"] = b

    if v1["survival"] is not None and v2["survival"] is not None:
        (cl1, cs1), (cl2, cs2) = v1["survival"], v2["survival"]
        common = sorted(set(cl1) & set(cl2))
        per_stage = {st: sum(1 for k in common if cl1[k].get(st) != cl2[k].get(st)) for st in NZI_SURVIVAL_STAGES}
        bad = sorted(int(k) for k in common if cs1.get(k) != cs2.get(k) or any(cl1[k].get(st) != cl2[k].get(st) for st in NZI_SURVIVAL_STAGES))
        diffs["survival"] = {"n_common": len(common), "per_stage_mismatch": per_stage,
                             "current_stage_mismatch": sum(1 for k in common if cs1.get(k) != cs2.get(k)), "bad": bad,
                             "drift": sum(1 for k in bad if k in drift), "tie": sum(1 for k in bad if k not in drift and ties.get(k)),
                             "mapping": [k for k in bad if k not in drift and not ties.get(k)]}

    report = build_report(ids, v1, v2, raw1.get("missing_investors", []), diffs, drift, ties)
    (args.out / "report.md").write_text(report)
    (args.out / "attribution.json").write_text(json.dumps({"drift": drift, "ties": ties}, indent=1, default=str))
    for name in ("rounds", "investors", "companies", "buckets"):
        for side in (v1, v2):
            if isinstance(side.get(name), pd.DataFrame):
                side[name].to_csv(args.out / f"{name}_{side['label'].replace(' ', '_')}.csv", index=False)
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
