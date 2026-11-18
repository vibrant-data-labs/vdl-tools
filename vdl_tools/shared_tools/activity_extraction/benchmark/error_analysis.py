"""Where the errors and misses are: baseline against DeepSeek V4.1 on the 453 orgs. No calls.

Joins the stored judge verdicts (``two_model_cohort153/`` and ``two_model_sample300/``)
with each activity's own fields and each org's attributes: data source, One Earth
subject area (the climate tracker's ``org_classification``, dimension 'oe', largest
fraction), input size, and the baseline's coverage rating. Prints the breakdowns and
writes ``error_activities.csv``, ``error_orgs.csv`` and ``error_misses.csv`` to
``error_analysis/`` for browsing; ``failure_dossier`` samples from them.

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.error_analysis
"""

import json
import re

import pandas as pd
from sqlalchemy import text

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.benchmark import (
    JUDGES_DIR,
    SAMPLE_FILE,
    TWO_MODEL_DIRS,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session

OUT = JUDGES_DIR / "error_analysis"
DEEPSEEK = "baseten:deepseek-v4.1-flash"
NAMES = {mj.BASELINE_RUN: "baseline", mj.run_key(DEEPSEEK): "deepseek"}
SOURCES = {
    "crunchbase_for_profit": "Crunchbase For Profit",
    "candid_nonprofit": "Candid Non Profit",
}
# A keyword match on a miss's name; treat it as a hint (many misses match none).
THEMES = [
    ("research & monitoring", r"research|monitor|stud|survey|data|science"),
    ("education & training", r"educat|train|workshop|curricul|school|course|class|teach"),
    ("advocacy & policy", r"advoca|policy|legislat|lobby|campaign"),
    ("grants & financing", r"grant|fund|financ|loan|invest|scholarship"),
    ("land & habitat work", r"restor|habitat|conserv|land|easement|preserve|stewardship|trail"),
    (
        "products & services",
        r"product|service|sale|sell|software|platform|manufactur|install|consult",
    ),
    ("events & outreach", r"event|festival|tour|outreach|volunteer|community"),
    ("publishing & media", r"publish|newsletter|media|podcast|report|magazine|blog"),
]


def theme(name: str) -> str:
    n = name.lower()
    return next((t for t, p in THEMES if re.search(p, n)), "other")


def show(title: str, frame) -> None:
    print(f"\n=== {title} ===")
    print(frame.to_string())


def rate(frame: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Error share and counts per group and model, side by side."""
    g = frame.groupby(by + ["model"]).agg(n=("error", "size"), errors=("error", "sum"))
    g["share"] = (g["errors"] / g["n"]).round(3)
    return g.unstack("model")[["n", "errors", "share"]]


def build_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Per-activity, per-org and per-miss tables, written to OUT and returned."""
    schema = json.loads(SAMPLE_FILE.read_text())["source_schema"]
    precision = pd.concat(
        [pd.read_csv(JUDGES_DIR / d / "precision.csv") for d in TWO_MODEL_DIRS.values()]
    )
    precision["model"] = precision["run"].map(NAMES)
    precision = precision[~precision["outcome"].isin(mj.UNJUDGED)]
    precision["error"] = precision["outcome"].isin(mj.ERROR_OUTCOMES)
    strata = {o["org_id"]: o["stratum"] for o in json.loads(SAMPLE_FILE.read_text())["orgs"]}

    with get_session() as s:
        orgs = probe.load_orgs(s, list(strata)) + probe.load_cohort(s, n_orgs=10_000)
        runs = mj.load_runs(s, orgs, [DEEPSEEK])
        recall_judge = mj.JUDGE_SPECS["recall"]
        cache = mj.make_judge_cache(s, recall_judge)
        misses = []
        for run in runs:
            pairs = mj.judge_inputs("recall", run, orgs, {}, runs[0])
            rows = mj.stored_judge_rows(s, cache, recall_judge, [g for g, _ in pairs])
            for g, row in rows.items():
                v = mj.parse_verdict(row, recall_judge.response_model)
                for m in v.missed if v else []:
                    misses.append(
                        {"model": NAMES[run.key], "org_id": g.split("|", 1)[1], **m.model_dump()}
                    )
        ids = [o.org_id for o in orgs]
        meta = s.execute(
            text(
                f"select uid, data_source, org_type from {schema}.organization where uid = any(:i)"
            ),
            {"i": ids},
        ).all()
        subject = s.execute(
            text(f"""
                select distinct on (org_uid) org_uid, level0 from {schema}.org_classification
                where org_uid = any(:i) and dimension = 'oe' order by org_uid, fraction desc
                """),
            {"i": ids},
        ).all()
        s.rollback()

    profiles = {r.key: r.profiles for r in runs}
    source = {u: f"{d} {t}" for u, d, t in meta}
    area = dict(subject)
    org_rows = []
    for o in orgs:
        base = profiles[mj.BASELINE_RUN].get(o.org_id)
        org_rows.append(
            {
                "org_id": o.org_id,
                "sample": "sample300" if o.org_id in strata else "cohort153",
                "source": SOURCES.get(strata.get(o.org_id)) or source.get(o.org_id, "unknown"),
                "subject_area": area.get(o.org_id, "unknown"),
                "input_chars": len(o.input_text),
                "pages": len(re.findall(r"^URL: https?://", o.input_text, re.M)),
                "coverage": base.coverage if base else None,
            }
        )
    org_info = pd.DataFrame(org_rows).set_index("org_id")

    # Each activity's own fields, from the extraction itself.
    fields = [
        {
            "model": model,
            "org_id": org_id,
            "activity_index": i,
            "activity": a.activity,
            "sector": a.sector,
            "certainty": a.certainty,
        }
        for key, model in NAMES.items()
        for org_id, prof in profiles[key].items()
        for i, a in enumerate(prof.activities, start=1)
    ]
    acts = precision.merge(
        pd.DataFrame(fields), on=["model", "org_id", "activity_index"], how="left"
    ).join(org_info, on="org_id")
    per_org = (
        acts.groupby(["model", "org_id"])
        .agg(activities=("error", "size"), errors=("error", "sum"))
        .reset_index()
        .join(org_info, on="org_id")
    )
    miss = pd.DataFrame(misses).join(org_info, on="org_id")
    miss = miss[miss["org_id"].isin(set(acts["org_id"]))]

    OUT.mkdir(parents=True, exist_ok=True)
    acts.to_csv(OUT / "error_activities.csv", index=False)
    per_org.to_csv(OUT / "error_orgs.csv", index=False)
    miss.to_csv(OUT / "error_misses.csv", index=False)
    return acts, per_org, miss


def report(acts: pd.DataFrame, per_org: pd.DataFrame, miss: pd.DataFrame) -> None:
    # 1. Errors per org against activities per org.
    per_org = per_org.copy()
    per_org["listed"] = pd.cut(
        per_org["activities"], [0, 1, 2, 3, 5, 8, 100], labels=["1", "2", "3", "4-5", "6-8", "9+"]
    )
    t = (
        per_org.groupby(["listed", "model"], observed=True)
        .agg(
            orgs=("errors", "size"),
            errors_per_org=("errors", "mean"),
            error_share=("errors", "sum"),
        )
        .unstack("model")
    )
    for m in NAMES.values():
        n = per_org[per_org.model == m].groupby("listed", observed=True)["activities"].sum()
        t[("error_share", m)] = (t[("error_share", m)] / n).round(3)
    show("1a. Errors by activities listed per org", t.round(2))
    for m in NAMES.values():
        g = per_org[per_org.model == m]
        srt = g["errors"].sort_values(ascending=False).cumsum() / max(g["errors"].sum(), 1)
        k = int((srt < 0.5).sum()) + 1
        print(
            f"  {m}: corr(activities, errors) {g['activities'].corr(g['errors']):.2f}; "
            f"orgs with no error {(g['errors'] == 0).mean():.0%}; "
            f"half of all errors in {k} orgs ({k / len(g):.0%} of orgs)"
        )

    # 2. By kind of org.
    acts = acts.copy()
    show("2a. By data source", rate(acts, ["source"]))
    show("2b. By subject area", rate(acts, ["subject_area"]))
    acts["input_size"] = pd.cut(
        acts["input_chars"], [0, 3000, 8000, 20000, 1e9], labels=["<3k", "3-8k", "8-20k", "20k+"]
    )
    show("2c. By input size (chars)", rate(acts, ["input_size"]))
    show("2d. By the baseline's coverage rating", rate(acts, ["coverage"]))

    # 3. By kind of activity.
    show("3a. By the model's own prominence", rate(acts, ["model_prominence"]))
    show("3b. By the judge's prominence", rate(acts, ["judge_prominence"]))
    show("3c. By the model's own recurrence", rate(acts, ["model_recurrence"]))
    show("3d. By the judge's label", rate(acts, ["label"]))
    show("3e. By the model's certainty", rate(acts, ["certainty"]))
    core = acts[
        (acts.judge_prominence == "core") & acts.judge_recurrence.isin(["ongoing", "recurring"])
    ]
    print("\n  Errors that sit in core, running work (judge's view), share of each model's errors:")
    for m in NAMES.values():
        n_err = len(acts[(acts.model == m) & acts.error])
        n_core = len(core[(core.model == m) & core.error])
        print(f"   {m}: {n_core} of {n_err} ({n_core / max(n_err, 1):.0%})")
    err = acts[acts.error]
    show("3f. Error types", err.groupby(["outcome", "model"]).size().unstack("model").fillna(0))
    mf = err.assign(field=err["material_fields"].fillna("").str.split("; ")).explode("field")
    mf = mf[mf["field"] != ""]
    show(
        "3g. Fields with a material issue, among errors",
        mf.groupby(["field", "model"])
        .size()
        .unstack("model")
        .fillna(0)
        .sort_values("deepseek", ascending=False),
    )
    show(
        "3h. Material fields by data source (DeepSeek)",
        mf[mf.model == "deepseek"].groupby(["source", "field"]).size().unstack("field").fillna(0),
    )

    # 4. Misses.
    lw = miss[miss["kind"] == "line_of_work"]
    show(
        "4a. Missed lines of work by the judge's prominence of the miss",
        lw.groupby(["prominence", "model"]).size().unstack("model").fillna(0),
    )
    orgs_per = per_org.groupby(["model", "source"]).size().unstack("model")
    by_src = (
        lw.groupby(["source", "model"]).size().unstack("model").reindex(orgs_per.index).fillna(0)
    )
    show("4b. Missed lines of work per org, by data source", (by_src / orgs_per).round(2))
    orgs_area = per_org.groupby(["model", "subject_area"]).size().unstack("model")
    by_area = (
        lw.groupby(["subject_area", "model"])
        .size()
        .unstack("model")
        .reindex(orgs_area.index)
        .fillna(0)
    )
    show("4c. Missed lines of work per org, by subject area", (by_area / orgs_area).round(2))
    lw = lw.assign(theme=lw["name"].map(theme))
    show(
        "4d. Missed lines of work by theme (keyword match on the miss's name)",
        lw.groupby(["theme", "model"])
        .size()
        .unstack("model")
        .fillna(0)
        .sort_values("baseline", ascending=False),
    )


def main() -> None:
    report(*build_tables())


if __name__ == "__main__":
    main()
