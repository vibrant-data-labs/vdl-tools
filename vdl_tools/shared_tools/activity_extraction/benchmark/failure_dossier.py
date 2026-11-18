"""A reading sample of errors and misses with their full texts, for naming failure modes. No calls.

Round 1 draws from ``error_analysis/`` (run ``error_analysis`` first) with errors spread
across outcome and material field and misses across data source, so rare kinds appear.
Round 2 is a plain random draw per model from what round 1 did not take, so its counts
estimate how common each kind is. For each item it writes the activity as extracted
(every field), the precision judge's full verdict (work check, field notes, field
issues) and the source around the evidence; for a miss, the recall judge's note, the
model's whole list, and the source around the miss's evidence.

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.failure_dossier --round 1
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.failure_dossier --round 2

Writes ``errors.md`` and ``misses.md`` (round 1) or ``errors_2.md`` and ``misses_2.md``,
and the sampled keys as ``sample_<round>.csv``, to ``failure_dossier/``. The tags
from reading both rounds are in ``failure_dossier/reading_notes.md``.
"""

import argparse
import re

import pandas as pd

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.benchmark import JUDGES_DIR
from vdl_tools.shared_tools.database_cache.database_utils import get_session

OUT = JUDGES_DIR / "failure_dossier"
ANALYSIS = JUDGES_DIR / "error_analysis"
DEEPSEEK = "baseten:deepseek-v4.1-flash"
NAMES = {mj.BASELINE_RUN: "baseline", mj.run_key(DEEPSEEK): "deepseek"}
SEED = 7
# Round 1 sizes; round 2 draws ROUND_2_ERRORS and ROUND_2_MISSES per model.
N_ERRORS = {"baseline": 16, "deepseek": 24}
N_MISSES = {"baseline": 12, "deepseek": 8}
ROUND_2_ERRORS, ROUND_2_MISSES = 20, 10
WINDOW = 1500
ERROR_KEY = ["model", "org_id", "activity_index"]
MISS_KEY = ["model", "org_id", "name"]


def spread(frame: pd.DataFrame, by: str, n: int) -> pd.DataFrame:
    """n rows, round-robin across the groups so rare kinds still appear."""
    groups = [g.sample(frac=1, random_state=SEED) for _, g in frame.groupby(by)]
    rows, i = [], 0
    while len(rows) < n and any(i < len(g) for g in groups):
        rows += [g.iloc[i] for g in groups if i < len(g)][: n - len(rows)]
        i += 1
    return pd.DataFrame(rows)


def key(frame: pd.DataFrame, cols: list[str]) -> pd.Series:
    return frame[cols].astype(str).agg("|".join, axis=1)


def draw(round_: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The errors and line-of-work misses to read in this round."""
    acts = pd.read_csv(ANALYSIS / "error_activities.csv")
    misses = pd.read_csv(ANALYSIS / "error_misses.csv")
    errors = acts[acts.error].copy()
    errors["stratum"] = (
        errors["outcome"] + ":" + errors["material_fields"].fillna("").str.split("; ").str[0]
    )
    lw = misses[misses.kind == "line_of_work"]
    err_sample = pd.concat(
        spread(errors[errors.model == m], "stratum", n) for m, n in N_ERRORS.items()
    )
    miss_sample = pd.concat(spread(lw[lw.model == m], "source", n) for m, n in N_MISSES.items())
    if round_ == 2:
        errors = errors[~key(errors, ERROR_KEY).isin(key(err_sample, ERROR_KEY))]
        lw = lw[~key(lw, MISS_KEY).isin(key(miss_sample, MISS_KEY))]
        err_sample = pd.concat(
            errors[errors.model == m].sample(ROUND_2_ERRORS, random_state=SEED) for m in N_ERRORS
        )
        miss_sample = pd.concat(
            lw[lw.model == m].sample(ROUND_2_MISSES, random_state=SEED) for m in N_MISSES
        )
    return err_sample, miss_sample


def load_texts(org_ids: list[str]):
    """Each org's input, both runs' profiles, and the stored precision verdicts."""
    with get_session() as s:
        stored = {o.org_id: o for o in probe.load_cohort(s, n_orgs=10_000)}
        missing = [o for o in org_ids if o not in stored]
        if missing:
            stored |= {o.org_id: o for o in probe.load_orgs(s, missing)}
        orgs = [stored[o] for o in org_ids]
        runs = mj.load_runs(s, orgs, [DEEPSEEK])
        judge = mj.JUDGE_SPECS["precision"]
        cache = mj.make_judge_cache(s, judge)
        verdicts = {}
        for run in runs:
            pairs = mj.judge_inputs("precision", run, orgs, {}, runs[0])
            for g, row in mj.stored_judge_rows(s, cache, judge, [g for g, _ in pairs]).items():
                verdicts[(NAMES[run.key], g.split("|", 1)[1])] = mj.parse_verdict(
                    row, judge.response_model
                )
        s.rollback()
    profiles = {NAMES[r.key]: r.profiles for r in runs}
    return {o.org_id: o for o in orgs}, profiles, verdicts


def excerpt(source: str, quote: str) -> str:
    """The source around the quote, or the start of the source when it isn't found."""
    words = re.findall(r"\w+", quote or "")
    at = -1
    for k in (8, 5, 3):
        if len(words) >= k:
            m = re.search(r"\W+".join(map(re.escape, words[:k])), source, re.I)
            if m:
                at = m.start()
                break
    if at < 0:
        return "(evidence not found verbatim)\n" + source[: 2 * WINDOW]
    lo, hi = max(0, at - WINDOW), min(len(source), at + WINDOW)
    return ("…" if lo else "") + source[lo:hi] + ("…" if hi < len(source) else "")


def activity_block(a) -> str:
    return "\n".join(f"- **{k}:** {v}" for k, v in a.model_dump().items())


def error_pages(err_sample, by_id, profiles, verdicts, round_: int) -> str:
    lines = [f"# Errors: reading sample, round {round_}", ""]
    for n, r in enumerate(err_sample.itertuples(), start=1):
        org = by_id[r.org_id]
        a = profiles[r.model][r.org_id].activities[r.activity_index - 1]
        v = verdicts.get((r.model, r.org_id))
        vv = (
            next((x for x in v.verdicts if x.activity_index == r.activity_index), None)
            if v
            else None
        )
        material = r.material_fields if isinstance(r.material_fields, str) else "—"
        lines += [
            f"## E{n}. {r.model} · {mj.org_name(org.input_text)} ({r.org_id})",
            f"{r.source} · {r.subject_area} · {r.input_chars} chars · outcome **{r.outcome}** "
            f"· material: {material}",
            "",
            f"### Activity {r.activity_index}",
            activity_block(a),
            "",
            "### Judge",
        ]
        if vv:
            lines += [
                f"- **label:** {vv.label} · overlap {vv.overlap} · prominence {vv.prominence} "
                f"· recurrence {vv.recurrence}",
                f"- **work_check:** {vv.work_check}",
                f"- **field_notes:** {vv.field_notes}",
            ]
            lines += [
                f"- issue: {i.field} · {i.support} · {i.impact} · “{i.claim}”"
                for i in vv.field_issues
            ]
        lines += [
            "",
            "### Source around the evidence",
            "```",
            excerpt(org.input_text, a.evidence),
            "```",
            "",
        ]
    return "\n".join(lines)


def miss_pages(miss_sample, by_id, profiles, round_: int) -> str:
    lines = [f"# Misses: reading sample, round {round_}", ""]
    for n, r in enumerate(miss_sample.itertuples(), start=1):
        org = by_id[r.org_id]
        prof = profiles[r.model].get(r.org_id)
        listed = (
            [f"{i}. {a.activity} — {a.what_happens}" for i, a in enumerate(prof.activities, 1)]
            if prof
            else []
        )
        lines += [
            f"## M{n}. {r.model} · {mj.org_name(org.input_text)} ({r.org_id})",
            f"{r.source} · {r.subject_area} · {r.input_chars} chars "
            f"· judge prominence {r.prominence}",
            "",
            f"- **missed:** {r.name}",
            f"- **evidence:** {r.evidence}",
            f"- **nearest listed:** {r.nearest_activity}",
            f"- **difference:** {r.difference}",
            "",
            "### The model's list",
            *listed,
            "",
            "### Source around the miss",
            "```",
            excerpt(org.input_text, r.evidence),
            "```",
            "",
        ]
    return "\n".join(lines)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--round", type=int, choices=(1, 2), default=1)
    round_ = parser.parse_args(argv).round
    suffix = "" if round_ == 1 else f"_{round_}"

    err_sample, miss_sample = draw(round_)
    OUT.mkdir(parents=True, exist_ok=True)
    pd.concat(
        [err_sample[ERROR_KEY].assign(item="error"), miss_sample[MISS_KEY].assign(item="miss")]
    ).to_csv(OUT / f"sample_{round_}.csv", index=False)
    by_id, profiles, verdicts = load_texts(sorted(set(err_sample.org_id) | set(miss_sample.org_id)))
    (OUT / f"errors{suffix}.md").write_text(
        error_pages(err_sample, by_id, profiles, verdicts, round_)
    )
    (OUT / f"misses{suffix}.md").write_text(miss_pages(miss_sample, by_id, profiles, round_))
    print(f"{len(err_sample)} errors, {len(miss_sample)} misses -> {OUT}")


if __name__ == "__main__":
    main()
