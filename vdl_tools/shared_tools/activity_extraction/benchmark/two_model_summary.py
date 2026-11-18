"""Baseline (gpt-5.4-mini medium) against DeepSeek V4.1 Flash on the 153 cohort and the 300 sample.

Reads the judge outputs and pooled-F1 counts in ``two_model_cohort153/`` and
``two_model_sample300/`` (run ``model_judges``, ``pooled_reference`` and
``pooled_misses`` for each first) and the file ``latency_price`` writes; makes no
calls. Writes ``two_model_summary.md`` to the judges' report folder.

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.two_model_summary
"""

import json

import numpy as np
import pandas as pd

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction.benchmark import (
    JUDGES_DIR,
    SAMPLE_FILE,
    TWO_MODEL_DIRS,
)

DEEPSEEK_RUN = "baseten-deepseek-v4.1-flash"
WEIGHTS = (("low", 1.0), ("central", 0.5), ("high", 0.0))
GROUPS = (
    "all 453",
    "153 cohort (mostly Candid nonprofits)",
    "300 sample",
    "300: Crunchbase for-profits",
    "300: Candid nonprofits",
)
STRATA = {
    "300: Crunchbase for-profits": "crunchbase_for_profit",
    "300: Candid nonprofits": "candid_nonprofit",
}


def load_frames() -> dict[str, dict[str, pd.DataFrame]]:
    return {
        name: {
            table: pd.read_csv(JUDGES_DIR / folder / f"{file}.csv")
            for table, file in (
                ("precision", "precision"),
                ("recall", "recall"),
                ("counts", "pooled_f1_counts"),
            )
        }
        for name, folder in TWO_MODEL_DIRS.items()
    }


def group_orgs(frames, strata: dict[str, str], group: str) -> dict[str, set]:
    """The org ids in a reporting group, per source folder."""
    c153 = set(frames["cohort153"]["recall"].org_id)
    s300 = set(frames["sample300"]["recall"].org_id)
    if group == "153 cohort (mostly Candid nonprofits)":
        return {"cohort153": c153}
    if group == "300 sample":
        return {"sample300": s300}
    if group == "all 453":
        return {"cohort153": c153, "sample300": s300}
    return {"sample300": {o for o in s300 if strata.get(o) == STRATA[group]}}


def collect(frames, strata, group: str, table: str) -> pd.DataFrame:
    parts = [
        frames[src][table][frames[src][table].org_id.isin(ids)]
        for src, ids in group_orgs(frames, strata, group).items()
    ]
    return pd.concat(parts, ignore_index=True)


def f1(p: float, r: float) -> float:
    return 2 * p * r / (p + r) if p + r else 0.0


def pooled(counts: pd.DataFrame, model: str, w: float) -> tuple[float, float, float]:
    """Micro precision, recall and F1 with the misses' added lines weighted w."""
    p = counts[f"{model}_ok"].sum() / counts[f"{model}_listed"].sum()
    covered = counts[f"{model}_covers_real"].sum() + w * counts[f"{model}_covers_added"].sum()
    total = counts.real_lines.sum() + w * (counts.added_listed.sum() + counts.added_new.sum())
    r = covered / total
    return p, r, f1(p, r)


def per_org_f1(counts: pd.DataFrame, model: str, w: float) -> pd.Series:
    out = {}
    for row in counts.itertuples():
        listed = getattr(row, f"{model}_listed")
        total = row.real_lines + w * (row.added_listed + row.added_new)
        if not listed or not total:
            continue
        p = getattr(row, f"{model}_ok") / listed
        r = (
            getattr(row, f"{model}_covers_real") + w * getattr(row, f"{model}_covers_added")
        ) / total
        out[row.org_id] = f1(p, r)
    return pd.Series(out)


def ci(d: np.ndarray) -> str:
    lo, hi = mj.paired_ci(d)
    return f"{d.mean():+.3f} [{lo:+.3f}, {hi:+.3f}]"


def main() -> None:
    strata = {o["org_id"]: o["stratum"] for o in json.loads(SAMPLE_FILE.read_text())["orgs"]}
    frames = load_frames()
    lines = [
        "# Baseline (gpt-5.4-mini medium) vs DeepSeek V4.1 Flash",
        "",
        "Judges: precision v3.2 and recall v2.1 (gpt-6-sol), alignment and miss matching "
        "gpt-5.4-mini medium. Pooled recall is against one shared list of lines of work per "
        "org: high counts only lines a model listed correctly, low adds every line the recall "
        "judges found that no list had, central counts those as half a line. Intervals are "
        "per-org paired bootstrap 95% CIs (DeepSeek − baseline).",
        "",
        "## Accuracy",
        "",
        "| group | orgs | baseline errors | DeepSeek errors | error share diff "
        "| baseline missed lines/org | DeepSeek missed lines/org | missed diff |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for g in GROUPS:
        p, r = collect(frames, strata, g, "precision"), collect(frames, strata, g, "recall")
        p = p[~p.outcome.isin(mj.UNJUDGED)].copy()
        p["err"] = p.outcome.isin(mj.ERROR_OUTCOMES)
        share = p.groupby(["run", "org_id"]).err.mean().unstack(0).dropna()
        r = r[r.judged]
        miss = r.pivot(index="org_id", columns="run", values="missed_line_of_work").dropna()
        lines.append(
            f"| {g} | {r.org_id.nunique()} | {p[p.run == 'baseline'].err.mean():.1%} "
            f"| {p[p.run == DEEPSEEK_RUN].err.mean():.1%} "
            f"| {ci((share[DEEPSEEK_RUN] - share['baseline']).to_numpy())} "
            f"| {miss['baseline'].mean():.2f} | {miss[DEEPSEEK_RUN].mean():.2f} "
            f"| {ci((miss[DEEPSEEK_RUN] - miss['baseline']).to_numpy())} |"
        )
    lines += [
        "",
        "## Pooled F1 (precision; recall and F1 as low / central / high)",
        "",
        "| group | baseline P | baseline recall | baseline F1 | DeepSeek P | DeepSeek recall "
        "| DeepSeek F1 | per-org F1 diff, central |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for g in GROUPS:
        c = collect(frames, strata, g, "counts")
        cells = []
        for model in ("baseline", "deepseek"):
            res = [pooled(c, model, w) for _, w in WEIGHTS]
            cells += [
                f"{res[0][0]:.3f}",
                " / ".join(f"{x[1]:.3f}" for x in res),
                " / ".join(f"{x[2]:.3f}" for x in res),
            ]
        diff = (per_org_f1(c, "deepseek", 0.5) - per_org_f1(c, "baseline", 0.5)).dropna()
        lines.append(f"| {g} | " + " | ".join(cells) + f" | {ci(diff.to_numpy())} |")

    lat = pd.read_csv(JUDGES_DIR / "two_model_sample300" / "latency_price.csv")
    lines += [
        "",
        "## Latency and price (300 sample, both models run at the same time, "
        "8 parallel calls each)",
        "",
        "| model | group | orgs | latency median s | latency p90 s | $ per 1k orgs "
        "| reasoning tok (med) |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in lat.to_dict("records"):
        lines.append(
            f"| {row['model']} | {row['group']} | {row['orgs']} | {row['latency median s']} "
            f"| {row['latency p90 s']} | {row['$ per 1k orgs']:.2f} "
            f"| {row['reasoning tok (med)']} |"
        )
    lines += [
        "",
        "On the 153 cohort only DeepSeek has wall-clock timings (2026-11-12, 4 parallel calls); "
        "the baseline there has OpenAI server timestamps only, which on the 300 read about 1.5x "
        "the wall-clock re-run.",
    ]
    out = JUDGES_DIR / "two_model_summary.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
