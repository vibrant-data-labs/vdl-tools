"""Add the recall judges' misses to the pooled reference, and report recall and F1 as a range.

Run ``pooled_reference`` first: it writes the clusters that stand for the lines of work
some model listed. Each recall judge (one per model) also names lines of work that
model's list misses. A miss can be a line another model listed, the same miss another
judge reported, or a line no model listed; one ``gpt-5.4-mini`` call per org sorts
them. Keys are ``<JUDGE_RUN>_misses<tag>|<org>``, on a prompt written here, so no
stored row is touched.

Recall is then reported three ways, all against the same lines for every model:

- high: only lines some model listed correctly (every unmatched miss is the judge's error);
- low: those plus every line the misses add (every miss is real);
- central: the added lines counted as half a line (about half the line_of_work misses
  looked real in the 2026-11-10 human review).

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_misses \\
        --dir two_model_cohort153 --tag _2m          # estimate only
    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_misses \\
        --dir two_model_cohort153 --tag _2m --run    # call for orgs not yet stored

Writes ``pooled_misses.csv`` and ``pooled_f1_counts.csv`` (per-org counts, so any
group of orgs can be re-scored) to the report folder.
"""

import argparse
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.benchmark import JUDGES_DIR
from vdl_tools.shared_tools.activity_extraction.benchmark.pooled_reference import short_names
from vdl_tools.shared_tools.database_cache.database_utils import get_session

CENTRAL_WEIGHT = 0.5
WEIGHTS = (("low", 1.0), ("central", CENTRAL_WEIGHT), ("high", 0.0))
# Names shown per listed line; a chained cluster can carry dozens.
MAX_NAMES = 6


class MissMatch(BaseModel):
    """Where one reported miss belongs; the note comes before the decision."""

    miss: int = Field(description="The miss's number, M<n>")
    note: str = Field(
        description="One sentence: which listed line or earlier miss is closest, and why it is "
        "or is not the same work"
    )
    match: Literal["listed_line", "earlier_miss", "new_line"]
    line: int | None = Field(description="For listed_line, the line's number L<n>; else null")
    same_as_miss: int | None = Field(
        description="For earlier_miss, the earlier miss's number; else null"
    )


class MissMatching(BaseModel):
    """One decision for every reported miss, in order."""

    matches: list[MissMatch]


INSTRUCTIONS = """\
You are deduplicating notes about what an organization does. Several reviewers each read \
the organization's website and a list of its activities, and each reported lines of work \
they thought a list was missing. You get the lines of work already listed (each shown by \
the names different lists gave it) and every reported miss, with the reviewer's quote \
from the website.

For each miss, in order, decide one of:
- listed_line — it is the same line of work as a listed line. Give the line's number.
- earlier_miss — it is not listed, but it is the same line of work as an earlier miss in \
this input. Give that miss's number.
- new_line — it is a line of work that neither the listed lines nor an earlier miss covers.

Same line of work means the same kind of work (doing it, funding it, training others, \
advocating, researching, selling a product), carried out by the same party, aimed at the \
same thing. Different names, programs, sites, seasons, or audiences of the same kind of \
work are the same line. A miss that is a part or a slice of a listed line is that line. \
Judge only whether the work is the same; do not judge whether the miss is real or important.

Write the note first, then the decision. Return one decision for every miss, in order.\
"""

MATCH_JUDGE = mj.Judge(
    "alignment",  # the kind of call, for the price lookup; this is its own prompt
    None,
    MissMatching,
    mj.ALIGN_MODEL,
    mj.ALIGN_REASONING,
    instructions=INSTRUCTIONS,
    prompt_name="activity_extraction_pooled_miss_matching_v1",
)


def match_key(tag: str, org_id: str) -> str:
    return f"{mj.JUDGE_RUN}_misses{tag}|{org_id}"


def collect_and_match(args, clusters: pd.DataFrame, short: dict[str, str]):
    """Every line_of_work miss per org, and the stored (or, with --run, new) matching verdicts."""
    complete = set(clusters.org_id)
    with get_session() as s:
        pool = (
            probe.load_orgs(s, probe.sample_org_ids(args.org_file))
            if args.org_file
            else probe.load_cohort(s, n_orgs=10_000)
        )
        orgs = [o for o in pool if o.org_id in complete]
        runs = mj.load_runs(s, orgs, args.routes)
        recall_judge = mj.JUDGE_SPECS["recall"]
        recall_cache = mj.make_judge_cache(s, recall_judge)
        misses = {}  # org -> [(model, MissedActivityV2)]
        for run in runs:
            pairs = mj.judge_inputs("recall", run, orgs, {}, runs[0])
            rows = mj.stored_judge_rows(s, recall_cache, recall_judge, [g for g, _ in pairs])
            for g, row in rows.items():
                v = mj.parse_verdict(row, recall_judge.response_model)
                for m in v.missed if v else []:
                    if m.kind == "line_of_work":
                        misses.setdefault(g.split("|", 1)[1], []).append((short[run.key], m))

        names = {o.org_id: mj.org_name(o.input_text) for o in orgs}
        inputs, layout = [], {}
        for org, reported in sorted(misses.items()):
            lines = clusters[clusters.org_id == org]
            listed = []
            for n, c in enumerate(lines.itertuples(), start=1):
                labels = list(dict.fromkeys(x.split(": ", 1)[1] for x in c.names.split("; ")))
                listed.append(f"L{n}. " + " / ".join(labels[:MAX_NAMES]))
            missed = [
                f'M{n}. {m.name} — quote: "{m.evidence}"'
                for n, (_, m) in enumerate(reported, start=1)
            ]
            body = (
                f"ORGANIZATION: {names[org]}\n\nLINES OF WORK ALREADY LISTED\n"
                + "\n".join(listed)
                + "\n\nREPORTED MISSES\n"
                + "\n".join(missed)
            )
            inputs.append((match_key(args.tag, org), mj.scrub(body)))
            layout[org] = (list(lines.cluster), reported)

        cache = mj.make_judge_cache(s, MATCH_JUDGE)
        before = mj.stored_judge_rows(s, cache, MATCH_JUDGE, [g for g, _ in inputs])
        todo = [p for p in inputs if p[0] not in before or before[p[0]].num_errors]
        out_tokens = mj.typical_output_tokens(s, cache.prompt.id, MATCH_JUDGE.model) or 600
        cost = mj.estimate_cost(
            MATCH_JUDGE, mj.estimate_tokens(todo, cache.prompt.prompt_str), len(todo), out_tokens
        )
        n_misses = sum(len(r) for r in misses.values())
        print(
            f"{n_misses} line_of_work misses in {len(inputs)} orgs; "
            f"{len(todo)} calls to make, ~${cost:.2f}"
        )
        if args.run and todo:
            cache.bulk_get_cache_or_run(
                todo,
                read_from_cache=True,
                write_to_cache=True,
                max_workers=8,
                max_errors=2,
                n_per_commit=20,
                **MATCH_JUDGE.kwargs(),
            )
        stored = {
            g: mj.parse_verdict(r, MissMatching)
            for g, r in mj.stored_judge_rows(s, cache, MATCH_JUDGE, [g for g, _ in inputs]).items()
        }
        s.rollback()
    return layout, stored


def resolve(layout, stored, clusters, tag):
    """Each miss to a listed cluster or a new line; the clusters it promotes; conflicts."""
    promoted, new_lines, conflicts, rows = set(), [], [], []
    unmatched_orgs = 0
    for org, (cluster_ids, reported) in layout.items():
        verdict = stored.get(match_key(tag, org))
        if verdict is None:
            unmatched_orgs += 1
            continue
        by_miss = {d.miss: d for d in verdict.matches}
        target = {}  # miss number -> ("cluster", id) or ("new", key)
        for n, (model, m) in enumerate(reported, start=1):
            d = by_miss.get(n)
            if d and d.match == "listed_line" and d.line and 1 <= d.line <= len(cluster_ids):
                target[n] = ("cluster", cluster_ids[d.line - 1])
            elif (
                d and d.match == "earlier_miss" and d.same_as_miss in target and d.same_as_miss < n
            ):
                target[n] = target[d.same_as_miss]
            else:  # new_line, or a decision the judge left out or got out of range
                target[n] = ("new", (org, n))
            kind, ref = target[n]
            rows.append(
                {
                    "org_id": org,
                    "model": model,
                    "miss": m.name,
                    "evidence": m.evidence,
                    "decision": d.match if d else "missing",
                    "note": d.note if d else "",
                    "resolved_to": f"{kind}:{ref}",
                }
            )
            if kind == "cluster":
                c = clusters.loc[ref]
                if not c.real:
                    promoted.add(ref)
                if c[f"ok_{model}"]:
                    conflicts.append((model, org, m.name, c.names))
        new_lines += sorted({ref for kind, ref in target.values() if kind == "new"})
    return promoted, new_lines, conflicts, rows, unmatched_orgs


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--run", action="store_true", help="Call for orgs not yet stored")
    parser.add_argument("--routes", nargs="+", default=list(mj.CANDIDATE_ROUTES))
    parser.add_argument("--org-file", default=None, help="A sample file instead of the cohort")
    parser.add_argument("--dir", required=True, help=f"Report folder under {JUDGES_DIR}")
    parser.add_argument(
        "--tag",
        default="",
        help="Key suffix for a different set of models on the same orgs: a matching row is "
        "keyed by org, so without it a rerun on other clusters would overwrite the stored verdict",
    )
    args = parser.parse_args(argv)
    folder = JUDGES_DIR / args.dir

    clusters = pd.read_csv(folder / "pooled_clusters.csv").reset_index(names="cluster")
    complete = set(clusters.org_id)
    short = short_names([mj.BASELINE_RUN, *(mj.run_key(r) for r in args.routes)])
    models = tuple(short.values())

    layout, stored = collect_and_match(args, clusters, short)
    if not args.run:
        return
    promoted, new_lines, conflicts, rows, unmatched_orgs = resolve(
        layout, stored, clusters, args.tag
    )
    pd.DataFrame(rows).to_csv(folder / "pooled_misses.csv", index=False)

    prec = pd.read_csv(folder / "precision.csv")
    prec = prec[prec.org_id.isin(complete) & ~prec.outcome.isin(mj.UNJUDGED)]
    prec["model"] = prec.run.map(short)
    precision = prec.groupby("model").outcome.apply(lambda o: (~o.isin(mj.ERROR_OUTCOMES)).mean())
    real = clusters[clusters.real]
    added = clusters.loc[sorted(promoted)]
    n_new = len(new_lines)

    def recall(model: str, weight: float) -> float:
        covered = real[f"ok_{model}"].sum() + weight * added[f"ok_{model}"].sum()
        return covered / (len(real) + weight * (len(added) + n_new))

    print(
        f"\n{len(complete)} orgs; {len(real)} lines some model listed correctly; the misses "
        f"add {len(added)} listed-but-not-accepted lines and {n_new} lines no model listed"
        f"{f'; {unmatched_orgs} orgs without a matching verdict' if unmatched_orgs else ''}"
    )
    print(f"miss decisions: {pd.DataFrame(rows).decision.value_counts().to_dict()}")
    table = []
    for model in models:
        P = precision[model]
        out = {"model": model, "precision": round(P, 3)}
        for label, w in WEIGHTS:
            R = recall(model, w)
            out[f"recall {label}"] = round(R, 3)
            out[f"F1 {label}"] = round(2 * P * R / (P + R), 3)
        table.append(out)
    print(pd.DataFrame(table).to_string(index=False))

    by_org = {
        org: (
            real[real.org_id == org],
            added[added.org_id == org],
            sum(1 for o, _ in new_lines if o == org),
        )
        for org in sorted(complete)  # sorted, so the bootstrap draws repeat from run to run
    }
    counts = []
    for org, (r, a, k) in by_org.items():
        row = {"org_id": org, "real_lines": len(r), "added_listed": len(a), "added_new": k}
        for model in models:
            g = prec[(prec.model == model) & (prec.org_id == org)]
            row[f"{model}_listed"] = len(g)
            row[f"{model}_ok"] = int((~g.outcome.isin(mj.ERROR_OUTCOMES)).sum())
            row[f"{model}_covers_real"] = int(r[f"ok_{model}"].sum())
            row[f"{model}_covers_added"] = int(a[f"ok_{model}"].sum())
        counts.append(row)
    pd.DataFrame(counts).to_csv(folder / "pooled_f1_counts.csv", index=False)

    p_org = prec.groupby(["model", "org_id"]).outcome.apply(
        lambda o: (~o.isin(mj.ERROR_OUTCOMES)).mean()
    )
    print("\nPer-org F1, paired vs baseline (95% CI)")
    for label, w in WEIGHTS:
        f1 = {}
        for model in models:
            vals = {}
            for org, (r, a, k) in by_org.items():
                total = len(r) + w * (len(a) + k)
                if total == 0 or (model, org) not in p_org:
                    continue
                R = (r[f"ok_{model}"].sum() + w * a[f"ok_{model}"].sum()) / total
                P = p_org[(model, org)]
                vals[org] = 2 * P * R / (P + R) if P + R else 0.0
            f1[model] = pd.Series(vals)
        cells = []
        for model in models[1:]:
            d = (f1[model] - f1["baseline"]).dropna().to_numpy()
            lo, hi = mj.paired_ci(d)
            cells.append(f"{model} {d.mean():+.3f} [{lo:+.3f}, {hi:+.3f}]")
        print(f"  {label:8s} " + "; ".join(cells))

    print(
        "\nCross-check: misses a model's own judge reported on a line the clustering says it covers"
    )
    by_model = pd.Series([m for m, *_ in conflicts]).value_counts().to_dict()
    print({m: f"{by_model.get(m, 0)} of {sum(1 for x in rows if x['model'] == m)}" for m in models})


if __name__ == "__main__":
    main()
