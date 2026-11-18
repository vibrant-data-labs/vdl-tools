"""Model-by-model agreement and a pooled reference of lines of work, from stored rows only.

Reads the pairwise alignments (baseline against each candidate from ``model_judges``;
between two candidates, keys ``<JUDGE_RUN>_x_<a>__<b>``), links activities that any
alignment puts in one group, and treats each connected cluster as one line of work. A
cluster is a real line when at least one model has an activity in it that the
precision judge accepted as ``line_of_work``. Each model's pooled recall is the share
of an org's real lines it covers with an accepted activity. Lines no model listed (the
recall judges' misses) are added by ``pooled_misses``.

Union-find chains: 61 of 722 clusters on the four-model cohort run had more than 8
members, so cluster coverage is generous to every model alike.

Run::

    python -m vdl_tools.shared_tools.activity_extraction.benchmark.pooled_reference \\
        --dir two_model_cohort153 [--org-file data_out/activity_extraction_probe/sample300.json]

Writes ``pooled_members.csv`` and ``pooled_clusters.csv`` to the report folder, which
must hold the ``precision.csv`` that ``model_judges`` wrote for the same orgs and routes.
"""

import argparse
from itertools import combinations
from math import comb

import numpy as np
import pandas as pd

from vdl_tools.shared_tools.activity_extraction import model_judges as mj
from vdl_tools.shared_tools.activity_extraction import model_probe as probe
from vdl_tools.shared_tools.activity_extraction.benchmark import JUDGES_DIR
from vdl_tools.shared_tools.database_cache.database_utils import get_session


def short_names(run_keys: list[str]) -> dict[str, str]:
    """Run keys to report names: 'baseten-deepseek-v4.1-flash' -> 'deepseek'."""
    return {k: ("baseline" if k == mj.BASELINE_RUN else k.split("-")[1]) for k in run_keys}


def groups(alignment: mj.ListAlignment, n_a: int, n_b: int) -> list[tuple[list, list]]:
    """Groups with the scorer's repair: bad or repeated indices dropped, left-outs added."""
    seen_a, seen_b, out = set(), set(), []
    for g in alignment.groups:
        ga = [i for i in g.a if 1 <= i <= n_a and i not in seen_a]
        gb = [i for i in g.b if 1 <= i <= n_b and i not in seen_b]
        seen_a |= set(ga)
        seen_b |= set(gb)
        if ga or gb:
            out.append((ga, gb))
    out += [([i], []) for i in set(range(1, n_a + 1)) - seen_a]
    out += [([], [i]) for i in set(range(1, n_b + 1)) - seen_b]
    return out


def pair_key(a: str, b: str) -> str:
    """The given_id prefix of the stored alignment between two runs."""
    return f"{mj.JUDGE_RUN}_{b}" if a == mj.BASELINE_RUN else f"{mj.JUDGE_RUN}_x_{a}__{b}"


def load_alignments(routes: list[str], org_file: str | None):
    """The runs (baseline first), every org, and each stored pairwise alignment by key."""
    with get_session() as s:
        if org_file:
            orgs = probe.load_orgs(s, probe.sample_org_ids(org_file))
        else:
            orgs = probe.load_cohort(s, n_orgs=10_000)
        runs = {r.key: r for r in mj.load_runs(s, orgs, routes)}
        judge = mj.JUDGE_SPECS["alignment"]
        cache = mj.make_judge_cache(s, judge)
        gid = {
            f"{pair_key(a, b)}|{o.org_id}": (a, b, o.org_id)
            for a, b in combinations(list(runs), 2)
            for o in orgs
        }
        stored = {
            g: mj.parse_verdict(r, mj.ListAlignment)
            for g, r in mj.stored_judge_rows(s, cache, judge, list(gid)).items()
        }
        s.rollback()
    return runs, gid, stored


def cluster_org(org, keys, runs, edges, outcome, short):
    """Union-find over one org's activities; one row per cluster and one per member."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for k in keys:
        for i in range(1, len(runs[k].profiles[org].activities) + 1):
            find((k, i))
    for nodes in edges[org]:
        for n in nodes[1:]:
            parent[find(n)] = find(nodes[0])
    comps = {}
    for n in list(parent):
        comps.setdefault(find(n), []).append(n)
    clusters, members = [], []
    for nodes in comps.values():
        status = {n: outcome.get((n[0], org, n[1]), ("unjudged", None)) for n in nodes}
        ok = {n for n, (oc, _) in status.items() if oc not in mj.ERROR_OUTCOMES + mj.UNJUDGED}
        row = {
            "org_id": org,
            "size": len(nodes),
            "real": any(status[n][1] == "line_of_work" for n in ok),
            "names": "; ".join(
                f"{short[k]}: {runs[k].profiles[org].activities[i - 1].activity}"
                for k, i in sorted(nodes)
            ),
        }
        for k in keys:
            row[f"has_{short[k]}"] = any(n[0] == k for n in nodes)
            row[f"ok_{short[k]}"] = any(n[0] == k for n in ok)
        clusters.append(row)
        members.append(([n for n in nodes], len({k for k, _ in nodes})))
    return clusters, members


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--routes", nargs="+", default=list(mj.CANDIDATE_ROUTES))
    parser.add_argument("--org-file", default=None, help="A sample file instead of the cohort")
    parser.add_argument("--dir", required=True, help=f"Report folder under {JUDGES_DIR}")
    args = parser.parse_args(argv)
    folder = JUDGES_DIR / args.dir
    prec = pd.read_csv(folder / "precision.csv")
    outcome = {(r.run, r.org_id, r.activity_index): (r.outcome, r.label) for r in prec.itertuples()}

    runs, gid, stored = load_alignments(args.routes, args.org_file)
    keys = list(runs)
    short = short_names(keys)

    pair_scores, edges = [], {}
    for g, (a, b, org) in gid.items():
        al = stored.get(g)
        pa, pb = runs[a].profiles.get(org), runs[b].profiles.get(org)
        if al is None or not (pa and pa.activities and pb and pb.activities):
            continue
        n_a, n_b = len(pa.activities), len(pb.activities)
        pair_scores.append({"a": a, "b": b, "org_id": org, **mj.score_alignment(n_a, n_b, al)})
        for ga, gb in groups(al, n_a, n_b):
            edges.setdefault(org, []).append([(a, i) for i in ga] + [(b, i) for i in gb])

    ps = pd.DataFrame(pair_scores)
    print("Pairwise content / granularity agreement (mean per org)")
    for metric in ("content_agreement", "granularity_agreement"):
        names = [short[k] for k in keys]
        m = pd.DataFrame(index=names, columns=names, dtype=float)
        for (a, b), g in ps.groupby(["a", "b"]):
            m.loc[short[a], short[b]] = m.loc[short[b], short[a]] = g[metric].mean()
        print(f"\n{metric}\n{m.round(3).to_string()}")
    print("\norgs per pair:", ps.groupby(["a", "b"]).size().to_dict())

    # Only orgs where every pair of runs has an alignment.
    complete = [o for o, g in ps.groupby("org_id") if len(g) == comb(len(keys), 2)]
    clusters, rows = [], []
    for org in complete:
        org_clusters, org_members = cluster_org(org, keys, runs, edges, outcome, short)
        for row, (nodes, n_models) in zip(org_clusters, org_members, strict=True):
            clusters.append(row)
            rows += [
                {
                    "org_id": org,
                    "run": k,
                    "activity_index": i,
                    "cluster": len(clusters) - 1,
                    "n_models": n_models,
                    "cluster_size": len(nodes),
                }
                for k, i in nodes
            ]
    cl = pd.DataFrame(clusters)
    pd.DataFrame(rows).to_csv(folder / "pooled_members.csv", index=False)
    cl.to_csv(folder / "pooled_clusters.csv", index=False)
    print(
        f"\n{len(complete)} orgs with every pairwise alignment; {len(cl)} clusters, "
        f"{cl.real.sum()} real lines ({cl.real.sum() / len(complete):.2f} per org); cluster "
        f"size median {cl['size'].median():.0f}, max {cl['size'].max()}, "
        f">8 members: {(cl['size'] > 8).sum()}"
    )

    real = cl[cl.real]
    print("\nReal lines listed by how many models (any activity / an accepted activity)")
    n_any = real[[f"has_{short[k]}" for k in keys]].sum(axis=1)
    n_ok = real[[f"ok_{short[k]}" for k in keys]].sum(axis=1)
    print(
        pd.DataFrame(
            {"any": n_any.value_counts().sort_index(), "accepted": n_ok.value_counts().sort_index()}
        )
    )
    solo = real[n_ok == 1]
    print("\nReal lines only one model covers (accepted), by model")
    print({short[k]: int(solo[f"ok_{short[k]}"].sum()) for k in keys})

    # Pooled precision, recall and F1, before the misses are added.
    p = prec[prec.org_id.isin(complete) & ~prec.outcome.isin(mj.UNJUDGED)]
    out, per_org = [], []
    for k in keys:
        pk = p[p.run == k]
        P = (~pk.outcome.isin(mj.ERROR_OUTCOMES)).mean()
        R = real[f"ok_{short[k]}"].mean()
        out.append(
            {
                "model": short[k],
                "precision": round(P, 3),
                "pooled recall": round(R, 3),
                "F1": round(2 * P * R / (P + R), 3),
                "lines covered / org": round(real[f"ok_{short[k]}"].sum() / len(complete), 2),
            }
        )
        g_p = pk.groupby("org_id").outcome.apply(lambda o: (~o.isin(mj.ERROR_OUTCOMES)).mean())
        g_r = real.groupby("org_id")[f"ok_{short[k]}"].mean()
        j = pd.concat([g_p.rename("P"), g_r.rename("R")], axis=1).dropna()
        j["F1"] = np.where(j.P + j.R > 0, 2 * j.P * j.R / (j.P + j.R), 0.0)
        per_org.append(j.F1.rename(short[k]))
    print(f"\nPooled reference, {real.shape[0]} real lines in {len(complete)} orgs (micro)")
    print(pd.DataFrame(out).to_string(index=False))
    f1 = pd.concat(per_org, axis=1)
    print("\nPer-org F1, paired vs baseline")
    for k in keys[1:]:
        d = (f1[short[k]] - f1["baseline"]).dropna().to_numpy()
        lo, hi = mj.paired_ci(d)
        print(
            f"  {short[k]}: {f1[short[k]].mean():.3f} vs {f1['baseline'].mean():.3f}, "
            f"diff {d.mean():+.3f} [{lo:+.3f}, {hi:+.3f}], n={len(d)}"
        )


if __name__ == "__main__":
    main()
