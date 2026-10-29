"""Score supplied subjects, or reselect stored probabilities without inference."""

import argparse
import hashlib
import json
from dataclasses import asdict
from importlib import import_module
from pathlib import Path

import pandas as pd

from ...fingerprints import fingerprint
from .engine import (
    MatchPolicy,
    ScoringResult,
    apply_policy,
    score_activities,
    score_subjects,
    select_matches,
)
from .pipeline import group_matches, rollup_paths
from .taxonomies import Taxonomy, load_taxonomy


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--activities", type=Path, help="Existing activity parquet (compatibility input)"
    )
    inputs.add_argument("--subjects", type=Path, help="Parquet with subject_id and supplied state")
    inputs.add_argument("--stored-run", type=Path, help="Reselect saved scores offline")
    tax = parser.add_mutually_exclusive_group(required=True)
    tax.add_argument(
        "--taxonomy", choices=["one_earth", "drawdown"], help="Convenience built-in loader"
    )
    tax.add_argument(
        "--taxonomy-spec",
        type=Path,
        help="JSON: name, columns, nodes with built questions; optional levels/variant/subject_kind",
    )
    parser.add_argument("--taxonomy-path", type=Path)
    parser.add_argument("--variant")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--thresholds", default="0.7,0.5")
    parser.add_argument(
        "--descend", default="auto", help="auto (selected config default), none, or probability"
    )
    parser.add_argument(
        "--namespace", default="", help="Caller namespace for subject cache identity"
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    if args.taxonomy_spec:
        raw = args.taxonomy_spec.read_bytes()
        spec = json.loads(raw)
        columns = tuple(spec["columns"])
        levels = spec.get(
            "levels",
            [
                dict(idx=i, name=c, key_col=c, output_col=c, parent_filters=list(columns[:i]))
                for i, c in enumerate(columns)
            ],
        )
        taxonomy = Taxonomy(
            spec["name"],
            columns,
            levels,
            pd.DataFrame(spec["nodes"]),
            hashlib.sha256(raw).hexdigest(),
            str(args.taxonomy_spec),
            spec.get("variant", "base"),
            spec.get("subject_kind"),
        )
        default_policy = MatchPolicy()
    else:
        if args.taxonomy_path is None:
            parser.error("--taxonomy-path is required with --taxonomy")
        taxonomy = load_taxonomy(args.taxonomy, args.taxonomy_path, variant=args.variant)
        default_policy = import_module(f"{__package__}.{args.taxonomy}").default_policy()
    descend = (
        default_policy.descend
        if args.descend == "auto"
        else (None if args.descend == "none" else float(args.descend))
    )
    policy = MatchPolicy(tuple(float(v) for v in args.thresholds.split(",")), descend)
    if args.stored_run:
        generic = (args.stored_run / "subjects.parquet").exists()
        inputs_path = args.stored_run / ("subjects.parquet" if generic else "activities.parquet")
        subjects = pd.read_parquet(inputs_path)
        scored = ScoringResult(
            pd.read_parquet(args.stored_run / "scores.parquet"),
            pd.read_parquet(args.stored_run / "usage.parquet"),
            json.loads((args.stored_run / "scoring_manifest.json").read_text()),
        )
    else:
        generic = args.subjects is not None
        subjects = pd.read_parquet(args.subjects if generic else args.activities)
        if not generic:
            if "state" not in subjects:
                from ...activity_extraction.pipeline import activity_state

                subjects["state"] = subjects.apply(activity_state, axis=1)
            if "cache_id" not in subjects:
                subjects["cache_id"] = [
                    fingerprint([str(r.uid), r.state]) for r in subjects.itertuples()
                ]
        scorer = score_subjects if generic else score_activities
        scored = scorer(subjects, taxonomy, namespace=args.namespace, max_workers=args.workers)
    paths = (select_matches if generic else apply_policy)(scored, taxonomy, policy)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    for stem, frame in (
        ("subjects" if generic else "activities", subjects),
        ("scores", scored.scores),
        ("usage", scored.usage),
        ("subject_paths" if generic else "activity_paths", paths),
    ):
        frame.to_parquet(args.output_dir / f"{stem}.parquet", index=False)
    if not generic or "entity_id" in subjects:
        grouped = (
            group_matches(paths, subjects, taxonomy)
            if generic
            else rollup_paths(paths, subjects, taxonomy)
        )
        grouped.to_json(args.output_dir / "organization_paths.json", orient="records", indent=2)
    taxonomy.nodes.to_json(args.output_dir / "nodes.json", orient="records", indent=2)
    (args.output_dir / "scoring_manifest.json").write_text(
        json.dumps(scored.lineage, indent=2) + "\n"
    )
    (args.output_dir / "selection_manifest.json").write_text(
        json.dumps(
            {
                "policy": asdict(policy),
                "policy_sha256": policy.sha256,
                "score_source": str(args.stored_run) if args.stored_run else "new_scoring",
                "max_level": len(taxonomy.columns) - 1,
            },
            indent=2,
        )
        + "\n"
    )
    print(args.output_dir)


if __name__ == "__main__":
    main()
