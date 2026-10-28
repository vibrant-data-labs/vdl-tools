"""Score existing parsed activities, or reselect stored scores without inference."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from ...activity_extraction.pipeline import activity_state, fingerprint
from .engine import MatchPolicy, ScoringResult, apply_policy, score_activities
from .pipeline import rollup_paths
from .taxonomies import load_taxonomy


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--activities", type=Path, help="Parsed activity parquet (required when scoring)"
    )
    parser.add_argument("--taxonomy", choices=["one_earth", "drawdown"], required=True)
    parser.add_argument("--taxonomy-path", type=Path, required=True)
    parser.add_argument("--variant")
    parser.add_argument(
        "--stored-run", type=Path, help="Reselect these scores offline; never calls a model"
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="New, nonexisting run directory"
    )
    parser.add_argument("--thresholds", default="0.7,0.5")
    parser.add_argument(
        "--descend", default="auto", help="auto (Drawdown 0.6), none, or probability"
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    if args.activities is None and args.stored_run is None:
        parser.error("--activities or --stored-run is required")
    if args.activities is not None and args.stored_run is not None:
        parser.error("--activities and --stored-run are mutually exclusive")
    taxonomy = load_taxonomy(args.taxonomy, args.taxonomy_path, variant=args.variant)
    descend = (
        (0.6 if args.taxonomy == "drawdown" else None)
        if args.descend == "auto"
        else None
        if args.descend == "none"
        else float(args.descend)
    )
    policy = MatchPolicy(tuple(float(v) for v in args.thresholds.split(",")), descend)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    if args.stored_run:
        activities = pd.read_parquet(args.stored_run / "activities.parquet")
        scored = ScoringResult(
            pd.read_parquet(args.stored_run / "scores.parquet"),
            pd.read_parquet(args.stored_run / "usage.parquet"),
            json.loads((args.stored_run / "scoring_manifest.json").read_text()),
        )
    else:
        activities = pd.read_parquet(args.activities)
        if "state" not in activities:
            activities["state"] = activities.apply(activity_state, axis=1)
        if "cache_id" not in activities:
            activities["cache_id"] = [
                fingerprint([str(r.uid), r.state]) for r in activities.itertuples()
            ]
        scored = score_activities(activities, taxonomy, max_workers=args.workers)
    paths = apply_policy(scored, taxonomy, policy)
    org_paths = rollup_paths(paths, activities, taxonomy)
    for stem, frame in (
        ("activities", activities),
        ("scores", scored.scores),
        ("usage", scored.usage),
        ("activity_paths", paths),
    ):
        frame.to_parquet(args.output_dir / f"{stem}.parquet", index=False)
    taxonomy.nodes.to_json(args.output_dir / "nodes.json", orient="records", indent=2)
    org_paths.to_json(args.output_dir / "organization_paths.json", orient="records", indent=2)
    (args.output_dir / "scoring_manifest.json").write_text(
        json.dumps(scored.lineage, indent=2) + "\n"
    )
    (args.output_dir / "selection_manifest.json").write_text(
        json.dumps(
            {
                "policy": asdict(policy),
                "policy_sha256": policy.sha256,
                "score_source": str(args.stored_run) if args.stored_run else "new_scoring",
                "rollup": "all_activity_union",
                "max_level": 2,
            },
            indent=2,
        )
        + "\n"
    )
    print(args.output_dir)


if __name__ == "__main__":
    main()
