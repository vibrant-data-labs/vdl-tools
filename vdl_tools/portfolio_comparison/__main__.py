"""CLI for engagement stages, run from an engagement repo root:

    python -m vdl_tools.portfolio_comparison pin-baseline
    python -m vdl_tools.portfolio_comparison intake
    python -m vdl_tools.portfolio_comparison match
    python -m vdl_tools.portfolio_comparison status
"""

import argparse
import json
from pathlib import Path

from vdl_tools.portfolio_comparison import run as runners


def main():
    parser = argparse.ArgumentParser(prog="python -m vdl_tools.portfolio_comparison")
    parser.add_argument(
        "stage",
        choices=["pin-baseline", "intake", "match", "status",
                 "export-customer", "import-customer", "set-id", "finalize",
                 "enrich", "enrich-acquire", "enrich-scrape", "enrich-summarize"],
    )
    parser.add_argument(
        "--root", default=".", help="engagement repo root (default: cwd)"
    )
    parser.add_argument(
        "--file", default=None, help="filled-in spreadsheet (import-customer)"
    )
    setid = parser.add_argument_group("set-id", "record an out-of-process source id")
    setid.add_argument("--row", default=None, help="customer_row_id")
    setid.add_argument("--name", default=None, help="customer name (must match one row)")
    setid.add_argument("--cb-id", default=None, help="Crunchbase uuid")
    setid.add_argument("--nzi-id", default=None, help="Netzero Insights id")
    setid.add_argument("--coresignal-id", default=None, help="Coresignal org id")
    setid.add_argument("--by", default=None, help="who found it (e.g. zein)")
    setid.add_argument("--note", default="", help="where/how it was found")
    setid.add_argument(
        "--no-verify", action="store_true",
        help="skip the live source-API check of the id",
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()
    if not (root / "engagement.yaml").exists():
        hint = (
            " — this looks like the TEMPLATE repo; run from your engagement "
            "repo (e.g. cd ~/dev/vdl/engagement-<customer>)"
            if (root / "engagement.yaml.example").exists()
            else " — run from an engagement repo root (or pass --root)"
        )
        parser.error(f"no engagement.yaml in {root}{hint}")

    if args.stage == "pin-baseline":
        universe = runners.run_pin_baseline(root)
        print(f"baseline universe: {len(universe)} orgs")
    elif args.stage == "intake":
        payload = runners.run_intake(root)
        print(json.dumps(payload["preflight"], indent=2))
    elif args.stage == "match":
        id_mapping = runners.run_match(root)
        print(id_mapping["status"].value_counts(dropna=False).to_string())
    elif args.stage == "export-customer":
        from vdl_tools.portfolio_comparison.review_apps.customer_export import (
            export_customer_roundtrip,
        )

        print(export_customer_roundtrip(root))
    elif args.stage == "import-customer":
        if not args.file:
            parser.error("import-customer requires --file")
        from vdl_tools.portfolio_comparison.review_apps.customer_export import (
            import_customer_responses,
        )

        id_mapping = import_customer_responses(root, args.file)
        print(id_mapping["status"].value_counts(dropna=False).to_string())
    elif args.stage == "set-id":
        if not args.by:
            parser.error("set-id requires --by <your name>")
        from vdl_tools.portfolio_comparison.finalize import set_manual_id

        set_manual_id(
            root, decided_by=f"vdl:{args.by}",
            row_id=args.row, name=args.name,
            cb_id=args.cb_id, nzi_id=args.nzi_id,
            coresignal_id=args.coresignal_id,
            note=args.note, verify=not args.no_verify,
        )
    elif args.stage == "finalize":
        from vdl_tools.portfolio_comparison.finalize import run_finalize

        print(run_finalize(root))
    elif args.stage == "enrich":
        from vdl_tools.portfolio_comparison.enrichment.pipeline import run_enrich

        out = run_enrich(root)
        print(f"{out['text_for_taxonomy'].notna().sum()} of {len(out)} rows "
              "carry text_for_taxonomy; stages recorded in pipeline_state.json")
    elif args.stage == "enrich-acquire":
        from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
        from vdl_tools.portfolio_comparison.enrichment.acquire import acquire_records
        from vdl_tools.portfolio_comparison.finalize import FINAL_BASENAME
        import pandas as pd

        config = EngagementConfig.from_yaml(root / "engagement.yaml")
        results_dir = config.results_dir()
        final = pd.read_parquet(results_dir / f"{FINAL_BASENAME}.parquet")
        acquired = acquire_records(final, results_dir)
        print(f"{len(acquired)} rows -> {results_dir}/acquired_records.parquet")
    elif args.stage == "enrich-scrape":
        from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
        from vdl_tools.portfolio_comparison.enrichment.acquire import ACQUIRED_BASENAME
        from vdl_tools.portfolio_comparison.enrichment.scrape import scrape_texts
        import pandas as pd

        config = EngagementConfig.from_yaml(root / "engagement.yaml")
        results_dir = config.results_dir()
        acquired = pd.read_parquet(results_dir / f"{ACQUIRED_BASENAME}.parquet")
        scraped = scrape_texts(acquired, results_dir)
        print(scraped["text_quality"].value_counts(dropna=False).to_string())
    elif args.stage == "enrich-summarize":
        from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
        from vdl_tools.portfolio_comparison.enrichment.acquire import ACQUIRED_BASENAME
        from vdl_tools.portfolio_comparison.enrichment.scrape import SCRAPED_BASENAME
        from vdl_tools.portfolio_comparison.enrichment.summarize import (
            build_general_summaries,
        )
        import pandas as pd

        config = EngagementConfig.from_yaml(root / "engagement.yaml")
        results_dir = config.results_dir()
        acquired = pd.read_parquet(results_dir / f"{ACQUIRED_BASENAME}.parquet")
        scraped = pd.read_parquet(results_dir / f"{SCRAPED_BASENAME}.parquet")
        out = build_general_summaries(acquired, scraped, results_dir)
        print(f"{out['text_for_taxonomy'].notna().sum()} of {len(out)} rows carry text_for_taxonomy")
    elif args.stage == "status":
        print(runners.run_status(root))


if __name__ == "__main__":
    main()
