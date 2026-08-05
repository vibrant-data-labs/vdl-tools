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
                 "export-customer", "import-customer"],
    )
    parser.add_argument(
        "--root", default=".", help="engagement repo root (default: cwd)"
    )
    parser.add_argument(
        "--file", default=None, help="filled-in spreadsheet (import-customer)"
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()

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
    elif args.stage == "status":
        print(runners.run_status(root))


if __name__ == "__main__":
    main()
