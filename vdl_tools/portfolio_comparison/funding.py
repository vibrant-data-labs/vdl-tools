"""Dollar weights for the compare stage — both sides of the comparison.

Portfolio side: the customer's own deployed dollars, read from the columns
``engagement.yaml`` declares under ``funding.portfolio_amounts`` and keyed
back to pipeline rows by ``customer_row_id`` (same recipe intake used, so
the join is exact, not by name). Ecosystem side: the baseline's
``Total_Funding_$`` — or the ``Funding_<year>`` columns summed over
``funding.ecosystem_window`` so both sides cover the same years.
"""

import json
from pathlib import Path

import pandas as pd

from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.intake import profile_inputs as pi

AMOUNT_COL = "customer_amount_usd"
TOTAL_FUNDING_COL = "Total_Funding_$"


def _to_usd(series: pd.Series) -> pd.Series:
    """'$1,250,000' / 1250000 / '' -> float or NaN."""
    cleaned = (
        series.astype(str).str.replace(r"[$,\s]", "", regex=True)
        .replace({"": None, "nan": None, "None": None})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def load_portfolio_amounts(config: EngagementConfig,
                           results_dir: str | Path) -> pd.Series:
    """customer_row_id -> dollars (NaN when the row has no amount at all).

    Amounts are summed across the declared columns per row; a row whose
    declared cells are all blank stays NaN so coverage counts stay honest.
    """
    declared = config.funding.get("portfolio_amounts") or {}
    if not declared:
        return pd.Series(dtype=float, name=AMOUNT_COL)
    from vdl_tools.portfolio_comparison.run import _read_customer_file

    profiles = json.loads(
        (Path(results_dir) / "intake_profile.json").read_text())["files"]
    by_label = {p["file"]: p for p in profiles}
    out = {}
    for label, cols in declared.items():
        profile = by_label.get(label)
        if profile is None:
            raise ValueError(f"funding.portfolio_amounts.{label}: not in intake_profile.json "
                             "(run intake first)")
        inverse = {v: k for k, v in profile["column_mapping"].items()
                   if v != "passthrough"}
        df = _read_customer_file(config.input_path(label))
        missing = [c for c in cols if str(c) not in df.columns]
        if missing:
            raise ValueError(
                f"funding.portfolio_amounts.{label}: columns {missing} not in "
                f"{config.inputs[label]} (have {list(df.columns)})")
        amounts = pd.concat([_to_usd(df[str(c)]) for c in cols], axis=1).sum(
            axis=1, min_count=1)
        name_col, url_col = inverse["name"], inverse.get("url")
        for i, (name, url, usd) in enumerate(zip(
                df[name_col], df[url_col] if url_col else [None] * len(df), amounts)):
            rid = pi.make_row_id(label, name, url or "", i)
            out[rid] = usd
    return pd.Series(out, name=AMOUNT_COL, dtype=float)


def ecosystem_funding_usd(eco: pd.DataFrame, window: list | None) -> pd.Series:
    """Per-org ecosystem dollars: Total_Funding_$ or the windowed year sum."""
    if window:
        cols = [f"Funding_{y}" for y in range(window[0], window[1] + 1)]
        missing = [c for c in cols if c not in eco.columns]
        if missing:
            raise ValueError(f"baseline lacks {missing} for funding.ecosystem_window")
        return eco[cols].apply(pd.to_numeric, errors="coerce").sum(axis=1, min_count=1)
    if TOTAL_FUNDING_COL not in eco.columns:
        raise ValueError(f"baseline lacks {TOTAL_FUNDING_COL}")
    return pd.to_numeric(eco[TOTAL_FUNDING_COL], errors="coerce")


def funding_basis(window: list | None) -> str:
    """Ledger label for which ecosystem dollars were used."""
    return (f"Funding_{window[0]}..{window[1]}" if window else TOTAL_FUNDING_COL)
