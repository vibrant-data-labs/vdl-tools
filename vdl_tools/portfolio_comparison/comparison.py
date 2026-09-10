"""Phase 3, stage 1 — portfolio-vs-ecosystem comparison tables.

Compares the enriched portfolio against the engagement's pinned baseline
universe on One Earth categories. Three tables, written as CSV (+ ledger
entry ``compare``):

- ``comparison_pillar``:     shares by pillar — ecosystem / full portfolio /
                             invested / passed, with portfolio-vs-ecosystem tilt
- ``comparison_subpillar``:  the same at sub-pillar depth (level1)
- ``comparison_conversion``: per pillar, how much deal flow OSP saw vs how
                             often they invested (invested / (invested+passed))

Dedupe rule (spec): one org counted once per ``matched_id`` — customer files
list some orgs twice. Shares use each side's orgs-with-category as the
denominator; ``n_`` columns carry the raw counts so nothing hides.
"""

import ast
import hashlib
import json
import time
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.state import PipelineState

PILLAR_BASENAME = "comparison_pillar"
SUBPILLAR_BASENAME = "comparison_subpillar"
CONVERSION_BASENAME = "comparison_conversion"


def _primary(value):
    """Baseline taxonomy cells are repr-encoded lists; portfolio's are strings."""
    if isinstance(value, str) and value.startswith("["):
        try:
            value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value
    if isinstance(value, list):
        return value[0] if value else None
    return value


def load_ecosystem(results_dir: str | Path) -> pd.DataFrame:
    """The pinned baseline universe with primary pillar/sub-pillar columns."""
    results_dir = Path(results_dir)
    u = json.loads((results_dir / "baseline_universe.json").read_text())
    ids = (u.get("ids") or u.get("universe")) if isinstance(u, dict) else [
        r["uid"] if isinstance(r, dict) else r for r in u
    ]
    b = pd.read_json(results_dir / "baseline" / "cb_cd_li_meta.json",
                     convert_dates=False)
    eco = b[b["uid"].isin(set(ids))].copy()
    for depth in (0, 1):
        col = next(c for c in eco.columns if f"level{depth}" in c.lower())
        eco[f"_lvl{depth}"] = eco[col].map(_primary)
    type_col = next((c for c in eco.columns if c.lower() == "org type"), None)
    eco["_org_type"] = eco[type_col] if type_col else "No Data"
    return eco


def _share_table(eco_series, port_df, level_col) -> pd.DataFrame:
    def pct(series):
        return (series.value_counts(normalize=True) * 100).round(1)

    invested = port_df[port_df["disposition"] == "invested"]
    passed = port_df[port_df["disposition"] == "passed"]
    table = pd.DataFrame({
        "n_ecosystem": eco_series.value_counts(),
        "ecosystem_pct": pct(eco_series),
        "n_portfolio": port_df[level_col].value_counts(),
        "portfolio_pct": pct(port_df[level_col]),
        "invested_pct": pct(invested[level_col]),
        "passed_pct": pct(passed[level_col]),
    }).fillna(0)
    table["tilt_vs_eco"] = (table["portfolio_pct"] - table["ecosystem_pct"]).round(1)
    table.index.name = "category"
    return table.sort_values("ecosystem_pct", ascending=False)


def run_compare(engagement_root: str | Path) -> dict[str, pd.DataFrame]:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    t0 = time.time()

    eco = load_ecosystem(results_dir)
    port = pd.read_parquet(results_dir / "enriched_portfolio.parquet")
    port = port.drop_duplicates(subset="matched_id")
    with_pillar = port[port["level0_one_earth_category"].notna()]

    def _conversion(port_df):
        c = port_df.groupby("level0_one_earth_category")["disposition"].agg(
            n_invested=lambda s: int((s == "invested").sum()),
            n_passed=lambda s: int((s == "passed").sum()),
        )
        c["conversion_rate"] = (
            (c["n_invested"] / (c["n_invested"] + c["n_passed"]))
            .astype(float).round(3)
            if len(c) else pd.Series(dtype=float)
        )
        c.index.name = "pillar"
        return c.sort_values("conversion_rate", ascending=False)

    # Three segments (Zein's spec): blended, then for-profit-only, then
    # nonprofit-only — each pairing the matching side of the ecosystem
    # ("Org Type") with the matching side of the portfolio (entity_type).
    segments = {
        "": (eco, with_pillar),
        "_forprofit": (eco[eco["_org_type"] == "For Profit"],
                       with_pillar[with_pillar["entity_type"] == "for_profit"]),
        "_nonprofit": (eco[eco["_org_type"] == "Non Profit"],
                       with_pillar[with_pillar["entity_type"] == "nonprofit"]),
    }
    tables = {}
    for suffix, (eco_seg, port_seg) in segments.items():
        tables[PILLAR_BASENAME + suffix] = _share_table(
            eco_seg["_lvl0"].dropna(), port_seg, "level0_one_earth_category")
        tables[SUBPILLAR_BASENAME + suffix] = _share_table(
            eco_seg["_lvl1"].dropna(),
            port_seg[port_seg["level1_one_earth_category"].notna()],
            "level1_one_earth_category")
        tables[CONVERSION_BASENAME + suffix] = _conversion(port_seg)
    # The blended tables also carry the ecosystem org-type split columns
    # (used by the blended chart's 4-series view).
    for label, key in (("forprofit", "For Profit"), ("nonprofit", "Non Profit")):
        for name, lvl in ((PILLAR_BASENAME, "_lvl0"), (SUBPILLAR_BASENAME, "_lvl1")):
            _s = eco.loc[eco["_org_type"] == key, lvl].dropna()
            tables[name][f"eco_{label}_pct"] = (
                _s.value_counts(normalize=True) * 100).round(1)
            tables[name][f"n_eco_{label}"] = _s.value_counts()
            tables[name] = tables[name].fillna(0)

    for name, df in tables.items():
        df.to_csv(results_dir / f"{name}.csv")

    pillar_path = results_dir / f"{PILLAR_BASENAME}.csv"
    state = PipelineState(config.root)
    state.record_stage(
        "compare",
        n_ecosystem=int(eco["_lvl0"].notna().sum()),
        n_portfolio_orgs=len(port),
        n_portfolio_with_pillar=len(with_pillar),
        artifact_sha256=hashlib.sha256(pillar_path.read_bytes()).hexdigest()[:16],
        seconds=int(time.time() - t0),
    )
    logger.info("compare: %d ecosystem vs %d portfolio orgs -> %s",
                int(eco["_lvl0"].notna().sum()), len(with_pillar),
                ", ".join(f"{n}.csv" for n in tables))
    return tables
