"""Phase 3, stage 1 — portfolio-vs-ecosystem comparison tables.

Compares the enriched portfolio against the engagement's pinned baseline
universe on One Earth categories. Tables written as CSV (+ ledger entry
``compare``), each in three segments — blended, ``_forprofit``,
``_nonprofit`` (each side's org type paired):

- ``comparison_pillar``:     shares by pillar — ecosystem / full portfolio /
                             invested / passed, with portfolio-vs-ecosystem tilt
- ``comparison_subpillar``:  the same at sub-pillar depth (level1)
- ``comparison_conversion``: per pillar, how much deal flow the customer saw
                             vs how often they invested (invested / (invested+passed))
- ``comparison_pillar_funding`` / ``comparison_subpillar_funding``: the same
  shares weighted by dollars — ecosystem ``Total_Funding_$`` (or the
  ``funding.ecosystem_window`` year sum) against the customer's own amounts
  (``funding.portfolio_amounts``). ``portfolio_funding.csv`` lists the dollars
  per org that fed them. Written only when the baseline carries funding.

Dedupe rule (spec): one org counted once — per ``matched_id`` where there is
one, else per ``customer_row_id`` (orgs mapped from customer text alone have
no matched id and must not collapse into each other). Dollars sum across a
twice-listed org's rows; its category comes from the first. Shares use each
side's orgs-with-category (or dollars-with-category) as the denominator;
``n_``/``_usd`` columns carry the raw values so nothing hides.
"""

import ast
import hashlib
import json
import time
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.funding import (
    AMOUNT_COL, TOTAL_FUNDING_COL, ecosystem_funding_usd, funding_basis,
    load_portfolio_amounts,
)
from vdl_tools.portfolio_comparison.state import PipelineState

PILLAR_BASENAME = "comparison_pillar"
SUBPILLAR_BASENAME = "comparison_subpillar"
CONVERSION_BASENAME = "comparison_conversion"
FUNDING_SUFFIX = "_funding"
PORTFOLIO_FUNDING_BASENAME = "portfolio_funding"
ECO_USD = "_usd"


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


def dedupe_portfolio(port: pd.DataFrame) -> pd.DataFrame:
    """One row per org (module docstring); amounts summed across an org's rows."""
    key = port["matched_id"].where(
        port["matched_id"].notna(), "row:" + port["customer_row_id"].astype(str))
    out = port.loc[~key.duplicated()].copy()
    if AMOUNT_COL in port.columns:
        out[AMOUNT_COL] = key.loc[out.index].map(
            port.groupby(key)[AMOUNT_COL].sum(min_count=1))
    return out


def _pct(series: pd.Series) -> pd.Series:
    return (series.value_counts(normalize=True) * 100).round(1)


def _share_table(eco_series, port_df, level_col) -> pd.DataFrame:
    invested = port_df[port_df["disposition"] == "invested"]
    passed = port_df[port_df["disposition"] == "passed"]
    table = pd.DataFrame({
        "n_ecosystem": eco_series.value_counts(),
        "ecosystem_pct": _pct(eco_series),
        "n_portfolio": port_df[level_col].value_counts(),
        "portfolio_pct": _pct(port_df[level_col]),
        "invested_pct": _pct(invested[level_col]),
        "passed_pct": _pct(passed[level_col]),
    }).fillna(0)
    table["tilt_vs_eco"] = (table["portfolio_pct"] - table["ecosystem_pct"]).round(1)
    table.index.name = "category"
    return table.sort_values("ecosystem_pct", ascending=False)


def _usd_share(df, level_col, usd_col) -> tuple[pd.Series, pd.Series]:
    """(dollars by category, percent of the side's dollars) — percent is NaN
    across the board when the side has no dollars, 0 for absent categories."""
    usd = df.groupby(level_col)[usd_col].sum()
    total = usd.sum()
    pct = (usd / total * 100).round(1) if total > 0 else usd * float("nan")
    return usd, pct


def _funding_table(eco_df, port_df, level_col) -> pd.DataFrame:
    """Dollar shares by category, with org shares alongside for the contrast."""
    eco_cat = eco_df[eco_df[level_col].notna()]
    has_amt = port_df[port_df[AMOUNT_COL].notna()]
    eco_usd, eco_pct = _usd_share(eco_cat, level_col, ECO_USD)
    cols = {
        "n_ecosystem": eco_cat[level_col].value_counts(),
        "n_ecosystem_with_usd": eco_cat.loc[eco_cat[ECO_USD] > 0, level_col].value_counts(),
        "ecosystem_pct": _pct(eco_cat[level_col]),
        "ecosystem_usd": eco_usd,
        "ecosystem_funding_pct": eco_pct,
        "n_portfolio_with_amount": has_amt[level_col].value_counts(),
    }
    for label, frame in (("portfolio", has_amt),
                         ("invested", has_amt[has_amt["disposition"] == "invested"]),
                         ("passed", has_amt[has_amt["disposition"] == "passed"])):
        cols[f"{label}_usd"], cols[f"{label}_funding_pct"] = _usd_share(
            frame, level_col, AMOUNT_COL)
    table = pd.DataFrame(cols)
    # Counts and dollars: absent category = 0. Percents: 0 only when that
    # side has dollars at all; a side with none stays NaN, never a fake 0%.
    for c in table.columns:
        if not c.endswith("_pct") or table[c].notna().any():
            table[c] = table[c].fillna(0)
    table["tilt_funding_vs_eco"] = (
        table["portfolio_funding_pct"] - table["ecosystem_funding_pct"]).round(1)
    table.index.name = "category"
    return table.sort_values("ecosystem_funding_pct", ascending=False)


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


def run_compare(engagement_root: str | Path) -> dict[str, pd.DataFrame]:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    t0 = time.time()

    eco = load_ecosystem(results_dir)
    window = config.funding.get("ecosystem_window")
    with_funding = bool(window) or TOTAL_FUNDING_COL in eco.columns
    if with_funding:
        eco[ECO_USD] = ecosystem_funding_usd(eco, window)
    else:
        logger.warning("compare: baseline has no %s — funding tables skipped",
                       TOTAL_FUNDING_COL)

    port = pd.read_parquet(results_dir / "enriched_portfolio.parquet")
    amounts = load_portfolio_amounts(config, results_dir)
    port[AMOUNT_COL] = port["customer_row_id"].map(amounts) if len(amounts) else float("nan")
    port = dedupe_portfolio(port)
    with_pillar = port[port["level0_one_earth_category"].notna()]

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
    levels = ((PILLAR_BASENAME, "_lvl0", "level0_one_earth_category"),
              (SUBPILLAR_BASENAME, "_lvl1", "level1_one_earth_category"))
    tables = {}
    for suffix, (eco_seg, port_seg) in segments.items():
        for name, eco_lvl, port_lvl in levels:
            port_lvl_seg = port_seg[port_seg[port_lvl].notna()]
            tables[name + suffix] = _share_table(
                eco_seg[eco_lvl].dropna(), port_lvl_seg, port_lvl)
            if with_funding:
                tables[name + FUNDING_SUFFIX + suffix] = _funding_table(
                    eco_seg[[eco_lvl, ECO_USD]].rename(columns={eco_lvl: port_lvl}),
                    port_lvl_seg, port_lvl)
        tables[CONVERSION_BASENAME + suffix] = _conversion(port_seg)
    # The blended tables also carry the ecosystem org-type split columns
    # (used by the blended chart's 4-series view).
    for label, key in (("forprofit", "For Profit"), ("nonprofit", "Non Profit")):
        eco_side = eco[eco["_org_type"] == key]
        for name, lvl, _ in levels:
            _s = eco_side[lvl].dropna()
            tables[name][f"eco_{label}_pct"] = _pct(_s)
            tables[name][f"n_eco_{label}"] = _s.value_counts()
            tables[name] = tables[name].fillna(0)
            if with_funding:
                usd, pct = _usd_share(eco_side[eco_side[lvl].notna()], lvl, ECO_USD)
                t = tables[name + FUNDING_SUFFIX]
                t[f"eco_{label}_usd"] = usd
                t[f"eco_{label}_funding_pct"] = pct
                t[[f"eco_{label}_usd", f"eco_{label}_funding_pct"]] = (
                    t[[f"eco_{label}_usd", f"eco_{label}_funding_pct"]].fillna(0))

    for name, df in tables.items():
        df.to_csv(results_dir / f"{name}.csv")

    ledger: dict = {}
    if len(amounts):
        # The dollars behind the funding tables, one line per org — how a
        # reviewer audits which grants landed in which category.
        keep = [c for c in ("customer_row_id", "customer_name", "entity_type",
                            "disposition", "matched_id", "level0_one_earth_category",
                            "level1_one_earth_category", AMOUNT_COL) if c in port.columns]
        funded = port.loc[port[AMOUNT_COL].notna(), keep]
        funded.to_csv(results_dir / f"{PORTFOLIO_FUNDING_BASENAME}.csv", index=False)
        total_usd = float(funded[AMOUNT_COL].sum())
        ledger.update(
            n_portfolio_with_amount=len(funded),
            portfolio_amount_usd=round(total_usd),
            portfolio_amount_mapped_pct=(
                round(100 * float(funded.loc[
                    funded["level0_one_earth_category"].notna(), AMOUNT_COL].sum())
                      / total_usd, 1) if total_usd else None),
        )
    if with_funding:
        ledger["ecosystem_funding_basis"] = funding_basis(window)

    pillar_path = results_dir / f"{PILLAR_BASENAME}.csv"
    state = PipelineState(config.root)
    state.record_stage(
        "compare",
        n_ecosystem=int(eco["_lvl0"].notna().sum()),
        n_portfolio_orgs=len(port),
        n_portfolio_with_pillar=len(with_pillar),
        artifact_sha256=hashlib.sha256(pillar_path.read_bytes()).hexdigest()[:16],
        seconds=int(time.time() - t0),
        **ledger,
    )
    logger.info("compare: %d ecosystem vs %d portfolio orgs -> %s",
                int(eco["_lvl0"].notna().sum()), len(with_pillar),
                ", ".join(f"{n}.csv" for n in tables))
    return tables
