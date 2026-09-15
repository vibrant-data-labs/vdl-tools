"""Phase 3, stage 2 — sourcing: where to look for more of what the customer funds.

Turns the comparison's diagnosis ("the customer's company deal flow looks
like the market, not like its thesis") into two lists the customer can act
on, both drawn from the pinned baseline's network nodes:

- ``sourcing_companies``: landscape companies working in the same One Earth
  solutions as the customer's own investments in the configured pillars,
  excluding anything already in the customer's files (invested or passed).
- ``sourcing_backers``: the investors and programs behind those companies,
  ranked by how *concentrated* they are in that set — the share of a
  backer's landscape companies that fall in it — with a minimum count, so
  high-volume generalists (an accelerator that backs everything) rank low
  and a small specialist ranks high.

Config (``engagement.yaml`` → ``sourcing``), all explicit:
  pillars:               customer investments in these pillars seed the
                         solution set (e.g. Nature Conservation, Regenerative
                         Agriculture)
  sub_pillars:           extra seeds by sub-pillar (e.g. Cross-Cutting Nature)
  exclude_solutions:     solutions to leave out of the seed set
  min_backer_companies:  a backer needs at least this many aligned companies
  min_backer_share:      ...and at least this share of its landscape companies
                         in the aligned set (0–1)
"""

import hashlib
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.comparison import dedupe_portfolio
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.intake.profile_inputs import normalize_domain
from vdl_tools.portfolio_comparison.state import PipelineState

COMPANIES_BASENAME = "sourcing_companies"
BACKERS_BASENAME = "sourcing_backers"
L0, L1, L2 = ("level0_one_earth_category", "level1_one_earth_category",
              "level2_one_earth_category")
DEFAULTS = {"min_backer_companies": 3, "min_backer_share": 0.25}


def _listed(value) -> list:
    return list(value) if isinstance(value, (list, tuple)) else []


def load_nodes(results_dir: str | Path) -> pd.DataFrame:
    raw = json.loads((Path(results_dir) / "baseline" / "cft_network_cleaned.json").read_text())
    nodes = raw["nodes"] if isinstance(raw, dict) and "nodes" in raw else raw
    return pd.DataFrame(nodes)


def seed_solutions(port: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, list[str]]:
    """The customer's funded companies in the configured pillars/sub-pillars,
    and the One Earth solutions they work in (solution depth only — a seed
    categorized to pillar depth contributes nothing rather than a whole pillar)."""
    inv = port[(port["entity_type"] == "for_profit") & (port["disposition"] == "invested")]
    seeds = inv[inv[L0].isin(cfg.get("pillars", [])) | inv[L1].isin(cfg.get("sub_pillars", []))]
    solutions = sorted(set(seeds[L2].dropna()) - set(cfg.get("exclude_solutions", [])))
    return seeds, solutions


def _domains(series: pd.Series) -> set[str]:
    return {normalize_domain(u) for u in series.dropna().astype(str) if normalize_domain(u)}


def find_companies(nodes: pd.DataFrame, solutions: list[str], port: pd.DataFrame) -> pd.DataFrame:
    """Landscape for-profits in any seed solution, minus the customer's own rows."""
    fp = nodes[nodes["For-Profit vs Non-Profit"].astype(str).str.contains("For", na=False)].copy()
    sol = set(solutions)
    fp["matching_solutions"] = fp["One Earth Solutions Only"].map(
        lambda v: sorted(sol & set(_listed(v))))
    hits = fp[fp["matching_solutions"].map(len) > 0].copy()
    seen_ids = set(port["matched_id"].dropna().astype(str))
    seen_domains = _domains(port["customer_url"]) | _domains(port["matched_url"])
    hits["_domain"] = hits["Website"].map(lambda u: normalize_domain(u) if isinstance(u, str) else "")
    already = hits["uid"].astype(str).isin(seen_ids) | hits["_domain"].isin(seen_domains)
    hits = hits[~already]
    out = pd.DataFrame({
        "company": hits["Name"],
        "uid": hits["uid"],
        "website": hits["Website"],
        "hq_state": hits.get("HQ State"),
        "funding_stage": hits.get("Funding Stage"),
        "total_funding_usd": pd.to_numeric(hits.get("Total Funding"), errors="coerce"),
        "year_last_funded": pd.to_numeric(hits.get("Year Last Funded"), errors="coerce"),
        "pillar": hits["One Earth Pillars"].map(lambda v: (_listed(v) or [None])[0]),
        "sub_pillars": hits["One Earth Sub-Pillars"].map(lambda v: "; ".join(_listed(v))),
        "matching_solutions": hits["matching_solutions"].map("; ".join),
        "n_matching": hits["matching_solutions"].map(len),
        "investors": hits["Investors"].map(lambda v: "; ".join(_listed(v))),
        "summary": hits.get("Summary"),
    })
    return out.sort_values(["n_matching", "year_last_funded"], ascending=[False, False])


def rank_backers(nodes: pd.DataFrame, companies: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, float]:
    """Investors/programs behind the aligned companies, ranked by concentration."""
    fp = nodes[nodes["For-Profit vs Non-Profit"].astype(str).str.contains("For", na=False)]
    total_by_backer: Counter = Counter()
    for inv in fp["Investors"]:
        for name in set(_listed(inv)):
            total_by_backer[name] += 1
    n_with_investors = int(fp["Investors"].map(lambda v: len(_listed(v)) > 0).sum())
    aligned_uids = set(companies["uid"].astype(str))
    aligned = fp[fp["uid"].astype(str).isin(aligned_uids)]
    by_backer: dict[str, list] = defaultdict(list)
    for _, row in aligned.iterrows():
        for name in set(_listed(row["Investors"])):
            by_backer[name].append(row)
    base_rate = len(aligned) / n_with_investors if n_with_investors else 0.0
    sol_of = dict(zip(companies["uid"].astype(str), companies["matching_solutions"]))
    rows = []
    for name, comps in by_backer.items():
        n_aligned, n_total = len(comps), total_by_backer[name]
        share = n_aligned / n_total if n_total else 0.0
        if n_aligned < cfg["min_backer_companies"] or share < cfg["min_backer_share"]:
            continue
        sol_counts = Counter(s for c in comps for s in sol_of.get(str(c["uid"]), "").split("; ") if s)
        rows.append({
            "backer": name,
            "n_aligned_companies": n_aligned,
            "n_landscape_companies": n_total,
            "aligned_share_pct": round(100 * share, 1),
            "lift_vs_landscape": round(share / base_rate, 1) if base_rate else None,
            "solutions": "; ".join(f"{s} ({k})" for s, k in sol_counts.most_common(3)),
            "example_companies": "; ".join(sorted(c["Name"] for c in comps)[:4]),
        })
    backers = pd.DataFrame(rows, columns=[
        "backer", "n_aligned_companies", "n_landscape_companies", "aligned_share_pct",
        "lift_vs_landscape", "solutions", "example_companies"])
    return backers.sort_values(["aligned_share_pct", "n_aligned_companies"],
                               ascending=[False, False]), base_rate


def run_sourcing(engagement_root: str | Path) -> dict[str, pd.DataFrame]:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    cfg = {**DEFAULTS, **(config.sourcing or {})}
    if not cfg.get("pillars") and not cfg.get("sub_pillars"):
        raise ValueError("engagement.yaml: sourcing.pillars (or sub_pillars) must name "
                         "which of the customer's investments seed the search")
    results_dir = config.results_dir()
    t0 = time.time()

    port = dedupe_portfolio(pd.read_parquet(results_dir / "enriched_portfolio.parquet"))
    nodes = load_nodes(results_dir)
    seeds, solutions = seed_solutions(port, cfg)
    companies = find_companies(nodes, solutions, port)
    backers, base_rate = rank_backers(nodes, companies, cfg)

    companies.to_csv(results_dir / f"{COMPANIES_BASENAME}.csv", index=False)
    backers.to_csv(results_dir / f"{BACKERS_BASENAME}.csv", index=False)
    PipelineState(config.root).record_stage(
        "sourcing",
        n_seed_investments=len(seeds),
        n_seed_without_solution=int(seeds[L2].isna().sum()),
        n_solutions=len(solutions),
        n_companies=len(companies),
        n_backers=len(backers),
        aligned_base_rate_pct=round(100 * base_rate, 1),
        min_backer_companies=cfg["min_backer_companies"],
        min_backer_share=cfg["min_backer_share"],
        artifact_sha256=hashlib.sha256(
            (results_dir / f"{COMPANIES_BASENAME}.csv").read_bytes()).hexdigest()[:16],
        seconds=int(time.time() - t0),
    )
    logger.info("sourcing: %d seed investments -> %d solutions -> %d landscape companies, %d backers",
                len(seeds), len(solutions), len(companies), len(backers))
    return {COMPANIES_BASENAME: companies, BACKERS_BASENAME: backers,
            "solutions": pd.Series(solutions, name="solution")}
