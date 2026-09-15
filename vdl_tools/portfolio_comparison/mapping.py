"""Phase 3, stage 3 — map input: the customer's organizations merged into the
landscape, in the landscape's own schema, ready for the network/player build.

The landscape map (climate-landscape's ``run_cft_us.py``) builds an embedding
network from each organization's ``Summary`` and lays it out; the customer's
organizations become ordinary members of that build. This stage only prepares
the input — one row per landscape organization in the pinned universe plus one
row per customer organization not already in it — and tags every customer row:

- ``<Customer> Portfolio`` tag list (name from ``mapping.portfolio_tag``): the
  umbrella tag on every customer organization,
  plus ``"Funded by <Customer>"`` or ``"Evaluated by <Customer>, passed"``.
- ``<Customer> Grant $``: the customer's own grant dollars (funded nonprofits).

Customer organizations already in the landscape keep the landscape row (its
funding, investors, geo…) and just gain the tags; the rest carry what the
engagement produced (summary, website, One Earth categories, location) and
blanks elsewhere. Organizations with no text at all cannot join an embedding
network and are listed in the ledger, not the input.
"""

import hashlib
import time
from pathlib import Path

import numpy as np
import pandas as pd

from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.portfolio_comparison.comparison import dedupe_portfolio, load_ecosystem
from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
from vdl_tools.portfolio_comparison.funding import AMOUNT_COL, load_portfolio_amounts
from vdl_tools.portfolio_comparison.intake.profile_inputs import normalize_domain
from vdl_tools.portfolio_comparison.state import PipelineState

MAP_INPUT_BASENAME = "map_input"
TAXONOMY_COLS = [
    "all_level0_one_earth_category", "level0_one_earth_category",
    "all_level1_one_earth_category", "level1_one_earth_category",
    "all_level2_one_earth_category", "level2_one_earth_category",
    "all_level3_one_earth_category", "level3_one_earth_category",
    "one_earth_category", "cat_level_one_earth_category",
]
GEO_COLS = ["Latitude", "Longitude", "City", "State", "Country"]
ORG_TYPE = {"for_profit": "For Profit", "nonprofit": "Non Profit"}


def _listify(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def tag_names(config: EngagementConfig) -> dict:
    cfg = config.mapping or {}
    customer = cfg.get("customer_label") or config.customer.replace("-", " ").title()
    return {
        "portfolio": cfg.get("portfolio_tag", f"{customer} Portfolio"),
        "funded": f"Funded by {customer}",
        "passed": f"Evaluated by {customer}, passed",
        "grant": f"{customer} Grant $",
        "source": cfg.get("data_source_label", customer),
    }


def _status_tags(row, names) -> list:
    return [names["portfolio"], names["funded"] if row["disposition"] == "invested" else names["passed"]]


def customer_rows_as_landscape(port: pd.DataFrame, names: dict, template_cols: list,
                               list_cols: list = ()) -> pd.DataFrame:
    """Customer organizations not in the landscape, in the landscape's columns."""
    text = port["Summary"].where(port["Summary"].notna(), port.get("text_for_taxonomy"))
    out = pd.DataFrame(index=port.index, columns=template_cols, dtype=object)
    out["uid"] = "customer:" + port["customer_row_id"].astype(str)
    out["profile_name"] = port["customer_name"]
    out["Organization"] = port["customer_name"]
    out["Summary"] = text
    out["Website"] = port["customer_url"].where(port["customer_url"].notna(), port["matched_url"])
    out["Org Type"] = port["entity_type"].map(ORG_TYPE)
    out["Data Source"] = names["source"]
    for col in TAXONOMY_COLS + GEO_COLS:
        if col in port.columns:
            out[col] = port[col].map(_listify)
    # Landscape list-valued columns (tags, investors, keywords...) must be
    # lists on every row — the player build does set/len operations on them.
    for col in list_cols:
        if out[col].isna().all():
            out[col] = [[] for _ in range(len(port))]
    return out


def list_columns(eco: pd.DataFrame) -> list:
    return [c for c in eco.columns
            if eco[c].map(lambda v: isinstance(v, list)).any()]


def build_map_input(engagement_root: str | Path) -> pd.DataFrame:
    config = EngagementConfig.from_yaml(Path(engagement_root) / "engagement.yaml")
    results_dir = config.results_dir()
    names = tag_names(config)
    t0 = time.time()

    eco = load_ecosystem(results_dir).drop(columns=["_lvl0", "_lvl1", "_org_type"])
    port = dedupe_portfolio(pd.read_parquet(results_dir / "enriched_portfolio.parquet"))
    port[AMOUNT_COL] = port["customer_row_id"].map(load_portfolio_amounts(config, results_dir))
    grant = port[AMOUNT_COL].where(
        (port["entity_type"] == "nonprofit") & (port["disposition"] == "invested"))

    # Customer organizations already in the landscape: by node uid, else by website domain.
    eco_uid = eco["uid"].astype(str)
    eco_domain = eco["Website"].map(lambda u: normalize_domain(u) if isinstance(u, str) else "")
    port_domain = port["customer_url"].map(lambda u: normalize_domain(u) if isinstance(u, str) else "")
    by_uid = port["matched_id"].astype(str).map(dict(zip(eco_uid, eco.index)))
    by_domain = port_domain.map({d: i for d, i in zip(eco_domain, eco.index) if d})
    eco_row = by_uid.where(by_uid.notna(), by_domain)
    in_landscape = eco_row.notna()

    eco[names["portfolio"]] = [[] for _ in range(len(eco))]
    eco[names["grant"]] = np.nan
    for pi, ei in eco_row[in_landscape].items():
        eco.at[ei, names["portfolio"]] = _status_tags(port.loc[pi], names)
        eco.at[ei, names["grant"]] = grant.loc[pi]

    extra = port[~in_landscape]
    has_text = extra["Summary"].notna() | extra["text_for_taxonomy"].notna()
    new_rows = customer_rows_as_landscape(extra[has_text], names, list(eco.columns),
                                          list_cols=list_columns(eco))
    new_rows[names["portfolio"]] = [_status_tags(r, names) for _, r in extra[has_text].iterrows()]
    new_rows[names["grant"]] = grant.loc[extra[has_text].index].values

    combined = pd.concat([eco, new_rows], ignore_index=True)
    # JSON, not parquet: landscape columns mix lists and scalars, and the
    # landscape build reads JSON anyway.
    out_path = results_dir / f"{MAP_INPUT_BASENAME}.json"
    combined.to_json(out_path, orient="records")
    textless = extra.loc[~has_text, "customer_name"].tolist()
    PipelineState(config.root).record_stage(
        "map_input",
        n_landscape=len(eco),
        n_customer=len(port),
        n_customer_in_landscape=int(in_landscape.sum()),
        n_customer_added=len(new_rows),
        n_customer_textless=len(textless),
        customer_textless=textless,
        portfolio_tag=names["portfolio"],
        artifact_sha256=hashlib.sha256(out_path.read_bytes()).hexdigest()[:16],
        seconds=int(time.time() - t0),
    )
    logger.info("map_input: %d landscape + %d customer rows (%d already in landscape, %d textless skipped)",
                len(eco), len(new_rows), int(in_landscape.sum()), len(textless))
    return combined
