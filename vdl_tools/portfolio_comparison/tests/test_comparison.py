"""Phase 3 compare stage: shares, tilt, conversion, funding weights."""

import json

import pandas as pd
import pytest

from vdl_tools.portfolio_comparison.comparison import run_compare
from vdl_tools.portfolio_comparison.intake import profile_inputs as pi

ENGAGEMENT_YAML = (
    "engagement:\n  customer: test-co\n  vertical: climate\n"
    "  match_objective: text\n"
    "  baseline_run:\n    name: x\n    version: v\n"
    "    enriched_uri: e.json\n    network_nodes_uri: n.json\n"
    "    source: crunchbase\n    taxonomy: oneearth\n"
    "    taxonomy_version: \"1\"\n"
    "  inputs:\n    companies: c.xlsx\n    nonprofits: n.csv\n"
)

ENERGY, NATURE = "Energy Transition", "Nature Conservation"


def _write_baseline(results):
    # Ecosystem: 2 Energy for-profits (repr-list encoded), 2 Nature
    # nonprofits; all-time dollars 100/300/50/50, windowed 10/10/50/40.
    rows = [
        (ENERGY, "Renewable Power", "For Profit", 100, 10, 0),
        (ENERGY, "Energy Efficiency", "For Profit", 300, 0, 10),
        (NATURE, "Land Conservation", "Non Profit", 50, 50, 0),
        (NATURE, "Land Conservation", "Non Profit", 50, 0, 40),
    ]
    eco = pd.DataFrame([
        {"uid": f"u{i}", "Org Type": org_type,
         "level0_one_earth_category": f"['{lvl0}']",
         "level1_one_earth_category": f"['{lvl1}']",
         "Total_Funding_$": usd, "Funding_2021": y1, "Funding_2022": y2}
        for i, (lvl0, lvl1, org_type, usd, y1, y2) in enumerate(rows)])
    (results / "baseline").mkdir(parents=True)
    eco.to_json(results / "baseline" / "cb_cd_li_meta.json")
    (results / "baseline_universe.json").write_text(
        json.dumps([f"u{i}" for i in range(4)]))


def _port_row(rid, entity_type, matched_id, disposition, lvl0, lvl1):
    return {"customer_row_id": rid, "entity_type": entity_type,
            "matched_id": matched_id, "disposition": disposition,
            "level0_one_earth_category": lvl0, "level1_one_earth_category": lvl1}


@pytest.fixture
def engagement(tmp_path):
    (tmp_path / "engagement.yaml").write_text(ENGAGEMENT_YAML)
    results = tmp_path / "data" / "results"
    _write_baseline(results)
    # Portfolio: 3 matched for-profits (one listed twice under matched_id m1)
    # + 2 nonprofits mapped from customer text alone (no matched_id).
    port = pd.DataFrame([
        _port_row("r1", "for_profit", "m1", "invested", ENERGY, "Renewable Power"),
        _port_row("r1b", "for_profit", "m1", "invested", ENERGY, "Renewable Power"),
        _port_row("r2", "for_profit", "m2", "passed", ENERGY, "Energy Efficiency"),
        _port_row("r3", "for_profit", "m3", "invested", NATURE, "Land Conservation"),
        _port_row("r4", "nonprofit", None, "invested", NATURE, "Land Conservation"),
        _port_row("r5", "nonprofit", None, "invested", ENERGY, "Energy Efficiency"),
    ])
    port.to_parquet(results / "enriched_portfolio.parquet")
    return tmp_path


def test_compare_shares_tilt_and_conversion(engagement):
    tables = run_compare(engagement)
    pillar = tables["comparison_pillar"]
    # Ecosystem 50/50; portfolio deduped to 5 orgs (m1 once; the two
    # no-matched_id nonprofits must NOT collapse into one) = 3 Energy, 2 Nature.
    assert pillar.at[ENERGY, "ecosystem_pct"] == 50.0
    assert pillar.at[ENERGY, "n_portfolio"] == 3
    assert pillar.at[ENERGY, "portfolio_pct"] == 60.0
    assert pillar.at[ENERGY, "tilt_vs_eco"] == 10.0
    assert pillar.at[NATURE, "n_portfolio"] == 2

    conv = tables["comparison_conversion"]
    assert conv.at[ENERGY, "n_invested"] == 2  # m1 counted once, r5 kept
    assert conv.at[ENERGY, "n_passed"] == 1
    assert conv.at[ENERGY, "conversion_rate"] == 0.667
    assert conv.at[NATURE, "conversion_rate"] == 1.0

    sub = tables["comparison_subpillar"]
    assert sub.at["Land Conservation", "ecosystem_pct"] == 50.0

    state = json.loads((engagement / "pipeline_state.json").read_text())
    assert state["stages"]["compare"]["n_portfolio_with_pillar"] == 5
    assert (engagement / "data/results/comparison_pillar.csv").exists()
    # Segments pair each side's org type.
    fp = tables["comparison_pillar_forprofit"]
    assert fp.at[ENERGY, "portfolio_pct"] == 66.7
    np_ = tables["comparison_pillar_nonprofit"]
    assert np_.at[ENERGY, "n_portfolio"] == 1 and np_.at[NATURE, "n_portfolio"] == 1
    assert np_.at[NATURE, "ecosystem_pct"] == 100.0
    assert (engagement / "data/results/comparison_conversion_nonprofit.csv").exists()


def test_ecosystem_funding_without_portfolio_amounts(engagement):
    """No funding config: ecosystem dollar shares still come out (all-time
    Total_Funding_$); portfolio dollar columns are NaN, never a fake 0%."""
    tables = run_compare(engagement)
    f = tables["comparison_pillar_funding"]
    assert f.at[ENERGY, "ecosystem_usd"] == 400 and f.at[NATURE, "ecosystem_usd"] == 100
    assert f.at[ENERGY, "ecosystem_funding_pct"] == 80.0
    assert f.at[ENERGY, "ecosystem_pct"] == 50.0  # org share kept for the contrast
    assert f["portfolio_funding_pct"].isna().all()
    assert f["n_portfolio_with_amount"].eq(0).all()
    assert f.at[ENERGY, "eco_forprofit_funding_pct"] == 100.0
    state = json.loads((engagement / "pipeline_state.json").read_text())["stages"]["compare"]
    assert state["ecosystem_funding_basis"] == "Total_Funding_$"
    assert "n_portfolio_with_amount" not in state
    assert not (engagement / "data/results/portfolio_funding.csv").exists()


@pytest.fixture
def funded_engagement(tmp_path):
    """Customer nonprofit file with per-year grant columns declared as the
    portfolio amounts; row ids reproduce intake's recipe so the join is exact."""
    (tmp_path / "engagement.yaml").write_text(
        ENGAGEMENT_YAML
        + "  funding:\n    portfolio_amounts:\n      nonprofits: [\"2021\", \"2022\"]\n"
        "    ecosystem_window: [2021, 2022]\n")
    results = tmp_path / "data" / "results"
    _write_baseline(results)
    src = pd.DataFrame({
        "Org": ["Alpha", "Beta", "Gamma", "Gamma", "Delta"],
        "Website": ["alpha.org", "", "gamma.org", "gamma.org", "delta.org"],
        "2021": [1000, "$500", 100, 400, None],
        "2022": [2000, None, None, None, ""],
    })
    src.to_csv(tmp_path / "n.csv", index=False)
    (results / "intake_profile.json").write_text(json.dumps({"files": [{
        "file": "nonprofits", "default_entity_type": "nonprofit",
        "column_mapping": {"Org": "name", "Website": "url",
                           "2021": "passthrough", "2022": "passthrough"},
    }]}))
    rid = [pi.make_row_id("nonprofits", n, u, i)
           for i, (n, u) in enumerate(zip(src["Org"], src["Website"]))]
    port = pd.DataFrame([
        _port_row(rid[0], "nonprofit", None, "invested", NATURE, "Land Conservation"),
        _port_row(rid[1], "nonprofit", None, "invested", ENERGY, "Energy Efficiency"),
        _port_row(rid[2], "nonprofit", "mG", "invested", ENERGY, "Energy Efficiency"),
        _port_row(rid[3], "nonprofit", "mG", "invested", ENERGY, "Energy Efficiency"),
        _port_row(rid[4], "nonprofit", None, "passed", NATURE, "Land Conservation"),
    ])
    port.to_parquet(results / "enriched_portfolio.parquet")
    return tmp_path


def test_funding_weighted_shares(funded_engagement):
    tables = run_compare(funded_engagement)
    f = tables["comparison_pillar_funding"]
    # Portfolio dollars: Alpha 3000 (Nature); Beta 500 + Gamma 100+400 summed
    # across its two rows (Energy) = 1000; Delta has no amount -> counted in
    # org shares only. Nature 75% / Energy 25% of $4000.
    assert f.at[NATURE, "portfolio_usd"] == 3000 and f.at[ENERGY, "portfolio_usd"] == 1000
    assert f.at[NATURE, "portfolio_funding_pct"] == 75.0
    assert f.at[NATURE, "n_portfolio_with_amount"] == 1
    assert f.at[ENERGY, "n_portfolio_with_amount"] == 2  # Gamma once
    assert f.at[NATURE, "invested_funding_pct"] == 75.0
    assert f["passed_funding_pct"].isna().all()  # passed rows carry no dollars
    # Ecosystem windowed to 2021-2022: Energy 20, Nature 90.
    assert f.at[ENERGY, "ecosystem_usd"] == 20 and f.at[NATURE, "ecosystem_usd"] == 90
    assert f.at[NATURE, "ecosystem_funding_pct"] == 81.8
    assert f.at[NATURE, "tilt_funding_vs_eco"] == round(75.0 - 81.8, 1)
    assert f.at[NATURE, "n_ecosystem_with_usd"] == 2 and f.at[ENERGY, "n_ecosystem_with_usd"] == 2

    counts = tables["comparison_pillar"]
    assert counts.at[NATURE, "n_portfolio"] == 2  # Delta still counted as an org
    sub = tables["comparison_subpillar_funding_nonprofit"]
    assert sub.at["Land Conservation", "portfolio_funding_pct"] == 75.0

    funded = pd.read_csv(funded_engagement / "data/results/portfolio_funding.csv")
    assert len(funded) == 3 and funded["customer_amount_usd"].sum() == 4000
    state = json.loads((funded_engagement / "pipeline_state.json").read_text())["stages"]["compare"]
    assert state["n_portfolio_with_amount"] == 3
    assert state["portfolio_amount_usd"] == 4000
    assert state["portfolio_amount_mapped_pct"] == 100.0
    assert state["ecosystem_funding_basis"] == "Funding_2021..2022"


def test_funding_config_validation(tmp_path):
    from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig

    p = tmp_path / "engagement.yaml"
    p.write_text(ENGAGEMENT_YAML + "  funding:\n    portfolio_amounts:\n      grants: [\"2021\"]\n")
    with pytest.raises(ValueError, match="portfolio_amounts.grants: no such input"):
        EngagementConfig.from_yaml(p)
    p.write_text(ENGAGEMENT_YAML + "  funding:\n    ecosystem_window: 2021\n")
    with pytest.raises(ValueError, match="ecosystem_window"):
        EngagementConfig.from_yaml(p)
