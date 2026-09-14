"""Engagement findings notebook (marimo) — charts with the code showing.

Launch from the ENGAGEMENT REPO ROOT (marimo edit to see/modify code):

    marimo edit \
        ~/dev/vdl/vdl-tools/vdl_tools/portfolio_comparison/review_apps/findings.py

Interactive altair charts; all comparison math computed in visible
cells from the raw ecosystem + enriched portfolio (org shares and dollar
shares). The compare stage writes the same tables as recorded CSVs for the
ledger. Unmapped-class counts cite nomatch_analysis.md (second-reader
verified).
"""

import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import pandas as pd

    from vdl_tools.portfolio_comparison.comparison import (
        ECO_USD,
        dedupe_portfolio,
        load_ecosystem,
    )
    from vdl_tools.portfolio_comparison.engagement_config import EngagementConfig
    from vdl_tools.portfolio_comparison.funding import (
        AMOUNT_COL,
        ecosystem_funding_usd,
        load_portfolio_amounts,
    )

    R = Path.cwd() / "data" / "results"
    config = EngagementConfig.from_yaml(Path.cwd() / "engagement.yaml")
    enriched = pd.read_parquet(R / "enriched_portfolio.parquet")
    # Raw comparison inputs — the tables are computed in the next cell so the
    # math is visible and extensible here.
    eco = load_ecosystem(R)  # pinned universe + _lvl0/_lvl1/_org_type
    window = config.funding.get("ecosystem_window")
    eco[ECO_USD] = ecosystem_funding_usd(eco, window)  # dollars, windowed
    # One row per org: matched_id where there is one, else customer_row_id
    # (orgs mapped from customer text alone must not collapse together).
    # The customer's own dollars ride along, summed per org.
    amounts = load_portfolio_amounts(config, R)
    enriched[AMOUNT_COL] = enriched["customer_row_id"].map(amounts)
    orgs = dedupe_portfolio(enriched)
    port = orgs[orgs["level0_one_earth_category"].notna()]
    FOREST, MOSS, GOLD, LIGHT = "#2C5F2D", "#97BC62", "#D9A21B", "#D5DFD2"
    return (
        AMOUNT_COL,
        ECO_USD,
        FOREST,
        GOLD,
        LIGHT,
        MOSS,
        alt,
        eco,
        mo,
        orgs,
        pd,
        port,
        window,
    )


@app.cell
def _(AMOUNT_COL, ECO_USD, eco, pd, port):
    # ---- All comparison math, in the open. Org shares and dollar shares
    # side by side; dollars = ecosystem Funding_<year> over the engagement
    # window vs the customer's own amounts (funding.portfolio_amounts).
    def pct(s):
        return (s.value_counts(normalize=True) * 100).round(1)

    def pct_usd(df, level_col, usd_col):
        usd = df.groupby(level_col)[usd_col].sum()
        return (usd / usd.sum() * 100).round(1) if usd.sum() > 0 else usd * float("nan")

    def share_table(eco_df, port_df, level_col="level0_one_earth_category"):
        t = pd.DataFrame(
            {
                "n_ecosystem": eco_df[level_col].value_counts(),
                "ecosystem_pct": pct(eco_df[level_col]),
                "ecosystem_funding_pct": pct_usd(eco_df, level_col, ECO_USD),
                "n_portfolio": port_df[level_col].value_counts(),
                "portfolio_pct": pct(port_df[level_col]),
                "invested_pct": pct(
                    port_df.loc[port_df["disposition"] == "invested", level_col]
                ),
                "passed_pct": pct(
                    port_df.loc[port_df["disposition"] == "passed", level_col]
                ),
            }
        ).fillna(0)
        t["tilt_vs_eco"] = (t["portfolio_pct"] - t["ecosystem_pct"]).round(1)
        t.index.name = "category"
        return t.sort_values("ecosystem_pct", ascending=False)

    def funding_table(eco_df, port_df, level_col="level0_one_earth_category"):
        """Dollar shares: where capital concentrates on each side."""
        funded = port_df[port_df[AMOUNT_COL].notna()]
        t = pd.DataFrame(
            {
                "ecosystem_pct": pct(eco_df[level_col]),
                "ecosystem_usd": eco_df.groupby(level_col)[ECO_USD].sum(),
                "ecosystem_funding_pct": pct_usd(eco_df, level_col, ECO_USD),
                "n_portfolio_with_amount": funded[level_col].value_counts(),
                "portfolio_usd": funded.groupby(level_col)[AMOUNT_COL].sum(),
                "portfolio_funding_pct": pct_usd(funded, level_col, AMOUNT_COL),
                "portfolio_pct": pct(port_df[level_col]),
            }
        )
        t[["ecosystem_usd", "portfolio_usd", "n_portfolio_with_amount"]] = t[
            ["ecosystem_usd", "portfolio_usd", "n_portfolio_with_amount"]
        ].fillna(0)
        if t["portfolio_funding_pct"].notna().any():
            t["portfolio_funding_pct"] = t["portfolio_funding_pct"].fillna(0)
        t["tilt_funding_vs_eco"] = (
            t["portfolio_funding_pct"] - t["ecosystem_funding_pct"]
        ).round(1)
        t.index.name = "category"
        return t.sort_values("ecosystem_funding_pct", ascending=False)

    def conversion(port_df):
        c = port_df.groupby("level0_one_earth_category")["disposition"].agg(
            n_invested=lambda s: int((s == "invested").sum()),
            n_passed=lambda s: int((s == "passed").sum()),
        )
        c["conversion_rate"] = (
            c["n_invested"] / (c["n_invested"] + c["n_passed"])
        ).round(3)
        c.index.name = "pillar"
        return c.sort_values("conversion_rate", ascending=False)

    # The baseline carries the raw repr-list taxonomy columns too, so pick the
    # primary-category columns out before renaming onto the portfolio's names.
    eco_l0 = eco[["_lvl0", "_org_type", ECO_USD]].rename(
        columns={"_lvl0": "level0_one_earth_category"}
    )
    eco_l1 = eco[["_lvl1", "_org_type", ECO_USD]].rename(
        columns={"_lvl1": "level1_one_earth_category"}
    )
    eco_fp, eco_np = (eco_l0[eco_l0["_org_type"] == t] for t in ("For Profit", "Non Profit"))
    port_fp = port[port["entity_type"] == "for_profit"]
    port_np = port[port["entity_type"] == "nonprofit"]

    pillar = share_table(eco_l0, port)
    pillar["eco_forprofit_pct"] = pct(eco_fp["level0_one_earth_category"])
    pillar["eco_nonprofit_pct"] = pct(eco_np["level0_one_earth_category"])
    pillar = pillar.fillna(0)
    pillar_fp = share_table(eco_fp, port_fp)
    pillar_np = share_table(eco_np, port_np)
    conv = conversion(port)
    conv_fp = conversion(port_fp)
    conv_np = conversion(port_np)

    fund = funding_table(eco_l0, port)
    fund_np = funding_table(eco_np, port_np)
    _sub = "level1_one_earth_category"
    fund_np_sub = funding_table(
        eco_l1[eco_l1["_org_type"] == "Non Profit"], port_np[port_np[_sub].notna()], _sub
    )
    return (
        conv,
        conv_fp,
        conv_np,
        fund,
        fund_np,
        fund_np_sub,
        pillar,
        pillar_fp,
        pillar_np,
    )


@app.cell
def _(mo):
    mo.md(f"""
    # One Small Planet vs. the US climate ecosystem
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ecosystem Context
    """)
    return


@app.cell
def _(pillar):
    pillar.reset_index()
    return


@app.cell
def _(FOREST, GOLD, MOSS, SLATE, fund, pillar_chart, window):
    _years = f"{window[0]}–{window[1]}" if window else "all-time"
    funding_chart = pillar_chart(
        fund,
        {
            "ecosystem_pct": "Ecosystem organizations",
            "ecosystem_funding_pct": f"Ecosystem dollars ({_years})",
        },
        [MOSS, SLATE, GOLD, FOREST],
        "Where the orgs are vs where the money is",
        ylabel="% of ecosystem",
    )
    funding_chart
    return


@app.cell
def _(FOREST, GOLD, MOSS, alt, mo, pillar, pillar_fp, pillar_np):
    def pillar_chart(df, series, colors, title, ylabel="% of orgs with a pillar"):
        """Grouped bars per pillar; series = {csv_column: display name}."""
        _long = df.reset_index().melt(
            id_vars="category",
            value_vars=list(series),
            var_name="series",
            value_name="pct",
        )
        _long["series"] = _long["series"].map(series)
        return mo.ui.altair_chart(
            alt.Chart(_long)
            .mark_bar()
            .encode(
                x=alt.X(
                    "series:N",
                    title=None,
                    axis=None,
                    sort=list(series.values()),
                ),
                y=alt.Y("pct:Q", title=ylabel),
                color=alt.Color(
                    "series:N",
                    title=None,
                    scale=alt.Scale(
                        domain=list(series.values()), range=colors
                    ),
                    legend=alt.Legend(orient="bottom"),
                ),
                column=alt.Column(
                    "category:N",
                    title=None,
                    header=alt.Header(labelFontSize=11),
                ),
                tooltip=["category", "series", "pct"],
            )
            .properties(width=110, height=260, title=title)
        )

    SLATE = "#50808E"
    mo.vstack(
        [
            mo.md("## Where OSP sits in the landscape"),
            pillar_chart(
                pillar,
                {
                    "ecosystem_pct": "Ecosystem",
                    "portfolio_pct": "OSP full deal flow",
                    "invested_pct": "OSP 'invested' only",
                },
                [MOSS, SLATE, GOLD, FOREST],
                "Full Ecosystem",
            ),
            pillar_chart(
                pillar_fp,
                {
                    "ecosystem_pct": "Ecosystem for-profits",
                    "portfolio_pct": "OSP companies: deal flow",
                    "invested_pct": "OSP companies: invested",
                },
                [MOSS, GOLD, FOREST],
                "For-profits only",
            ),
            pillar_chart(
                pillar_np,
                {
                    "ecosystem_pct": "Ecosystem nonprofits",
                    "portfolio_pct": "OSP grants: deal flow",
                    "invested_pct": "OSP grants: invested",
                },
                [SLATE, GOLD, FOREST],
                "Nonprofits only",
            ),
        ]
    )
    return SLATE, pillar_chart


@app.cell
def _(FOREST, GOLD, MOSS, SLATE, alt, fund_np, fund_np_sub, mo, pillar_chart, window):
    # ---- Dollar-weighted view. OSP's own dollars exist only for grants (the
    # for-profit sheet carries no check sizes), so this is the nonprofit
    # segment: OSP grant dollars vs Candid-recorded grant dollars received by
    # ecosystem nonprofits, same years.
    _years = f"{window[0]}–{window[1]}" if window else "all-time"
    _osp_total = fund_np["portfolio_usd"].sum()
    _n_funded = int(fund_np["n_portfolio_with_amount"].sum())

    def usd_sub_chart(df, title, top=10):
        """Horizontal bars: dollar share by sub-pillar, both sides."""
        # Union of each side's top-N so neither side's concentration hides.
        _top = set(df.nlargest(top, "ecosystem_funding_pct").index) | set(
            df.nlargest(top, "portfolio_funding_pct").index
        )
        _keep = df[df.index.isin(_top)]
        _long = _keep.reset_index().melt(
            id_vars="category",
            value_vars=["ecosystem_funding_pct", "portfolio_funding_pct"],
            var_name="series",
            value_name="pct",
        )
        _long["series"] = _long["series"].map(
            {
                "ecosystem_funding_pct": f"Ecosystem nonprofit dollars ({_years})",
                "portfolio_funding_pct": f"OSP grant dollars ({_years})",
            }
        )
        _order = _keep.sort_values("portfolio_funding_pct", ascending=False).index.tolist()
        return mo.ui.altair_chart(
            alt.Chart(_long)
            .mark_bar()
            .encode(
                y=alt.Y("category:N", sort=_order, title=None),
                x=alt.X("pct:Q", title="% of dollars"),
                yOffset="series:N",
                color=alt.Color(
                    "series:N",
                    title=None,
                    scale=alt.Scale(range=[SLATE, FOREST]),
                    legend=alt.Legend(orient="bottom"),
                ),
                tooltip=["category", "series", "pct"],
            )
            .properties(width=620, height=300, title=title)
        )

    mo.vstack(
        [
            mo.md("## Where the dollars go — grants"),
            pillar_chart(
                fund_np,
                {
                    "ecosystem_pct": "Ecosystem nonprofits (orgs)",
                    "ecosystem_funding_pct": f"Ecosystem nonprofit dollars ({_years})",
                    "portfolio_pct": "OSP grantees (orgs)",
                    "portfolio_funding_pct": f"OSP grant dollars ({_years})",
                },
                [MOSS, SLATE, GOLD, FOREST],
                "Nonprofits: orgs vs dollars, by pillar",
                ylabel="% of segment",
            ),
            usd_sub_chart(fund_np_sub, "Nonprofits: dollar share by sub-pillar"),
            mo.md(
                f"*OSP grant dollars: ${_osp_total / 1e6:,.1f}M across {_n_funded} "
                f"mapped grantees ({_years}). Ecosystem nonprofit dollars = grants "
                f"received as recorded by Candid. Companies are not dollar-weighted: "
                f"OSP's investment amounts are not in the source data.*"
            ),
        ]
    )
    return


@app.cell
def _(FOREST, LIGHT, alt, conv, conv_fp, conv_np, mo):
    def conv_chart(df, title):
        """Stacked invested/passed bars with conversion-rate labels."""
        _long = df.reset_index().melt(
            id_vars=["pillar", "conversion_rate"],
            value_vars=["n_invested", "n_passed"],
            var_name="outcome",
            value_name="n",
        )
        _long["outcome"] = _long["outcome"].map(
            {"n_invested": "Invested", "n_passed": "Passed"}
        )
        _order = df.sort_values(
            "conversion_rate", ascending=False
        ).index.tolist()
        _bars = (
            alt.Chart(_long)
            .mark_bar()
            .encode(
                y=alt.Y("pillar:N", sort=_order, title=None),
                x=alt.X("n:Q", title="deals with a taxonomy match"),
                color=alt.Color(
                    "outcome:N",
                    title=None,
                    scale=alt.Scale(
                        domain=["Invested", "Passed"], range=[FOREST, LIGHT]
                    ),
                    legend=alt.Legend(orient="bottom"),
                ),
                order=alt.Order("outcome:N"),
                tooltip=["pillar", "outcome", "n", "conversion_rate"],
            )
        )
        _totals = df.reset_index()
        _totals["total"] = _totals["n_invested"] + _totals["n_passed"]
        _labels = (
            alt.Chart(_totals)
            .mark_text(align="left", dx=6, color=FOREST, fontWeight="bold")
            .encode(
                y=alt.Y("pillar:N", sort=_order),
                x="total:Q",
                text=alt.Text("conversion_rate:Q", format=".0%"),
            )
        )
        return mo.ui.altair_chart(
            (_bars + _labels).properties(width=620, height=200, title=title)
        )

    _nature = conv.at["Nature Conservation", "conversion_rate"]
    _energy = conv.at["Energy Transition", "conversion_rate"]
    mo.vstack(
        [
            mo.md("## Deals seen vs deals done"),
            conv_chart(conv, "Blended: OSP passes on energy, converts on nature"),
            conv_chart(conv_fp, "Companies only"),
            conv_chart(conv_np, "Nonprofit grants only"),
            mo.md(
                f"*Labels = conversion rate. Blended: a nature deal is "
                f"{_nature / _energy:.1f}× likelier to be funded than an "
                f"energy deal ({_nature:.0%} vs {_energy:.0%}).*"
            ),
        ]
    )
    return


@app.cell
def _(mo, orgs):
    # What didn't map — counts computed here; the adjudication (which
    # refusals are genuinely out of scope vs walk misses vs taxonomy gaps)
    # lives in data/results/nomatch_analysis.md, and every correction that
    # survived the adversarial gate is in taxonomy_overrides.json.
    _no_pillar = orgs[orgs["level0_one_earth_category"].isna()]
    _no_text = int(_no_pillar["text_for_taxonomy"].isna().sum())
    _cust = orgs["status"] == "unmatched_final"
    mo.md(f"""
    ## What didn't map — {len(_no_pillar)} orgs, mostly signal

    - **{_no_text} have no usable text** even after the customer round-trip.
    - **{len(_no_pillar) - _no_text} had text and were refused** by the walk.
      Two review rounds (Run B: 74 rows; customer-text batch: 28 rows) found the
      large majority genuinely outside the solutions taxonomy — humanitarian,
      health, cultural and Indigenous-sovereignty work, non-climate businesses
      among passed deals — with walk misses corrected via the overrides file and
      a residue of taxonomy gaps (water access, adaptation & resilience,
      Indigenous biocultural stewardship).
    - **{int(_cust.sum())} orgs were invisible to standard data sources**; OSP's
      own descriptions placed {int((_cust & orgs['level0_one_earth_category'].notna()).sum())}
      of them (incl. the six P1-ruling placements).

    Row-level evidence: `data/results/nomatch_analysis.md`. The customer-facing
    narrative is `notebooks/report.py` in the engagement repo.
    """)
    return


if __name__ == "__main__":
    app.run()
