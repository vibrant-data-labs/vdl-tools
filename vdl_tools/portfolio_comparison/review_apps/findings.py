"""Engagement findings notebook (marimo) — charts with the code showing.

Launch from the ENGAGEMENT REPO ROOT (marimo edit to see/modify code):

    PYTHONPATH=<vdl-tools> marimo edit \
        <vdl-tools>/vdl_tools/portfolio_comparison/review_apps/findings.py

Interactive altair charts; all comparison math computed in visible
cells from the raw ecosystem + enriched portfolio (extend freely, e.g.
funding-weighted cuts). Unmapped-class counts cite
nomatch_analysis.md (second-reader verified).
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

    from vdl_tools.portfolio_comparison.comparison import load_ecosystem

    R = Path.cwd() / "data" / "results"
    enriched = pd.read_parquet(R / "enriched_portfolio.parquet")
    # Raw comparison inputs — the tables are computed in the next cell so the
    # math is visible and extensible here (the compare stage writes the same
    # tables as recorded CSVs for the ledger).
    eco = load_ecosystem(R)          # pinned universe + _lvl0/_lvl1/_org_type
    port = enriched.drop_duplicates(subset="matched_id")
    port = port[port["level0_one_earth_category"].notna()]
    FOREST, MOSS, GOLD, LIGHT = "#2C5F2D", "#97BC62", "#D9A21B", "#D5DFD2"
    return (FOREST, GOLD, LIGHT, MOSS, R, alt, eco, enriched, mo, pd, port)


@app.cell
def _(eco, pd, port):
    # ---- All comparison math, in the open. Extend freely — e.g. the
    # ecosystem frame carries funding columns (Total_Funding_$ etc.), and
    # per-org funding fractions by taxonomy path live in
    # taxonomy_mapping_distributed_funding.json for funding-weighted cuts.
    def share_table(eco_series, port_df, level_col="level0_one_earth_category"):
        def pct(s):
            return (s.value_counts(normalize=True) * 100).round(1)

        t = pd.DataFrame({
            "n_ecosystem": eco_series.value_counts(),
            "ecosystem_pct": pct(eco_series),
            "n_portfolio": port_df[level_col].value_counts(),
            "portfolio_pct": pct(port_df[level_col]),
            "invested_pct": pct(port_df.loc[port_df["disposition"] == "invested",
                                            level_col]),
            "passed_pct": pct(port_df.loc[port_df["disposition"] == "passed",
                                          level_col]),
        }).fillna(0)
        t["tilt_vs_eco"] = (t["portfolio_pct"] - t["ecosystem_pct"]).round(1)
        t.index.name = "category"
        return t.sort_values("ecosystem_pct", ascending=False)

    def conversion(port_df):
        c = port_df.groupby("level0_one_earth_category")["disposition"].agg(
            n_invested=lambda s: int((s == "invested").sum()),
            n_passed=lambda s: int((s == "passed").sum()))
        c["conversion_rate"] = (
            c["n_invested"] / (c["n_invested"] + c["n_passed"])).round(3)
        c.index.name = "pillar"
        return c.sort_values("conversion_rate", ascending=False)

    eco_fp = eco[eco["_org_type"] == "For Profit"]
    eco_np = eco[eco["_org_type"] == "Non Profit"]
    port_fp = port[port["entity_type"] == "for_profit"]
    port_np = port[port["entity_type"] == "nonprofit"]

    pillar = share_table(eco["_lvl0"].dropna(), port)
    pillar["eco_forprofit_pct"] = (eco_fp["_lvl0"].dropna()
                                   .value_counts(normalize=True) * 100).round(1)
    pillar["eco_nonprofit_pct"] = (eco_np["_lvl0"].dropna()
                                   .value_counts(normalize=True) * 100).round(1)
    pillar = pillar.fillna(0)
    pillar_fp = share_table(eco_fp["_lvl0"].dropna(), port_fp)
    pillar_np = share_table(eco_np["_lvl0"].dropna(), port_np)
    conv = conversion(port)
    conv_fp = conversion(port_fp)
    conv_np = conversion(port_np)
    return conv, conv_fp, conv_np, pillar, pillar_fp, pillar_np

@app.cell
def _(enriched, mo):
    _matched = enriched["one_earth_category"].notna() & (
        enriched["one_earth_category"] != "NoMatch")
    mo.md(f"""
    # One Small Planet vs. the US climate ecosystem
    **Phase 1–2 findings** · {len(enriched)} portfolio orgs · 320 identity-matched ·
    {int(enriched['Summary'].notna().sum())} with text · {int(_matched.sum())}
    taxonomy-mapped · {int(enriched['Latitude'].notna().sum())} geocoded
    """)
    return


@app.cell
def _(FOREST, GOLD, MOSS, alt, mo, pillar, pillar_fp, pillar_np):
    def pillar_chart(df, series, colors, title):
        """Grouped bars per pillar; series = {csv_column: display name}."""
        _long = df.reset_index().melt(
            id_vars="category", value_vars=list(series),
            var_name="series", value_name="pct")
        _long["series"] = _long["series"].map(series)
        return mo.ui.altair_chart(
            alt.Chart(_long).mark_bar().encode(
                x=alt.X("series:N", title=None, axis=None,
                        sort=list(series.values())),
                y=alt.Y("pct:Q", title="% of orgs with a pillar"),
                color=alt.Color("series:N", title=None,
                                scale=alt.Scale(domain=list(series.values()),
                                                range=colors),
                                legend=alt.Legend(orient="bottom")),
                column=alt.Column("category:N", title=None,
                                  header=alt.Header(labelFontSize=11)),
                tooltip=["category", "series", "pct"],
            ).properties(width=110, height=260, title=title))

    SLATE = "#50808E"
    mo.vstack([
        mo.md("## Where OSP sits in the landscape"),
        pillar_chart(pillar, {
            "eco_forprofit_pct": "Ecosystem: for-profits",
            "eco_nonprofit_pct": "Ecosystem: nonprofits",
            "portfolio_pct": "OSP full deal flow",
            "invested_pct": "OSP invested only",
        }, [MOSS, SLATE, GOLD, FOREST],
            "Blended: for-profit and nonprofit climate have opposite shapes"),
        pillar_chart(pillar_fp, {
            "ecosystem_pct": "Ecosystem for-profits",
            "portfolio_pct": "OSP companies: deal flow",
            "invested_pct": "OSP companies: invested",
        }, [MOSS, GOLD, FOREST],
            "For-profits only: OSP runs energy-light vs the investable universe"),
        pillar_chart(pillar_np, {
            "ecosystem_pct": "Ecosystem nonprofits",
            "portfolio_pct": "OSP grants: deal flow",
            "invested_pct": "OSP grants: invested",
        }, [SLATE, GOLD, FOREST],
            "Nonprofits only: OSP grants vs the nonprofit landscape"),
        mo.md("*The blended ecosystem hides a split: for-profit climate is "
              "energy-dominated (63.5% Energy Transition) while nonprofit "
              "climate is nature-dominated (63.2% Nature Conservation). The "
              "segment charts pair each side of OSP with its own universe.*"),
    ])
    return


@app.cell
def _(FOREST, LIGHT, alt, conv, conv_fp, conv_np, mo):
    def conv_chart(df, title):
        """Stacked invested/passed bars with conversion-rate labels."""
        _long = df.reset_index().melt(
            id_vars=["pillar", "conversion_rate"],
            value_vars=["n_invested", "n_passed"],
            var_name="outcome", value_name="n")
        _long["outcome"] = _long["outcome"].map(
            {"n_invested": "Invested", "n_passed": "Passed"})
        _order = df.sort_values("conversion_rate",
                                ascending=False).index.tolist()
        _bars = alt.Chart(_long).mark_bar().encode(
            y=alt.Y("pillar:N", sort=_order, title=None),
            x=alt.X("n:Q", title="deals with a taxonomy match"),
            color=alt.Color("outcome:N", title=None,
                            scale=alt.Scale(domain=["Invested", "Passed"],
                                            range=[FOREST, LIGHT]),
                            legend=alt.Legend(orient="bottom")),
            order=alt.Order("outcome:N"),
            tooltip=["pillar", "outcome", "n", "conversion_rate"],
        )
        _totals = df.reset_index()
        _totals["total"] = _totals["n_invested"] + _totals["n_passed"]
        _labels = alt.Chart(_totals).mark_text(
            align="left", dx=6, color=FOREST, fontWeight="bold").encode(
            y=alt.Y("pillar:N", sort=_order), x="total:Q",
            text=alt.Text("conversion_rate:Q", format=".0%"),
        )
        return mo.ui.altair_chart(
            (_bars + _labels).properties(width=620, height=200, title=title))

    mo.vstack([
        mo.md("## Deals seen vs deals done"),
        conv_chart(conv, "Blended: OSP passes on energy, converts on nature"),
        conv_chart(conv_fp, "Companies only"),
        conv_chart(conv_np, "Nonprofit grants only"),
        mo.md("*Labels = conversion rate. Blended: a nature deal is 3.4\u00d7 "
              "likelier to be funded than an energy deal (50% vs 15%).*"),
    ])
    return


@app.cell
def _(R, enriched, mo, pd):
    # What didn't map — adjudicated numbers (see nomatch_analysis.md; the
    # review pool is exhausted: 7 corrections applied, 11 proposals rejected).
    _no_pillar = enriched[enriched["level0_one_earth_category"].isna()]
    _ask = pd.read_excel(sorted(R.glob("customer_review_*.xlsx"))[-1])
    _n_ask = len(set(_ask["ID (do not edit)"]) & set(_no_pillar["customer_row_id"]))
    _n_text = int(_no_pillar["text_for_taxonomy"].notna().sum())
    mo.md(f"""
    ## What didn't map, and why — {len(_no_pillar)} orgs, mostly signal

    - **{_n_ask} have no usable text** — fiscally sponsored projects,
      Indigenous-led and international orgs that never file US tax forms under
      their own names, plus dead/unreadable sites. *The {len(_ask)}-row customer
      ask covers these.*
    - **Of the {_n_text} with text: 52 are verifiably not climate** (reviewed
      against pillar definitions) — 36 of them passed deals: a finding about
      OSP's deal sources, not an error.
    - **7 are too vague to map**; the rest were reviewed and rejected, or await
      One Earth taxonomy amendments (adaptation & resilience, water supply,
      Indigenous biocultural stewardship). Every corrected placement lives in
      `taxonomy_overrides.json`; full evidence in `data/results/nomatch_analysis.md`.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Key findings
    1. **For-profit and nonprofit climate are opposite worlds** — companies are
       63.5% Energy Transition, nonprofits 63.2% Nature Conservation. Against the
       investable (for-profit) universe, OSP's deal flow is energy-light and
       nature-heavy — the reverse of the blended-ecosystem read.
    2. **Conversion tells the strategy**: 50% on nature vs 15% on energy — a
       nature-conviction investor swimming in energy deal flow.
    3. **46 portfolio orgs are invisible to standard data infrastructure** —
       mapping them requires OSP's own words (ask is out).
    4. **The taxonomy has blind spots OSP's portfolio exposes**: adaptation,
       water supply, Indigenous biocultural work.

    *Next: OSP answers the ask · funding-weighted comparison · sub-pillar
    drill-downs · dashboard + written report.*
    """)
    return


@app.cell
def _(enriched):
    enriched
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
