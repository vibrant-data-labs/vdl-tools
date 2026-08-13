"""Engagement findings notebook (marimo) — charts with the code showing.

Launch from the ENGAGEMENT REPO ROOT (marimo edit to see/modify code):

    PYTHONPATH=<vdl-tools> marimo edit \
        <vdl-tools>/vdl_tools/portfolio_comparison/review_apps/findings.py

Interactive altair charts. Reads the compare-stage artifacts (comparison_*.csv), the enriched
portfolio, and the customer ask. Unmapped-class counts cite
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

    R = Path.cwd() / "data" / "results"
    pillar = pd.read_csv(R / "comparison_pillar.csv", index_col=0)
    pillar_fp = pd.read_csv(R / "comparison_pillar_forprofit.csv", index_col=0)
    pillar_np = pd.read_csv(R / "comparison_pillar_nonprofit.csv", index_col=0)
    conv = pd.read_csv(R / "comparison_conversion.csv", index_col=0)
    conv_fp = pd.read_csv(R / "comparison_conversion_forprofit.csv", index_col=0)
    conv_np = pd.read_csv(R / "comparison_conversion_nonprofit.csv", index_col=0)
    enriched = pd.read_parquet(R / "enriched_portfolio.parquet")
    FOREST, MOSS, GOLD, LIGHT = "#2C5F2D", "#97BC62", "#D9A21B", "#D5DFD2"
    return (FOREST, GOLD, LIGHT, MOSS, R, alt, conv, conv_fp, conv_np,
            enriched, mo, pd, pillar, pillar_fp, pillar_np)


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
def _(FOREST, GOLD, LIGHT, MOSS, R, alt, enriched, mo, pd):
    # Unmapped decomposition. The textless count = current customer ask rows;
    # class splits for the with-text rows come from the second-reader-
    # verified nomatch_analysis.md (52 out-of-scope / 7 vague / 9 recoverable).
    _no_pillar = enriched[enriched["level0_one_earth_category"].isna()]
    _ask = pd.read_excel(sorted(R.glob("customer_review_*.xlsx"))[-1])
    _n_ask = len(set(_ask["ID (do not edit)"]) & set(_no_pillar["customer_row_id"]))
    _df = pd.DataFrame({
        "why": [f"No usable text - below the data radar ({_n_ask})",
                "Verified outside climate scope (52)",
                "Text too vague to map (7)",
                "Recoverable via mapping improvements (9)"],
        "n": [_n_ask, 52, 7, 9],
        "color": [GOLD, MOSS, LIGHT, FOREST],
    })
    donut_chart = mo.ui.altair_chart(
        alt.Chart(_df).mark_arc(innerRadius=70).encode(
            theta="n:Q",
            color=alt.Color("why:N", title=None,
                            scale=alt.Scale(domain=_df["why"].tolist(),
                                            range=_df["color"].tolist()),
                            legend=alt.Legend(orient="right", labelLimit=320)),
            tooltip=["why", "n"],
        ).properties(width=340, height=300,
                     title=f"{len(_no_pillar)} orgs without a pillar - mostly signal, not failure"))
    mo.vstack([
        mo.md("## What didn't map, and why"),
        donut_chart,
        mo.md(f"""
- **{_n_ask} have no text anywhere** - fiscally sponsored projects,
  Indigenous-led and international orgs that never file US tax forms under
  their own names, plus dead/unreadable sites. *The {len(_ask)}-row customer
  ask covers these.*
- **52 are verifiably not climate** (reviewed against pillar definitions) -
  36 of them passed deals: a finding about OSP's deal sources, not an error.
- **9 expose the taxonomy's blind spots** - adaptation & resilience, water
  supply, Indigenous biocultural stewardship: candidate One Earth amendments.
  Full per-org evidence: `data/results/nomatch_analysis.md`.
"""),
    ])
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


if __name__ == "__main__":
    app.run()
