"""Engagement findings notebook (marimo) — charts with the code showing.

Launch from the ENGAGEMENT REPO ROOT (marimo edit to see/modify code):

    PYTHONPATH=<vdl-tools> marimo edit \
        <vdl-tools>/vdl_tools/portfolio_comparison/review_apps/findings.py

Reads the compare-stage artifacts (comparison_*.csv), the enriched
portfolio, and the customer ask. Unmapped-class counts cite
nomatch_analysis.md (second-reader verified).
"""

import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd

    R = Path.cwd() / "data" / "results"
    pillar = pd.read_csv(R / "comparison_pillar.csv", index_col=0)
    conv = pd.read_csv(R / "comparison_conversion.csv", index_col=0)
    enriched = pd.read_parquet(R / "enriched_portfolio.parquet")
    FOREST, MOSS, GOLD, LIGHT = "#2C5F2D", "#97BC62", "#D9A21B", "#D5DFD2"
    return FOREST, GOLD, LIGHT, MOSS, R, conv, enriched, mo, pd, pillar, plt


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
def _(FOREST, GOLD, MOSS, mo, pillar, plt):
    _fig, _ax = plt.subplots(figsize=(9, 4.2))
    _x = range(len(pillar))
    _w = 0.27
    for _i, (_col, _color, _label) in enumerate([
        ("ecosystem_pct", MOSS, "US climate ecosystem"),
        ("portfolio_pct", GOLD, "OSP full deal flow"),
        ("invested_pct", FOREST, "OSP invested only"),
    ]):
        _bars = _ax.bar([p + (_i - 1) * _w for p in _x], pillar[_col],
                        _w, color=_color, label=_label)
        _ax.bar_label(_bars, fmt="%.0f", fontsize=8, color="#555")
    _ax.set_xticks(list(_x),
                   [c.replace(" ", "\n") for c in pillar.index], fontsize=9)
    _ax.set_ylabel("% of orgs with a pillar")
    _ax.legend(frameon=False, fontsize=9)
    _ax.spines[["top", "right"]].set_visible(False)
    _ax.set_title("The invested book mirrors the ecosystem; deal flow doesn't",
                  fontsize=12, loc="left", color=FOREST, fontweight="bold")
    mo.vstack([
        mo.md("## Where OSP sits in the landscape"),
        _fig,
        mo.md("*Invested holdings track the landscape's shape (Nature "
              "Conservation leads). Deal flow runs 21 points light on Nature "
              "Conservation and heavy on Energy Transition.*"),
    ])
    return


@app.cell
def _(FOREST, LIGHT, conv, mo, plt):
    _c = conv.sort_values("conversion_rate")
    _fig, _ax = plt.subplots(figsize=(9, 3.6))
    _ax.barh(_c.index, _c["n_invested"], color=FOREST, label="Invested")
    _ax.barh(_c.index, _c["n_passed"], left=_c["n_invested"], color=LIGHT,
             label="Passed")
    for _i, (_pillar, _row) in enumerate(_c.iterrows()):
        _ax.text(_row["n_invested"] + _row["n_passed"] + 1.5, _i,
                 f"{_row['conversion_rate']:.0%}", va="center", fontsize=10,
                 color=FOREST, fontweight="bold")
    _ax.set_xlabel("deals with a taxonomy match")
    _ax.legend(frameon=False, fontsize=9, loc="lower right")
    _ax.spines[["top", "right"]].set_visible(False)
    _ax.set_title("OSP passes on energy, converts on nature "
                  "(label = conversion rate)", fontsize=12, loc="left",
                  color=FOREST, fontweight="bold")
    mo.vstack([
        mo.md("## Deals seen vs deals done"),
        _fig,
        mo.md("*A nature deal in OSP's pipeline is 3.4× likelier to be funded "
              "than an energy deal (50% vs 15% conversion).*"),
    ])
    return


@app.cell
def _(FOREST, GOLD, LIGHT, MOSS, R, enriched, mo, pd, plt):
    # Unmapped decomposition. The 46 = current customer ask (textless rows);
    # class splits for the 68 with-text rows come from the second-reader-
    # verified nomatch_analysis.md (52 out-of-scope / 7 vague / 9 recoverable).
    _no_pillar = enriched[enriched["level0_one_earth_category"].isna()]
    _ask = pd.read_excel(sorted(R.glob("customer_review_*.xlsx"))[-1])
    _n_ask = len(set(_ask["ID (do not edit)"]) & set(_no_pillar["customer_row_id"]))
    _classes = {
        f"No usable text — below the data radar ({_n_ask})": (_n_ask, GOLD),
        "Verified outside climate scope (52)": (52, MOSS),
        "Text too vague to map (7)": (7, LIGHT),
        "Recoverable via mapping improvements (9)": (9, FOREST),
    }
    _fig, _ax = plt.subplots(figsize=(7.5, 3.8))
    _ax.pie([v for v, _ in _classes.values()], labels=list(_classes),
            colors=[c for _, c in _classes.values()],
            wedgeprops=dict(width=0.42), textprops=dict(fontsize=9),
            autopct=lambda p: f"{p:.0f}%", pctdistance=0.79)
    _ax.set_title(f"{len(_no_pillar)} orgs without a pillar — mostly signal, "
                  "not failure", fontsize=12, loc="left", color=FOREST,
                  fontweight="bold")
    mo.vstack([
        mo.md("## What didn't map, and why"),
        _fig,
        mo.md(f"""
- **{_n_ask} have no text anywhere** — fiscally sponsored projects,
  Indigenous-led and international orgs that never file US tax forms under
  their own names, plus dead/unreadable sites. *The {len(_ask)}-row customer
  ask covers these.*
- **52 are verifiably not climate** (reviewed against pillar definitions) —
  36 of them passed deals: a finding about OSP's deal sources, not an error.
- **9 expose the taxonomy's blind spots** — adaptation & resilience, water
  supply, Indigenous biocultural stewardship: candidate One Earth amendments.
  Full per-org evidence: `data/results/nomatch_analysis.md`.
"""),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
## Key findings
1. **OSP's invested book mirrors the climate ecosystem's shape; its deal flow
   doesn't** — energy-heavy, nature-light.
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
