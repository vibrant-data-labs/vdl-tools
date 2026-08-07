"""Enriched-portfolio workbench + triage (marimo app, read-only).

Launch from the ENGAGEMENT REPO ROOT:

    PYTHONPATH=<vdl-tools> marimo run \
        <vdl-tools>/vdl_tools/portfolio_comparison/review_apps/explore_enrichment.py

Two jobs (Zein, 2026-08-07): an analyst workbench (all 64 columns behind
marimo's native dataframe explorer + taxonomy drill-down) and a triage
surface (every not-fully-enriched row diagnosed with its remedy). Fixes go
through `set-id`, the customer ask, or the pipeline — never this app.
"""

import marimo

__generated_with = "0.23.14"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import re
    from pathlib import Path

    import marimo as mo
    import pandas as pd

    ROOT = Path.cwd()
    RESULTS = ROOT / "data" / "results"
    enriched = pd.read_parquet(RESULTS / "enriched_portfolio.parquet")
    state = json.loads((ROOT / "pipeline_state.json").read_text())
    decisions = [json.loads(line) for line in
                 (RESULTS / "decisions.jsonl").read_text().splitlines()] \
        if (RESULTS / "decisions.jsonl").exists() else []
    return decisions, enriched, mo, pd, re, state


@app.cell
def _(decisions, enriched, mo, pd, state):
    _matched = enriched["one_earth_category"].notna() & (
        enriched["one_earth_category"] != "NoMatch")
    _stages = pd.DataFrame([
        {"stage": n, **{k: str(v)[:70] for k, v in i.items()}}
        for n, i in state.get("stages", {}).items()])
    mo.vstack([
        mo.md(f"# Portfolio workbench — {len(enriched)} rows · "
              f"{int(_matched.sum())} taxonomy-matched · "
              f"{len(decisions)} decisions"),
        mo.accordion({"📜 Run ledger": mo.ui.table(_stages, selection=None)}),
    ])
    return


@app.cell
def _(enriched, mo, pd, re):
    # ---- Triage: why is each not-fully-enriched row the way it is, and
    # what's the remedy?
    _INVESTOR = re.compile(
        r"\b(ventures?|capital|fund|holdings|investments?|vc)\b", re.I)

    def _diagnose(r):
        has_text = pd.notna(r.get("text_for_taxonomy"))
        matched = pd.notna(r.get("one_earth_category")) and \
            r.get("one_earth_category") != "NoMatch"
        if has_text and matched:
            return None, None
        if not has_text:
            q = str(r.get("text_quality") or "no_url")
            if q in ("dead", "parked", "thin"):
                return f"site {q}, no other text", "customer ask (new URL or description)"
            return "no url, no source text", "customer ask (description)"
        name_blob = f"{r.get('customer_name')} {str(r.get('Summary'))[:200]}"
        if _INVESTOR.search(str(r.get("customer_name") or "")) or \
                _INVESTOR.search(name_blob[:60]):
            return "investor entity", "policy: map investors separately?"
        return "has text, no taxonomy fit", "review text / re-run walk"

    _diag = enriched.apply(
        lambda r: pd.Series(_diagnose(r), index=["problem", "remedy"]), axis=1)
    triage = pd.concat([enriched, _diag], axis=1)
    triage_rows = triage[triage["problem"].notna()]
    _counts = triage_rows.groupby(["problem", "remedy"]).size().reset_index(name="n")
    mo.vstack([
        mo.md(f"## Triage — {len(triage_rows)} rows not fully enriched"),
        mo.ui.table(_counts.sort_values("n", ascending=False), selection=None),
    ])
    return (triage_rows,)


@app.cell
def _(mo, triage_rows):
    t_problem = mo.ui.dropdown(
        options=["(all)"] + sorted(triage_rows["problem"].unique()),
        value="(all)", label="Problem")
    t_problem
    return (t_problem,)


@app.cell
def _(mo, t_problem, triage_rows):
    _v = triage_rows if t_problem.value == "(all)" else \
        triage_rows[triage_rows["problem"] == t_problem.value]
    _cols = [c for c in (
        "customer_row_id", "customer_name", "entity_type", "disposition",
        "problem", "remedy", "customer_url", "text_quality", "matched_source",
        "cb_id", "nzi_id") if c in _v.columns]
    mo.vstack([
        mo.md(f"{len(_v)} rows — `customer_row_id` is what `set-id --row` "
              "and the decision log key on"),
        mo.ui.table(_v[_cols].reset_index(drop=True), page_size=20,
                    selection=None),
    ])
    return


@app.cell
def _(enriched, mo):
    # ---- Taxonomy drill-down: pillar → sub-pillar → solution counts.
    _lvls = ["level0_one_earth_category", "level1_one_earth_category",
             "level2_one_earth_category", "level3_one_earth_category"]
    _t = enriched[enriched[_lvls[0]].notna()]
    drill = (_t.groupby(_lvls, dropna=False).size().reset_index(name="n")
             .sort_values("n", ascending=False))
    drill.columns = ["Pillar", "Sub-pillar", "Solution", "Sub-term", "n"]
    mo.vstack([mo.md("## Taxonomy drill-down"),
               mo.ui.table(drill, page_size=15, selection=None)])
    return


@app.cell
def _(enriched, mo):
    mo.md("## Workbench — every column, marimo's native explorer")
    return


@app.cell
def _(enriched, mo):
    mo.ui.dataframe(enriched)
    return


if __name__ == "__main__":
    app.run()
