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
    return triage, triage_rows


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
def _(mo, triage):
    # Selectable entity table (with the triage diagnosis columns, so
    # problem rows filter/sort in place): pick a row, full record below.
    _view_cols = [c for c in (
        "customer_name", "entity_type", "disposition", "problem", "remedy",
        "matched_source", "level0_one_earth_category", "one_earth_category",
        "customer_row_id") if c in triage.columns]
    wb_table = mo.ui.table(
        triage[_view_cols].reset_index(drop=True),
        selection="single", page_size=15,
    )
    mo.vstack([mo.md("## Entities — select a row for full detail "
                     "(sort/search the `problem` column to diagnose)"),
               wb_table])
    return (wb_table,)


@app.cell
def _(mo, pd, triage, wb_table):
    _sel = pd.DataFrame(wb_table.value)
    if len(_sel) == 0:
        _out = mo.md("*select a row above*")
    else:
        _row = triage[
            triage["customer_row_id"] == _sel.iloc[0]["customer_row_id"]
        ].iloc[0]
        _short, _long = [], []
        for _col, _val in _row.items():
            # List-valued columns (all_level* taxonomy paths) flatten to text.
            if isinstance(_val, (list, tuple)) or type(_val).__name__ == "ndarray":
                _val = ", ".join(str(x) for x in _val)
                if not _val:
                    continue
            elif pd.isna(_val) or str(_val).strip() == "":
                continue
            if len(str(_val)) > 180:
                _long.append((_col, str(_val)))
            else:
                _short.append((_col, str(_val).replace("|", "/")))
        _field_md = "\n".join(
            f"| {c} | {v} |" for c, v in _short)
        _texts_md = "\n\n".join(
            f"**{c}**\n\n{v}" for c, v in _long)
        _out = mo.md(f"""
### {_row['customer_name']}
| field | value |
|---|---|
{_field_md}

{_texts_md}
""")
    _out
    return


if __name__ == "__main__":
    app.run()
