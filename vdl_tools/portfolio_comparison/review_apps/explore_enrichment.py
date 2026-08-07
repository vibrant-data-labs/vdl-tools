"""Explore an engagement's enriched portfolio + its lineage (marimo app).

Launch from the ENGAGEMENT REPO ROOT:

    PYTHONPATH=<vdl-tools> marimo run \
        <vdl-tools>/vdl_tools/portfolio_comparison/review_apps/explore_enrichment.py

Read-only: this app never writes. Data changes happen through the pipeline
(`enrich`) and the decision log, never here.
"""

import marimo

__generated_with = "0.23.14"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    from pathlib import Path

    import marimo as mo
    import pandas as pd

    ROOT = Path.cwd()
    RESULTS = ROOT / "data" / "results"
    enriched = pd.read_parquet(RESULTS / "enriched_portfolio.parquet")
    state = json.loads((ROOT / "pipeline_state.json").read_text())
    decisions = []
    dec_path = RESULTS / "decisions.jsonl"
    if dec_path.exists():
        decisions = [json.loads(line) for line in dec_path.read_text().splitlines()]
    return RESULTS, decisions, enriched, json, mo, pd, state


@app.cell
def _(decisions, enriched, mo, pd, state):
    _stages = pd.DataFrame([
        {"stage": name, **{k: str(v)[:60] for k, v in info.items()}}
        for name, info in state.get("stages", {}).items()
    ])
    _matched = enriched["one_earth_category"].notna() & (
        enriched["one_earth_category"] != "NoMatch"
    )
    header = mo.vstack([
        mo.md(f"""
# Enriched portfolio explorer
**{len(enriched)} rows** · {int(_matched.sum())} taxonomy-matched ·
{int(enriched["Latitude"].notna().sum()) if "Latitude" in enriched.columns else 0} geocoded ·
{len(decisions)} human decisions on record
"""),
        mo.accordion({
            "📜 Run ledger (pipeline_state.json)": mo.ui.table(_stages, selection=None),
        }),
    ])
    header
    return


@app.cell
def _(enriched, mo):
    _counts = (
        enriched["level0_one_earth_category"].fillna("(unmatched)").value_counts()
    )
    _max = _counts.max()
    _lines = ["| Pillar | n | |", "|---|---|---|"]
    for _name, _n in _counts.items():
        _lines.append(f"| {_name} | {_n} | {'▓' * max(1, int(28 * _n / _max))} |")
    mo.vstack([mo.md("## Portfolio by One Earth pillar"),
               mo.md("\n".join(_lines))])
    return


@app.cell
def _(enriched, mo):
    f_type = mo.ui.dropdown(
        options=["(all)"] + sorted(enriched["entity_type"].dropna().unique()),
        value="(all)", label="Entity type")
    f_disp = mo.ui.dropdown(
        options=["(all)"] + sorted(enriched["disposition"].dropna().unique()),
        value="(all)", label="Disposition")
    f_pillar = mo.ui.dropdown(
        options=["(all)"] + sorted(
            enriched["level0_one_earth_category"].dropna().unique()),
        value="(all)", label="Pillar")
    f_search = mo.ui.text(placeholder="name contains…", label="Search")
    return f_disp, f_pillar, f_search, f_type


@app.cell
def _(enriched, f_disp, f_pillar, f_search, f_type, mo):
    _v = enriched
    if f_type.value != "(all)":
        _v = _v[_v["entity_type"] == f_type.value]
    if f_disp.value != "(all)":
        _v = _v[_v["disposition"] == f_disp.value]
    if f_pillar.value != "(all)":
        _v = _v[_v["level0_one_earth_category"] == f_pillar.value]
    if f_search.value.strip():
        _v = _v[_v["customer_name"].astype(str).str.contains(
            f_search.value.strip(), case=False, regex=False)]
    _cols = [c for c in (
        "customer_name", "entity_type", "disposition", "matched_source",
        "level0_one_earth_category", "one_earth_category", "text_quality"
        if "text_quality" in _v.columns else "enrichment_ready",
        "city", "state") if c in _v.columns]
    filtered = _v
    mo.vstack([
        mo.md("## Browse"),
        mo.hstack([f_type, f_disp, f_pillar, f_search]),
        mo.md(f"{len(_v)} rows"),
        mo.ui.table(_v[_cols].reset_index(drop=True), selection=None,
                    page_size=15),
    ])
    return (filtered,)


@app.cell
def _(filtered, mo):
    row_pick = mo.ui.dropdown(
        options=sorted(filtered["customer_name"].astype(str).unique()) or ["—"],
        label="Inspect one org",
    )
    row_pick
    return (row_pick,)


@app.cell
def _(decisions, enriched, mo, pd, row_pick):
    _hit = enriched[enriched["customer_name"].astype(str) == str(row_pick.value)]
    if len(_hit) == 0:
        mo.md("*pick an org above*")
    else:
        _r = _hit.iloc[0]

        def _f(v, n=400):
            return (str(v)[:n] + "…") if pd.notna(v) and len(str(v)) > n \
                else (str(v) if pd.notna(v) else "—")

        _path = " → ".join(
            str(_r.get(f"level{_i}_one_earth_category"))
            for _i in range(4)
            if pd.notna(_r.get(f"level{_i}_one_earth_category"))
        ) or "unmatched"
        _decs = [d for d in decisions
                 if d.get("customer_row_id") == _r["customer_row_id"]]
        _dec_md = "\n".join(
            f"- `{d['decided_at']}` **{d.get('gate')}** by {d.get('decided_by')}"
            f" — {d.get('reason', '')[:100]}"
            for d in _decs
        ) or "*no human decisions — fully automatic*"
        mo.vstack([
            mo.md(f"""
### {_r['customer_name']}
**Taxonomy:** {_path}

| source | description | website |
|---|---|---|
| customer | {_f(_r.get('customer_description'), 150)} | {_f(_r.get('customer_url'), 60)} |
| Crunchbase | {_f(_r.get('cb_description'), 150)} | {_f(_r.get('cb_website'), 60)} |
| NZI | {_f(_r.get('nzi_description'), 150)} | {_f(_r.get('nzi_website'), 60)} |
| GivingTuesday | {_f(_r.get('gt_unique_text'), 150)} | {_f(_r.get('gt_website'), 60)} |

**Synthesized summary:** {_f(_r.get('Summary'), 600)}

**Location:** {_f(_r.get('Location'), 80)} → ({_f(_r.get('Latitude'), 12)}, {_f(_r.get('Longitude'), 12)})

**Decision history:**

{_dec_md}
"""),
        ])
    return


if __name__ == "__main__":
    app.run()
