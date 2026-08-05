"""VDL match review (marimo app).

Launch from the ENGAGEMENT REPO ROOT (the app reads data/results/ relative
to its working directory):

    ~/.pyenv/versions/vdl-tools-312/bin/marimo run \
        ~/dev/vdl/vdl-tools/vdl_tools/portfolio_comparison/review_apps/match_review.py

Every submit writes the ID Mapping File and appends to decisions.jsonl —
decisions survive match reruns via replay.
"""

import marimo

__generated_with = "0.23.14"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    from pathlib import Path

    import marimo as mo
    import pandas as pd

    from vdl_tools.portfolio_comparison.matching.queue import (
        apply_decision,
        load_id_mapping,
    )

    RESULTS = Path.cwd() / "data" / "results"
    return RESULTS, apply_decision, json, load_id_mapping, mo, pd


@app.cell
def _(mo):
    get_version, set_version = mo.state(0)  # bump after each decision
    get_pos, set_pos = mo.state(0)
    return get_pos, get_version, set_pos, set_version


@app.cell
def _(mo):
    sort_ui = mo.ui.dropdown(
        options={
            "Top score first (easy wins)": "score_desc",
            "Low score first (hard ones)": "score_asc",
            "Entity type": "entity_type",
            "Queue order": "original",
        },
        value="Top score first (easy wins)",
        label="Sort",
    )
    return (sort_ui,)


@app.cell
def _(RESULTS, get_version, json, load_id_mapping, mo, sort_ui):
    get_version()  # reload after every decision
    id_mapping = load_id_mapping(RESULTS)
    queue_rows = json.loads((RESULTS / "review_queue.json").read_text())
    # Only rows still pending (decisions may have resolved some since the
    # queue file was built).
    still_pending = set(
        id_mapping[id_mapping["status"] == "needs_review"]["customer_row_id"]
    )
    pending = [r for r in queue_rows if r["customer_row_id"] in still_pending]

    def _top_score(r):
        return max((c["score"] for c in r.get("candidates") or []), default=0.0)

    if sort_ui.value == "score_desc":
        pending.sort(key=_top_score, reverse=True)
    elif sort_ui.value == "score_asc":
        pending.sort(key=_top_score)
    elif sort_ui.value == "entity_type":
        pending.sort(key=lambda r: (str(r.get("entity_type")), -_top_score(r)))

    n_done = len(queue_rows) - len(pending)
    mo.stop(not pending, mo.md("## ✅ Review queue is empty — all rows decided."))
    return id_mapping, n_done, pending


@app.cell
def _(get_pos, mo, n_done, pending, set_pos):
    pos = min(get_pos(), len(pending) - 1)
    row = pending[pos]
    nav = mo.hstack([
        mo.ui.button(label="◀ prev", on_click=lambda _: set_pos(max(0, pos - 1))),
        mo.md(f"**{pos + 1} / {len(pending)}** pending ({n_done} decided)"),
        mo.ui.button(label="next ▶", on_click=lambda _: set_pos(min(len(pending) - 1, pos + 1))),
    ], justify="center")
    return nav, pos, row


@app.cell
def _(mo, row):
    def _fmt(v):
        return "—" if v in (None, "", float("nan")) else str(v)

    def _link(url):
        if not url or not isinstance(url, str) or not url.strip():
            return "—"
        href = url if "://" in url else f"https://{url}"
        return f"[{url}]({href})"

    header = mo.md(f"""
### {row["customer_name"]}
| | |
|---|---|
| customer URL | {_link(row.get("customer_url"))} |
| EIN | {_fmt(row.get("customer_ein"))} |
| type / disposition | {row.get("entity_type")} / {row.get("disposition")} |
| out-of-universe | {_fmt(row.get("out_of_universe_reason"))} |
""")
    return (header,)


@app.cell
def _(mo, row):
    _res = row.get("research")
    if _res:
        _badge = {"reject_all": "🔴 reject all", "accept_candidate": "🟢 accept",
                  "baseline_match_found": "🟡 baseline match found (see candidates)",
                  "unsure": "⚪ unsure"}.get(_res["recommendation"], _res["recommendation"])
        _links = "  ·  ".join(f"[{_i+1}]({_u})" for _i, _u in enumerate(_res.get("sources", [])))
        research_md = mo.callout(mo.md(
            f"**Pre-research: {_badge}** — {_res['note']}  {_links}"
        ), kind="info")
    else:
        research_md = mo.md("")
    return (research_md,)


@app.cell
def _(mo, row):
    cands = row.get("candidates") or []

    import re as _re

    def _site(c):
        d = c["evidence"].get("domain")
        return f"[{d}](https://{d})" if d else "—"

    def _cb(c):
        permalink = c["evidence"].get("cb_permalink")
        if permalink:
            return f"[CB ↗](https://www.crunchbase.com/organization/{permalink})"
        # Older cached candidates: CB web URLs also resolve uuids.
        if c["method"] == "api_search" or _re.fullmatch(r"[0-9a-f-]{36}", str(c["matched_id"])):
            return f"[CB ↗](https://www.crunchbase.com/organization/{c['matched_id']})"
        return "—"

    def _desc(c):
        text = (c["evidence"].get("description") or "").strip().replace("|", "/")
        return text[:90] + "…" if len(text) > 90 else (text or "—")

    _lines = ["| # | Candidate | Site | CB | Signal · Score | Universe | Description |",
              "|---|---|---|---|---|---|---|"]
    for _i, _c in enumerate(cands):
        _uni = "in" if _c["evidence"].get("in_universe") else "**OUT**"
        _rc = " 🔁" if _c["evidence"].get("redirect_confirmed") else ""
        _lines.append(
            f"| {_i} | {_c['matched_name']} | {_site(_c)} | {_cb(_c)} | "
            f"{_c['method']} · {_c['score']:.2f}{_rc} | {_uni} | {_desc(_c)} |"
        )
    cand_table = mo.md("\n".join(_lines)) if cands else mo.md("")

    options = {}
    for _i, _c in enumerate(cands):
        _in_uni = "in universe" if _c["evidence"].get("in_universe") else "OUT of universe"
        options[
            f"[{_i}] {_c['matched_name']}  ·  {_c['evidence'].get('domain') or 'no domain'}"
            f"  ·  {_c['method']} {_c['score']:.2f}  ·  {_in_uni}"
        ] = _i
    options["✗ Reject all — none of these (goes to Tier 2 / customer)"] = "reject"
    options["✉ Send to customer round-trip"] = "customer"

    choice = mo.ui.radio(options=options, label="**Decision**")
    reviewer = mo.ui.text(value="zein", label="Reviewer")
    notes = mo.ui.text_area(placeholder="Why (optional — lands in decisions.jsonl)", label="Notes")
    submit = mo.ui.run_button(label="Record decision")
    return cand_table, cands, choice, notes, reviewer, submit


@app.cell
def _(cand_table, header, mo, nav, choice, notes, research_md, reviewer, sort_ui, submit):
    mo.vstack([mo.hstack([sort_ui]), nav, header, research_md, cand_table, choice,
               mo.hstack([reviewer, notes]), submit])
    return


@app.cell
def _(
    RESULTS, apply_decision, cands, choice, get_version, mo, notes,
    pd, reviewer, row, set_version, submit,
):
    mo.stop(not submit.value or choice.value is None)

    _decided_by = f"vdl:{reviewer.value.strip() or 'unknown'}"
    if choice.value == "reject":
        apply_decision(
            RESULTS, row["customer_row_id"],
            decided_by=_decided_by, status=pd.NA, reason=notes.value or "rejected all candidates",
            matched_id=None, matched_name=None, matched_url=None,
            match_method=None, confidence=None, in_universe=None,
        )
    elif choice.value == "customer":
        apply_decision(
            RESULTS, row["customer_row_id"],
            decided_by=_decided_by, status="customer_review",
            reason=notes.value or "needs customer input",
        )
    else:
        _cand = cands[choice.value]
        apply_decision(
            RESULTS, row["customer_row_id"],
            decided_by=_decided_by, status="vdl_reviewed",
            reason=notes.value or "accepted candidate",
            matched_id=_cand["matched_id"], matched_name=_cand["matched_name"],
            matched_url=_cand["matched_url"], match_method=_cand["method"],
            confidence=_cand["score"],
            in_universe=bool(_cand["evidence"].get("in_universe")),
            out_of_universe_reason=(
                None if _cand["evidence"].get("in_universe")
                else "excluded_by_landscape_filter"
            ),
        )
    set_version(get_version() + 1)  # reload data; pending shrinks in place
    return


if __name__ == "__main__":
    app.run()
