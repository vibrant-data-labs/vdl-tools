"""Reads notebook_location() paths the ways that work in a WASM export."""

import marimo

app = marimo.App(app_title="Fixture")


@app.cell
def _():
    import json
    import urllib.request

    import marimo as mo
    import pandas as pd

    _D = mo.notebook_location() / "public"
    table = pd.read_csv(str(_D / "table.csv"))

    _s = str(_D / "stats.json")
    if _s.startswith(("http://", "https://")):
        stats = json.loads(urllib.request.urlopen(_s).read().decode())
    else:
        with open(_s) as _f:
            stats = json.load(_f)
    return mo, stats, table


if __name__ == "__main__":
    app.run()
