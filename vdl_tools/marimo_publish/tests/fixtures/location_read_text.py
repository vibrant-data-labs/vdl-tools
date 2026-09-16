"""Reads JSON with .read_text() on a notebook_location() path.

Passes `python location_read_text.py` locally; in the WASM export it raises
AttributeError: 'URLPath' object has no attribute 'read_text'.
"""

import marimo

app = marimo.App(app_title="Fixture")


@app.cell
def _():
    import json

    import marimo as mo
    import pandas as pd

    _D = mo.notebook_location() / "public"
    table = pd.read_csv(str(_D / "table.csv"))
    stats = json.loads((_D / "stats.json").read_text())
    return mo, stats, table


if __name__ == "__main__":
    app.run()
