"""
Plots for a scored causal network (Altair).
"""

import altair as alt


def keystone_vs_upstream_scatter(nodes,
                                 out_html=None,              # save the interactive chart here (optional)
                                 x="Upstream Rank",          # more upstream = further right
                                 y="Keystone Rank",          # more leverage = higher
                                 color="Pillar",             # categorical column; falls back to Causal Cluster
                                 size="Catalytic Score",
                                 tooltip=("Name", "Root Factor", "Pillar", "Catalytic Score"),
                                 width=600, height=450, title=None):
    """
    Scatter of every factor's keystone leverage against its upstream position.
    The keystone factors sit in the upper right. Drag to highlight a region.
    Returns the Altair chart (and saves it to `out_html` when given).
    """
    df = nodes.copy()
    if color not in df.columns:
        color = "Causal Cluster" if "Causal Cluster" in df.columns else None
    tooltip = [t for t in tooltip if t in df.columns]

    def axis(col):
        rank_scale = alt.Scale(domain=(0, 100)) if "Rank" in col else alt.Undefined
        return dict(title=col, scale=rank_scale)

    brush = alt.selection_interval()
    encoding = dict(x=alt.X(f"{x}:Q", **axis(x)), y=alt.Y(f"{y}:Q", **axis(y)), tooltip=tooltip)
    if size in df.columns:
        encoding["size"] = alt.Size(f"{size}:Q", legend=None, scale=alt.Scale(range=[20, 600]))
    if color is not None:
        encoding["color"] = alt.condition(brush, alt.Color(f"{color}:N"), alt.value("lightgray"))

    chart = (alt.Chart(df).mark_circle(opacity=0.8)
             .encode(**encoding)
             .add_params(brush)
             .properties(width=width, height=height, title=title or f"{y} vs {x}"))
    if out_html is not None:
        chart.save(str(out_html))
        print(f"Wrote {out_html}")
    return chart
