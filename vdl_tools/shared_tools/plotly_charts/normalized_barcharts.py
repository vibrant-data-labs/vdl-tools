import pandas as pd
from plotly import express as px
from plotly import graph_objects as go
from plotly.subplots import make_subplots

import vdl_tools.py2mappr.vdl_palette as pal

from vdl_tools.shared_tools.plotly_charts.plotly_template import pio


pio.templates.default = "vibrant_white"
template = pio.templates[pio.templates.default]


def create_aggregated_df(
    df,
    groupby_columns=[],
    agg_func="count",
    agg_column="uid",
    normalize_by_total_columns=None,
):

    groupby_columns = list(set(groupby_columns))
    if groupby_columns:
        agg_df = (
            df
            .groupby(groupby_columns)
            .agg({agg_column: agg_func})
            .reset_index()
        )
    else:
        agg_df = pd.DataFrame(df.agg({agg_column: agg_func})).T

    agg_df["aggregated_value"] = agg_df[agg_column]
    if normalize_by_total_columns:
        if len(groupby_columns) < 1:
            raise ValueError("normalize_by_total_column requires at least 1 groupby columns")

        
        if len(set(normalize_by_total_columns).intersection(set(groupby_columns))) > 0 and len(groupby_columns) > 1:
            normalized_column_totals = (
                agg_df
                .groupby(normalize_by_total_columns)
                .sum()
                [agg_column]
                .to_dict()
            )
            if len(normalize_by_total_columns) > 1:

                agg_df["join_key"] = agg_df.apply(
                    lambda x: tuple(x[normalize_by_total_columns].to_list()),
                    axis=1
                )
            else:
                agg_df["join_key"] = agg_df[normalize_by_total_columns[0]]

            agg_df['totals'] = agg_df["join_key"].map(normalized_column_totals)
        else:
            agg_df['totals'] = agg_df[agg_column].sum()
    else:
        agg_df['totals'] = agg_df[agg_column].sum()
    agg_df["normalized_value"] = agg_df["aggregated_value"] / agg_df["totals"]

    return agg_df


def create_aggregated_plot(
    df,
    xaxis_column=None,
    color_by_column=None,
    facet_by_column=None,
    agg_func="count",
    agg_column="uid",
    normalize_by_total_columns=None,
    chart_type="bar",
    barmode="group",
    yaxis_title=None,
    n_plot_rows=None,
    n_plot_cols=None,
):

    groupby_columns = [
        xaxis_column,
    ]
    if color_by_column:
        groupby_columns.append(color_by_column)
    if facet_by_column:
        groupby_columns.append(facet_by_column)
    agg_df = create_aggregated_df(
        df=df,
        groupby_columns=groupby_columns,
        agg_func=agg_func,
        agg_column=agg_column,
        normalize_by_total_columns=normalize_by_total_columns,
    )

    agg_df = agg_df.sort_values(by="aggregated_value", ascending=False)

    if chart_type == "bar":
        chart_func = px.bar
    elif chart_type == "line":
        chart_func = px.line
    elif chart_type == "scatter":
        chart_func = px.scatter
    elif chart_type == "area":
        chart_func = px.area
    else:
        raise ValueError(f"Invalid chart type: {chart_type}")
    
    chart_kwargs = {}

    if xaxis_column:
        chart_kwargs["x"] = xaxis_column
    if color_by_column:
        chart_kwargs["color"] = color_by_column
    if facet_by_column:
        chart_kwargs["facet_col"] = facet_by_column
    if not normalize_by_total_columns:
        chart_kwargs["y"] = "aggregated_value"
    else:
        chart_kwargs["y"] = "normalized_value"

    agg_df.sort_values(by=xaxis_column, inplace=True)
    fig = chart_func(
        agg_df,
        **chart_kwargs,
    )

    # When faceting a plot, if the facet column and x-axis column are unique combinations,
    # the other faceted plots will have lots of blanks (this is evident when we have level1 on x and level0 on facet)
    # This code uses the work that plotly did above, but re-arranges as subplots
    if facet_by_column:
        facet_col_order = (
            agg_df
            .groupby(facet_by_column)
            .sum()
            .sort_values(
                by="aggregated_value",
                ascending=False
            )
            .index
        )

        n_plot_rows = n_plot_rows or 1
        n_plot_cols = n_plot_cols or len(facet_col_order)

        new_fig = make_subplots(
            rows=1,
            cols=len(facet_col_order),
            shared_yaxes=True,
            horizontal_spacing=0.03,
        )
        traces = [trace for trace in fig.select_traces()]
        for i, trace in enumerate(traces):
            xaxis_name = trace.xaxis
            col_n = 1
            if len(xaxis_name) > 1:
                col_n = int(xaxis_name[1:])
            new_fig.add_trace(trace, row=1, col=col_n)
        new_fig.layout.annotations = fig.layout.annotations

        for i, annotation in enumerate(new_fig.layout.annotations):
            annotation_text = annotation.text.split('=')[-1]
            annotation.update(text=annotation_text)

        new_fig.update_layout(
            legend={"tracegroupgap": 0},
        )
        fig = new_fig
    fig.update_layout(
        barmode=barmode,
    )
    
    # Remove the y-axis ticks and labels for all but the first col
    fig.update_yaxes(
        patch={"showticklabels": False, "visible": False},
        selector=lambda x: len(x.anchor) > 1,
    )
    # # Remove the x-axis ticks and labels for all but the first row
    # fig.update_xaxes(
    #     patch={"showticklabels": False, "visible": False},
    #     selector=lambda axis: len(axis.anchor) > 1,
    # )

    if yaxis_title:
        fig.update_layout(
            yaxis_title=yaxis_title,
        )
    return fig


def create_aggregated_scatter_plot(
    df,
    group_column,
    xaxis_column,
    yaxis_column,
    xaxis_agg_column,
    yaxis_agg_column,
    xaxis_agg_func="count",
    yaxis_agg_func="count",
    color_by_column=None,
    facet_by_column=None,
    x_normalize_by_total_column=None,
    y_normalize_by_total_column=None,
    # chart_type="bar",
    title=None,
    xaxis_title=None,
    yaxis_title=None,
    n_plot_rows=None,
    n_plot_cols=None,
):

    groupby_columns = [group_column]
    if color_by_column and color_by_column != group_column:
        groupby_columns.append(color_by_column)
    if facet_by_column and facet_by_column != group_column:
        groupby_columns.append(facet_by_column)
    
    x_groupby_columns = list(set(groupby_columns + [xaxis_column]))
    y_groupby_columns = list(set(groupby_columns + [yaxis_column]))
    x_agg_df = create_aggregated_df(
        df=df,
        groupby_columns=x_groupby_columns,
        agg_column=xaxis_agg_column,
        agg_func=xaxis_agg_func,
        normalize_by_total_columns=[x_normalize_by_total_column],
    )

    y_agg_df = create_aggregated_df(
        df=df,
        groupby_columns=y_groupby_columns,
        agg_column=yaxis_agg_column,
        agg_func=yaxis_agg_func,
        normalize_by_total_columns=[y_normalize_by_total_column],
    )

    x_agg_df["join_col"] = x_agg_df[groupby_columns].apply(lambda x: "_".join(x), axis=1)
    y_agg_df["join_col"] = y_agg_df[groupby_columns].apply(lambda x: "_".join(x), axis=1)

    y_cols = [
        "join_col",
        "aggregated_value",
        "normalized_value"
    ]
    if yaxis_column not in x_groupby_columns:
        y_cols.append(yaxis_column)


    agg_df = x_agg_df.merge(
        y_agg_df[y_cols],
        on="join_col",
        suffixes=("_x", "_y")
    )

    chart_kwargs = {}

    if x_normalize_by_total_column:
        chart_kwargs["x"] = "normalized_value_x"
        if not xaxis_title:
            xaxis_title = "Normalized " + xaxis_agg_column
    else:
        chart_kwargs["x"] = "aggregated_value_x"
        if not xaxis_title:
            xaxis_title = xaxis_agg_column

    if y_normalize_by_total_column:
        chart_kwargs["y"] = "normalized_value_y"
        if not yaxis_title:
            yaxis_title = "Normalized " + yaxis_agg_column
    else:
        chart_kwargs["y"] = "aggregated_value_y"
        if not yaxis_title:
            yaxis_title = yaxis_agg_column

    hover_data = list(set([
        group_column,
        xaxis_column,
        yaxis_column,
        chart_kwargs["x"],
        chart_kwargs["y"],
    ]))

    if color_by_column:
        chart_kwargs["color"] = color_by_column
        hover_data.append(color_by_column)
    if facet_by_column:
        chart_kwargs["facet_col"] = facet_by_column
        hover_data.append(facet_by_column)

    agg_df.sort_values(by=chart_kwargs["x"], inplace=True)

    fig = px.scatter(
        agg_df,
        hover_data=hover_data,
        **chart_kwargs,
    )

    # When faceting a plot, if the facet column and x-axis column are unique combinations,
    # the other faceted plots will have lots of blanks (this is evident when we have level1 on x and level0 on facet)
    # This code uses the work that plotly did above, but re-arranges as subplots
    if facet_by_column:
        facet_col_order = (
            agg_df
            .groupby(facet_by_column)
            .sum()
            .sort_values(
                by="aggregated_value_x",
                ascending=False
            )
            .index
        )

        n_plot_rows = n_plot_rows or 1
        n_plot_cols = n_plot_cols or len(facet_col_order)

        new_fig = make_subplots(
            rows=1,
            cols=len(facet_col_order),
            shared_yaxes=True,
            horizontal_spacing=0.03,
        )
        traces = [trace for trace in fig.select_traces()]
        for i, trace in enumerate(traces):
            xaxis_name = trace.xaxis
            col_n = 1
            if len(xaxis_name) > 1:
                col_n = int(xaxis_name[1:])
            new_fig.add_trace(trace, row=1, col=col_n)
        new_fig.layout.annotations = fig.layout.annotations

        for i, annotation in enumerate(new_fig.layout.annotations):
            annotation_text = annotation.text.split('=')[-1]
            annotation.update(text=annotation_text)

        new_fig.update_layout(
            legend={"tracegroupgap": 0},
        )
        fig = new_fig
    fig.update_layout(
        barmode="group",
    )

    # Remove the y-axis ticks and labels for all but the first col
    fig.update_yaxes(
        patch={"showticklabels": False, "visible": False},
        selector=lambda x: len(x.anchor) > 1,
    )

    if yaxis_title:
        fig.update_layout(
            {
                "yaxis": {
                    "title": yaxis_title,
                }
            }
        )

    default_xaxis_title = fig.layout.xaxis.title.text
    xaxis_title = xaxis_title or default_xaxis_title
    # Remove the current xaxis title and replace with an annotation
    # that is centered on the plot
    fig.update_layout(
        xaxis_title=None,
    )
    fig.add_annotation(
        x=0.5,
        y=-0.15,
        xref="paper",
        yref="paper",
        text=xaxis_title,
        showarrow=False,
        font=dict(
            size=14,
        ),
    )

    return fig
