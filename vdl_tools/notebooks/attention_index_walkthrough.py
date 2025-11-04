import marimo

__generated_with = "unknown"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # Attention Index

    The Attention Index is a method for creating a single value for a taxonomy category / level to demonstrate how much "Attention" was given to that category. "Neglectedness" is a little more natural to think about -- this is 1-Neglectedness.

    It is calculated based on 3 key attributes  
    1. Number of Fractional Orgs for that Category  
    2. Total (distributed) funding for that Category  
    3. Funding per Company (Total Fundig) / (Number of Fractional)

    We scale each of these metrics by the min/max for them and then take the geometric mean of the 3. That is then re-scaled between min/max (then we found taking cube root better spreads it).

    This notebook will walk through the process.
    """
    )
    return


@app.cell
def _(funding_rounds_taxonomy_mapping, mo):
    DISCLOSED_STAGES_ORDERED = [
        'grant', 'equity_crowdfunding', 'initial_coin_offering', 'angel', 'pre_seed', 'seed',
        'series_a', 'series_b', 'series_c', 'series_d', 'series_e', 'series_f',
        'series_g', 'series_h', 'series_i', 'series_j', 'corporate_round', 'secondary_market',
        # 'private_equity',
        'post_ipo_equity', 'post_ipo_debt', 'post_ipo_secondary', "Philanthropy"
    ]
    rounds_filter = mo.ui.multiselect(
        options=DISCLOSED_STAGES_ORDERED,
        value=[
            'angel',
            'pre_seed',
            'seed',
            'series_a',
        ],
        label="**Select Rounds to filter by**"
    )

    years_avail = funding_rounds_taxonomy_mapping['funding_year'].agg(["min", "max"])
    year_filter = mo.ui.range_slider(
        start=years_avail['min'],
        stop=years_avail['max'],
        label="**Select Date Range**",
        show_value=True,
    )

    taxonomy_mapping_level_choice = mo.ui.number(
        start=0,
        stop=2,
        value=2,
        label="**Choose a level to map to**"
    )


    final_root_factor = mo.ui.number(start=1, stop=5, value=3, label="**Final Root Rescaling**")
    mo.vstack([rounds_filter, year_filter, taxonomy_mapping_level_choice, final_root_factor], justify="start").callout()
    return (
        final_root_factor,
        rounds_filter,
        taxonomy_mapping_level_choice,
        year_filter,
    )


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    import altair as alt
    import numpy as np
    return alt, mo, np, pd


@app.cell
def _(alt, base_tax_aggregated_w_scaled_geo_mean):
    # replace _df with your data source
    _chart = (
        alt.Chart(base_tax_aggregated_w_scaled_geo_mean)
        .mark_circle()
        .encode(
            x=alt.X(field='Attention Index', type='quantitative'),
            y=alt.Y(field='tax_map_level1', type='nominal', sort="tax_map_level0"),
            color=alt.Color(field='tax_map_level0', type='nominal'),
            tooltip=[
                alt.Tooltip(field='tax_map_level2'),
                alt.Tooltip(field='Attention Index', format=',.2f'),
                alt.Tooltip(field='tax_map_level1'),
                alt.Tooltip(field='tax_map_level0')
            ]
        )
        .properties(
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ## Walk Through
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Starting Data
    We will start with the data set that powers our Dashboards. It is essentially the Taxonomy Mapping results joined to the funding rounds data (for venture) and some data manipulation to include Non-Profits in there.

    **Note:** You can only calculate an attention index for the lowest level that is present in the taxonomy mapping distributed funding results. So if you ran the pipeline with `max_level=2` (default in Climate Landscape) then that's the lowest level we can calculate our index for.

    _Using that for convenience of not re-running at the "Approach" (subterm / level3)_
    """
    )
    return


@app.cell
def _(pd):
    funding_rounds_taxonomy_mapping = pd.read_csv("attention_index_raw_data.csv")

    # # We will filter out the no matches. 
    funding_rounds_taxonomy_mapping = funding_rounds_taxonomy_mapping[funding_rounds_taxonomy_mapping['tax_map_level0'] != "No Match"]

    # We will filter out the fundingfracs that are 0 since those got aggregated to a attributed to a different category in our pipeline
    funding_rounds_taxonomy_mapping[funding_rounds_taxonomy_mapping['tax_map_fundingfrac'] > 0]
    funding_rounds_taxonomy_mapping[funding_rounds_taxonomy_mapping['distributed_funding'] > 0]

    funding_rounds_taxonomy_mapping["funding_investment_type"] = funding_rounds_taxonomy_mapping.apply(lambda x: "Philanthropy" if x['meta_data_source'] == 'Candid' else x['funding_investment_type'], axis=1)

    funding_rounds_taxonomy_mapping
    return (funding_rounds_taxonomy_mapping,)


@app.cell
def _(mo):
    mo.md("""____""")
    return


@app.cell
def _(mo):
    mo.md(r"""### Filter Based on Filters""")
    return


@app.cell
def _(funding_rounds_taxonomy_mapping, mo, rounds_filter, year_filter):
    rounds_filter_value = rounds_filter.value
    years_filter_value = year_filter.value

    filtered_df = (
        funding_rounds_taxonomy_mapping[
            (funding_rounds_taxonomy_mapping['funding_year'] >= years_filter_value[0]) &
            (funding_rounds_taxonomy_mapping['funding_year'] <= years_filter_value[1])
        ]
    ).copy()

    if rounds_filter_value:
        filtered_df = filtered_df[filtered_df['funding_investment_type'].isin(rounds_filter_value)]

    mo.ui.table(filtered_df, show_column_summaries=True)
    return (filtered_df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 1. Aggregate at the taxonomy level / company level to get the starting numbers aggregated
    """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(filtered_df, taxonomy_mapping_level_choice):
    aggregation_columns = [
        'meta_organization',
        'meta_uid',
    ]

    taxonomy_level_columns = []
    for i in range(0, taxonomy_mapping_level_choice.value + 1):
        taxonomy_level_columns.append(f"tax_map_level{i}")

    orged_aggregation_df = (
        filtered_df.groupby(aggregation_columns + taxonomy_level_columns)
        .agg({
            "distributed_funding": "sum", # Sum up all the distributed_funding for all the lower levels 
            "tax_map_fundingfrac": "mean", # Because we are at the round level this will have been multiplied for each round, so take the mean of it
        })
    ).reset_index()

    orged_aggregation_df.rename(columns={
        "distributed_funding": "total_funding",
        "tax_map_fundingfrac": "fractional_orgs",
    }, inplace=True)

    orged_aggregation_df["whole_orgs"] = 1 ## Add a column for when we double count

    orged_aggregation_df = orged_aggregation_df.reset_index()

    assert orged_aggregation_df['total_funding'].sum().round() == filtered_df['distributed_funding'].sum().round()

    orged_aggregation_df
    return orged_aggregation_df, taxonomy_level_columns


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 2. Aggregate at the lowest level

    This will get us the totals for our 3 metrics of interest for each of the categories we are interested in.
    """
    )
    return


@app.cell
def _(orged_aggregation_df, taxonomy_level_columns):
    metric_column_names = [
        "total_funding",
        "fractional_orgs",
        # "whole_orgs"
    ]

    base_tax_aggregated = orged_aggregation_df.groupby(taxonomy_level_columns).sum()[metric_column_names].reset_index()

    base_tax_aggregated['funding_per_company'] = base_tax_aggregated['total_funding'] / base_tax_aggregated['fractional_orgs']
    metric_column_names.append('funding_per_company')


    base_tax_aggregated
    return base_tax_aggregated, metric_column_names


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step TODO: Add in the raw taxonomy to ensure we add in the levels that didn't get anything mapped to it!
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 3. Get the Min / Max for each of the metrics

    We will want to scale all the metrics for the min/max of that metric so need to calculate that
    """
    )
    return


@app.cell
def _(base_tax_aggregated):
    min_max_metrics = base_tax_aggregated.agg({
        "total_funding": ["min", "max"],
        "fractional_orgs": ["min", "max"],
        "funding_per_company": ["min", "max"],
    })
    min_max_metrics
    return (min_max_metrics,)


@app.cell
def _(base_tax_aggregated, metric_column_names, min_max_metrics):
    base_tax_aggregated_w_min_max = base_tax_aggregated

    min_max_column_names = []
    for _col in metric_column_names:
        for agg_name in ["min", "max"]:
            base_tax_aggregated_w_min_max[f"{_col}_{agg_name}"] = min_max_metrics[_col][agg_name]
            min_max_column_names.append(f"{_col}_{agg_name}")

    base_tax_aggregated_w_min_max
    return base_tax_aggregated_w_min_max, min_max_column_names


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 4. Scale each of the Metrics by their respective Min/Max

    ```python
    def max_min_normalization(
        value: float,
        min_value: float,
        max_value: float,
    ):    # Convert to float, catch conversion errors
        if any(pd.isna([value, min_value, max_value])):
            return np.nan
        if any([not x for x in [value, min_value, max_value]]):
            return np.nan

        diff = max_value - min_value
        # Avoid division by zero
        if diff == 0:
            if max_value > 0:
                return 0.5
            else:
                return 0
        value_normalized = (value - min_value) / diff
        return value_normalized
    ```
    """
    )
    return


@app.function
def max_min_normalization(
    value: float,
    min_value: float,
    max_value: float,
):    # Convert to float, catch conversion errors
    diff = max_value - min_value
    # Avoid division by zero
    if diff == 0:
        if max_value > 0:
            return 0.5
        else:
            return 0
    value_normalized = (value - min_value) / diff
    return value_normalized


@app.cell
def _(
    base_tax_aggregated,
    base_tax_aggregated_w_min_max,
    metric_column_names,
    min_max_column_names,
    taxonomy_level_columns,
):
    base_tax_aggregated_scaled = base_tax_aggregated_w_min_max.copy()

    scale_column_names = []
    for _col in metric_column_names:
        base_tax_aggregated_scaled[f"scale_{_col}"] = (
            base_tax_aggregated.apply(
                lambda x: max_min_normalization(x[_col], min_value=x[f"{_col}_min"], max_value=x[f"{_col}_max"]),
                axis=1
            )
        )
        scale_column_names.append(f"scale_{_col}")

    base_tax_aggregated_scaled[
        taxonomy_level_columns +
        scale_column_names +
        metric_column_names +
        min_max_column_names
    ]
    return base_tax_aggregated_scaled, scale_column_names


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 5. Combine the scaled metric with Geometric Mean

    ```python
    def geometric_mean_values(
        values,
        remove_zero=True # whether to remove zero values
    ):
        values = np.array(values, dtype=float)
        if remove_zero:
            values = values[values != 0]
        if (len(values) == 0):
            return 0  # Return NaN if the input is empty or all NaN
        else:
            # Create the product of the absolute values
            product = np.prod(np.abs(values))
            geo_mean = np.power(product, 1 / len(values))
            return geo_mean
    ```
    """
    )
    return


@app.cell
def _(np):
    def geometric_mean_values(
        values,
        remove_zero=True # whether to remove zero values
    ):
        values = np.array(values, dtype=float)
        if remove_zero:
            values = values[values != 0]
        if (len(values) == 0):
            return 0  # Return NaN if the input is empty or all NaN
        else:
            # Create the product of the absolute values
            product = np.prod(np.abs(values))
            geo_mean = np.power(product, 1 / len(values))
            return geo_mean
    return (geometric_mean_values,)


@app.cell
def _(
    base_tax_aggregated_scaled,
    geometric_mean_values,
    metric_column_names,
    min_max_column_names,
    scale_column_names,
    taxonomy_level_columns,
):
    base_tax_aggregated_w_geo_mean = base_tax_aggregated_scaled.copy()

    geo_mean_column = "geo_mean"
    base_tax_aggregated_w_geo_mean[geo_mean_column] = base_tax_aggregated_w_geo_mean.apply(
        lambda x: geometric_mean_values(x[scale_column_names]),
        axis=1
    )

    base_tax_aggregated_w_geo_mean = base_tax_aggregated_w_geo_mean[
        taxonomy_level_columns +
        [geo_mean_column] +
        scale_column_names +
        metric_column_names +
        min_max_column_names
    ]

    base_tax_aggregated_w_geo_mean.sort_values(geo_mean_column, ascending=False)
    return base_tax_aggregated_w_geo_mean, geo_mean_column


@app.cell
def _(mo):
    mo.md(
        r"""
    ___
    ### Step 6. Rescale the Geometric Mean by the Min/Max
    And take the cubed root. We found that's the best way to spread it out better.
    """
    )
    return


@app.cell
def _(
    base_tax_aggregated_w_geo_mean,
    final_root_factor,
    geo_mean_column,
    metric_column_names,
    min_max_column_names,
    scale_column_names,
    taxonomy_level_columns,
):
    min_geo_mean = base_tax_aggregated_w_geo_mean[geo_mean_column].min()
    max_geo_mean = base_tax_aggregated_w_geo_mean[geo_mean_column].max()
    base_tax_aggregated_w_scaled_geo_mean = base_tax_aggregated_w_geo_mean.copy()

    base_tax_aggregated_w_scaled_geo_mean["Attention Index"] = base_tax_aggregated_w_scaled_geo_mean[geo_mean_column].apply(
        lambda x: max_min_normalization(x, min_geo_mean, max_geo_mean) ** (1./final_root_factor.value)
    ).copy()

    base_tax_aggregated_w_scaled_geo_mean = base_tax_aggregated_w_scaled_geo_mean[
        taxonomy_level_columns +
        ["Attention Index", geo_mean_column] +
        scale_column_names +
        metric_column_names +
        min_max_column_names
    ]

    base_tax_aggregated_w_scaled_geo_mean.sort_values("Attention Index", ascending=False)
    return (base_tax_aggregated_w_scaled_geo_mean,)


@app.cell
def _():
    from plotly import express as px
    from plotly import graph_objects as go
    return (go,)


@app.cell
def _(base_tax_aggregated_w_scaled_geo_mean, go):
    fig = go.Figure()
    for level0 in ['Energy Transition', 'Regenerative Agriculture', 'Nature Conservation']:
        fig.add_trace(
            go.Histogram(
                x=base_tax_aggregated_w_scaled_geo_mean[base_tax_aggregated_w_scaled_geo_mean['tax_map_level0'] == level0]['Attention Index'],
                name=level0
            ),
        )

    fig.update_layout(
        barmode='overlay',
        title={"text": "Attention Index Distribution", "x": .5},
        xaxis_title="Attention Index",
        yaxis_title="Number of Solutions"
    )
    fig.update_traces(opacity=0.5)
    fig.show()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
