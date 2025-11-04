import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import json

    import pandas as pd

    from vdl_tools.shared_tools.project_config import get_paths
    from vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping import (
        redistribute_funding_fracs,
    )
    from vdl_tools.shared_tools.climate_landscape.add_taxonomy_mapping import (
        load_one_earth_taxonomy,
        remove_mapping_name_suffix_from_taxonomy_results,
    )

    from vdl_tools.shared_tools.climate_landscape import (
        funding_mapping_combination_utils as fmcu,
    )
    return (
        fmcu,
        load_one_earth_taxonomy,
        pd,
        remove_mapping_name_suffix_from_taxonomy_results,
    )


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    TAXONOMY_FILE = "../shared-data/data/taxonomies/oneearth/OE Solutions Terms 20250502_expanded_VDL.xlsx"

    META_FILENAME = "../climate-landscape/data/results/cb_cd_li_meta.json"
    # Undistributed ones so we distribute to whichever levl
    TAXONOMY_MAPPING_RESULTS_FILE = "../grantham-neglectedness/data/results/cft_one_earth_taxonomy_mapping_results.json"

    DISTRIBUTED_FUNDING_LEVEL = 3

    CANDID_FUNDING_FILE = (
        "../shared-data/data/candid/2025_08_19/candid_orgs_cleaned.xlsx"
    )
    FUNDING_ROUND_FILE = "../shared-data/data/crunchbase/2025_09_01/organizations_funding_rounds.json"
    return (
        CANDID_FUNDING_FILE,
        FUNDING_ROUND_FILE,
        META_FILENAME,
        TAXONOMY_FILE,
        TAXONOMY_MAPPING_RESULTS_FILE,
    )


@app.cell
def taxonomy_mapping_results(taxonomy_mapping_results):
    taxonomy_mapping_results
    return


@app.cell
def _(
    fmcu,
    load_one_earth_taxonomy,
    pd,
    remove_mapping_name_suffix_from_taxonomy_results,
):
    def load_files(
        taxonomy_path,
        taxonomy_mapping_results_path,
        meta_df_path,
        funding_round_path,
        candid_funding_path,
        add_geo_engineering=False,
    ):
        taxonomy = load_one_earth_taxonomy(
            taxonomy_path, add_geo_engineering=add_geo_engineering
        )
        taxonomy_mapping_results = pd.read_json(taxonomy_mapping_results_path)
        if add_geo_engineering:
            category_suffix = "one_earth"
        else:
            category_suffix = "one_earth_category"
        taxonomy_mapping_results = (
            remove_mapping_name_suffix_from_taxonomy_results(
                taxonomy_mapping_results, category_suffix
            )
        )
        meta_df = pd.read_json(meta_df_path)
        round_df = fmcu.load_cb_round_data(funding_round_path)
        candid_funding_raw = pd.read_excel(candid_funding_path)
        candid_funding_long = fmcu.reshape_candid_funding(
            candid_funding_raw, id_col="id"
        )
        combined_funding_df = fmcu.combine_funding_data(
            round_df, candid_funding_long
        )

        return (
            taxonomy,
            taxonomy_mapping_results,
            meta_df,
            round_df,
            candid_funding_long,
            combined_funding_df,
        )
    return (load_files,)


@app.cell
def _():
    from vdl_tools.shared_tools.attention_index.attention_index import (
        AttentionIndexer,
    )
    return (AttentionIndexer,)


@app.cell
def _(
    CANDID_FUNDING_FILE,
    FUNDING_ROUND_FILE,
    META_FILENAME,
    TAXONOMY_FILE,
    TAXONOMY_MAPPING_RESULTS_FILE,
    load_files,
):
    (
        taxonomy,
        taxonomy_mapping_results,
        meta_df,
        round_df,
        candid_funding_long,
        combined_funding_df,
    ) = load_files(
        taxonomy_path=TAXONOMY_FILE,
        taxonomy_mapping_results_path=TAXONOMY_MAPPING_RESULTS_FILE,
        meta_df_path=META_FILENAME,
        funding_round_path=FUNDING_ROUND_FILE,
        candid_funding_path=CANDID_FUNDING_FILE,
        add_geo_engineering=True,
    )
    return (
        candid_funding_long,
        combined_funding_df,
        meta_df,
        round_df,
        taxonomy,
        taxonomy_mapping_results,
    )


@app.cell
def _(
    AttentionIndexer,
    candid_funding_long,
    combined_funding_df,
    meta_df,
    round_df,
    taxonomy,
    taxonomy_mapping_results,
):
    aier = AttentionIndexer(
        taxonomy=taxonomy,
        taxonomy_mapping_results=taxonomy_mapping_results,
        taxonomy_mapping_id_col="uid",
        meta_df=meta_df.copy(),
        round_df=round_df.copy(),
        candid_funding_long=candid_funding_long.copy(),
        combined_funding_df=combined_funding_df.copy(),
        additional_rooting_factor=3,
        min_year=2018,
        max_year=2025,
    )
    return (aier,)


@app.cell
def _(aier):
    attention_index = aier.calculate_attention_index(max_level=3)
    attention_index
    return (attention_index,)


@app.cell
def _(attention_index):
    import altair as alt

    _attention_index = attention_index

    _sort_order = attention_index.sort_values(
        "zero_max_geometric_mean_level_1", ascending=False
    )["tax_map_level1"].unique()
    # replace _df with your data source
    _chart = (
        alt.Chart(
            _attention_index,
            height=800,
            width=1200,
        )
        .mark_circle()
        .encode(
            x=alt.X(
                field="min_max_scale_min_max_geometric_mean_level_3",
                type="quantitative",
            ),
            # x=alt.X(field='min_max_scale_min_max_geometric_mean_level_2', type='quantitative'),
            y=alt.Y(field="tax_map_level1", type="nominal", sort=_sort_order),
            color=alt.Color(field="tax_map_level1", type="nominal"),
            tooltip=[
                alt.Tooltip(field="tax_map_level1"),
                alt.Tooltip(field="tax_map_level2"),
                alt.Tooltip(field="tax_map_level3"),
                alt.Tooltip(field="min_max_scale_min_max_geometric_mean_level_3"),
            ],
        )
        .properties(
            # height=290,
            # width='container',
            config={"axis": {"grid": True}}
        )
    )
    _chart
    return (alt,)


@app.cell
def _():
    return


@app.cell
def _(
    AttentionIndexer,
    candid_funding_long,
    combined_funding_df,
    meta_df,
    round_df,
    taxonomy,
    taxonomy_mapping_results,
):
    start_year = 2010

    approach_results = {}
    solution_results = {}

    for start_year in range(2010, 2021):
        end_year = start_year + 3
        print(start_year)
        _aier = AttentionIndexer(
            taxonomy=taxonomy,
            taxonomy_mapping_results=taxonomy_mapping_results,
            taxonomy_mapping_id_col="uid",
            meta_df=meta_df.copy(),
            round_df=round_df.copy(),
            candid_funding_long=candid_funding_long.copy(),
            combined_funding_df=combined_funding_df.copy(),
            additional_rooting_factor=3,
            min_year=start_year,
            max_year=end_year,
            rounds_to_include=["pre_seed", "seed", "angel", "series_a"],
            distributed_funding_level=3,
        )

        _approach_attention_index = _aier.calculate_attention_index(max_level=3)
        _approach_attention_index["start_year"] = start_year
        _approach_attention_index["end_year"] = end_year

        approach_results[start_year] = _approach_attention_index

        _solution_attention_index = _approach_attention_index.drop_duplicates(
            subset=["tax_map_level0", "tax_map_level1", "tax_map_level2"]
        ).copy()
        solution_results[start_year] = _solution_attention_index
    return approach_results, solution_results


@app.cell
def _(approach_results, pd, solution_results):
    total_approach_results = pd.concat(approach_results.values())
    total_solution_results = pd.concat(solution_results.values())

    approach_metric_cols = [
        x for x in total_solution_results.columns if x.endswith("level_3")
    ]
    approach_metric_cols

    approach_metric_cols.append("tax_map_level3")

    total_solution_results = total_solution_results.drop(
        approach_metric_cols, axis=1
    )
    return total_approach_results, total_solution_results


@app.cell
def _():
    return


@app.cell
def _(total_approach_results, total_solution_results):
    total_approach_results.to_json(
        "/Users/zeintawil/Downloads/approach_results.json", orient="records"
    )


    total_solution_results.to_json(
        "/Users/zeintawil/Downloads/solutions_results.json", orient="records"
    )
    return


@app.cell
def _(total_solution_results):
    energy_total_solution_results = total_solution_results[
        total_solution_results["tax_map_level0"] == "Energy Transition"
    ]

    energy_total_solution_results = energy_total_solution_results[
        energy_total_solution_results["tax_map_level2"].apply(
            lambda x: ":" not in x and "No_" not in x
        )
    ]
    return (energy_total_solution_results,)


@app.cell
def _(alt, energy_total_solution_results, mo):
    # replace _df with your data source
    _chart = (
        alt.Chart(energy_total_solution_results)
        .mark_line()
        .encode(
            x=alt.X(field="end_year", type="nominal"),
            y=alt.Y(
                field="min_max_scale_min_max_geometric_mean_level_2",
                type="quantitative",
            ),
            color=alt.Color(field="tax_map_level2", type="nominal"),
            tooltip=[
                alt.Tooltip(field="end_year", format=",.0f"),
                alt.Tooltip(
                    field="min_max_scale_min_max_geometric_mean_level_2",
                    aggregate="mean",
                    format=",.2f",
                ),
                alt.Tooltip(field="tax_map_level2"),
                alt.Tooltip(field="tax_map_level1"),
            ],
        )
        .properties(
            height=290,
            width="container",
            title="Attention over Time Energy Transition",
            config={"axis": {"grid": False}},
        )
    )
    mo.ui.altair_chart(_chart)
    _chart
    return


@app.cell
def _(alt, energy_total_solution_results, mo):
    # replace _df with your data source
    _chart = (
        alt.Chart(
            energy_total_solution_results[
                energy_total_solution_results["tax_map_level1"]
                == "Renewable Power"
            ]
        )
        .mark_line()
        .encode(
            x=alt.X(field="end_year", type="nominal"),
            y=alt.Y(
                field="min_max_scale_min_max_geometric_mean_level_2",
                type="quantitative",
            ),
            color=alt.Color(field="tax_map_level2", type="nominal"),
            tooltip=[
                alt.Tooltip(field="end_year", format=",.0f"),
                alt.Tooltip(
                    field="min_max_scale_min_max_geometric_mean_level_2",
                    aggregate="mean",
                    format=",.2f",
                ),
                alt.Tooltip(field="tax_map_level2"),
                alt.Tooltip(field="tax_map_level1"),
            ],
        )
        .properties(
            height=290,
            width="container",
            title="Attention over Time Renewable Power",
            config={"axis": {"grid": False}},
        )
    )

    mo.ui.altair_chart(_chart)
    _chart
    return


@app.cell
def _(alt, energy_total_solution_results, mo):
    # replace _df with your data source
    _chart = (
        alt.Chart(
            energy_total_solution_results[
                (
                    energy_total_solution_results["tax_map_level1"]
                    == "Renewable Power"
                )
                & (energy_total_solution_results["start_year"].isin([2010, 2020]))
            ]
        )
        .mark_line()
        .encode(
            x=alt.X(field="end_year", type="nominal"),
            y=alt.Y(
                field="min_max_scale_min_max_geometric_mean_level_2",
                type="quantitative",
            ),
            color=alt.Color(field="tax_map_level2", type="nominal"),
            tooltip=[
                alt.Tooltip(field="end_year", format=",.0f"),
                alt.Tooltip(
                    field="min_max_scale_min_max_geometric_mean_level_2",
                    aggregate="mean",
                    format=",.2f",
                ),
                alt.Tooltip(field="tax_map_level2"),
                alt.Tooltip(field="tax_map_level1"),
            ],
        )
        .properties(
            height=290,
            width="container",
            title="Attention Change Renewable Power",
            config={"axis": {"grid": False}},
        )
    )
    mo.ui.altair_chart(_chart)
    _chart
    return


@app.cell
def _(alt, mo, total_solution_results):
    # replace _df with your data source
    total_subpill_results = total_solution_results.drop_duplicates(
        subset=["tax_map_level0", "tax_map_level1", "start_year"]
    )

    total_subpill_results = total_subpill_results[
        total_subpill_results["tax_map_level1"].apply(
            lambda x: "No_" not in x and "Cross" not in x
        )
    ]

    _chart = (
        alt.Chart(total_subpill_results)
        .mark_line()
        .encode(
            x=alt.X(field="end_year", type="nominal"),
            y=alt.Y(
                field="min_max_scale_min_max_geometric_mean_level_1",
                type="quantitative",
            ),
            color=alt.Color(field="tax_map_level1", type="nominal"),
            tooltip=[
                alt.Tooltip(field="end_year", format=",.0f"),
                alt.Tooltip(
                    field="min_max_scale_min_max_geometric_mean_level_1",
                    aggregate="mean",
                    format=",.2f",
                ),
                alt.Tooltip(field="tax_map_level1"),
            ],
        )
        .properties(
            height=290,
            width="container",
            title="Attention over Time by Subpillars",
            config={"axis": {"grid": False}},
        )
    )
    mo.ui.altair_chart(_chart)
    return (total_subpill_results,)


@app.cell
def _(alt, mo, total_subpill_results):
    # replace _df with your data source


    _chart = (
        alt.Chart(
            total_subpill_results[
                total_subpill_results["start_year"].isin([2010, 2020])
            ]
        )
        .mark_line()
        .encode(
            x=alt.X(field="end_year", type="nominal"),
            y=alt.Y(
                field="min_max_scale_min_max_geometric_mean_level_1",
                type="quantitative",
            ),
            color=alt.Color(field="tax_map_level1", type="nominal"),
            tooltip=[
                alt.Tooltip(field="end_year", format=",.0f"),
                alt.Tooltip(
                    field="min_max_scale_min_max_geometric_mean_level_1",
                    aggregate="mean",
                    format=",.2f",
                ),
                alt.Tooltip(field="tax_map_level1"),
            ],
        )
        .properties(
            height=290,
            width="container",
            title="Attention Change Subpillars",
            config={"axis": {"grid": False}},
        )
    )
    mo.ui.altair_chart(_chart)
    # _chart
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
