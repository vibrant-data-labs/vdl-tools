import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import json

    import pandas as pd

    from vdl_tools.shared_tools.project_config import get_paths
    from vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping import redistribute_funding_fracs
    from vdl_tools.shared_tools.climate_landscape.add_taxonomy_mapping import load_one_earth_taxonomy, remove_mapping_name_suffix_from_taxonomy_results

    from vdl_tools.shared_tools.climate_landscape import funding_mapping_combination_utils as fmcu
    return (
        fmcu,
        load_one_earth_taxonomy,
        pd,
        redistribute_funding_fracs,
        remove_mapping_name_suffix_from_taxonomy_results,
    )


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    TAXONOMY_FILE = '../shared-data/data/taxonomies/oneearth/OE Solutions Terms 20250502_expanded.xlsx'

    META_FILENAME = '../climate-landscape/data/results/cb_cd_li_meta.json'
    # Undistributed ones so we distribute to whichever levl
    TAXONOMY_MAPPING_RESULTS_FILE = '../climate-landscape/data/results/one_earth_taxonomy_mapping_results.json'

    DISTRIBUTED_FUNDING_LEVEL = 3

    CANDID_FUNDING_FILE = '../shared-data/data/candid/2025_08_19/candid_orgs_cleaned.xlsx'
    FUNDING_ROUND_FILE = '../shared-data/data/crunchbase/2025_09_01/organizations_funding_rounds.json'
    return (
        CANDID_FUNDING_FILE,
        FUNDING_ROUND_FILE,
        META_FILENAME,
        TAXONOMY_FILE,
        TAXONOMY_MAPPING_RESULTS_FILE,
    )


@app.cell
def _():

    # taxonomy = load_one_earth_taxonomy(
    #     TAXONOMY_FILE,
    #     add_geo_engineering=False
    # )

    # taxonomy_mapping_results = pd.read_json(TAXONOMY_MAPPING_RESULTS_FILE)

    # taxonomy_mapping_results = remove_mapping_name_suffix_from_taxonomy_results(taxonomy_mapping_results, "one_earth_category")
    return


@app.cell
def _(taxonomy_mapping_results):
    taxonomy_mapping_results
    return


@app.cell
def _():
    # distributed_funding_df = redistribute_funding_fracs(
    #     df=taxonomy_mapping_results,
    #     taxonomy=taxonomy,
    #     id_attr='id',
    #     keepcols=['Organization'],
    #     max_level=DISTRIBUTED_FUNDING_LEVEL,
    # )

    # distributed_funding_df['uid'] = distributed_funding_df['id']
    return


@app.cell
def _():
    # meta_df = pd.read_json(META_FILENAME)
    return


@app.cell
def _():
    # round_df = fmcu.load_cb_round_data(FUNDING_ROUND_FILE)
    # candid_funding_raw = pd.read_excel(CANDID_FUNDING_FILE)
    # candid_funding_long = fmcu.reshape_candid_funding(candid_funding_raw, id_col='id')
    # combined_funding_df = fmcu.combine_funding_data(round_df, candid_funding_long)
    return


@app.cell
def _():
    # meta_df_prefix = "meta"
    # funding_df_prefix = "funding"
    # tax_map_prefix = "tax_map"

    # meta_df_renamed = fmcu.rename_df_cols(meta_df, meta_df_prefix)
    # combined_funding_df_renamed = fmcu.rename_df_cols(combined_funding_df, funding_df_prefix)
    # distr_funding_renamed = fmcu.rename_df_cols(distributed_funding_df, tax_map_prefix)
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
    ):
        taxonomy = load_one_earth_taxonomy(
            taxonomy_path,
            add_geo_engineering=False
        )
        taxonomy_mapping_results = pd.read_json(taxonomy_mapping_results_path)
        taxonomy_mapping_results = remove_mapping_name_suffix_from_taxonomy_results(taxonomy_mapping_results, "one_earth_category")
        meta_df = pd.read_json(meta_df_path)
        round_df = fmcu.load_cb_round_data(funding_round_path)
        candid_funding_raw = pd.read_excel(candid_funding_path)
        candid_funding_long = fmcu.reshape_candid_funding(candid_funding_raw, id_col='id')
        combined_funding_df = fmcu.combine_funding_data(round_df, candid_funding_long)

        return taxonomy, taxonomy_mapping_results, meta_df, round_df, candid_funding_long, combined_funding_df
    return (load_files,)


@app.cell
def _():
    from vdl_tools.shared_tools.attention_index.attention_index import AttentionIndexer
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
        candid_funding_path=CANDID_FUNDING_FILE
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
        taxonomy_mapping_id_col='id',
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
def _(taxonomy_mapping_results):
    taxonomy_mapping_results.drop('FundingFrac', axis=1, inplace=True)
    return


@app.cell
def _(taxonomy_mapping_results):
    copy_df = taxonomy_mapping_results[taxonomy_mapping_results['id'] == '00062c60-a35d-43e5-8a93-611741edce8d'].copy()
    return (copy_df,)


@app.cell
def _(copy_df):
    copy_df
    return


@app.cell
def _(copy_df, mo):
    edited_df = mo.ui.data_editor(copy_df)

    edited_df
    return (edited_df,)


@app.cell
def _(edited_df):
    edited_df.value
    return


@app.cell
def _(aier):
    aier.taxonomy
    return


@app.cell
def _(aier, edited_df, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=edited_df.value.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=1,
    )
    return


@app.cell
def _(aier, copy_df, redistribute_funding_fracs):

    redistribute_funding_fracs(
        df=copy_df.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=1,
    )
    return


@app.cell
def _(aier, edited_df, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=edited_df.value.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=2,
    )
    return


@app.cell
def _(aier, copy_df, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=copy_df.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=2,
    )
    return


@app.cell
def _(aier, copy_df, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=copy_df.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=3,
    )
    return


@app.cell
def _(aier, edited_df, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=edited_df.value.copy(),
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=3,
    )
    return


@app.cell
def _(aier, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=aier.taxonomy_mapping_results[aier.taxonomy_mapping_results['id'] == '00062c60-a35d-43e5-8a93-611741edce8d'],
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=2,
    )
    return


@app.cell
def _(aier, redistribute_funding_fracs):
    redistribute_funding_fracs(
        df=aier.taxonomy_mapping_results[aier.taxonomy_mapping_results['id'] == '00062c60-a35d-43e5-8a93-611741edce8d'],
        taxonomy=aier.taxonomy,
        id_attr=aier._taxonomy_mapping_id_col,
        keepcols=[],
        max_level=3,
    )
    return


@app.cell
def _(aier):
    attention_index = aier.calculate_attention_index(max_level=3)
    attention_index
    return (attention_index,)


@app.cell
def _(attention_index):
    import altair as alt

    _attention_index = attention_index

    _sort_order = attention_index.sort_values('zero_max_geometric_mean_level_1', ascending=False)['tax_map_level1'].unique()
    # replace _df with your data source
    _chart = (
        alt.Chart(
            _attention_index,
            height=800,
            width=1200,
        )
        .mark_circle()
        .encode(
            x=alt.X(field='min_max_scale_min_max_geometric_mean_level_3', type='quantitative'),
            y=alt.Y(field='tax_map_level1', type='nominal', sort=_sort_order),
            color=alt.Color(field='tax_map_level1', type='nominal'),
            tooltip=[
                alt.Tooltip(field='tax_map_level1'),
                alt.Tooltip(field='tax_map_level2'),
                alt.Tooltip(field='tax_map_level3'),
                alt.Tooltip(field='min_max_scale_min_max_geometric_mean_level_3')
            ]
        )
        .properties(
            # height=290,
            # width='container',
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
def _():
    return


if __name__ == "__main__":
    app.run()
