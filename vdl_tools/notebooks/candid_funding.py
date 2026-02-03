import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    import plotly.express as px
    return pd, px


@app.cell
def _(pd):
    candid_main = pd.read_excel('../shared-data/data/candid/2025_09_08/candid_main_programs_w_yrs.xlsx')
    return (candid_main,)


@app.cell
def _(candid_main):
    candid_main
    return


@app.cell
def _(candid_main):
    funding_columns = [c for c in candid_main.columns if 'total_funding_' in c.lower()]
    return


@app.cell
def _(candid_main):
    contrib_all_columns = [c for c in candid_main.columns if 'contrib_all_' in c.lower()]
    contrib_other_columns = [c for c in candid_main.columns if 'contrib_other_' in c.lower()]
    gov_grants_columns = [c for c in candid_main.columns if 'gov_grants_' in c.lower()]
    return (contrib_all_columns,)


@app.cell
def _(candid_main):
    candid_main
    return


@app.cell
def _(candid_main, contrib_all_columns):
    reshaped_rows = []
    for _, row in candid_main.iterrows():
        ein = row['ein']
        for col in contrib_all_columns:
            year = col.split('_')[-1]
            if int(year) < 2020:
                continue
            reshaped_row = {
                'ein': ein,
                'organization_name': row['organization_name'],
                'year': int(year),
                'contrib_all': row[col],
                'contrib_other': row.get(f'contrib_other_{year}', None),
                'gov_grants': row.get(f'gov_grants_{year}', None),
                'total_funding': row.get(f'total_funding_{year}', None)
            }
            reshaped_rows.append(reshaped_row)
    return (reshaped_rows,)


@app.cell
def _(pd, reshaped_rows):
    long_data = pd.DataFrame(reshaped_rows)

    return (long_data,)


@app.cell
def _():
    import altair as alt
    return (alt,)


@app.cell
def _():
    248364 + 31301792
    return


@app.cell
def _(long_data):
    long_filtered_data = long_data[(long_data['total_funding'].notnull()) & (long_data['total_funding'] > 100000) & (long_data['contrib_all'] > 0)]
    long_filtered_data
    return (long_filtered_data,)


@app.cell
def _(alt):
    alt.data_transformers.enable("vegafusion")
    return


@app.cell
def _(long_filtered_data, px):
    px.scatter(
        long_filtered_data,
        x='total_funding',
        y='contrib_all',
        template="simple_white",
        hover_data=[ "ein", 'total_funding', 'contrib_all', 'year']
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
