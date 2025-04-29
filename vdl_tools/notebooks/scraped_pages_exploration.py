import marimo

__generated_with = "0.10.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    from vdl_tools.scrape_enrich.scraper.scrape_websites import scrape_websites_psql
    from vdl_tools.shared_tools.web_summarization.website_summarization_psql import make_group_text, PATHS_TO_KEEP, GENERIC_ORG_WEBSITE_PROMPT_TEXT
    return (
        GENERIC_ORG_WEBSITE_PROMPT_TEXT,
        PATHS_TO_KEEP,
        make_group_text,
        mo,
        scrape_websites_psql,
    )


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _(pd):
    df = pd.read_json('../climate-landscape/data/results/relevance_model_results.json')
    df = df[df['prediction_relevant'] == 1]
    return (df,)


@app.cell
def _(df):
    urls = df[df['Website'].notnull()]['Website'].tolist()
    return (urls,)


@app.cell
def _(scrape_websites_psql, urls):
    scraped_df = scrape_websites_psql(
        urls=urls,
    )
    return (scraped_df,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
