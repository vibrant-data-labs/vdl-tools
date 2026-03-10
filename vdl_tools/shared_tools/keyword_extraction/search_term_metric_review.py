import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells

    import altair as alt

    import marimo as mo
    import pandas as pd
    import numpy as np

    from vdl_tools.shared_tools.project_config import get_paths

    from vdl_tools.shared_tools.keyword_extraction.search_term_recall_calculations import (
        run_keyword_search_metrics,
        simulate_efficiency_cuts,
    )

    tab = "&nbsp;&nbsp;&nbsp;&nbsp;"


    def explain_metrics(
        search_metrics: pd.DataFrame,
        name: str,
        extra=None,
    ):
        precision = search_metrics[0]
        recall = search_metrics[1]
        metrics = search_metrics[2]

        # metrics comes in as a Series with a MultiIndex of (KW_HIT_Pos, RELEVANCE_LABEL).
        # Example structure:
        #   KW_HIT_Pos  RELEVANCE_LABEL
        #   True        False              82483
        #   True        True               60170
        #
        # Problem: If all keyword hits are True (or all False), one of the index values
        # won't exist, causing a KeyError when we try to access metrics.loc[False] or
        # metrics.loc[True]. Same issue applies to the RELEVANCE_LABEL level.
        #
        # Solution: Convert the MultiIndex Series to a DataFrame using unstack(), then
        # reindex to ensure both True and False exist in both the index (KW_HIT_Pos)
        # and columns (RELEVANCE_LABEL). Missing combinations get filled with 0.
        #
        # After unstack(), the DataFrame looks like:
        #   RELEVANCE_LABEL  False   True
        #   KW_HIT_Pos
        #   False              0       0    <- added by reindex if missing
        #   True           82483   60170
        metrics = metrics.unstack(fill_value=0)
        metrics = metrics.reindex(index=[False, True], columns=[False, True], fill_value=0)

        # Now we can safely access any combination of True/False without KeyError
        all_false_kw_hit = metrics.loc[False].sum()  # Sum across all RELEVANCE_LABEL for False KW_HIT_Pos
        all_true_kw_hit = metrics.loc[True].sum()    # Sum across all RELEVANCE_LABEL for True KW_HIT_Pos
        hit_true_rate = all_true_kw_hit / (all_false_kw_hit + all_true_kw_hit)

        all_relevant_hits = metrics.loc[:, True].sum()  # Sum across all KW_HIT_Pos for True RELEVANCE_LABEL
        relevant_hit_rate = all_relevant_hits / metrics.values.sum()

        true_and_relevant_hits = metrics.loc[True, True]  # Keyword hit was True AND document was relevant

        # Convert back to MultiIndex Series format for display consistency.
        # stack() is the inverse of unstack() - it pivots the columns back into a MultiIndex level.
        metrics = metrics.stack()

        pr_text = f"""
    ###{name}:
    {tab}**All True Keyword Hits**: {all_true_kw_hit} ({hit_true_rate:.2%} of total)
    {tab}**All Relevant Hits**: {all_relevant_hits} ({relevant_hit_rate:.2%} of total)
    {tab}**True & Relevant Hits**: {true_and_relevant_hits}
    {tab}**Recall**: {recall:.2f}  ({true_and_relevant_hits} out of {all_relevant_hits})
    {tab}**Precision**: {precision:.2f}  ({true_and_relevant_hits} out of {all_true_kw_hit})
    {mo.ui.table(metrics.reset_index())}
    """
        if extra:
            pr_text += extra

        return mo.md(pr_text).callout()


    def create_search_artifacts(
        df,
        text_field,
        relevance_label_field,
        id_field,
        keywords,
    ):
        search_metrics = run_keyword_search_metrics(
            keywords,
            df,
            text_field=text_field,
            label_field=relevance_label_field,
        )

        df_with_extracted_keywords = search_metrics[3]

        baseline_explain = explain_metrics(
            search_metrics,
            name="10000 Random Non-Profits",
        )

        keywords_with_hits = set(
            df_with_extracted_keywords[
                df_with_extracted_keywords["KW_Extracted_Len"] > 0
            ]["KW_Extracted"]
            .explode()
            .to_list()
        )

        keywords_df = pd.DataFrame(keywords, columns=["term"])
        keywords_df["found_in_doc"] = keywords_df["term"].apply(
            lambda x: x in keywords_with_hits
        )
        keywords_df = keywords_df.sort_values("found_in_doc", ascending=False)
        keywords_table = mo.ui.table(keywords_df, page_size=10)

        term_sim_log, efficiency_df = simulate_efficiency_cuts(
            df_with_extracted_keywords, id_field=id_field,
        )


        # replace _df with your data source

        base = alt.Chart(term_sim_log)

        recall_pct_chart = base.mark_circle(color="blue").encode(
            x=alt.X(field="Terms_Removed", type="quantitative"),
            y=alt.Y(field="Recall_Pct", type="quantitative"),
            tooltip=[
                alt.Tooltip(field="Terms_Removed", format=",.0f"),
                alt.Tooltip(field="Recall_Pct", format=",.2f"),
                alt.Tooltip(field="FP_Hits_Avoided", format=",.2f"),
            ],
        )

        fp_hits_avoided = base.mark_circle(color="red").encode(
            x=alt.X(field="Terms_Removed", type="quantitative"),
            y=alt.Y(field="FP_Hits_Avoided", type="quantitative"),
            tooltip=[
                alt.Tooltip(field="Terms_Removed", format=",.0f"),
                alt.Tooltip(field="Recall_Pct", format=",.2f"),
                alt.Tooltip(field="FP_Hits_Avoided", format=",.2f"),
            ],
        )


        _chart = (
            alt.layer(recall_pct_chart, fp_hits_avoided)
            .resolve_scale(y="independent")
            .properties(height=290, width="container", config={"axis": {"grid": True}})
        )
        tradeoff_chart = mo.ui.altair_chart(_chart, chart_selection="point")

        return baseline_explain, keywords_table, efficiency_df, tradeoff_chart



    def create_updated_metrics(
        tradeoff_chart,
        efficiency_df,
        df,
        text_field,
        label_field,
    ):
        cutoff_index = None
        if tradeoff_chart.value.shape[0] > 0:
            cutoff_index = tradeoff_chart.value.iloc[-1]["Terms_Removed"].astype(int)

        search_terms = efficiency_df[cutoff_index:]["term"].to_list()
        _updated_search_metrics = run_keyword_search_metrics(
            search_terms,
            df,
            text_field=text_field,
            label_field=label_field,
        )

        updated_metrics = explain_metrics(
            _updated_search_metrics, "10000 Random Candid with Pruned Terms"
        )
        keyword_df = mo.ui.table(
            pd.DataFrame(search_terms, columns=["term"]), page_size=10
        )
        writeup = mo.md(f"""
    # Updated Metrics after Pruning
    *Click a point On the Chart to See Cutoff*
    {updated_metrics}
    # Keywords Used After Pruning
    {keyword_df}
    """)
        return writeup


@app.cell
def _():
    # Run the Below Uncommented 2 cells and with the values changed for your own use case
    return


@app.cell
def _():
    # from vdl_tools.shared_tools.keyword_extraction.single_clean_search_term_metric_review import create_search_artifacts, create_updated_metrics

    # df = <load_df>

    # text_field = <text_field_in_df>
    # relevance_label_field = <relevance_label_in_df>
    # id_field = <id_field_in_df>
    # keywords = <search_terms_list>

    # baseline_explain, keywords_md, efficiency_df, tradeoff_chart = create_search_artifacts(
    #     df,
    #     text_field,
    #     relevance_label_field,
    #     id_field,
    #     keywords,
    # )

    # writeup = mo.md(f"""
    #     # Baseline
    #     {baseline_explain}
    #     {keywords_md}
    #     # Efficiency Chart
    #     {mo.ui.table(efficiency_df)}
    #     # Tradeoff Chart
    #     {tradeoff_chart}
    # """)

    # writeup
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
