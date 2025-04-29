import marimo

__generated_with = "0.3.2"
app = marimo.App(
    width="full",
    layout_file="layouts/experience_sensitivity_analysis.grid.json",
)


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import jsonlines
    import numpy as np
    return jsonlines, mo, np, pd


@app.cell
def __():
    import plotly.express as px
    return px,


@app.cell
def __():
    from vdl_tools.shared_tools.project_relevance.add_categorical_skills_to_linkedin_3 import (
        calculate_project_relevance,
        set_project_relevance,
        add_keywords,
        PROJECT_RELEVANCE_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
    )
    from vdl_tools.shared_tools.project_relevance.add_founder_toplines_to_venture_cft_4 import (
        add_project_relevance_to_venture_df,
    )

    from vdl_tools.shared_tools.project_config import get_paths


    PATHS = get_paths()
    return (
        PATHS,
        PROJECT_RELEVANCE_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
        add_keywords,
        add_project_relevance_to_venture_df,
        calculate_project_relevance,
        get_paths,
        set_project_relevance,
    )


@app.cell
def __(PATHS, jsonlines):
    with jsonlines.open(PATHS["sensitivity_analysis_orgs"], "r") as _f:
        # simulations_orgs = [simulation for simulation in _f]
        simulations_orgs = []
        for simulation in _f:
            simulation_orgs_ = []
            for org in simulation:
                uid = org.pop('uid', None)
                if uid:
                    org['uuid'] = uid
                simulation_orgs_.append(org)
            simulations_orgs.append(simulation_orgs_)
    return org, simulation, simulation_orgs_, simulations_orgs, uid


@app.cell
def __(simulations_orgs):
    simulations_orgs[0]
    return


@app.cell
def __(PATHS, pd):
    venture_df_raw = pd.read_json(PATHS["venture_orgs_w_relevance"])
    manufacturing_keywords = {
        "manufacturing",
        "electric",
        "industrial",
        "plants",
        "energy storage",
        "battery",
        "electricity",
        "electric vehicle",
        "infrastructure",
        "vehicles",
        "recycling",
        "fuel",
        "solar power",
        "charging",
        "construction",
        "buildings",
        "hardware",
        "sensor",
        "utilities",
        "packaging",
        "electricity grid",
        "drilling",
        "robotics",
        "vehicle charging",
        "electric vehicle charging",
        "truck",
        "lithium-ion battery",
        "mining",
        "carbon capture & storage",
        "power generation",
    }
    venture_df = venture_df_raw[
        ~venture_df_raw["contains_project_relevant"].isnull()
    ].copy()
    return manufacturing_keywords, venture_df, venture_df_raw


@app.cell
def __(np, pd, venture_df):
    def get_metrics(
        simulation_run, metric_name, grouper_columns=None, quantiles=(25, 75)
    ):
        metric_avgs = None
        for org_list in simulation_run:
            sample_pd = pd.DataFrame(org_list)
            sample_pd = sample_pd[sample_pd[metric_name].notnull()].copy()
            venture_df_sub = venture_df[
                # ["uid", "org_need_project_knowledge", "years_exp_before_founding"]
                ["uuid", "years_exp_before_founding"]
            ].copy()
            venture_df_sub["lte_5_years_exp"] = (
                venture_df_sub["years_exp_before_founding"] <= 5
            )
            venture_df_sub["lte_10_years_exp"] = (
                venture_df_sub["years_exp_before_founding"] <= 10
            )
            venture_df_sub["gte_10_years_exp"] = (
                venture_df_sub["years_exp_before_founding"] >= 10
            )
            venture_df_sub["gte_15_years_exp"] = (
                venture_df_sub["years_exp_before_founding"] >= 15
            )
            sample_pd = sample_pd.merge(
                # venture_df_sub, left_on="uid", right_on="uid"
                venture_df_sub, left_on="uuid", right_on="uuid"
            )
            sample_pd = sample_pd.drop_duplicates()

            if grouper_columns:
                metric_dict = (
                    sample_pd.groupby(grouper_columns)[metric_name]
                    .mean()
                    .to_dict()
                )
                if metric_avgs is None:
                    metric_avgs = {k: [v] for k, v in metric_dict.items()}
                else:
                    for key, value in metric_dict.items():
                        metric_avgs[key].append(value)
            else:
                avg = sample_pd[metric_name].mean()
                if metric_avgs is None:
                    metric_avgs = [avg]
                else:
                    metric_avgs.append(avg)
        if grouper_columns:
            return {
                k: get_metrics_simple(v, quantiles=quantiles)
                for k, v in metric_avgs.items()
            }
        else:
            return get_metrics_simple(metric_avgs, quantiles=quantiles)


    def get_metrics_simple(series, quantiles=(25, 75)):
        return {
            "mean": np.mean(series),
            "std_dev": np.std(series),
            "median": np.median(series),
            "iqr_25_75": (
                np.percentile(series, quantiles[0]),
                np.percentile(series, quantiles[1]),
            ),
            "all_avgs": series,
        }
    return get_metrics, get_metrics_simple


@app.cell
def __(get_metrics, pd, px):
    def make_comparison_violin_plots(
        simulations_orgs,
        column_name,
        grouper_columns=None,
        title=None,
        yaxis_title=None,
        plot_type="violin",
        rename_dict=None,
    ):
        grouper_columns = grouper_columns or []
        sample_metrics = get_metrics(
            simulations_orgs, column_name, grouper_columns
        )
        column_names = []
        if len(grouper_columns) > 1:
            for key in sample_metrics.keys():
                column_name_parts = []
                for i, groupon_value in enumerate(key):
                    column_name_parts.append(
                        f"{grouper_columns[i]}={bool(groupon_value)}"
                    )

                column_names.append("_".join(column_name_parts))
            plot_df = pd.DataFrame.from_dict(
                {str(k): v["all_avgs"] for k, v in sample_metrics.items()}
            )
        elif len(grouper_columns) == 1:
            column_names = [
                f"{grouper_columns[0]}={bool(key)}"
                for key in sample_metrics.keys()
            ]
            plot_df = pd.DataFrame.from_dict(
                {str(k): v["all_avgs"] for k, v in sample_metrics.items()}
            )
        else:
            column_names = ["all"]
            plot_df = pd.DataFrame(sample_metrics["all_avgs"])

        if rename_dict:
            column_names = [rename_dict.get(col, col) for col in column_names]
        plot_df.columns = column_names


        if plot_type == "box":
            fig = px.box(
                plot_df,
                template="simple_white",
                color_discrete_sequence=["limegreen"],
            )
        else:
            fig = px.violin(
                plot_df,
                template="simple_white",
                color_discrete_sequence=["limegreen"],
                box=True,
            )

        yaxis_title = (
            yaxis_title
            if yaxis_title
            else " ".join(column_name.split("_")).title()
        )
        fig.update_layout(yaxis_title=yaxis_title, xaxis_title=None, title=title)
        return fig
    return make_comparison_violin_plots,


@app.cell
def __(get_metrics, px):
    def create_plot(
        avgs,
        mean,
        std_dev,
        median,
        iqr_25_75,
        column_name="contains_project_relevant",
        title="Project Relevance",
        add_std_dev_bars=False,
        add_iqr_bars=True,
    ):

        fig = px.histogram(
            avgs,
            title=title,
            template="simple_white",
            color_discrete_sequence=["limegreen"],
            opacity=0.7,
        )

        if add_std_dev_bars:
            fig.add_vline(x=mean, line_width=3, line_dash="solid", line_color="red")
            fig.add_vline(
                x=mean + 2 * std_dev,
                line_width=3,
                line_dash="dash",
                line_color="red",
            )
            fig.add_vline(
                x=mean - 2 * std_dev,
                line_width=3,
                line_dash="dash",
                line_color="red",
            )

        if add_iqr_bars:
            fig.add_vline(x=median, line_width=3, line_dash="solid", line_color="blue")
            fig.add_vline(
                x=iqr_25_75[1],
                line_width=3,
                line_dash="dash",
                line_color="blue",
            )
            fig.add_vline(
                x=iqr_25_75[0],
                line_width=3,
                line_dash="dash",
                line_color="blue",
            )

        fig.update_layout(
            xaxis_title=title,
            yaxis_title="Frequency",
            showlegend=False,
        )
        return fig


    def create_plot_get_metrics(
        simulation_list,
        column_name="contains_project_relevant",
        title="Project Relevance",
        quantiles=(25, 75),
        add_std_dev_bars=False,
        add_iqr_bars=True,
    ):
        metrics = get_metrics(
            simulation_list,
            column_name,
            quantiles=quantiles,
        )

        avgs = metrics["all_avgs"]
        mean = metrics["mean"]
        std = metrics["std_dev"]
        median = metrics["median"]
        iqr_25_75 = metrics["iqr_25_75"]
        return create_plot(
            avgs,
            mean,
            std,
            median,
            iqr_25_75,
            column_name,
            title,
            add_std_dev_bars=add_std_dev_bars,
            add_iqr_bars=add_iqr_bars,
        )
    return create_plot, create_plot_get_metrics


@app.cell
def __(create_plot_get_metrics, simulations_orgs):
    create_plot_get_metrics(
        simulations_orgs,
        "contains_project_relevant",
        "Distribution of mean number of organizations with at least 1 'project experienced' founder",
        quantiles=(25, 75),
        add_std_dev_bars=False,
        add_iqr_bars=True,
    )
    return


@app.cell
def __():
    # fig = create_plot_get_metrics(
    #     simulations_orgs,
    #     "contains_project_relevant_idf",
    #     "Distribution of mean number of organizations with at least 1 'project experienced' founder (using idf column)",
    #     quantiles=(25, 75),
    # )
    # fig
    return


@app.cell
def __():
    # create_plot_get_metrics(
    #     simulations_orgs,
    #     "only_tech_majority",
    #     "Distribution of mean number of organizations with 'tech only' founders",
    #     quantiles=(25, 75),
    # )
    return


@app.cell
def __(create_plot, get_metrics_simple, venture_df):
    _experience_metrics = get_metrics_simple(
        venture_df["years_exp_before_founding"].dropna().tolist()
    )
    create_plot(
        venture_df["years_exp_before_founding"].dropna(),
        _experience_metrics["mean"],
        _experience_metrics["std_dev"],
        _experience_metrics["median"],
        _experience_metrics["iqr_25_75"],
        "years_exp_before_founding",
        "Years of Experience Before Founding",
    )
    # px.histogram(venture_df["years_exp_before_founding"], template="simple_white", color_discrete_sequence=["limegreen"], opacity=0.7)
    return


@app.cell
def __():
    # make_comparison_violin_plots(
    #     simulations_orgs,
    #     "contains_project_relevant",
    #     ["org_need_project_knowledge"],
    #     title="Distribution of mean number of organizations with at least 1 'project experienced' founder",
    #     rename_dict={"org_need_project_knowledge=False": "Not Hard Tech", "org_need_project_knowledge=True": "Hard Tech"}
    # )
    return


@app.cell
def __():
    # make_comparison_violin_plots(
    #     simulations_orgs,
    #     "contains_project_relevant_idf",
    #     ["org_need_project_knowledge"],
    #     title="Distribution of mean number of organizations with at least 1 'project experienced (idf)' founder",
    #     rename_dict={"org_need_project_knowledge=False": "Not Hard Tech", "org_need_project_knowledge=True": "Hard Tech"},
    # )
    return


@app.cell
def __(make_comparison_violin_plots, simulations_orgs):
    make_comparison_violin_plots(
        simulations_orgs,
        "contains_project_relevant_idf",
        ["lte_5_years_exp"],
        title="Distribution of mean number of organizations with at least 1 'project experienced' founder",
    )
    return


@app.cell
def __():
    # make_comparison_violin_plots(
    #     simulations_orgs,
    #     "only_tech_majority",
    #     ["org_need_project_knowledge", "lte_5_years_exp"],
    #     title="Distribution of mean number of organizations with all 'tech majority' founders",
    # )
    return


@app.cell
def __(mo, slides):
    mo.carousel(slides)
    return


@app.cell
def __(
    mo,
    questions_md,
    relevant_experience_md,
    tech_experience_md,
    years_experience_md,
):
    slides = [
        mo.md("#Founder Experience"),
        mo.md("## Do climate tech founders have “project relevant” experience?\n\n### Elemental provides knowledge and capacity building to its portfolio companies in order to help them access project funding. This knowledge and capacity building is necessary because accessing this money or operating a project / manufacturing process is complex and requires a deep understanding of the capital stack, market, and the regulatory environment.\n\n###Ideally, VDL would show that companies that receive this, perform better than those that don't. However, this is difficult to prove as we don't know what companies outside of the Elemental portfolio receive this type of training."),
        questions_md,
        relevant_experience_md,
        tech_experience_md,
        years_experience_md,
        # relevant_experience_company_type_md,
        # relevant_experience_company_type_years_md,
        mo.md(""),
    ]
    return slides,


@app.cell
def __(mo):
    questions_md = mo.md(
    """##In lieu of that, if we can show there is a knowledge gap in the founding team's experience, we can show that the knowledge and capacity building is necessary. We asked:
    1. What percentage of companies have at least 1 founder with “project relevant” experience?
    1. What percentage of of companies have all founders with a Science, Research, or Tech background?
    1. What percentage of companies have founders with an average of 5 years of experience or less?
    """
    )
    return questions_md,


@app.cell
def __(create_plot_get_metrics, mo, simulations_orgs):
    relevant_experience_hist = create_plot_get_metrics(
        simulations_orgs,
        "contains_project_relevant_idf",
        "Companies with at least 1 'project experienced' founder",
        quantiles=(25, 75),
    )
    relevant_experience_hist.update_layout(height=600, width=1500)
    relevant_experience_md = mo.md(f"##More than 60% of companies **don't** have at least 1 founder with “project relevant” experience\n\n {mo.as_html(relevant_experience_hist)}")
    return relevant_experience_hist, relevant_experience_md


@app.cell
def __(create_plot_get_metrics, mo, simulations_orgs):
    tech_experience_hist = create_plot_get_metrics(
        simulations_orgs,
        "only_tech_majority",
        "Companies with 'tech only' founders",
        quantiles=(25, 75),
    )
    tech_experience_hist.update_layout(height=600, width=1500)
    tech_experience_md = mo.md(f"## The large majority of companies don't only have 'tech founders' \n\n {mo.as_html(tech_experience_hist)}")
    return tech_experience_hist, tech_experience_md


@app.cell
def __(create_plot, get_metrics_simple, mo, venture_df):
    reasonable_values_experiences = venture_df[(-2 < venture_df["years_exp_before_founding"]) & (venture_df["years_exp_before_founding"] < 50)]["years_exp_before_founding"]
    _experience_metrics = get_metrics_simple(
        reasonable_values_experiences.dropna().tolist()
    )
    experience_plot = create_plot(
        reasonable_values_experiences.dropna(),
        _experience_metrics["mean"],
        _experience_metrics["std_dev"],
        _experience_metrics["median"],
        _experience_metrics["iqr_25_75"],
        "years_exp_before_founding",
        "Years of Experience Before Founding",
    )
    experience_plot.update_layout(height=600, width=1500)
    years_experience_md = mo.md(f"## Most companies have founders with over 10 years of experience before founding \n\n {mo.as_html(experience_plot)}")
    return (
        experience_plot,
        reasonable_values_experiences,
        years_experience_md,
    )


@app.cell
def __():
    # relevant_experience_company_type = make_comparison_violin_plots(
    #     simulations_orgs,
    #     "contains_project_relevant_idf",
    #     ["org_need_project_knowledge"],
    #     plot_type="box",
    #     title="Companies with at least 1 'project experienced' founder by company type",
    #     yaxis_title="Ratio of Companies",
    #     rename_dict={"org_need_project_knowledge=False": "Not Hard Tech / Large Scale", "org_need_project_knowledge=True": "Hard Tech / Large Scale"},
    # )
    # relevant_experience_company_type.update_layout(height=600, width=1500)
    # relevant_experience_company_type_md = mo.md(f"## The overall trend holds for companies regardless if their primary product would require having project relevant experience {mo.as_html(relevant_experience_company_type)}")
    return


@app.cell
def __():
    # rename_dict={
    #     "org_need_project_knowledge=False_lte_5_years_exp=False": "Not Hard Tech / Large Scale<br>Experienced Founders",
    #     "org_need_project_knowledge=False_lte_5_years_exp=True": "Not Hard Tech / Large Scale<br>Not Experienced Founders",
    #     "org_need_project_knowledge=True_lte_5_years_exp=False": "Hard Tech / Large Scale<br>Experienced Founders",
    #     "org_need_project_knowledge=True_lte_5_years_exp=True": "Hard Tech / Large Scale<br>Not Experienced Founders",
    # }

    # relevant_experience_company_type_years = make_comparison_violin_plots(
    #     simulations_orgs,
    #     "contains_project_relevant_idf",
    #     ["org_need_project_knowledge", "lte_5_years_exp"],
    #     plot_type="box",
    #     title="Companies with at least 1 'project experienced' founder by company type and founder experience",
    #     yaxis_title="Ratio of Companies",
    #     rename_dict=rename_dict,
    # )
    # relevant_experience_company_type_years.update_layout(height=600, width=1500)
    # relevant_experience_company_type_years_md = mo.md(f"## The overall trend also holds for companies regardless if their primary product would require having project relevant experience and the number of years their founders worked before founding their company {mo.as_html(relevant_experience_company_type_years)}")
    return


@app.cell
def __(venture_df):
    venture_df.columns
    return


@app.cell
def __():
    # Update the figure size
    return


@app.cell
def __():
    # venture_df_raw['org_need_project_knowledge'].value_counts()
    return


@app.cell
def __():
    3649 / (3649 + 784)
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
