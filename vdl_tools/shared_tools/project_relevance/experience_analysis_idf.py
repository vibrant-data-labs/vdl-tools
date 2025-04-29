import marimo

__generated_with = "0.3.2"
app = marimo.App(
    width="full",
    layout_file="layouts/experience_analysis.grid.json",
)


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    return mo, pd


@app.cell
def __():
    import plotly.express as px
    return px,


@app.cell
def __():
    from vdl_tools.shared_tools.project_relevance.add_categorical_skills_to_linkedin_3 import (
        calculate_project_relevance,
        set_project_relevance,
        set_tech_majority,
        add_keywords,
        PROJECT_RELEVANCE_THRESHOLD,
        PROJECT_RELEVANCE_IDF_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
        calculate_term_idf,
    )
    from vdl_tools.shared_tools.project_relevance.add_founder_toplines_to_venture_cft_4 import (
        add_project_relevance_to_venture_df,
    )
    return (
        PROJECT_RELEVANCE_IDF_THRESHOLD,
        PROJECT_RELEVANCE_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
        add_keywords,
        add_project_relevance_to_venture_df,
        calculate_project_relevance,
        calculate_term_idf,
        set_project_relevance,
        set_tech_majority,
    )


@app.cell
def __(
    PROJECT_RELEVANCE_IDF_THRESHOLD,
    PROJECT_RELEVANCE_THRESHOLD,
    TECH_RELEVANCE_THRESHOLD,
    mo,
):
    project_relevance_threshold = mo.ui.number(
        start=0.01,
        stop=0.99,
        step=0.01,
        value=PROJECT_RELEVANCE_THRESHOLD,
        label="Project Relevance Threshold",
    )
    project_relevance_idf_threshold = mo.ui.number(
        start=0.1,
        stop=5,
        step=0.1,
        value=PROJECT_RELEVANCE_IDF_THRESHOLD,
        label="Project Relevance (IDF) Threshold",
    )
    tech_relevance_threshold = mo.ui.number(
        start=0.01,
        stop=0.99,
        step=0.01,
        value=TECH_RELEVANCE_THRESHOLD,
        label="Tech Relevance Threshold",
    )
    return (
        project_relevance_idf_threshold,
        project_relevance_threshold,
        tech_relevance_threshold,
    )


@app.cell
def __():
    from vdl_tools.shared_tools.project_config import get_paths


    PATHS = get_paths()
    return PATHS, get_paths


@app.cell
def __(mo):
    update_button = mo.ui.button(label="Update Labeled Skills Data")
    return update_button,


@app.cell
def __(
    mo,
    project_relevance_idf_threshold,
    project_relevance_threshold,
    tech_relevance_threshold,
    update_button,
):
    mo.hstack(
        [
            project_relevance_threshold,
            project_relevance_idf_threshold,
            tech_relevance_threshold,
            update_button,
        ]
    )
    return


@app.cell
def __(PATHS, pd):
    linkedin_df = pd.read_json(PATHS["clean_linkedin_profiles"])
    venture_df = pd.read_json(PATHS["venture_orgs_w_founding_year"])
    skills_df = pd.read_csv(PATHS["labeled_skills_hierarchy"])
    return linkedin_df, skills_df, venture_df


@app.cell
def __():
    # manufacturing_keywords = {
    #     "manufacturing",
    #     "electric",
    #     "industrial",
    #     "plants",
    #     "energy storage",
    #     "battery",
    #     "electricity",
    #     "electric vehicle",
    #     "infrastructure",
    #     "vehicles",
    #     "recycling",
    #     "fuel",
    #     "solar power",
    #     "charging",
    #     "construction",
    #     "buildings",
    #     "hardware",
    #     "sensor",
    #     "utilities",
    #     "packaging",
    #     "electricity grid",
    #     "drilling",
    #     "robotics",
    #     "vehicle charging",
    #     "electric vehicle charging",
    #     "truck",
    #     "lithium-ion battery",
    #     "mining",
    #     "carbon capture & storage",
    #     "power generation",
    # }
    # venture_df["org_need_project_knowledge"] = venture_df["Keywords"].apply(
    #     lambda x: "Yes"
    #     if any(word in manufacturing_keywords for word in x)
    #     else "No"
    # )
    return


@app.cell
def __(update_button):
    update_button
    return


@app.cell
def __(add_keywords, linkedin_df, skills_df):
    linkedin_profiles_w_skills = add_keywords(linkedin_df, skills_df)
    return linkedin_profiles_w_skills,


@app.cell
def __(calculate_term_idf, linkedin_profiles_w_skills):
    term_idf = calculate_term_idf(linkedin_profiles_w_skills["skills_tags_list"])
    return term_idf,


@app.cell
def __(
    PROJECT_RELEVANCE_THRESHOLD,
    TECH_RELEVANCE_THRESHOLD,
    add_project_relevance_to_venture_df,
    calculate_project_relevance,
    linkedin_profiles_w_skills,
    project_relevance_idf_threshold,
    project_relevance_threshold,
    set_project_relevance,
    set_tech_majority,
    skills_df,
    tech_relevance_threshold,
    term_idf,
    update_button,
    venture_df,
):
    update_button
    linkedin_profiles_w_skills_w_relevance = calculate_project_relevance(
        skills_df=skills_df,
        df_w_skills=linkedin_profiles_w_skills,
        term_idf=term_idf,
        project_relevance_threshold=PROJECT_RELEVANCE_THRESHOLD,
        tech_relevance_threshold=TECH_RELEVANCE_THRESHOLD,
    )
    linkedin_profiles_w_skills_w_relevance = set_project_relevance(
        linkedin_profiles_w_skills_w_relevance,
        project_relevance_threshold.value,
        project_relevance_idf_threshold.value,
    )
    linkedin_profiles_w_skills_w_relevance = set_tech_majority(
        linkedin_profiles_w_skills_w_relevance, tech_relevance_threshold.value
    )

    venture_df_w_project_relevance = add_project_relevance_to_venture_df(
        venture_df, linkedin_profiles_w_skills_w_relevance
    )
    return (
        linkedin_profiles_w_skills_w_relevance,
        venture_df_w_project_relevance,
    )


@app.cell
def __(mo):
    mo.md("""## Distribution of founders with 'project relevant' experience""")
    return


@app.cell
def __(
    linkedin_profiles_w_skills_w_relevance,
    project_relevance_threshold,
    px,
):
    _fig = px.histogram(
        linkedin_profiles_w_skills_w_relevance,
        x="project_relevant_ratio",
        cumulative=True,
    )
    _fig.update_layout(
        title="Cumulative Distribution of founders' skill relevance to projects",
        xaxis_title="% of founder's skills that are 'project relevant'",
        yaxis_title="Cumulative Count",
    )

    # Add a verticle line at the threshold
    _fig.add_vline(
        x=project_relevance_threshold.value, line_dash="dash", line_color="red"
    )
    return


@app.cell
def __(
    linkedin_profiles_w_skills_w_relevance,
    project_relevance_threshold,
    px,
):
    _fig = px.histogram(
        linkedin_profiles_w_skills_w_relevance,
        x="project_relevant_idf_ratio",
        cumulative=True,
    )
    _fig.update_layout(
        title="Distribution of founders' skill relevance to projects",
        xaxis_title="% of founder's skills that are 'project relevant' (idf)",
        yaxis_title="Count",
    )

    # Add a verticle line at the threshold
    _fig.add_vline(
        x=project_relevance_threshold.value, line_dash="dash", line_color="red"
    )
    return


@app.cell
def __(linkedin_profiles_w_skills_w_relevance, mo):
    key_columns = [
        "name",
        "project_relevant_ratio",
        "project_relevant_idf_ratio",
        "skills_tags_list",
        "member_experience_collection",
        # "is_project_relevant",
        # "is_project_relevant_idf",
    ]
    sort_column = "project_relevant_idf_ratio"
    mo.ui.table(
        linkedin_profiles_w_skills_w_relevance[key_columns].sort_values(
            sort_column, ascending=False
        ),
        page_size=100,
    )
    return key_columns, sort_column


@app.cell
def __(linkedin_profiles_w_skills_w_relevance):
    linkedin_profiles_w_skills_w_relevance["is_project_relevant_idf"].sum()
    return


@app.cell
def __(linkedin_profiles_w_skills_w_relevance, mo):
    def write_summary_of_ventures_with_relevance(
        venture_df_w_project_relevance, column
    ):
        num_orgs_w_project_rel_founder = venture_df_w_project_relevance[
            column
        ].sum()
        total_num_orgs = venture_df_w_project_relevance.shape[0]
        total_num_orgs_w_founder_linked_in = (
            venture_df_w_project_relevance[column].notna().sum()
        )
        total_orgs_w_skill = linkedin_profiles_w_skills_w_relevance[
            "org_uid"
        ].nunique()

        venture_df_w_project_relevance_w_founder_linkedins = (
            venture_df_w_project_relevance[
                venture_df_w_project_relevance[column].notna()
            ]
        )

        return mo.callout(
            f"""{num_orgs_w_project_rel_founder} of {total_num_orgs_w_founder_linked_in} ({round(num_orgs_w_project_rel_founder/total_orgs_w_skill, 3)*100}%) organizations have at least 1 founder considered to have '{column}' skills. ({total_num_orgs} in total but {total_num_orgs - total_num_orgs_w_founder_linked_in} are missing linkedin data)""",
            kind="info",
        )
    return write_summary_of_ventures_with_relevance,


@app.cell
def __(
    venture_df_w_project_relevance,
    write_summary_of_ventures_with_relevance,
):
    write_summary_of_ventures_with_relevance(
        venture_df_w_project_relevance, "contains_project_relevant"
    )
    return


@app.cell
def __(
    venture_df_w_project_relevance,
    write_summary_of_ventures_with_relevance,
):
    write_summary_of_ventures_with_relevance(
        venture_df_w_project_relevance, "contains_project_relevant_idf"
    )
    return


@app.cell
def __(venture_df_w_project_relevance):
    (
        venture_df_w_project_relevance["contains_project_relevant"]
        == venture_df_w_project_relevance["contains_project_relevant_idf"]
    ).sum()
    return


@app.cell
def __(mo, venture_df_w_project_relevance):
    mo.ui.table(
        venture_df_w_project_relevance[
            ["Name", "contains_project_relevant", "contains_project_relevant_idf"]
        ].sort_values("contains_project_relevant_idf", ascending=False)
    )
    return


@app.cell
def __(venture_df_w_project_relevance_w_founder_linkedins):
    venture_df_w_project_relevance_w_founder_linkedins[
        "org_need_project_knowledge"
    ].value_counts()
    return


@app.cell
def __(px, venture_df_w_project_relevance_w_founder_linkedins):
    def create_theme_bar_charts(groupby_column="Keyword Theme"):
        themes_w_projects = (
            venture_df_w_project_relevance_w_founder_linkedins.groupby(
                groupby_column
            )["contains_project_relevant"]
            .agg(["sum", "count"])
            .sort_values("sum", ascending=False)
        )
        themes_w_projects.rename(
            columns={
                "sum": "num_orgs_w_project_rel_founder",
                "count": "total_num_orgs",
            },
            inplace=True,
        )

        themes_w_projects_reset = themes_w_projects.reset_index()
        themes_w_projects_reset[groupby_column] = themes_w_projects_reset[
            groupby_column
        ].apply(lambda x: x[:30] if isinstance(x, str) else x)
        themes_w_projects_reset.set_index(groupby_column, inplace=True)
        themes_w_projects_reset.replace({False: 0, True: 1}, inplace=True)

        bar_fig = px.bar(
            themes_w_projects_reset[
                themes_w_projects_reset["total_num_orgs"] > 10
            ],
            title=f"Orgs w/ Project Relevant Founder by {groupby_column}",
        )

        bar_fig.update_layout(
            yaxis_title="Number of organizations",
            xaxis_title=groupby_column,
        )
        # remove the legend
        # bar_fig.update_layout(showlegend=False)

        # Rotate the x-axis labels
        bar_fig.update_xaxes(tickangle=45)
        return bar_fig


    create_theme_bar_charts("Keyword Theme")
    return create_theme_bar_charts,


@app.cell
def __(create_theme_bar_charts):
    create_theme_bar_charts("org_need_project_knowledge")
    return


@app.cell
def __(create_theme_bar_charts):
    create_theme_bar_charts("Funding Category")
    return


@app.cell
def __(pd):
    import requests
    import io


    def get_skills_df_from_web():
        r = requests.get(
            "https://docs.google.com/spreadsheets/d/e/2PACX-1vRzr-yXg5BgmV5zAEVYiQqYrGOXirt-P9CAN8rhOz3IXSCreeYMNth0Uou_BIPNApEUiKC6qeV_c6Al/pub?gid=161648650&single=true&output=csv"
        )

        skills_df = pd.read_csv(io.StringIO(r.text))
        return skills_df
    return get_skills_df_from_web, io, requests


@app.cell
def __():
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
