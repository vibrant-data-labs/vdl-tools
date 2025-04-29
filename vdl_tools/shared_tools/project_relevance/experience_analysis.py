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
        add_keywords,
        PROJECT_RELEVANCE_THRESHOLD as RELEVANCE_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
    )
    from vdl_tools.shared_tools.project_relevance.add_founder_toplines_to_venture_cft_4 import (
        add_project_relevance_to_venture_df,
    )
    return (
        RELEVANCE_THRESHOLD,
        TECH_RELEVANCE_THRESHOLD,
        add_keywords,
        add_project_relevance_to_venture_df,
        calculate_project_relevance,
        set_project_relevance,
    )


@app.cell
def __(RELEVANCE_THRESHOLD, mo):
    relevance_threshold = mo.ui.number(
        start=0.01,
        stop=0.99,
        step=0.01,
        value=RELEVANCE_THRESHOLD,
        label="Relevance Threshold",
    )
    return relevance_threshold,


@app.cell
def __():
    from vdl_tools.shared_tools.project_config import get_paths


    PATHS = get_paths()
    return PATHS, get_paths


@app.cell
def __(mo):
    methodolgy_md = mo.md(
        """## Background
    Elemental provides knowledge and capacity building to its portfolio companies in order to help them access project funding. This knowledge and capacity building is necessary because accessing this money or operating a project / manufacturing process is complex and requires a deep understanding of the capital stack, market, and the regulatory environment.

    Ideally, VDL would show that companies that receive this, perform better than those that don't. However, this is difficult to prove as we don't know what companies outside of the Elemental portfolio receive this type of training.

    In lieu of that, if we can show there is a knowledge gap in the founding team's experience, we can show that the knowledge and capacity building is necessary.

    Using founder LinkedIn profiles, we were able to identify the reported skills and experiences of each founder in our dataset of venture stage companies. From this language, we identified words that would indicate the founder has "project relevant" experience. We then used this language to identify which founders would be considered to have "project relevant" experience based on the frequency of these words appearing in their LinkedIn profiles.

    We then mapped determined if each company had at least 1 founder who had this "project relevant" experience.

    ## Methodology

    1. Create a set of venture stage companies
    1. Create a set of founders and retrieve their LinkedIn profiles
    1. Create a set of skills and experiences that would indicate "project relevant" experience
    1. Map the skills and experiences to the founders
    Based on the frequency of the skills and experiences, determine if each founder has "project relevant" experience
    Determine if each company has at least 1 founder with "project relevant" experience
    Identify companies that would be more likely to need a founder with "project relevant" experience based on their line of business (hardware vs software)

    ## Next Steps
    - Receive feedback on this overall approach
    - Receive feedback / Refine on the type of language that would be best for identifying "project relevant" experience
    - Refine the language to identify companies that would be more likely to need a founder with "project relevant" experience based on their line of business (hardware vs software)
    - Find comparable set of companies that would be more likely to need a founder with "project relevant" experience based on their line of business that are not necessarily climate focused.
      - Compare the pool and experiences of the climate focused founders to those in manufacturing writ large to determine if the climate founder set is unique in their need for project relevant capacity building.

    """
    )
    methodolgy_md
    return methodolgy_md,


@app.cell
def __(mo):
    mo.md(""" ## Skills to indicate 'project experience' """)
    return


@app.cell
def __(mo):
    skills_iframe = mo.Html(
        '<iframe src="https://docs.google.com/spreadsheets/d/1yMymmE37pJWqxTmseBMf1jV6OoF-i2GchQ74-em44X8/edit?usp=sharing?widget=true&amp;headers=false;rm=minimal&amp;single=true&amp;" width="1200" height="800"></iframe>',
    )

    skills_iframe
    return skills_iframe,


@app.cell
def __(mo):
    update_button = mo.ui.button(label="Update Labeled Skills Data")
    return update_button,


@app.cell
def __(mo, relevance_threshold, update_button):
    mo.hstack([relevance_threshold, update_button])
    return


@app.cell
def __(PATHS, pd):
    linkedin_df = pd.read_json(PATHS["clean_linkedin_profiles"])
    venture_df = pd.read_json(PATHS["venture_orgs_w_founding_year"])
    # skills_df = pd.read_csv(PATHS["labeled_skills_hierarchy"])
    return linkedin_df, venture_df


@app.cell
def __(venture_df):
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
    venture_df["org_need_project_knowledge"] = venture_df["Keywords"].apply(
        lambda x: "Yes"
        if any(word in manufacturing_keywords for word in x)
        else "No"
    )
    return manufacturing_keywords,


@app.cell
def __(get_skills_df_from_web, update_button):
    update_button
    skills_df = get_skills_df_from_web()
    return skills_df,


@app.cell
def __(add_keywords, linkedin_df, skills_df):
    linkedin_profiles_w_skills = add_keywords(linkedin_df, skills_df)
    return linkedin_profiles_w_skills,


@app.cell
def __():
    return


@app.cell
def __(
    RELEVANCE_THRESHOLD,
    add_project_relevance_to_venture_df,
    calculate_project_relevance,
    linkedin_profiles_w_skills,
    relevance_threshold,
    skills_df,
    venture_df,
):
    linkedin_profiles_w_skills_w_relevance = calculate_project_relevance(
        skills_df,
        linkedin_profiles_w_skills,
        RELEVANCE_THRESHOLD,
    )
    linkedin_profiles_w_skills_w_relevance = (
        linkedin_profiles_w_skills_w_relevance,
        relevance_threshold.value,
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
def __(linkedin_profiles_w_skills_w_relevance, mo, relevance_threshold):
    num_founders_w_rel = linkedin_profiles_w_skills_w_relevance[
        "is_project_relevant"
    ].sum()
    num_founders = linkedin_profiles_w_skills_w_relevance.shape[0]
    mo.callout(
        f"{num_founders_w_rel} of {num_founders} ({round(num_founders_w_rel/num_founders, 3)*100}%) founders would be considered to have 'project relevant' experience based on a threshold of {relevance_threshold.value}.",
        kind="info",
    )
    return num_founders, num_founders_w_rel


@app.cell
def __(linkedin_profiles_w_skills_w_relevance, px, relevance_threshold):
    fig = px.histogram(
        linkedin_profiles_w_skills_w_relevance,
        x="project_relevant_ratio",
        cumulative=True,
    )
    fig.update_layout(
        title="Cumulative Distribution of founders' skill relevance to projects",
        xaxis_title="% of founder's skills that are 'project relevant'",
        yaxis_title="Cumulative Count",
    )

    # Add a verticle line at the threshold
    fig.add_vline(x=relevance_threshold.value, line_dash="dash", line_color="red")
    return fig,


@app.cell
def __(
    linkedin_profiles_w_skills_w_relevance,
    mo,
    venture_df_w_project_relevance,
):
    num_orgs_w_project_rel_founder = venture_df_w_project_relevance[
        "contains_project_relevant"
    ].sum()
    total_num_orgs = venture_df_w_project_relevance.shape[0]
    total_num_orgs_w_founder_linked_in = (
        venture_df_w_project_relevance["contains_project_relevant"].notna().sum()
    )
    total_orgs_w_skill = linkedin_profiles_w_skills_w_relevance[
        "org_uid"
    ].nunique()

    venture_df_w_project_relevance_w_founder_linkedins = (
        venture_df_w_project_relevance[
            venture_df_w_project_relevance["contains_project_relevant"].notna()
        ]
    )

    mo.callout(
        f"{num_orgs_w_project_rel_founder} of {total_num_orgs_w_founder_linked_in} ({round(num_orgs_w_project_rel_founder/total_orgs_w_skill, 3)*100}%) organizations have at least 1 founder considered to have 'project relevant' skills. ({total_num_orgs} in total but {total_num_orgs - total_num_orgs_w_founder_linked_in} are missing linkedin data)",
        kind="info",
    )
    return (
        num_orgs_w_project_rel_founder,
        total_num_orgs,
        total_num_orgs_w_founder_linked_in,
        total_orgs_w_skill,
        venture_df_w_project_relevance_w_founder_linkedins,
    )


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
