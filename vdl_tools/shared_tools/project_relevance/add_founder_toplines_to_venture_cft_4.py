import pandas as pd
import numpy as np

from vdl_tools.shared_tools.project_config import get_paths


PATHS = get_paths()


MANUFACTURING_KEYWORDS = {
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


def add_project_relevance_to_venture_df(venture_df, linkedin_profiles_w_skills):

    def _rounded_average(series):
        clean_series = [x for x in series if not np.isnan(x)]
        return np.floor(np.mean(clean_series))

    org_uid_to_all_founders_top_lines = (
        linkedin_profiles_w_skills.groupby("org_uid")
        .aggregate({
            "is_project_relevant": "sum",
            "is_project_relevant_idf": "sum",
            "is_tech_majority": "sum",
            "is_tech_large_majority": "sum",
            "first_experience_year": _rounded_average,
            "name": "count"
        })
        [["is_project_relevant", "is_project_relevant_idf", "is_tech_majority", "is_tech_large_majority", "first_experience_year", "name"]]
        .rename(columns={"first_experience_year": "mean_first_experience_year", "name": "total"})
        .to_dict(orient="index")
    )

    def _contains_project_relevant(x, column="is_project_relevant"):
        all_founders_relevance = org_uid_to_all_founders_top_lines.get(x, {}).get(column)
        if all_founders_relevance is not None:
            return all_founders_relevance > 0
        else:
            return None

    def _contains_only_tech_relevant(x, metric_name="is_tech_majority"):
        all_founders_relevance = org_uid_to_all_founders_top_lines.get(x, {}).get(metric_name)
        total_count = org_uid_to_all_founders_top_lines.get(x, {}).get('total')
        if all_founders_relevance is not None:
            return all_founders_relevance == total_count
        else:
            return None


    venture_df["contains_project_relevant"] = venture_df["uuid"].apply(_contains_project_relevant, args=("is_project_relevant",))
    venture_df["contains_project_relevant_idf"] = venture_df["uuid"].apply(_contains_project_relevant, args=("is_project_relevant_idf",))
    venture_df["only_tech_majority"] = venture_df["uuid"].apply(_contains_only_tech_relevant, "is_tech_majority")
    venture_df["only_tech_large_majority"] = venture_df["uuid"].apply(_contains_only_tech_relevant, "is_tech_large_majority")
    venture_df["mean_first_experience_year"] = venture_df["uuid"].apply(lambda x: org_uid_to_all_founders_top_lines.get(x, {}).get('mean_first_experience_year'))
    venture_df["years_exp_before_founding"] = venture_df["founded_year"] - venture_df["mean_first_experience_year"]

    # venture_df["org_need_project_knowledge"] = venture_df["Keywords"].apply(
    #     lambda x: True
    #     if any(word in MANUFACTURING_KEYWORDS for word in x)
    #     else False
    # )

    return venture_df


def main():
    venture_df = pd.read_json(PATHS["venture_orgs_w_founding_year"])
    linkedin_profiles_w_skills = pd.read_json(PATHS["annotated_linkedin_profiles"])

    venture_df = add_project_relevance_to_venture_df(venture_df, linkedin_profiles_w_skills)
    venture_df.to_json(PATHS["venture_orgs_w_relevance"], orient="records")
    return venture_df

if __name__ == "__main__":
    venture_df = main()