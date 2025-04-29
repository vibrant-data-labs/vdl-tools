import datetime as dt
import argparse
import random

import jsonlines
import numpy as np
import pandas as pd

from vdl_tools.shared_tools.project_relevance.add_categorical_skills_to_linkedin_3 import (
    calculate_project_relevance,
    calculate_term_idf,
    add_keywords,
    PROJECT_RELEVANCE_THRESHOLD,
    PROJECT_RELEVANCE_IDF_THRESHOLD,
    TECH_RELEVANCE_THRESHOLD,
)
from vdl_tools.shared_tools.project_relevance.add_founder_toplines_to_venture_cft_4 import (
    add_project_relevance_to_venture_df,
)

from vdl_tools.shared_tools.project_config import get_paths


PATHS = get_paths()


def remove_random_skills(user_skills_list):
    len_skills = len(user_skills_list)
    if len_skills > 3:
        num_skills_to_remove = random.randint(0, min(4, np.ceil(len_skills / 4)))
        return random.sample(user_skills_list, len_skills - num_skills_to_remove)
    else:
        return user_skills_list


def add_random_skills(user_skills_list, all_skill_list):
    len_skills = len(user_skills_list)
    if len_skills > 0:
        num_skills_to_add = random.randint(0, 3)
        return list(set(user_skills_list + random.sample(all_skill_list, num_skills_to_add)))
    else:
        return user_skills_list


def change_user_skills(user_skills_list, all_skill_list):
    user_skills_list = remove_random_skills(user_skills_list)
    user_skills_list = add_random_skills(user_skills_list, all_skill_list)
    return user_skills_list


def create_random_iteration(
    skills_df,
    linkedin_profiles_w_skills,
    term_idf,
    venture_df,
    write_to_file=True,
):
    # Changes relevance thresholds
    project_relevance_threshold = (
        PROJECT_RELEVANCE_THRESHOLD + np.random.normal(0, 0.05)
    )
    # Changes relevance thresholds
    project_relevance_idf_threshold = (
        PROJECT_RELEVANCE_IDF_THRESHOLD + np.random.normal(0, 0.05)
    )
    tech_relevance_threshold = TECH_RELEVANCE_THRESHOLD + np.random.normal(
        0, 0.05
    )

    # Randomly switch the project_relevant flag for each skill with a probability of 0.5%
    skills_df_random = skills_df.copy()
    skills_df_random["project_relevant"] = (
        skills_df_random["project_relevant"]
        .apply(
            lambda x: int(not x) if np.random.random() <= 0.005 else int(x)
        )
    )

    # Randomizes user skills by removing and adding random skills
    linkedin_profiles_w_skills_run = linkedin_profiles_w_skills.copy()
    linkedin_profiles_w_skills_run["skills_tags_list"] = (
        linkedin_profiles_w_skills_run["skills_tags_list"]
        .apply(
            lambda x: change_user_skills(x, list(term_idf.keys()))
        )
    )

    li_w_skills_relevance = calculate_project_relevance(
        skills_df_random,
        linkedin_profiles_w_skills_run,
        term_idf=term_idf,
        project_relevance_threshold=project_relevance_threshold,
        project_relevance_idf_threshold=project_relevance_idf_threshold,
        tech_relevance_threshold=tech_relevance_threshold,
    )

    venture_df_relevance = add_project_relevance_to_venture_df(
        venture_df,
        li_w_skills_relevance,
    )

    # Store
    if write_to_file:
        with jsonlines.open(PATHS["sensitivity_analysis_orgs"], "a") as f:
            f.write(venture_df_relevance[
                [
                    "uuid",
                    "contains_project_relevant",
                    "contains_project_relevant_idf",
                    "only_tech_majority",
                    "only_tech_large_majority",
                ]
            ].to_dict(orient="records"))
        

        with jsonlines.open(PATHS["sensitivity_analysis_founders"], "a") as f:
            f.write(li_w_skills_relevance[
                [
                    "project_relevant_sum",
                    "project_relevant_idf_sum",

                    "tech_relevant_sum",
                    "tech_relevant_ratio",
                    
                    "project_relevant_ratio",
                    "project_relevant_idf_ratio",

                    "is_project_relevant",
                    "is_project_relevant_idf",

                    "is_tech_majority",
                    "is_tech_large_majority",

                    "org_uid",
                    "li_id",
                    "li_id_alt",
                ]
            ].to_dict(orient='records')
            )
    skills_df_random = None
    linkedin_profiles_w_skills_run = None

    return venture_df_relevance, li_w_skills_relevance

def main(n_iterations=1000):
    linkedin_df = pd.read_json(PATHS["clean_linkedin_profiles"])
    venture_df = pd.read_json(PATHS["venture_orgs_w_founding_year"])
    skills_df = pd.read_csv(PATHS["labeled_skills_hierarchy"])

    linkedin_profiles_add_skills = add_keywords(linkedin_df, skills_df)
    linkedin_profiles_w_skills = linkedin_profiles_add_skills[
        ~linkedin_profiles_add_skills["skills_tags_list"].apply(
            lambda x: len(x) == 0
        )
    ]
    term_idf = calculate_term_idf(linkedin_profiles_add_skills["skills_tags_list"])

    for i in range(n_iterations):
        if i % 10 == 0:
            print(f"{dt.datetime.now()}: Running iteration {i}")
        _, _ = create_random_iteration(
            skills_df,
            linkedin_profiles_w_skills,
            term_idf,
            venture_df,
            write_to_file=True,
        )

if __name__ == "__main__":
    import warnings

    # Suppress RuntimeWarning
    warnings.filterwarnings("ignore", category=RuntimeWarning)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--n_iterations",
        type=int,
        default=100,
        help="Number of iterations to run",
    )
    args = parser.parse_args()
    main(args.n_iterations)
    warnings.filterwarnings("default", category=RuntimeWarning)
