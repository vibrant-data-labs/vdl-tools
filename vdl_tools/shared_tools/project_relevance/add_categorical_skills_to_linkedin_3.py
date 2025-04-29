from collections import defaultdict

import pandas as pd
import numpy as np

from vdl_tools.scrape_enrich.tags_from_text import add_kwd_tags
from vdl_tools.shared_tools.project_config import get_paths


PATHS = get_paths()


PROJECT_RELEVANCE_THRESHOLD = 0.20
PROJECT_RELEVANCE_IDF_THRESHOLD = 0.7
TECH_RELEVANCE_THRESHOLD = 0.4


def get_higher_order_skill_counts(
    row,
    skill_to_middle,
    skill_to_parent,
    skill_to_project_relevant,
    term_idf,
):
    middle_skill_counts = defaultdict(int)
    parent_skill_counts = defaultdict(int)

    project_relevant_sum = 0
    tech_relevant_sum = 0
    tech_large_sum = 0

    project_relevant_idf_sum = 0
    tech_relevant_idf_sum = 0
    tech_large_idf_sum = 0

    matched_project_relevant_terms = {}

    for skill in row["skills_tags_list"]:
        skill_term_idf = term_idf.get(skill, 0)
        middle_skill = skill_to_middle[skill]
        middle_skill_counts[middle_skill] += 1
        parent_skill = skill_to_parent[skill]
        parent_skill_counts[parent_skill] += 1
        project_relevant_sum += skill_to_project_relevant[skill]
        proj_relevant_idf_score = skill_term_idf * skill_to_project_relevant[skill]
        project_relevant_idf_sum += proj_relevant_idf_score
        if proj_relevant_idf_score > 0:
            matched_project_relevant_terms[skill] = proj_relevant_idf_score
        if parent_skill in {"Technology and Engineering", "Science and Research"}:
            tech_relevant_sum += 1
            tech_relevant_idf_sum += skill_term_idf
        if parent_skill in {"Technology and Engineering", "Science and Research", "Design and Creativity"}:
            tech_large_sum += 1
            tech_large_idf_sum += skill_term_idf
    
    num_skills = len(row["skills_tags_list"])
    # Avoid division by zero
    num_skills = 1 if num_skills == 0 else num_skills
    return pd.Series(
        {
            "li_id": row["li_id"],
            "middle_skill_counts": middle_skill_counts,
            "parent_skill_counts": parent_skill_counts,

            "project_relevant_sum": project_relevant_sum,
            "project_relevant_idf_sum": project_relevant_idf_sum,

            "tech_relevant_sum": tech_relevant_sum,
            "tech_large_sum": tech_large_sum,
            "tech_relevant_idf_sum": tech_relevant_idf_sum,
            "tech_large_idf_sum": tech_large_idf_sum,
            "tech_relevant_ratio": tech_relevant_sum / num_skills,
            "project_relevant_ratio": project_relevant_sum / num_skills,
            "tech_large_ratio": tech_large_sum / num_skills,
            "tech_relevant_idf_ratio": tech_relevant_idf_sum / num_skills,
            "project_relevant_idf_ratio": project_relevant_idf_sum / num_skills,
            "tech_large_idf_ratio": tech_large_idf_sum / num_skills,
            "matched_project_relevant_terms": matched_project_relevant_terms,
        }
    )


def add_keywords(linkedin_df, skills_df):

    # Make the skills df look like the keyword tags df format
    keyword_tags_formatted_rows = []
    for _, row in skills_df.iterrows():
        keyword_tags_formatted_rows.append(
            {
                "master_term": row['skill'],
                "search_term": [row['skill']], # It's a list
                "add_related": [], # list that I don't know what it does,
                "broad_category": row['middle_skill'],
            }
        )
    skills_keyword_df = pd.DataFrame(keyword_tags_formatted_rows)

    linkedin_df["skills_joined"] = linkedin_df["skills"].apply(lambda x: "|".join(x) if len(x) > 0 else "")
    linkedin_df["experience_descriptions"] = linkedin_df["experiences"].apply(
            lambda x: " // ".join(x)
        )

    df_w_skills = add_kwd_tags(
        linkedin_df,
        skills_keyword_df,  # tag mapping dictionary
        # file to store tagging results
        PATHS["project_relevance"] / "skills_tags.xlsx",
        PATHS["project_relevance"],  # tag directory
        blacklist=[],  # additional tags to exclude
        idCol="li_id",  # merging col
        kwds="skills_tags",  # name of tag col
        textcols=[
            "summary",
            "skills_joined",
            "experience_descriptions",
        ],  # text columns to search
        format_tagmap=True,  # explode search terms
        master_term="master_term",  # name col with master term
        search_terms="search_term",  # name of col with list of search terms
        add_related="add_related",  # name of col with manual list of add_related
        # add unigrams within 2 or more grams.
        add_unigrams=False,
        add_bigrams=False,  # add bigrams within 3 or more grams
        loadexisting=False,  # True = don't run search just load prior results
    )
    return df_w_skills


def calculate_project_relevance(
    skills_df,
    df_w_skills,
    term_idf,
    project_relevance_threshold=PROJECT_RELEVANCE_THRESHOLD,
    project_relevance_idf_threshold=PROJECT_RELEVANCE_IDF_THRESHOLD,
    tech_relevance_threshold=TECH_RELEVANCE_THRESHOLD,
):
    skill_to_middle = skills_df.set_index("skill").to_dict()["middle_skill"]
    skill_to_parent = skills_df.set_index("skill").to_dict()["parent_skill"]
    skill_to_project_relevant = skills_df.set_index("skill").to_dict()["project_relevant"]

    id_w_higher_orders = df_w_skills.apply(get_higher_order_skill_counts, axis=1, args=(skill_to_middle, skill_to_parent, skill_to_project_relevant,term_idf))
    df_w_skills_w_higher_order = df_w_skills.merge(id_w_higher_orders, on="li_id")

    df_w_skills_w_higher_order = set_project_relevance(
        df_w_skills_w_higher_order,
        relevance_threshold=project_relevance_threshold,
        relevance_idf_threshold=project_relevance_idf_threshold
    )
    df_w_skills_w_higher_order = set_tech_majority(df_w_skills_w_higher_order, tech_relevance_threshold)

    return df_w_skills_w_higher_order

def set_project_relevance(
        df_w_skills_w_higher_order,
        relevance_threshold=PROJECT_RELEVANCE_THRESHOLD,
        relevance_idf_threshold=PROJECT_RELEVANCE_IDF_THRESHOLD
    ):
    df_w_skills_w_higher_order['is_project_relevant'] = df_w_skills_w_higher_order['project_relevant_ratio'] >= relevance_threshold
    df_w_skills_w_higher_order['is_project_relevant_idf'] = df_w_skills_w_higher_order['project_relevant_idf_ratio'] >= relevance_idf_threshold
    return df_w_skills_w_higher_order

def set_tech_majority(df_w_skills_w_higher_order, relevance_threshold=TECH_RELEVANCE_THRESHOLD):
    df_w_skills_w_higher_order['is_tech_majority'] = df_w_skills_w_higher_order['tech_relevant_ratio'] >= relevance_threshold
    df_w_skills_w_higher_order['is_tech_large_majority'] = df_w_skills_w_higher_order['tech_large_ratio'] >= relevance_threshold
    return df_w_skills_w_higher_order


def calculate_term_idf(skills_series):
    """Calculates the number of documents a term appears in.
    
    skills_series is a pd.Series of lists of skills.
    """
    term_counter = defaultdict(int)
    for skill_list in skills_series:
        for skill in skill_list:
            term_counter[skill] += 1
    term_idf = {k: 1 + np.log(len(skills_series) / v + 1)  for k, v in term_counter.items()}
    return term_idf


def main():
    linkedin_df = pd.read_json(PATHS["clean_linkedin_profiles"])
    skills_df = pd.read_csv(PATHS["labeled_skills_hierarchy"])

    df_w_skills = add_keywords(linkedin_df, skills_df)
    term_idf = calculate_term_idf(df_w_skills["skills_tags_list"])
    df_w_skills_w_higher_order = calculate_project_relevance(skills_df, df_w_skills, term_idf)

    df_w_skills_w_higher_order.to_json(PATHS["annotated_linkedin_profiles"])
    return df_w_skills_w_higher_order


if __name__ == "__main__":
    df_w_skills_w_higher_order = main()