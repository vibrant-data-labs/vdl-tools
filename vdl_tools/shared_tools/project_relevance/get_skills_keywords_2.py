from collections import defaultdict

import instructor
from more_itertools import chunked
from openai import OpenAI
import pandas as pd
from pydantic import BaseModel

from vdl_tools.shared_tools.project_config import get_paths


PATHS = get_paths()


client = instructor.patch(OpenAI(), mode=instructor.Mode.JSON)\


class SkillsList(BaseModel):
    """Model for describing the skills of a professional"""
    # The overall category for the skill
    skill_category: str
    # More specific skills and descriptions of the skills
    example_skills: list[str]


class UserSkills(BaseModel):
    user_skills: list[SkillsList]


def get_user_skills(skills_list, model="gpt-4.1-mini"):
    return client.chat.completions.create(
        model=model,
        temperature=0.5,
        max_tokens=4096,
        response_model=UserSkills,
        messages=[
            {
                "role": "user",
                "content": (
                    "Take this list of skills and assign each to a broader topic that would categorize a professional "
                    f"[{', '.join(skills_list)}]. "
                    "In the end we should have a broad taxonomy of skills for categorizing professionals. "
                )
            }
        ],
    )



def get_user_skills_10(skills_list, model_name, max_tokens=20000):
    return client.chat.completions.create(
        model=model_name,
        temperature=0.5,
        max_tokens=max_tokens,
        response_model=UserSkills,
        messages=[
            {
                "role": "user",
                "content": (
                    "Take this list of skills and assign each to a broader topic that would categorize a professional. "

                    "In the end we should have no more than 10 broad categories for professionals. \n"
                    "Each broad category should include the skills provided.\n"
                    "Please do not change the names of the example skills.\n"
                    "Please do not repeat example skills across different broad categories. Every skill should only be assigned to one broad category.\n"
                    "Please make sure to include all of the skills in the list.\n\n"
                    f"[{', '.join(skills_list)}]."
                )
            }
        ],
    )


if __name__ == "__main__":
    linkedin_df = pd.read_json(PATHS["clean_linkedin_profiles"])


    all_raw_skills = defaultdict(int)
    for _, row in linkedin_df.iterrows():
        for skill in row["skills"]:
            all_raw_skills[skill.lower()] += 1


    min_doc_count = 3
    all_raw_skills_list = [skill for skill, count in all_raw_skills.items() if count >= min_doc_count]


    all_skills_predicted = []
    for i, chunk in enumerate(chunked(all_raw_skills_list, 100)):
        all_skills_predicted.extend(get_user_skills(chunk, model="gpt-4-0125-preview").user_skills)


    skill_category_to_example_skills = defaultdict(set)
    for skill_list in all_skills_predicted:
        for skill in skill_list.example_skills:
            skill_category_to_example_skills[skill_list.skill_category.lower()].add(skill.lower())

    skill_category_to_example_skills = {k: list(v) for k, v in skill_category_to_example_skills.items()}

    parent_skills = get_user_skills_10(
        list(skill_category_to_example_skills.keys()),
        model_name="gpt-4-0125-preview",
        max_tokens=4096,
    )


    flat_skills = []
    for skill_list in parent_skills.user_skills:
        parent_skill = skill_list.skill_category
        for middle_category in skill_list.example_skills:
            middle_category_skills = skill_category_to_example_skills.get(middle_category)
            if not middle_category_skills:
                continue
            for skill in middle_category_skills:
                flat_skills.append(
                    {
                        "parent_skill": parent_skill,
                        "middle_skill": middle_category,
                        "skill": skill,
                    }
                )

    skills_df = pd.DataFrame(flat_skills)
    # Get rid of duplicates
    skills_df = (
        skills_df
        .groupby(["parent_skill", "middle_skill", "skill"])
        .size().reset_index(name="count")
        [["parent_skill", "middle_skill", "skill"]]
    )

    skills_df.to_csv(PATHS["skills_hierarchy_raw"], index=False)