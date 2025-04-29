import os
from textwrap import dedent
import pandas as pd
from dotenv import load_dotenv
from vdl_tools.shared_tools.all_source_organization_summarization import BASE_PROMPT
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    PromptResponseCacheSQL,
)
import vdl_tools.shared_tools.project_config as pc

import openai

paths = pc.get_paths()
# %%
openai.api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI()
model = "gpt-4.1-mini"

few_shot_df = pd.read_excel(paths["labeled_data"], engine="openpyxl")
few_shot_df = few_shot_df[~(few_shot_df["examples"].isna())][
    ["examples", "text_for_one_earth"]
].copy()
examples_txt = ""
for _, row in few_shot_df.iterrows():
    examples_txt += f"{row['text_for_one_earth']} \n Output:\n {row['examples']}\n\n"
OLD_PROMPT = dedent(
    f"""Evaluate each organization and assign it to one of the following categories based on its relationship to K–12 
    and community-based education:\n
    Horizon 1 (H1) – Organizations that improve, optimize, or support existing education systems or informal enrichment 
    models without altering the underlying structure. These may include schools, museums, zoos, or programs that deliver 
    traditional or supplementary learning experiences. \n
    
    Horizon 2 (H2) – Organizations that experiment with or scale emerging models that challenge traditional assumptions 
    but still connect to the current system. These might involve blended learning, learner-centered models, 
    interdisciplinary programs, or community-driven education initiatives. \n
    
    Horizon 3 (H3) – Organizations that aim to radically reimagine education—shifting power to learners, rethinking what 
    counts as learning, and creating entirely new systems or paradigms. These may decouple learning from schools, 
    disrupt age-based grouping, or deeply integrate purpose, agency, and equity. \n
    
    Not Relevant– If the organization does not focus on K–12 or community-based education—or is unrelated to education 
    altogether (e.g., not part of the philanthropic, public, or private investments that support innovation within 
    and around education)—classify it as Not Relevant. \n
    Focus only on Pre-K through 12th grade and community education; disregard colleges, universities, or adult learning unless explicitly 
    community college–based. Pay attention to terms such as"college preparatory"—these are still within scope. 
    Include organizations that act as ecosystem enablers or indirect education innovators, even if they do not directly 
    deliver instruction. Their work may be essential to enabling innovation. These may include: \n
    -Organizations working on policy, infrastructure, or systemic support (e.g., data systems, funding models, learning ecosystems).
    -Funders or coalitions that explicitly focus on K–12 or community education innovation. \n
    
    If classification is unclear or doubtful, assign the organization to the lowest plausible Horizon 
    (e.g., H1 if it could be H1 or Not Relevant). Favor inclusion over exclusion. \n
    
    Output a string with the category assigned (H1, H2, H3, or Not Relevant) and nothing else.\n
    Here are some examples to help you understand the categories:\n
    {examples_txt}
    """
).strip()

BASE_PROMPT = dedent(
    f"""
    Classify each organization as Relevant or Not Relevant to the education ecosystem.
    Guidelines:

    1. Focus only on relevance to the education ecosystem—whether the organization contributes to or enables educational 
    activity or innovation (H1, H2, or H3). Do not classify by horizon—just determine if it is Relevant or Not Relevant.\n
    2. Consider the following as Relevant:\n
    ### Museums, public libraries, discovery centers, science centers
    ### College prep enablers, charter schools
    ### Youth development organizations such as Big Brothers Big Sisters, Boys & Girls Clubs
    ### Any PreK–12 or community education organization
    \n
    3. Focus on organizations serving PreK through 12th grade, including teen-focused community or informal learning efforts.\n
    4. Disregard colleges, universities, or adult education providers unless they also offer teen or youth learning programs.\n
    5. Exclude organizations focused solely on corporate training, adult workforce upskilling, or professional certification, 
    unless there is a clear youth-facing or K–12 component.\n
    6. Include ecosystem enablers or indirect education innovators, even if they do not directly deliver instruction. This includes:\n
    ### Policy, infrastructure, or systemic support organizations (e.g., data systems, funding models, learning ecosystems)
    ### Funders, coalitions, or technical assistance providers explicitly focused on K–12 or community education innovation
\n
    7. If an organization’s core activity is entertainment (e.g., gaming, media, events), only consider it Relevant
    if it has a clear educational mission or direct engagement with child or youth learning.\n
    8. Include EdTech platforms or tools that support learning, instruction, or assessment in ways applicable to K–12 or 
    teen learners—even if they are also used in higher education. Tools focused on writing, inquiry, engagement, 
    or classroom support are typically relevant.\n
    9. Include college and career readiness platforms and tools that support K–12 students in planning education and 
    career pathways. These tools are relevant even if they also serve employers or higher education institutions.\n
    8. If the classification is unclear or doubtful, default to Relevant.\n

    Output Format:\n
    Output a string with the category assigned: "Relevant" or "Not Relevant"—nothing else.\n
"""
).strip()


def get_bulk_relevancy(
    ids_text_lists: list[tuple],
    use_cached_results: bool = True,
    prompt_string: str = BASE_PROMPT,
):
    ids_texts = []
    for id_, text_list in ids_text_lists:
        filtered_text_list = [x for x in text_list if x]
        text = "\n\n".join(
            [
                f"Description {i + 1}\n{text}"
                for i, text in enumerate(filtered_text_list)
            ]
        )
        ids_texts.append((id_, text))

    with get_session() as session:
        prompt_response = PromptResponseCacheSQL(
            session=session,
            prompt_str=prompt_string,
            prompt_name="education_relevance",
        )

        ids_to_response = prompt_response.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_results,
        )
    return {k: v["response_text"] for k, v in ids_to_response.items()}
