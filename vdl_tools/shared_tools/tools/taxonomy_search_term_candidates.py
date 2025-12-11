import json

import pandas as pd
from pydantic import BaseModel, Field

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.database_cache.database_utils import get_session


class SearchTerm(BaseModel):
    term: str = Field(description="The search term")
    chain_of_thought: str = Field(description="The chain of thought process for the search term")
    # confidence: float = Field(description="The confidence that the new term is related to the input search term")

class SearchTermExpansionResponse(BaseModel):
    expanded_search_terms: list[SearchTerm]



PROMPT_TEMPLATE = """
You are a helpful assistant that takes a topic definition and returns a list of search terms that are candidates for finding relevant organizations in Crunchbase or Candid.

Please bias towards terms that are more general and less specific as I'd like to have a broad range of results.

The broader topic should be related to: **%s**

I will provide you with:
1. A solution name
2. The parent category it belongs to
3. An expanded definition/description

Based on this information, generate 30-50 highly relevant Crunchbase search terms organized into logical categories (e.g., "Direct/Primary Terms", "Technology & Equipment", "Services & Solutions", etc.).

**Requirements:**
- Each search term should be on its own line (no OR operators)
- No AND operators for combining terms
- Terms should be specific and relevant to finding companies that provide this type of solution
- Organize terms into 4-6 meaningful categories
- Include variations of key terms (e.g., both "maker space" and "makerspace")
- Focus on what companies would call themselves or their products
- Include related technology, services, and market terms
- Do not include terms that are not related to the solution or subpillar
- Terms should be in English
"""

def generate_message(
    solution_name,
    parent_category,
    definition,
):
    base_template = f"""
    Here is the solution to analyze:

    **Solution:** {solution_name}
    **Parent Category:** {parent_category}

    **Expanded Definition:**
    {definition}
    """
    return base_template

class TaxonomySearchTermCandidates(InstructorPRC):
    def __init__(
        self,
        session,
        topic,
        prompt_str=None,
        prompt_id=None,
        response_model=SearchTermExpansionResponse,
        prompt_name="taxonomy_search_term_candidates",
        model="gpt-4.1",
    ):
        if not prompt_str:
            prompt_str = PROMPT_TEMPLATE % topic
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            response_model=response_model,
            prompt_name=prompt_name,
            model=model,
            prompt_id=prompt_id,
            filter_by_model=True,
        )
        self.topic = topic

def single_generates_taxonomy_search_term_candidates(
    topic,
    solution_name,
    parent_category,
    definition,
):
    with get_session() as session:
        taxonomy_search_term_candidates = TaxonomySearchTermCandidates(
            session=session,
            topic=topic,
        )
        message = generate_message(solution_name, parent_category, definition)
        return taxonomy_search_term_candidates.get_cache_or_run(
            given_id=f"{solution_name} - {parent_category}",
            text=message,
            use_cached_result=True,
        )

def batch_generates_taxonomy_search_term_candidates(
    topic,
    solutions_list,
    batch_size=50,
):
    with get_session() as session:
        taxonomy_search_term_candidates = TaxonomySearchTermCandidates(
            session=session,
            topic=topic,
        )
        given_ids_texts = [
            (f"{solution_name} $$$ {parent_category}",
            generate_message(solution_name, parent_category, definition))
            for solution_name, parent_category, definition in solutions_list
        ]
        ids_to_responses = taxonomy_search_term_candidates.bulk_get_cache_or_run(
            given_ids_texts=given_ids_texts,
            use_cached_result=True,
            n_per_commit=batch_size,
        )
    
    parsed_responses = []
    for given_id, response in ids_to_responses.items():
        solution_name, parent_category = given_id.split(" $$$ ")
        terms_list = json.loads(response["response_text"])["expanded_search_terms"]
        for term in terms_list:
            parsed_responses.append({
                "solution_name": solution_name,
                "parent_category": parent_category,
                "term": term["term"],
                "chain_of_thought": term["chain_of_thought"],
            })
    parsed_responses = pd.DataFrame(parsed_responses)

    return parsed_responses
