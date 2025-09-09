import json
from textwrap import dedent

import hdbscan
import numpy as np
import openai
import pandas as pd
from pydantic import BaseModel
from sklearn.metrics.pairwise import cosine_distances
from sklearn.preprocessing import StandardScaler

import vdl_tools.shared_tools.openai.openai_api_utils as oai_utils
import vdl_tools.shared_tools.project_config as pc
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.embedding_network.embedding_network import define_assistant_prompt
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger

paths = pc.get_paths()

class SubjectKeywords(BaseModel):
    keywords: list


class SubjectKeywordsResponseCache(InstructorPRC):
    def __init__(
        self,
        session,
        prompt_str,
        prompt_name=None,
        model="gpt-4.1-mini",
        response_model=SubjectKeywords,
    ):
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            response_model=response_model,
            model=model,
        )

def parse_keyword_list(response_obj):
    # Example GPT JSON structure:
    # {
    #   "keywords": ['keyword1', 'keyword2', ...]
    #
    # }
    resp_json = json.loads(
        response_obj["response_full"]["choices"][0]["message"]["content"]
    )
    kwds_set = set(resp_json.get("keywords", [])
    )
    return list(kwds_set)

def subject_kwds_prompt(max_keywords, subject, texts):

    # use gpt to extract subject kwords from texts
    preamble = dedent(
        f"""
        From these texts, give me all the literal phrases or words (1‑3 words) that are relevant to {subject}.
        Do NOT invent any new phrases or concepts. Only use what appears in the text.

**Rules:**  
- Each keyword should be **1–3 words long**  
- Limit to {max_keywords} keywords. 
- Only keep terms that someone could use to search for projects or solutions in {subject}
- Return a list of keywords

Texts:
        """
    ).strip()
    joined_text = "\n".join(texts)
    prompt = dedent(f"""{preamble} {joined_text} Keywords:""").strip()
    return prompt



def get_kword_list_from_text(
        subject=None,
        max_kwords=50,
        model="gpt-4.1-mini",
        sample_texts=None

):
    """
    Recursively extract relevant keywords from sample texts grouped by cluster.
    Returns:
        A deduplicated flat list of keywords.
    """
    

    assistant_prompt = define_assistant_prompt(subject)

    ids_text_prompts = []
    collected_keywords = []


    for clus, samples_or_dict in sample_texts.items():
        # Recursive case: if value is a dict, dive deeper
        if isinstance(samples_or_dict, dict):
            collected_keywords.extend(
                get_kword_list_from_text(
                    subject=subject,
                    max_kwords=max_kwords,
                    model="gpt-4.1-mini",
                    sample_texts=samples_or_dict
                )
            )
            continue
        # Base case: if value is a list, process the texts
        texts = samples_or_dict
        #prompt = subject_kwds_prompt(max_kwords, subject, texts)


        # Use the sampled texts from each cluster
        ids_text_prompts.append((clus, subject_kwds_prompt(max_kwords, subject, texts)))

    kw_prompt_name = "subject_keywords_for"
    if subject:
        kw_prompt_name += f"_{subject.replace(' ', '_').lower()}"
    logger.info(f"Generating {kw_prompt_name}")
    with get_session() as session:
        kw_cache = SubjectKeywordsResponseCache(
            prompt_str=assistant_prompt,
            model=model,
            session=session,
            prompt_name=kw_prompt_name,
        )
        raw_kw = kw_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_prompts,
            model=model,
            use_cached_result=True,
        )

    cluster_id_to_keywords = {
        clus: parse_keyword_list(resp)  # → list[str]
        for clus, resp in raw_kw.items()
    }
    for kw_list in cluster_id_to_keywords.values():
        # make it lower case
        kw_list = [kw.lower() for kw in kw_list if isinstance(kw, str)]
        collected_keywords.extend(kw_list)
    # deduplicate the collected keywords
    collected_keywords = sorted(list(set(collected_keywords)))
    return collected_keywords


