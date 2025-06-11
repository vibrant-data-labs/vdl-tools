import os
from collections import Counter
from textwrap import dedent
import json
import pathlib as pl

import numpy as np
import pandas as pd

import vdl_tools.shared_tools.openai.openai_api_utils as oai_utils
import vdl_tools.tag2network.Network.BuildNetwork as bn
import vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping as tm
import vdl_tools.shared_tools.project_config as pc

paths = pc.get_paths()
# %%

model = "gpt-4.1-mini"

# %%

original_rich_prompt = dedent(
    f"""You are an assistant responsible for finding common keywords in a set of entity descriptions.
                For the given input, output up to 'params.n_tags' shared keywords or phrases. Avoid broad terms such as
                'education', 'sustainability', 'energy'.
                Output only a comma-separated list of keywords or phrases"""
).strip()

changed_prompt_0401 = dedent(
    f"""
Provide a concise title that encapsulates the commonalities shared by the listed organizations in a subject cluster. 
    Ensure this title is as brief as possible, avoiding unnecessary words or phrases. 

    # Steps
    1. Analyze the provided descriptions to identify their shared characteristics.
    2. Summarize these characteristics into a short phrase or up to two very brief phrases (no more than 10 words total).
    3. Avoid filler words like "empowering", "enhancing," "commitment to," "through philanthropic support," etc. Focus on the essential theme.

    # Output Format
    A single, concise phrase or up to two short, clear phrases. 
    Example outputs:
    - Instead of "Enhancing Educational Access and Community Development through Philanthropic Support" → "Educational Access and Community Development"
    - Instead of "Commitment to Holistic Education and Community Engagement" → "Holistic Education"
    - Instead of "Empowering Youth through Innovative STEM Education and Community Engagement" → "Innovative STEM Education for Youth"
    - Instead of "Empowering Communities Through Accessible Arts Education and Creative Expression." → "Accessible Arts Education and Creative Expression"
    - Instead of "Empowering Communities through Education, Advocacy, and Environmental Stewardship. → "Education, Advocacy, and Environmental Stewardship"
    # Notes
    - The final title should capture the core theme while being as succinct as possible.
    - Use neutral, direct language without extraneous words.
    """
).strip()

from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import (
    PromptResponseCacheSQL,
)
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from pydantic import BaseModel


class ExtractedKeyword(BaseModel):
    keyword: str
    importance: float
    reasoning_for_importance: str


class ExtractedKeywords(BaseModel):
    keywords: list[ExtractedKeyword]


class KeywordsPromptResponseCache(InstructorPRC):
    def __init__(
        self,
        session,
        prompt_str,
        prompt_name=None,
        model="gpt-4.1-mini",
        response_model=ExtractedKeywords,
    ):
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            response_model=response_model,
            model=model,
        )


class SummaryPromptResponseCache(PromptResponseCacheSQL):
    def __init__(
        self,
        session,
        prompt_str,
        prompt_name=None,
        model="gpt-4.1-mini",
    ):
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
        )
        self.model = model


def define_assistant_prompt(subject):
    assistant_prompt = dedent(
        f"""You are a helpful assistant who is an expert in
        {subject}. Your replies focus on key insights, benefits, opportunities, and specific methods relevant to the topic.
        Please provide accurate information and avoid speculative or unfounded statements."""
    )
    return assistant_prompt


def get_text_for_entities(n_tags, entities_texts):
    n_tags_expanded = n_tags + 5
    # use gpt to extract cluster name from top n entities
    preamble = dedent(
        f"""
        You are an expert text analyst. Your task is to extract the most relevant keywords and key phrases from a set
        of entity descriptions that share common themes. Focus on the main ideas, themes, and especially the types of
        approaches, solutions, and innovations that best represent the set.

        Please follow these guidelines:

        1. Output up to {n_tags_expanded} shared keywords or phrases.
        2. Prioritize phrases over single words when they better capture a core idea (e.g., use "artificial intelligence" 
        instead of "intelligence", "personalized education" instead of "education", "sustainable farming" instead of 
        "sustainability", "energy storage" instead of "energy").
        3. Keep it concise: when possible, avoid phrases longer than two words (e.g., prefer "immersive learning" over "immersive 
        learning experiences", "personalized learning" over "personalized learning experiences", "holistic approaches" over 
        "holistic educational approaches").
        4. Focus on terms that describe *what* the entity does, not *how* they do it. 
        5. Avoid overly generic terms like "sustainability" or "innovation" unless they are critical to the topic.
        5. Focus on terms that someone might use to search for this type of content online.

        Your goal is to provide keywords and phrases that accurately represent the collective content, highlighting the key 
        approaches, solutions, and innovations. 
"""
    ).strip()
    descrs = [
        f"Entity description {idx}: {txt}" for idx, txt in enumerate(entities_texts)
    ]
    # Use f-string and dedent (fixing the backslash issue)
    descrs_joined = "\n".join(descrs)
    prompt = dedent(f"""{preamble} {descrs_joined} Keywords:""").strip()
    return prompt


def get_text_for_summaries(entities_texts, subject):

    # use gpt to extract cluster name from top n entities
    preamble = dedent(
        f"""
    You are an expert text analyst. Your task is to analyze a stratified random sample of organizations’ 
    descriptions from a similarity cluster, with an emphasis on their {subject} focus. Present your findings in a clear, 
    detailed manner without providing an overall summary or conclusion. Use headings or bullet points for navigation. 
    
    Specifically:
    1. Highlight the approaches and methods the organizations use.
    2. Describe the solutions or strategies they implement.
    3. Detail any innovations, technologies, or novel ideas they contribute.
    4. Identify the populations or communities they serve.
    5. Identify any subclusters that may be present, with a short bullet point for each.
    
    Concentrate on the {subject} topic, avoiding vague or overly generic language unless it is essential. 
    Keep it below 300 words. Your goal is to present a concise, well-structured encapsulation of the common elements 
    among these organizations, omitting any concluding statement.
    """
    ).strip()
    descrs = [
        f"Entity description {idx}: {txt}" for idx, txt in enumerate(entities_texts)
    ]
    # Use f-string and dedent (fixing the backslash issue)
    descrs_joined = "\n".join(descrs)
    prompt = dedent(f"""{preamble} {descrs_joined} Output:""").strip()
    return prompt


def get_short_sentence(entities_texts, subject):

    preamble = dedent(
        f"""
    Provide a concise title that encapsulates the commonalities shared by the listed organizations in a {subject} cluster. 
    Ensure this title is as brief as possible, avoiding unnecessary words or phrases. 

    # Steps
    1. Analyze the provided descriptions to identify their shared characteristics.
    2. Summarize these characteristics into a short phrase or up to two very brief phrases. Keep the title under 12 words total.
    3. Avoid filler words like "innovative", "innovation", "empowering", "enhancing," "commitment to," "through philanthropic support," etc. Focus on the essential theme.
    4. Focus on terms that describe *what* the organizations do, not *how* they do it.
    5. Avoid overly generic terms like "sustainability" or "innovation" unless they are critical to the topic.

    # Output Format
    A single, concise phrase or up to two short, clear phrases. 
    Example outputs:
    - Instead of "Enhancing Educational Access and Community Development through Philanthropic Support" → "Educational Access and Community Development"
    - Instead of "Commitment to Holistic Education and Community Engagement" → "Holistic Education"
    - Instead of "Empowering Youth through Innovative STEM Education and Community Engagement" → "Innovative STEM Education for Youth"
    - Instead of "Empowering Communities Through Accessible Arts Education and Creative Expression." → "Accessible Arts Education and Creative Expression"
    - Instead of "Empowering Communities through Education, Advocacy, and Environmental Stewardship. → "Education, Advocacy, and Environmental Stewardship"
    # Notes
    - Prioritize the characteristics shared by the majority, even if some organizations have unique or outlier details
    - The final title should capture the core theme while being as succinct as possible.
    - Use neutral, direct language without embellishments.
    """
    ).strip()

    descrs = [
        f"Entity description {idx}: {txt}" for idx, txt in enumerate(entities_texts)
    ]
    # Use f-string and dedent (fixing the backslash issue)
    descrs_joined = "\n".join(descrs)
    prompt = dedent(f"""{preamble} {descrs_joined} Output:""").strip()
    return prompt


def get_one_sentence(entities_texts, subject):

    preamble = dedent(
        f"""
    Examine the provided organization descriptions, focusing on their shared purpose or 
    function in the {subject} sector. Summarize these common characteristics in one or two concise sentences, 
    using clear, specific language that avoids relying on overused words 
    (e.g.,'innovation', 'innovative', 'empowering' or 'community engagement'). Highlight how each organization addresses 
    the needs in {subject}, including the methods, audiences, or fundamental goals they share.

    # Steps

    1. Analyze the provided description of organizations to identify their shared characteristics. 
    2. Identify their type. Are they mostly Universities, Community Colleges, companies, research groups, non-profits, 
    or other types of organizations? 
    2. Summarize these, and other shared traits into a single, descriptive sentence.
    3. Ensure the sentence is specific, yet broad enough to facilitate comparison with titles from other groups.

    # Output guidelines

    Do not need to start with " Organizations that...", or  "Organizations in the education sector ..". 
    Prioritize the characteristics shared by the majority, even if some organizations have unique or outlier details. 
    It is useful to identify the type of organizations present in the cluster, such as "Schools", "Non-profits", 
    "Research groups", "Companies", etc.

    # Examples

    ["Entity description 123: This nonprofit focuses on wildlife conservation and habitat restoration.", 
    "Entity description 456: An international organization dedicated to protecting endangered species."]

    Output: "Non-profits focused on conservation of wildlife and safeguarding natural habitats."

    # Notes

    - It should capture both the essence and distinctive shared feature of the organizations.
    - Strive for clarity and comparability to make sense across different clusters.
    """
    ).strip()
    descrs = [
        f"Entity description {idx}: {txt}" for idx, txt in enumerate(entities_texts)
    ]
    # Use f-string and dedent (fixing the backslash issue)
    descrs_joined = "\n".join(descrs)
    prompt = dedent(f"""{preamble} {descrs_joined} Output:""").strip()
    return prompt


def review_one_sentence(clusters_df, clusattr):
    preamble = dedent(
        f"""
    **Task:**  
    For each row in my data, create a new short description . You can use the information in the long description to 
    improve the output by adding any missing details that would further help differentiate the clusters.

    **Requirements:**  
    1. **Brevity:** Each short sentence should be concise (ideally under 12 words).  
    2. **Distinctiveness:** Each cluster’s short sentence must highlight its unique focus so that no two clusters end up with nearly identical titles.  
    3. **No Filler Words:** Avoid terms like “empowering,” “enhancing,” “commitment to,” etc. Focus on core themes (e.g., “STEM Education for Youth,” “Wildlife Conservation and Research”).  
    4. **Reflect Core Mission:** Capture the essential mission or unique aspect from the original description without adding new details or unrelated content.  
    5. **Fix‑or‑Differentiate:** After drafting all sentences, scan for any pair that shares the same two key nouns (e.g., “indigenous+rights” or “climate+finance”).
    If they clash, do one of the following:
    -Remove a non‑essential noun or adjective from one title to break the overlap, or
    -Add a distinctive qualifier drawn from the long description (e.g., a region, beneficiary, or method).
        • Re‑check until every title is uniquely worded.
        • Before finalizing, perform a check for near‑synonyms among the key nouns. If two titles use near synonymous key nouns, treat them as clashing and adjust as above.

    **Example of Before and After:**  
    - Original long sentence: “A nonprofit dedicated to empowering youth by enhancing educational access and providing mentorship.”  
    - revised short sentence: “Youth Mentorship and Educational Access."
    
    **Output:**
    - “Only Reply in JSON with the cluster identifier and the revised short sentence 
    ```json
    {{
        "Cluster_0": "Educational Access and Community Development",
        "Cluster_3": "Customized Learning and Local Engagement"
    }}
    ```
    """
    ).strip()
    # add clusters descriptsion as a dictionary with the first col as keys and the other 2 cols as values
    clusters_df = clusters_df.set_index(clusattr)
    clusters_dict = clusters_df.to_dict(orient="index")
    prompt = dedent(f"""{preamble} \n {clusters_dict} \n Output:""").strip()
    return prompt


def add_text_below_token_limit(cdf, textcol, model, max_tokens=120000):
    # Initialize the list to hold the selected texts
    cluster_texts = []
    total_tokens = 0
    idx = 0

    # Loop over the sorted entities and keep adding until token limit is reached
    while idx < len(cdf):
        # Get the current entity's text
        current_text = cdf.iloc[idx][textcol]

        # Calculate the number of tokens for the current text
        current_tokens = oai_utils.get_num_tokens([current_text], model)

        # Check if adding the current text exceeds the token limit
        if total_tokens + current_tokens <= max_tokens:
            # Add the text to the list if the token count is within the limit
            cluster_texts.append(current_text)
            total_tokens += current_tokens
        else:
            # Stop if the token limit is reached
            break

        # Move to the next entity
        idx += 1

    return cluster_texts


# Optionally filter out repeated keywords
def filter_keywords(cluster_kwd_dict, n_tags, filename=None):
    keyword_counter = Counter()
    for kw_list in cluster_kwd_dict.values():
        keyword_counter.update(kw_list)
    remove_keywords = {kw for kw, cnt in keyword_counter.items() if cnt > 2}
    df_keywords = pd.DataFrame(keyword_counter.items(), columns=["keyword", "count"])
    if filename:
        if not isinstance(filename, pl.Path):
            filename = pl.Path(filename)
        filename.mkdir(parents=True, exist_ok=True)
        df_keywords.to_csv(paths["embedding_clus"] / "keywords.csv", index=False)
    filtered_map = {}
    for cluster_id, kw_list in cluster_kwd_dict.items():
        filtered = [k for k in kw_list if k not in remove_keywords]
        filtered_map[cluster_id] = tuple(filtered[:n_tags])    # make a tuple so it is immutable and therefore hashable
    return filtered_map


def parse_keyword_response(response_obj):
    # Example GPT JSON structure:
    # {
    #   "keywords": [
    #       {"keyword": "XYZ", "importance": 10}, ...
    #   ]
    # }
    resp_json = json.loads(
        response_obj["response_full"]["choices"][0]["message"]["content"]
    )
    sorted_kwds = sorted(
        resp_json["keywords"], key=lambda x: x["importance"], reverse=True
    )
    return [kwd["keyword"] for kwd in sorted_kwds]




def get_cluster_sentences_from_text(
    nodesdf,
    textcol,
    clusattr='Cluster',
    subject=None,
    model="gpt-4.1-mini",
):

    clus_centrality = "ClusterCentrality"
    assistant_prompt = define_assistant_prompt(subject)
    nodesdf["wtd_cc"] = nodesdf[clus_centrality]

    # ids_text_prompts_all = []
    ids_text_sums_short = []
    ids_text_sums_all = []
    cluster_texts_all_list = {}

    for clus, cdf in nodesdf.groupby(clusattr):
        sorted_cdf = cdf.sort_values("wtd_cc", ascending=False)

        cluster_texts_all = add_text_below_token_limit(sorted_cdf, textcol, model)
        # a- short sentence
        short_sentences_all = get_short_sentence(cluster_texts_all, subject)
        ids_text_sums_short.append((clus, short_sentences_all))
        # b - longer one sentence
        one_sentences_all = get_one_sentence(cluster_texts_all, subject)
        ids_text_sums_all.append((clus, one_sentences_all))

        cluster_texts_all_list[clus] = cluster_texts_all

    # Get sentences
    prompt_name = "sentences_for_cluster"
    if subject:
        prompt_name += f"_{subject.replace(' ', '_').lower()}"
    with get_session() as session:
        sums_cache = SummaryPromptResponseCache(
            prompt_str=assistant_prompt,
            model=model,
            session=session,
            prompt_name=prompt_name,
        )
        cluster_id_to_shortsentence_all = sums_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_sums_short,
            model=model,
            use_cached_result=False,
        )
        cluster_id_to_onesentence_all = sums_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_sums_all,
            model=model,
            use_cached_result=False,
        )

    cluster_id_to_shorts_all = {
        clus: resp["response_text"]
        for clus, resp in cluster_id_to_shortsentence_all.items()
    }
    cluster_id_to_sums_all = {
        clus: resp["response_text"]
        for clus, resp in cluster_id_to_onesentence_all.items()
    }

    # Assign results back to nodesdf in separate columns
    nodesdf["clus_sentence_short"] = nodesdf[clusattr].map(cluster_id_to_shorts_all)
    nodesdf["clus_sentence_long"] = nodesdf[clusattr].map(cluster_id_to_sums_all)

    # # Capture how many entities went into the all-entities approach
    # nodesdf["clus_sentence_count_all"] = nodesdf["Cluster"].map(
    #     {clus: len(txt_arr) for clus, txt_arr in cluster_texts_all_list.items()}
    # )
    return nodesdf


def improve_one_sentences(nodesdf, clusattr='Cluster', subject="education", model="o3-mini"):

    df = nodesdf[
        [clusattr, "clus_sentence_short", "clus_sentence_long"]
    ].drop_duplicates()
    # print(df.shape)
    assistant_prompt = define_assistant_prompt(subject)
    # get the prompt for the cluster
    review_prompt = review_one_sentence(df, clusattr)

    response = oai_utils.CLIENT.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": [{"type": "text", "text": assistant_prompt + "\n"}],
            },
            {"role": "user", "content": [{"type": "text", "text": review_prompt}]},
        ],
        response_format={"type": "json_object"},
        temperature=1,
        max_completion_tokens=10000,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        store=False,
    )

    # get the response json
    json_resp = json.loads(response.choices[0].message.content)
    nodesdf["clus_sentence_reviewed"] = nodesdf[clusattr].map(json_resp)
    return nodesdf
    # nodesdf["clus_sentence_reviewed"] = nodesdf["Cluster"].map(cluster_id_to_review)
    # return nodesdf


def get_cluster_names_from_text(
    nodesdf,
    textcol,
    clusattr,
    clusname,               # column name for the cluster names
    n_tags,
    subject=None,
    n_entities=20,          # int → use top‑k; None → token‑limit fallback
    weight_param=None,
    model="gpt-4.1-mini",
):
    """
    Adds two columns to *nodesdf*:
        • clusname : list[str]   (up to n_tags keywords)
        • clus_sum : str         (one‑sentence summary)
    ------------
    • Optional *n_entities* (top‑k) with fallback to add_text_below_token_limit.

    """

    clus_centrality = "ClusterCentrality"
    assistant_prompt = define_assistant_prompt(subject)

    if weight_param is not None:
        nodesdf["wtd_cc"] = nodesdf[clus_centrality] * nodesdf[weight_param]
    else:
        nodesdf["wtd_cc"] = nodesdf[clus_centrality]

    ids_text_prompts, ids_text_sums = [], []

    for clus, cdf in nodesdf.groupby(clusattr):
        cdf_sorted = cdf.sort_values("wtd_cc", ascending=False)

        if n_entities is None:
            cluster_texts = add_text_below_token_limit(cdf_sorted, textcol, model)
        else:
            cluster_texts = cdf_sorted.iloc[:n_entities][textcol].values

        ids_text_prompts.append((clus, get_text_for_entities(n_tags, cluster_texts)))
        ids_text_sums.append((clus, get_text_for_summaries(cluster_texts, subject)))

    kw_prompt_name = "keywords_for_cluster"
    if subject:
        kw_prompt_name += f"_{subject.replace(' ', '_').lower()}"

    with get_session() as session:
        kw_cache = KeywordsPromptResponseCache(
            prompt_str=assistant_prompt,
            model=model,
            session=session,
            prompt_name=kw_prompt_name,
        )
        raw_kw = kw_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_prompts,
            model=model,
            use_cached_result=False,
        )

    cluster_id_to_keywords = {
        clus: parse_keyword_response(resp)  # → list[str]
        for clus, resp in raw_kw.items()
    }
    cluster_to_cleaned_keywords = filter_keywords(cluster_id_to_keywords, n_tags)

    sum_prompt_name = "keyword_cluster_summaries"
    if subject:
        sum_prompt_name += f"_{subject.replace(' ', '_').lower()}"

    with get_session() as session:
        sum_cache = SummaryPromptResponseCache(
            prompt_str=assistant_prompt,
            model=model,
            session=session,
            prompt_name=sum_prompt_name,
        )
        raw_sum = sum_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_sums,
            model=model,
            use_cached_result=False,
        )

    cluster_id_to_summaries = {
        clus: resp["response_text"] for clus, resp in raw_sum.items()
    }

    nodesdf[clusname] = nodesdf[clusattr].map(cluster_to_cleaned_keywords)
    nodesdf["clus_summary"] = nodesdf[clusattr].map(cluster_id_to_summaries)

    return nodesdf


def build_embedding_network(
    df, params: bn.BuildEmbeddingNWParams, debug=False, subject=None, n_entities=20
):
    emb_file = pl.Path("embeddings.npy")

    if debug and emb_file.exists():
        emb_matrix = np.load(emb_file)
    else:
        emb_matrix = tm.get_or_compute_embeddings(
            org_df=df, id_col=params.uid, text_col=params.textcol
        )
        if debug:
            np.save(emb_file, emb_matrix)

    sims = emb_matrix @ emb_matrix.T
    np.fill_diagonal(sims, 0)
    df.reset_index(drop=True, inplace=True)
    nodesdf, edgesdf, clusters = bn.buildSimilarityNetwork(df, sims.copy(), params)
    # compute and assign cluster names
    if params.clusName is not None:
        for idx, clattr in enumerate(clusters):
            clName = params.clusName if idx == 0 else f"{params.clusName}_L{idx + 1}"
            nodesdf = get_cluster_names_from_text(
                nodesdf,
                textcol=params.textcol,
                clusattr=clattr,
                clusname=clName,
                n_tags=params.n_tags,
                subject=subject,
                n_entities=n_entities,
            )
    return nodesdf, edgesdf, sims
