import os
from collections import Counter
from textwrap import dedent
import json
import pathlib as pl

import numpy as np
import pandas as pd
import re

import vdl_tools.shared_tools.openai.openai_api_utils as oai_utils
import vdl_tools.tag2network.Network.BuildNetwork as bn
import vdl_tools.shared_tools.taxonomy_mapping.taxonomy_mapping as tm
import vdl_tools.shared_tools.project_config as pc
from vdl_tools.shared_tools.tools.logger import logger

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


def format_kwds_prompt(n_tags, entities_texts):
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


def format_summaries_prompt(entities_texts, subject):

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


def format_short_sentence_prompt(entities_texts, subject):

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


def format_one_sentence_prompt(entities_texts, subject):

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


def format_prompt_review_titles(clusters_df, clusattr):
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

def sample_cluster_texts_by_percentile(
    nodesdf,
    textcol,
    clusattr="Cluster",
    max_samples=50,
    centrality_col="ClusterCentrality",
):
    """
    If cluster size <= max_samples: return ALL texts, sorted by centrality (desc).
    - Else: stratify by centrality percentiles, sample proportionally, and then
      refill if necessary to guarantee exactly max_samples.
    """
    cluster_samples = {}

    # Iterate over clusters
    for clus, cdf in nodesdf.groupby(clusattr):
        # Work on a COPY so the original df is untouched
        cdf = cdf.copy()
        n_total = len(cdf)
        n_samples = min(n_total, max_samples)

        # Small clusters: take all, sorted by centrality
        if n_total <= max_samples:
            out = (
                cdf.sort_values(centrality_col, ascending=False)[textcol]
                .tolist()
            )
            cluster_samples[clus] = out
            continue

        # --- Stratified Sampling for Large Clusters ---

        # Bin by centrality percentiles (5 bins). fewer bins may be returned.
        cdf.loc[:, "cent_bin"] = pd.qcut(
                cdf[centrality_col], 5, labels=False, duplicates="drop"
            )

        # Allocate sample sizes proportional to bin populations
        bin_counts = cdf["cent_bin"].value_counts(normalize=True).sort_index()
        raw_alloc = bin_counts * n_samples
        n_per_bin = np.floor(raw_alloc).astype(int)

        # Distribute the remainder from rounding
        remainder = n_samples - n_per_bin.sum()
        if remainder > 0:
            frac_part = raw_alloc - np.floor(raw_alloc)
            # Add remainder to bins with the largest fractional parts
            for idx in frac_part.nlargest(remainder).index:
                n_per_bin[idx] += 1

        # Draw initial samples from each bin
        sampled_indices = []
        for bin_idx, count in n_per_bin.items():
            if count <= 0:
                continue
            bin_cdf = cdf[cdf["cent_bin"] == bin_idx]

            # 1. GUARD: Never sample more than what's available in the bin
            n_to_sample = min(count, len(bin_cdf))

            sampled_indices.extend(
                    bin_cdf.sample(n=n_to_sample, random_state=42).index.tolist()
                )

        # 2. REFILL: Top up if we under-sampled due to small bins
        n_refill = n_samples - len(sampled_indices)
        if n_refill > 0:
            # Get indices of items not yet sampled
            remaining_indices = cdf.index.difference(sampled_indices)

            # Randomly sample from the remainder to meet the target
            refill_indices = np.random.choice(remaining_indices, n_refill, replace=False)
            sampled_indices.extend(refill_indices)

        # Retrieve the final list of texts using the sampled indices
        cluster_samples[clus] = cdf.loc[sampled_indices, textcol].tolist()

    return cluster_samples



def simple_text_sampling(nodesdf, clusattr, textcol, centrality_col, model):
    """
    Samples texts for each cluster, sorted by centrality, up to the model's token limit.
    Returns a dict: {cluster_id: [sampled_texts]}
    """
    sample_texts = {}
    clusters = nodesdf[clusattr].unique()
    for clus in clusters:
        cdf = nodesdf[nodesdf[clusattr] == clus].sort_values(centrality_col, ascending=False)
        sample_texts[clus] = add_text_below_token_limit(cdf, textcol, model)
    return sample_texts


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



# def _suffix_from_col_name(colname: str) -> str:
#     m = re.search(r'(_L\d+)$', str(colname))
#     return m.group(1) if m else ""



def improve_one_sentences(nodesdf,
                          clusattr='Cluster',
                          clusname=None,  # Use this for short sentence column name
                          subject="education",
                          model="o3-mini",
                          short_col=None,
                          long_col=None):
    """
    Improves and replaces the short one-sentence summaries for each cluster in the DataFrame.

    Parameters:
        nodesdf (pd.DataFrame): The input DataFrame containing cluster and sentence columns.
        clusattr (str): The column name for cluster identifiers.
        clusname (str, optional): Base name for the short sentence column. If provided, infers column names.
        subject (str): The subject area for prompt context.
        model (str): The model name to use for generating improved sentences.
        short_col (str, optional): The column name for the short sentence. If not provided, inferred from clusname or defaults.
        long_col (str, optional): The column name for the long sentence. If not provided, inferred from clusname or defaults.

    Returns:
        pd.DataFrame: A copy of the input DataFrame with the improved short sentence column replaced.

    Raises:
        KeyError: If any of the required columns are missing from the DataFrame.
    """
    out = nodesdf.copy()
    for col in (clusattr, short_col, long_col):
        if col not in out.columns:
            raise KeyError(f"Expected column '{col}' not found.")

        # Minimal table for prompt
    df_min = out[[clusattr, short_col, long_col]].drop_duplicates()
    
    assistant_prompt = define_assistant_prompt(subject)
    # get the prompt for the cluster
    review_prompt = format_prompt_review_titles(df_min, clusattr)

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
    out[short_col] = out[clusattr].map(json_resp)
    return out



def get_cluster_sentences_from_text(
    nodesdf,
    textcol,
    clusattr='Cluster',
    clusname=None,             # Use this for short sentence column name
    subject=None,
    model="gpt-4.1-mini",
    n_tags = None,
    n_entities=None,
    sample_texts=None,
    improve = False,
    improve_model="o3-mini",# Model for improving one-sentence summaries
    centrality_col = "ClusterCentrality"
):
    
    assistant_prompt = define_assistant_prompt(subject)

    ids_text_sums_short = []
    ids_text_sums_all = []

    # Use provided sample_texts if given, else fallback to internal sampling
    if sample_texts is not None:
        clusters = sample_texts.keys()
    else:
        sample_texts = simple_text_sampling(nodesdf, clusattr, textcol, centrality_col, model)
        clusters = sample_texts.keys()

    for clus in clusters:
        cluster_texts_all = sample_texts[clus]

        # short sentence prompt
        short_sentences_all = format_short_sentence_prompt(cluster_texts_all, subject)
        ids_text_sums_short.append((clus, short_sentences_all))

        # longer one sentence prompt
        one_sentences_all = format_one_sentence_prompt(cluster_texts_all, subject)
        ids_text_sums_all.append((clus, one_sentences_all))

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
            use_cached_result=True,
        )
        cluster_id_to_onesentence_all = sums_cache.bulk_get_cache_or_run(
            given_ids_texts=ids_text_sums_all,
            model=model,
            use_cached_result=True,
        )

    cluster_id_to_shorts_all = {
        clus: resp["response_text"]
        for clus, resp in cluster_id_to_shortsentence_all.items()
    }
    cluster_id_to_sums_all = {
        clus: resp["response_text"]
        for clus, resp in cluster_id_to_onesentence_all.items()
    }

    # Use clusname for short sentence column if provided; else fallback to 'clus_sentence_short'
    short_col = clusname if clusname else "clus_sentence_short"
    long_col = f"{clusname}_long" if clusname else "clus_sentence_long"

    nodesdf[short_col] = nodesdf[clusattr].map(cluster_id_to_shorts_all)
    nodesdf[long_col] = nodesdf[clusattr].map(cluster_id_to_sums_all)

    if improve:
        logger.info(f"Improving one-sentence summaries for {clusattr}...")
        nodesdf = improve_one_sentences(
            nodesdf,
            clusattr=clusattr,
            clusname=clusname,  # no special base needed; uses the names we just wrote
            subject=subject,
            model=improve_model,
            short_col=short_col,
            long_col=long_col

        )
    return nodesdf

def get_cluster_kwdnames_from_text(
        nodesdf,
        textcol,
        clusattr,
        clusname,  # column name for the cluster names
        n_tags,
        subject=None,
        n_entities=20,  # int → use top‑k; None → token‑limit fallback
        model="gpt-4.1-mini",
        sample_texts=None,
        improve=False,
        improve_model='o3-mini'

):
    """
    Adds one column to *nodesdf*:
        • clusname : list[str]   (up to n_tags keywords)

    If sample_texts are provided, it uses those; otherwise, it calculates new samples.
    """
    # Define centrality column directly
    centrality_col = "ClusterCentrality"

    assistant_prompt = define_assistant_prompt(subject)

    ids_text_prompts = []

    # Use pre-calculated sample texts if provided, otherwise sample new ones
    if sample_texts is not None:
        # If sample_texts are provided, use them directly
        cluster_samples = {clus: sample_texts[clus] for clus in nodesdf[clusattr].unique()}
    else:
        # Get the cluster samples using stratified sampling based on centrality
        cluster_samples = sample_cluster_texts_by_percentile(
            nodesdf, textcol, clusattr, max_samples=n_entities
        )

    # Iterate over the clusters and prepare the cluster texts
    for clus, sampled_texts in cluster_samples.items():
        # Use the sampled texts for each cluster
        ids_text_prompts.append((clus, format_kwds_prompt(n_tags, sampled_texts)))

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
    nodesdf[clusname] = nodesdf[clusattr].map(cluster_to_cleaned_keywords)
    return nodesdf


def get_cluster_summaries_from_text(
    nodesdf,
    textcol,
    clusattr,
    summary_col="cluster_summary",            # column name for the summaries
    subject=None,
    n_entities=20,
    model="gpt-4.1-mini",
):
    """
    Adds one column to *nodesdf*:
        • summary_col : str   (a longer, structured cluster summary)
    """
    centrality_col = "ClusterCentrality"
    assistant_prompt = define_assistant_prompt(subject)
    ids_text_sums = []

    for clus, cdf in nodesdf.groupby(clusattr):
        cdf_sorted = cdf.sort_values(centrality_col, ascending=False)
        if n_entities is None:
            cluster_texts = add_text_below_token_limit(cdf_sorted, textcol, model)
        else:
            cluster_texts = cdf_sorted.iloc[:n_entities][textcol].values
        ids_text_sums.append((clus, format_summaries_prompt(cluster_texts, subject)))
    "TODO: change to use already sampled texts if available"
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
            use_cached_result=True,
        )

    cluster_id_to_summaries = {
        clus: resp["response_text"] for clus, resp in raw_sum.items()
    }
    nodesdf[summary_col] = nodesdf[clusattr].map(cluster_id_to_summaries)
    return nodesdf

NAMING_FUNCTIONS = {
    "keywords": get_cluster_kwdnames_from_text,
    "title_sentence": get_cluster_sentences_from_text,
}

def build_embedding_network(
    df,
    params: bn.BuildEmbeddingNWParams,
    debug=False,
    subject=None,
    model="gpt-4.1-mini",
    clusattr='Cluster',
    n_entities=50,
    naming_strategy="keywords",
    improve = False,
    improve_model="o3-mini"  # Model for improving one-sentence summaries
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

    naming_func = NAMING_FUNCTIONS.get(naming_strategy, get_cluster_kwdnames_from_text)
    logger.info(f"Using naming function: {naming_func.__name__}")

    sampled_texts_by_level = {}
    # Assign cluster names with provided function or default
    if params.clusName is not None:
        for idx, clattr in enumerate(clusters):
            clName = params.clusName if idx == 0 else f"{params.clusName}_L{idx + 1}"
            logger.info(f"sampling texts for {clattr} with {n_entities} n_entities" )
            sampled_texts = sample_cluster_texts_by_percentile(
                nodesdf,
                textcol=params.textcol,
                clusattr=clattr,
                max_samples=n_entities
            )
            sampled_texts_by_level[clattr] = sampled_texts
            # Call the naming function, passing sampled texts
            logger.info(f"Assigning names for cluster {clattr} with {len(sampled_texts)} clusters")
            nodesdf = naming_func(
                nodesdf,
                textcol=params.textcol,
                clusattr=clattr,
                clusname=clName,
                n_tags=params.n_tags,
                subject=subject,
                n_entities=n_entities,
                sample_texts=sampled_texts,
                model=model,
                improve=improve,
                improve_model=improve_model
            )

    return nodesdf, edgesdf, sims, sampled_texts_by_level

