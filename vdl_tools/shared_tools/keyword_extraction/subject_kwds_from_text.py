import ast
import json
from textwrap import dedent

import hdbscan
import numpy as np
import pandas as pd
import vdl_tools.shared_tools.project_config as pc
from pydantic import BaseModel
from sklearn.metrics.pairwise import cosine_distances, cosine_similarity
from sklearn.preprocessing import StandardScaler
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.embed_texts_with_cache import embed_texts_with_cache
from vdl_tools.shared_tools.embedding_network.embedding_network import define_assistant_prompt
from vdl_tools.shared_tools.openai.prompt_response_cache_instructor import InstructorPRC
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


def extract_keywords_from_text(
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
                extract_keywords_from_text(
                    subject=subject,
                    max_kwords=max_kwords,
                    model="gpt-4.1-mini",
                    sample_texts=samples_or_dict
                )
            )
            continue
        # Base case: if value is a list, process the texts
        texts = samples_or_dict
        # prompt = subject_kwds_prompt(max_kwords, subject, texts)

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


def embed_keywords(
        kwd_list,
        used_cached_result=True,
        max_workers=3,
        embedding_provider="openai",
        embedding_model="text-embedding-3-small"
):
    """Embed a list of keywords using the specified OpenAI model.
    Returns:
        np.ndarray: Array of keyword embeddings.
    """
    # make a df with kwd list and an id column == kwd
    words_df = pd.DataFrame({"kwd": kwd_list})
    id_col = "kwd"
    words_col = "kwd"

    ids_text = words_df[[id_col, words_col]].values.tolist()
    embeddings = embed_texts_with_cache(
        ids_texts=ids_text,
        use_cached_result=used_cached_result,
        return_flat=True,
        max_workers=max_workers,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
    )
    return embeddings


def group_keywords_by_embedding_cluster(
        keywords_list,
        min_cluster_size: int = 2,
        output_csv_path="../shared-data/data/keyword_extraction_test/kwd_dic.csv"
):
    """Group keywords by embedding similarity using HDBSCAN clustering.
    Select a master term for each cluster based on centrality.
    Saves the grouped keywords to a CSV file with columns: tag, search_terms, review."""

    embeddings = embed_keywords(keywords_list)
    X_scaled = StandardScaler().fit_transform(embeddings)

    # Cluster
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=1)
    labels = clusterer.fit_predict(X_scaled)

    # Build clusters
    keyword_clusters = {}

    for label, kw, emb in zip(labels, keywords_list, embeddings):
        if label == -1:
            continue  # Skip outliers
        keyword_clusters.setdefault(label, []).append((kw, emb))

    # Select master term for each cluster based on embedding centrality
    raw_to_master = {}
    master_to_terms = {}
    for label, pairs in keyword_clusters.items():
        terms, embs = zip(*pairs)
        matrix = np.vstack(embs)
        centroid = matrix.mean(axis=0).reshape(1, -1)
        distances = cosine_distances(matrix, centroid).reshape(-1)
        master_idx = distances.argmin()
        master_term = terms[master_idx]
        for t in terms:
            raw_to_master[t] = master_term
        master_to_terms[master_term] = sorted(set(terms))
    # Identify outliers
    outliers = [kw for label, kw in zip(labels, keywords_list) if label == -1]
    logger.info(f"Identified {len(outliers)} outlier keywords not assigned to any cluster")
    # Save to CSV with list serialized as JSON string
    # Save: use str() instead of json.dumps()
    df_out = pd.DataFrame([
        {"tag": tag,
         "search_terms": str(terms),
         "review": False
         }  # Python-style string
        for tag, terms in master_to_terms.items()
    ])
    # Append outliers with review flag only (no suggestion)
    if outliers:
        outlier_rows = [
            {"tag": kw,
             "search_terms": str([kw]),
             "review": True}
            for kw in outliers
        ]
        df_out = pd.concat([df_out, pd.DataFrame(outlier_rows)], ignore_index=True)

    df_out.to_csv(output_csv_path, index=False)

    logger.info(f"Saved grouped keywords")
    return raw_to_master


def append_new_kwords_to_dict(kwd_dict_path,
                              list_new_kwds,
                              similarity_threshold=0.8,
                              emb_model="text-embedding-3-small",
                              output_csv_path=None):
    """
    Extend a keyword dictionary using embedding similarity.
    Matches new keywords to existing search terms or creates new tags.
    Newly added tags are also considered for subsequent matches.
    """

    # read the csv with existing kwd dict
    df = pd.read_csv(kwd_dict_path)
    # Parse the list-like strings into actual Python lists
    df["search_terms"] = df["search_terms"].apply(ast.literal_eval)
    # Flatten
    df_exploded = df.explode("search_terms")
    all_terms = df_exploded["search_terms"].tolist()
    term_to_tag = dict(zip(df_exploded["search_terms"], df_exploded["tag"]))

    # Embed all existing terms
    existing_embeddings = embed_keywords(all_terms, embedding_model=emb_model)
    existing_matrix = np.vstack(existing_embeddings)

    # Rebuild tag dictionary
    tag_to_terms = (
        df_exploded.groupby("tag")["search_terms"]
        .agg(lambda x: set(x))
        .to_dict()
    )
    # Prepare notes for new tags
    new_tag_notes = {}

    # Process new keywords
    existing_keywords = set(all_terms)
    unique_new_keywords = [kw for kw in list_new_kwds if kw not in existing_keywords]

    # Embed only the truly new keywords
    logger.info(
        f"Fetching or computing embeddings for {len(unique_new_keywords)} new keywords from {len(list_new_kwds)} total")
    new_embeddings = embed_keywords(unique_new_keywords, embedding_model=emb_model)
    logger.info(f"Assigning new kwords to existing master terms or creating new ones")
    # Process new keywords
    new_term_count = 0
    for kwd, emb in zip(unique_new_keywords, new_embeddings):
        similarities = cosine_similarity([emb], existing_matrix).flatten()
        best_idx = np.argmax(similarities)
        best_score = similarities[best_idx]
        best_match_term = all_terms[best_idx]
        best_tag = term_to_tag[best_match_term]

        if best_score >= similarity_threshold:
            tag_to_terms[best_tag].add(kwd)
            # logger.info(f"'{kwd}' added to tag '{best_tag}' (matched '{best_match_term}' with score {best_score:.2f})")
        else:
            tag_to_terms[kwd] = {kwd}  # new tag = the keyword itself
            term_to_tag[kwd] = kwd
            all_terms.append(kwd)
            # add new embedding to existing matrix - to allow matching of subsequent new keywords to it
            existing_matrix = np.vstack([existing_matrix, emb])
            new_tag_notes[kwd] = f"closest match: {best_match_term} (score: {best_score:.4f})"
            # logger.info(f"'{kwd}' created new tag (highest match '{best_match_term}' score {best_score:.2f})")
            new_term_count += 1

    logger.info(f"Added {new_term_count} new tags to existing keyword dictionary")
    # Save updated dictionary to a new file for review
    updated_df = pd.DataFrame([
        {"tag": tag,
         "search_terms": str(sorted(terms)),
         "notes": new_tag_notes.get(tag, "")}
        for tag, terms in tag_to_terms.items()
    ])

    if output_csv_path:
        updated_df.to_csv(output_csv_path, index=False)
    logger.info(f"Updated keyword dictionary saved to {output_csv_path}")

    return updated_df


if __name__ == "__main__":
    generate_new_kwd_dict = False
    expand_kwds_with_new = True

    if generate_new_kwd_dict:
        # read the txt with list of kwds

        with open('../shared-data/data/keyword_extraction_test/keywords.txt', 'r') as f:
            initial_kwds = [line.strip() for line in f if line.strip()]
        logger.info(f"Read {len(initial_kwds)} initial keywords")
        kwd_dict = group_keywords_by_embedding_cluster(initial_kwds)  # group them by embedding cluster
        df_kwds = pd.read_csv("../shared-data/data/keyword_extraction_test/kwd_dic.csv")

        # Parse the list-like strings into actual Python lists
        df_kwds["search_terms"] = df_kwds["search_terms"].apply(ast.literal_eval)

    if expand_kwds_with_new:
        # read new kwds from a text file
        with open('../shared-data/data/keyword_extraction_test/keywords_extended.txt', 'r') as f:
            new_kwds = [line.strip() for line in f if line.strip()]
        logger.info(f"Read {len(new_kwds)} new keywords")
        df_updated = append_new_kwords_to_dict(
            kwd_dict_path="../shared-data/data/keyword_extraction_test/kwd_dic.csv",
            list_new_kwds=new_kwds,
            similarity_threshold=0.8,
            emb_model="text-embedding-3-small",
            output_csv_path="../shared-data/data/keyword_extraction_test/kwd_dic_expanded.csv"
        )
