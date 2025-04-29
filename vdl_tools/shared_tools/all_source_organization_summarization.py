from sqlalchemy.orm.session import Session

from vdl_tools.shared_tools.openai.openai_api_utils import get_completion
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.logger import logger

BASE_SUMMARY_OF_SUMMARIES_PROMPT = (
    "You are writing a description paragraph about a given organization. "
    "You are given a set of various documents that describe what the organization does, some of their past projects, and products. "
    "We need to write a description that is specific enough to encapsulate their work but not specific on projects or products. "
    "Keep the description to 4-5 sentences and focus on their areas of work rather than biographical details. "
    "Keep the tone professional and avoid jargon. Only use information provided below."
)

def generate_summary_of_summaries(
    ids_text_lists: list[tuple],
    use_cached_results: bool=True,
    prompt_string: str=BASE_SUMMARY_OF_SUMMARIES_PROMPT,
    max_workers: int=10,
):
    """Creates a summary of a set of texts.

    Parameters
    ----------
    ids_text_lists : list[tuple]
        A tuple of (id, list[text]) where id is the id used for storage and the list of texts are text to be summarized
    session : Session
        SQLAlchemy session
    use_cached_results : bool, optional
        Whether to use cached results for previously run results, by default True

    Returns
    -------
    dict
        A dictionary with the id: list of dictionaries that each have the form:
        {id: str}
    """
    ids_texts = []
    for id_, text_list in ids_text_lists:
        filtered_text_list = [x for x in text_list if x]
        text = "\n\n".join([f"Description {i + 1}\n{text}" for i, text in enumerate(filtered_text_list)])
        ids_texts.append((id_, text))

    with get_session() as session:
        prompt_response = PromptResponseCacheSQL(
            session=session,
            prompt_str=prompt_string,
            prompt_name="summary_of_summaries",
        )

        ids_to_response = prompt_response.bulk_get_cache_or_run(
            given_ids_texts=ids_texts,
            use_cached_result=use_cached_results,
            max_workers=max_workers,
        )
    return {k: v['response_text'] for k, v in ids_to_response.items()}
