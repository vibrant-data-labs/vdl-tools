from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.database_cache.database_utils import get_session


def get_gpt_response(
    items,
    session=None,
    prompt_str=None,
    prompt_id=None,
    prompt_name=None,
    model="gpt-4.1-mini",
    use_cached_result=True,
    **kwargs
):
    with get_session(session) as passed_in_session:
        prc = PromptResponseCacheSQL(
            session=passed_in_session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
            prompt_id=prompt_id,
            **kwargs
        )
        if isinstance(items, list):
            if isinstance(items[0], str):
                keys_items = [(f"{prc.prompt.id}-{i}", item) for i, item in enumerate(items)]
            else:
                keys_items = items
        elif isinstance(items, dict):   
            keys_items = list(items.items())
        else:
            raise ValueError("items must be a list or a dict")

        response = prc.bulk_get_cache_or_run(
            given_ids_texts=keys_items,
            model=model,
            use_cached_result=use_cached_result,
            **kwargs
        )
        return [response[key]['response_text'] for key in response]