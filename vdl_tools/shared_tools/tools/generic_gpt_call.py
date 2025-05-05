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
                keys_items = [(f"{prc.prompt.id}-{prc.prompt.create_text_id(item)}", item) for i, item in enumerate(items)]
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
        return {k: v['response_text'] for k,v in response.items()}


if __name__ == "__main__":
    from vdl_tools.shared_tools.tools.generic_gpt_call import get_gpt_response
    translations_texts = ['me llamo eric y mi colego es Zein', "je m'appl zein'"]
    translation_prompt = "You are an expert translator. Translate the following into English"
    translations = get_gpt_response(translations_texts, prompt_str=translation_prompt)
    print(f"*****\nTranslating\n{translations}\n*****")


    location_detection_texts = ["Paris Hilton stayed at the Ritz Carlton in Bangkok", "Zein went to the Eiffel Tower in Paris"]
    location_detection_prompt = """
    You are an expert location detector. Detect the locations of the following text.
    Only return the locations, no other text. Think step by step.
    * Find the nouns that are locations
    * For each noun, check if it is a city, country, or landmark
    * If it is a city or country, return the city or country
    * If it is a landmark, return the landmark
    * If it is not a location, return None
    * Make sure it's not a person's name or a company name or other proper noun
    """
    locations = get_gpt_response(
        location_detection_texts,
        prompt_str=location_detection_prompt,
        model="gpt-4.1-mini",
        use_cached_result=False,
    )
    print(f"*****\nLocation detection\n{locations}\n*****")

