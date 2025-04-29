import os
import pandas as pd

from vdl_tools.shared_tools.openai.openai_api_utils import num_tokens_from_messages, get_num_tokens
from vdl_tools.shared_tools.openai.openai_constants import MODEL_DATA
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.shared_tools.tools.text_cleaning import check_for_repeating_sequences
from vdl_tools.shared_tools.web_summarization.page_choice.choose_pages import deterministic_len_key, ScrapedPageRecord


# Minimum number of tokens that need to be available for the summary
MIN_SUMMARY_LENGTH = 100
# Maximum number of tokens that can be used for the summary
MAX_SUMMARY_LENGTH = 500
MODEL = "gpt-4.1-mini"
MIN_TEXT_LENGTH = 50


def _make_full_url(record: ScrapedPageRecord):
    """Utitility function that concatenates the source and subpath of a page record to make the full url."""
    return os.path.join(record['source'].strip('/'), record['subpath'].strip('/'))


def make_group_text(
    prompt_str: str,
    record_group: pd.DataFrame | list[ScrapedPageRecord],
    model: str = "gpt-4.1-mini"
):
    """Makes the text for a group of page records for a given organization.
    This will be concatenated to the prompt_str text.

    Parameters
    ----------
    prompt_str: str
        The prompt (as a string) that will be used.

    record_group : list[ScrapedPageRecord]
        The list of page records for a single organization

    model : str, optional
        The OpenAI GPT model to be used for scoring, by default "gpt-4.1-mini"
        This is used to get the token count for the messages to ensure we don't
        exceed the max context window.

    Returns
    -------
    str
        The text for the group of page records
    """
    if isinstance(record_group, pd.DataFrame):
        record_group = record_group.to_dict(orient="records")
    messages = [
        {"role": "system", "content": prompt_str},
        {"role": "user", "content": ""},
    ]

    # We hash the text so need to be very determininstic when sorting
    record_group = sorted(record_group, key=lambda x: deterministic_len_key(x['subpath']))
    model_name = MODEL_DATA[model]["model_name"]
    # The number of tokens in the prompt without any text yet added
    current_message_tokens = num_tokens_from_messages(messages, model_name)
    # The number of tokens available for the prompt
    num_tokens_available = MODEL_DATA[model]["max_context_window"] - \
        current_message_tokens - MIN_SUMMARY_LENGTH

    JOIN_CHARS = "\n----\n"

    url_texts = []
    num_tokens_used = 0
    num_too_long = 0

    # print([x['subpath'] for x in record_group])
    for record in record_group:
        full_url = _make_full_url(record)
        if len(record['text']) < MIN_TEXT_LENGTH:
            logger.debug(
                "Skipping short text %s, only %s characters",
                full_url, len(record['text']),
            )
            continue

        record_text_w_url = f"URL: {full_url}\nTEXT: {record['text']}"

        num_tokens_in_record_w_url = get_num_tokens(
            JOIN_CHARS + record_text_w_url,
            model_name
        )

        if num_tokens_in_record_w_url + num_tokens_used < num_tokens_available:
            repeats_sequence, _ = check_for_repeating_sequences(record_text_w_url, (2, 3), 0.20)
            if repeats_sequence:
                logger.warning(
                    "Skipping text %s, repeating sequences",
                    full_url,
                )
                continue
            url_texts.append(record_text_w_url)
            num_tokens_used += num_tokens_in_record_w_url
        else:
            logger.warning(
                "Skipping text %s, too long (%s tokens)",
                full_url, num_tokens_in_record_w_url,
            )

    if not url_texts:
        logger.debug("No text for record url %s", record_group[0]['source'])
        if num_too_long == len(record_group):
            logger.warning("All records too long, using first record truncated")
            first_record = record_group[0]
            full_url = _make_full_url(first_record)
            record_text_w_url = f"URL: {full_url}\nTEXT: {first_record['text'][:5000]}"
            url_texts.append(record_text_w_url)
        else:
            logger.warning("No text for record url %s", record_group[0]['source'])
            return None

    return "\n----\n".join(url_texts)
