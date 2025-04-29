from unittest.mock import patch

import pytest

import vdl_tools.shared_tools.tools.log_utils as log
from vdl_tools.shared_tools.openai.prompt_response_cache import GeneralPromptResponseCache


PROMPT_STR = "Write me a summary of the following text."


@pytest.fixture()
def text_id():
    return "my_test_text_1"


@pytest.fixture()
def cache(text_id):
    cache = GeneralPromptResponseCache(
        prompt_str=PROMPT_STR,
    )
    yield cache
    log.info("Cleaning up test")
    cache.delete_by_id(text_id)


@patch(
    "shared_tools.openai.prompt_response_cache.get_completion"
)
def test_general_prompt_response_cache(
    get_completion_mock,
    text_id,
    cache,
):

    text = "My cat is named Ben and he went to the store.",
    test_summary_value = "This is the summary!"
    get_completion_mock.return_value = test_summary_value

    response_text = cache.get_cache_or_run(
        id=text_id,
        text=text,
    )

    # Make sure get_completion was called
    assert get_completion_mock.called_once()
    # Make sure the value is what we'd expect
    assert response_text == test_summary_value

    # Make sure the response was cached in S3
    assert cache.client.get_object(Bucket=cache.bucket, Key=cache.create_search_key(text_id))

    # Make sure the response was cached locally
    fp = cache._get_file_path(cache.create_search_key(text_id))
    assert fp.exists()


    get_completion_mock.return_value = "Some new summary that hopefully doesn't get used since we are cached"
    # Now call it again with the same id
    new_response_text = cache.get_cache_or_run(
        id=text_id,
        text="A different text to call with but the same id",
    )
    # Since it should be using the cached response, it shouldn't have been called and should be the original 1
    assert get_completion_mock.call_count == 1
    # Make sure it's using the original summary value
    assert new_response_text == test_summary_value
