import time
import datetime as dt

import pytest

from vdl_tools.shared_tools.web_summarization.website_summarization_cache import WebsiteSummarizationCache
from vdl_tools.shared_tools.tools.config_utils import get_configuration


PROMPT_STR = "This is a test prompt"


@pytest.mark.parametrize(
    "input_url,expected_ending",
    [
        # Standard checks
        ("https://vdl.org", "vdl.org"),
        ("https://www.vdl.org", "vdl.org"),
        ("https://www.vdl.org/my_website", "vdl.org_my_website"),

        # Test without https://
        ("vdl.org", "vdl.org"),
        ("www.vdl.org", "vdl.org"),

        # Test with http
        ("http://vdl.org", "vdl.org"),
        ("http://www.vdl.org", "vdl.org"),

        ("https://www.vdl.org/my_website?crazything=crazierthing", "vdl.org_my_website_crazything_crazierthing"),
        ("https://www.wikipedia.org/my_website?crazything=crazierthing", "wikipedia.org_my_website_crazything_crazierthing"),
        ("https://www.medium.com/my_website?crazything=crazierthing", "medium.com_my_website_crazything_crazierthing"),
        ("https://www.facebook.com/my_website?crazything=crazierthing", "facebook.com_my_website_crazything_crazierthing"),
    ]
)
def test_id_creation(input_url, expected_ending):
    cache = WebsiteSummarizationCache(
        prompt_str=PROMPT_STR,
    )
    prompt_id = cache.prompt.id

    assert cache.create_search_key(input_url) == f"{prompt_id}/{expected_ending}"


def test_delete_cached_item():
    configuration = get_configuration()
    if not configuration:
        print("No configuration file, skipping test")
        assert 1
    cache = WebsiteSummarizationCache(
        prompt_str=PROMPT_STR,
        aws_region=configuration["aws"]["region"],
        aws_access_key_id=configuration["aws"]["access_key_id"],
        aws_secret_access_key=configuration["aws"]["secret_access_key"],
        file_cache_directory=configuration["website_summary"]["website_summary_cache_dir"],
    )

    id_ = "https://www.websitesummarycache.com/"
    value = (
        "This is a fake summary used in a test that should be better written and use fixtures but "
        "until we have CI/CD executing in the cloud it's fine"
    )
    search_key = cache.create_search_key(id_)
    cache.store_item(id_, value)

    # Test directly in s3 because cache.get_cache_item will check in cache._cached_keys
    # But that is created on cache instantiation so wouldn't have new things.
    assert cache.client.get_object(Bucket=cache.bucket, Key=search_key)

    # Test directly in locally because cache.get_file_item will check in cache._local_cache_keys
    # But that is created on cache instantiation so wouldn't have new things.
    fp = cache._get_file_path(search_key)
    assert fp.exists()

    # This will add to the local caches
    cache = WebsiteSummarizationCache(
        prompt_str=PROMPT_STR,
        aws_region=configuration["aws"]["region"],
        aws_access_key_id=configuration["aws"]["access_key_id"],
        aws_secret_access_key=configuration["aws"]["secret_access_key"],
        file_cache_directory=configuration["website_summary"]["website_summary_cache_dir"],
    )
    # This adds it to _local_cache_keys
    cache.get_cache_item(id_)

    last_available_date= dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=90)
    exists_locally, _ = cache.get_file_item(search_key, last_available_date)
    assert exists_locally

    exists_s3, _ = cache.get_s3_item(search_key)
    assert exists_s3

    assert search_key in cache._cached_keys
    assert search_key in cache._local_cache_keys


    # Now delete and make sure it's not in anything
    cache.delete_by_id(id_)
    exists_s3, _ = cache.get_s3_item(search_key)
    assert search_key not in cache._cached_keys
    assert not exists_s3

    # Double check it's not in s3 and we are hitting weird cache things
    with pytest.raises(Exception):
        cache.client.get_object(Bucket=cache.bucket, Key=search_key)

    # Make sure it's not local
    exists_locally, _ = cache.get_file_item(search_key, last_available_date)
    assert not exists_locally
    assert search_key not in cache._local_cache_keys

    # Double check not local
    assert not fp.exists()

    assert not cache.get_cache_item(id_)


def test_delete_non_existent_cached_item():
    configuration = get_configuration()
    if not configuration:
        print("No configuration file, skipping test")
        assert 1
    cache = WebsiteSummarizationCache(
        prompt_str=PROMPT_STR,
        aws_region=configuration["aws"]["region"],
        aws_access_key_id=configuration["aws"]["access_key_id"],
        aws_secret_access_key=configuration["aws"]["secret_access_key"],
        file_cache_directory=configuration["website_summary"]["website_summary_cache_dir"],
    )

    id_ = "https://www.somerandomid.com/"
    assert not cache.delete_by_id(id_)
