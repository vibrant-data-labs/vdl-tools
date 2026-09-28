import os
from multiprocessing.pool import ThreadPool
import threading

import pandas as pd
from more_itertools import chunked

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.scraper.scrape_websites import extract_website_name, scrape_websites_psql
from vdl_tools.scrape_enrich.scraper.text_quality import classify_text_quality
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP
from vdl_tools.shared_tools.web_summarization.website_summarization_cache_psql import (
    WebsiteSummarizationCache,
    GENERIC_ORG_WEBSITE_PROMPT_TEXT,
)
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import DEFAULT_MODEL

# Verdicts a scrape can carry that mean "not the organization's own content" —
# see text_quality.py's module docstring for why these are never summarized
# rather than summarized-and-flagged.
_SKIP_SUMMARIZING_VERDICTS = ("parked", "blocked", "garbled", "dead", "empty")


def is_summarizable(text, num_errors: int = 0) -> bool:
    """Whether ``text`` is worth spending an LLM call to describe.

    False for a scrape that answered but yielded junk — a parked domain, a
    bot wall, binary mojibake, nothing at all. Asked to describe one of
    these anyway, the summarizer does not reliably say so: it sometimes
    invents a plausible description from the organization's name instead.
    """
    if not text:
        return False
    return classify_text_quality(text, num_errors) not in _SKIP_SUMMARIZING_VERDICTS


def summarize_scraped_df(
    scraped_df: pd.DataFrame,
    is_combined: bool = False,
    prompt_str: str = GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    prompt_name='generic_org_website',
    skip_existing: bool =True,
    n_per_commit: int = 50,
    max_workers: int = 5,
    max_errors: int = 1,
    model=DEFAULT_MODEL
) -> dict:
    """Runs website summarization on a dataframe that went through VDL's website_scraping code.

    Parameters
    ----------
    scraped_df : pd.DataFrame
        Dataframe formatted as that returned from `scrape_enrich.scraper.scrape_websites`
    filtering_keep_paths : tuple[str], optional
        Paths to keep, by default tuple(set(PATHS_TO_KEEP))
    prompt : str
        Prompt to use for the summarization

    Returns
    -------
    dict
       Source url to summarization
    """

    # filtered_pages_df = filter_pages(scraped_df, keep_paths=filtering_keep_paths)
    # filtered_pages_df["text"] = filtered_pages_df["text"].apply(clean_scraped_text)

    if not is_combined:
        scraped_df["text"] = scraped_df["text"].apply(clean_scraped_text)
        source_grouper = scraped_df.groupby("source")

        given_ids_texts = [
            (source, make_group_text(prompt_str, group))
            for source, group in source_grouper
        ]
        num_errors_by_source = {}

    else:
        given_ids_texts = scraped_df[["home_url", "combined_text"]].values.tolist()
        num_errors_by_source = (
            dict(zip(scraped_df["home_url"], scraped_df["num_errors"]))
            if "num_errors" in scraped_df.columns else {}
        )

    n_before = len(given_ids_texts)
    given_ids_texts = [
        (source, text) for source, text in given_ids_texts
        if is_summarizable(text, num_errors_by_source.get(source, 0))
    ]
    n_skipped = n_before - len(given_ids_texts)
    if n_skipped:
        logger.info(
            f"Skipping summarization for {n_skipped} scraped page(s) classified as "
            "junk (parked/blocked/garbled/dead/empty) rather than describable content"
        )

    logger.info(f"Summarizing {len(given_ids_texts)} websites")
    with get_session() as session:
        cache = WebsiteSummarizationCache(session=session,
                                          prompt_str=prompt_str,
                                          prompt_name=prompt_name,
                                          model=model)
        summaries = cache.bulk_get_cache_or_run(
            given_ids_texts,
            use_cached_result=skip_existing,
            n_per_commit=n_per_commit,
            max_workers=max_workers,
            max_errors=max_errors,

        )

    summaries = {k.rstrip("/"): v['response_text'] for k, v in summaries.items()}
    return summaries


def summarize_website(
    url: str,
    use_combined: bool = True,
    prompt_str: str = GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    skip_existing: bool = True,
    n_per_commit: int = 50,
    max_workers: int = 5,
    max_errors: int = 1,
    is_single_page: bool = False,
) -> str:
    """Summarizes a website by scraping it and then running summarization on the scraped content.

    Parameters
    ----------
    url : str
        URL of the website to summarize
    prompt_str : str
        Prompt to use for the summarization

    Returns
    -------
    str
        The summarization of the website
    """


    single_page_websites = [url] if is_single_page else []
    chosen_df = scrape_websites_psql(
        session=None,
        urls=[url],
        max_workers=1,
        summary_prompt=prompt_str,
        return_combined_res=use_combined,
        single_page_websites=single_page_websites,
    )

    summaries = summarize_scraped_df(
        chosen_df,
        is_combined=use_combined,
        prompt_str=prompt_str,
        skip_existing=skip_existing,
        n_per_commit=n_per_commit,
        max_workers=max_workers,
        max_errors=max_errors,
    )
    if not summaries:
        return None

    return list(summaries.values())[0]


def scrape_and_summarize_websites(
    urls: list[str],
    use_combined: bool = True,
    prompt_str: str = GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    skip_existing: bool = True,
    n_per_commit: int = 50,
    max_workers: int = 5,
    max_errors: int = 1,
    single_page_websites: list[str] = [],
) -> str:
    """Summarizes a website by scraping it and then running summarization on the scraped content.

    Parameters
    ----------
    url : str
        URL of the website to summarize
    prompt_str : str
        Prompt to use for the summarization

    Returns
    -------
    str
        The summarization of the website
    """

    original_to_normalized_urls = {url: extract_website_name(url) for url in urls}

    chosen_df = scrape_websites_psql(
        session=None,
        urls=urls,
        max_workers=1,
        summary_prompt=prompt_str,
        return_combined_res=use_combined,
        single_page_websites=single_page_websites,
    )

    summaries = summarize_scraped_df(
        chosen_df,
        is_combined=use_combined,
        prompt_str=prompt_str,
        skip_existing=skip_existing,
        n_per_commit=n_per_commit,
        max_workers=max_workers,
        max_errors=max_errors,
    )
    if not summaries:
        return None

    extracted_to_summary = {extract_website_name(original): summary for original, summary in summaries.items()}

    original_to_summaries = {
        original: extracted_to_summary[normalized]
        for original, normalized in original_to_normalized_urls.items() if normalized in extracted_to_summary
    }
    return original_to_summaries


if __name__ == "__main__":

    summary = summarize_website(
        'https://elementalimpact.com/funding-opportunities/commercial-projects/',
        use_combined=True,
        prompt_str=GENERIC_ORG_WEBSITE_PROMPT_TEXT,
        skip_existing=True,
        is_single_page=True,
    )

    summary2 = summarize_website(
        'https://elementalimpact.com/',
        use_combined=True,
        prompt_str=GENERIC_ORG_WEBSITE_PROMPT_TEXT,
        skip_existing=True,
    )
