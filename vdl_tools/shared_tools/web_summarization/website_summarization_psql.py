import os
from multiprocessing.pool import ThreadPool
import threading

import pandas as pd
from more_itertools import chunked

from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger
from vdl_tools.scrape_enrich.scraper.scrape_websites import scrape_websites_psql
from vdl_tools.shared_tools.tools.text_cleaning import clean_scraped_text
from vdl_tools.shared_tools.web_summarization.make_page_text import make_group_text
from vdl_tools.shared_tools.web_summarization.page_choice.constants import PATHS_TO_KEEP
from vdl_tools.shared_tools.web_summarization.website_summarization_cache_psql import (
    WebsiteSummarizationCache,
    GENERIC_ORG_WEBSITE_PROMPT_TEXT,
)


def summarize_scraped_df(
    scraped_df: pd.DataFrame,
    is_combined: bool = False,
    prompt_str: str = GENERIC_ORG_WEBSITE_PROMPT_TEXT,
    skip_existing: bool =True,
    n_per_commit: int = 50,
    max_workers: int = 5,
    max_errors: int = 1,
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

    else:
        given_ids_texts = scraped_df[["home_url", "combined_text"]].values.tolist()

    given_ids_texts = [
        (source, text) for source, text in given_ids_texts
        if text
    ]

    logger.info(f"Summarizing {len(given_ids_texts)} websites")
    with get_session() as session:
        cache = WebsiteSummarizationCache(session=session, prompt_str=prompt_str)
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
