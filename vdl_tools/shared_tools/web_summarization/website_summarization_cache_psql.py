from configparser import ConfigParser
from urllib.parse import urlparse

from vdl_tools.scrape_enrich.scraper.scrape_websites import BAD_URL_PATH_CHARS
from vdl_tools.shared_tools.openai.prompt_response_cache_sql import PromptResponseCacheSQL
from vdl_tools.shared_tools.tools.unique_ids import make_uuid



GENERIC_ORG_WEBSITE_PROMPT_TEXT = """
You are an analyst researching organizations in order to write a summary of their work.
Your data science team has scraped the websites of the organizations and it is your job to summarize the text to give a good description the organization.

It is important your description is objective and doesn't sound like marketing copy.

The text is scraped from websites, so please ignore junk or repetitive text.
Please do not mention anything regarding donations or how to fund the organization.
Please take your time and ensure the information is accurate and well written.
Please do not include any references to the website or suggest visiting the website for more information.
Please only include the summary and nothing but the summary.
Please only return a single summary.
Please ensure the response is in English.
Please do not include copyright, legal text, mentions of disclaimers, or other citations such as address.
Please do not make reference to visiting the website.

You will receive a set of webpage urls and the web text for a single organization. Each set will be delineated by a line break and <code>---</code> characters.

{INPUT TEXT}
{SUMMARY}
"""


class WebsiteSummarizationCache(PromptResponseCacheSQL):
    def __init__(
        self,
        session,
        prompt_str: str = GENERIC_ORG_WEBSITE_PROMPT_TEXT,
        prompt_name: str = "",
    ):

        # If None or "" is passed in
        prompt_str = prompt_str or GENERIC_ORG_WEBSITE_PROMPT_TEXT
        prompt_name = prompt_name or "generic_org_website"
        super().__init__(
            session=session,
            prompt_str=prompt_str,
            prompt_name=prompt_name,
        )
