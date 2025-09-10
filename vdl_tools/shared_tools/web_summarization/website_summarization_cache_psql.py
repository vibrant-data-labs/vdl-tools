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

GENERIC_ORG_WEBSITE_PROMPT_TEXT_STRUCTURED = """
You are an analyst researching organizations in order to write a summary of their work. Your data science team has scraped the websites of the organizations, and it is your job to summarize the text to provide a clear and objective description of each organization.

Guidelines:
- Ensure your description is objective and does not read like marketing copy.
- Ignore junk or repetitive text from the scraped source.
- Exclude any mention of donations or how to fund the organization.
- Focus on accuracy and quality of your summary; ensure information is well written and factually correct.
- Do not include references to the organization's website or suggest visiting it for more information.
- Write your response in English only.
- Exclude any copyright statements, legal text, disclaimers, citations, or physical addresses.
- Avoid overly generic or broad terms for fields such as "technologies" and "communities." Instead, be specific and list particular technologies and communities relevant to the organization.
- If you cannot provide a detailed response for a field, leave it blank (as per output format below).

You will receive a set of webpage URLs and the web text for a single organization, separated by a line break and --- characters.

## Output Format
Format your response as a JSON object with these exact fields (in this order):

{
  "summary": "<General objective description of the organization>",
  "technologies": [<Extract a concise list of the technologies that are used or created by the organization to solve a problem, eg.  "Mobile health clinics", "AI", "Water purification technology">],
  "products": [
    <List of products, services, solutions and/or activities offered by the organization that are relevant to their mission.
      - Non-profit organizations may focus on services or solutions rather than products.
    e.g., "eletric vehicles", "educational services">
  ],
  "geomentions": [<List of Cities, States, Countries, Regions and Continents tags that are mentioned.>],
  "communities": [<Groups served or collaborated with, e.g., "farmers", "researchers. These are not necessarily end consumers or clients, but the communities are explicitly mentioned in the text.">],
  "profit": "<If the organization model is clear, use 'For Profit' or 'Non Profit'. If not, leave this field null.>",
"justice": [
    <Extract only the initiatives or actions from the text that directly and explicitly relate to climate justice or equity. These should:
      - Address the needs of marginalized, underserved, or vulnerable communities
      - Promote inclusive access to resources, technology, or finance for climate adaptation or mitigation
      - Reduce inequality caused or worsened by environmental or climate factors

    Ignore generic economic development, general education, or broad sustainability statements unless they explicitly connect to justice, equity, or vulnerable populations.

    Return a list of just the qualifying initiatives. If none are found, return an empty list.>
  ]
}

- If a field has no information available, use an empty string for summary and empty arrays for geomentions, technologies, products, communities, and justice.
- For the 'profit' field, only provide a value if you are completely sure of the organization's model. Otherwise, leave it as null.
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
