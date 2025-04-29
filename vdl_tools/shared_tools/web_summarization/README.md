# Website Summarization

The Website Summarization works to provide a generalized summary for a set of website urls.

## Quick Start

Here's a quick demonstration of how to use the website summarization. Before starting ensure your `config.ini` has the additional
fields from website scraping and summarization as well as all AWS fields.

```
[website_scraping]
website_cache_dir = ./.cache/websites/

[website_summary]
website_summary_cache_dir = ./.cache/website_summaries/
```

## Scraping and summarizing directly from URLs
```python
import json

from shared_tools.tools.config_utils import get_configuration
from shared_tools.web_summarization.website_summarization import scrape_and_summarize

config = get_configuration()

URLS = [
    "www.vibrantdatalabs.org",
    "https://www.oneearth.org",
]
summaries = scrape_and_summarize(
    urls=URLS,
    configuration=config
)
```

```
In [5]: print(json.dumps(summaries, indent=2))
{
  "https://www.oneearth.org": "One Earth is a nonprofit organization dedicated to accelerating collective action to address the climate crisis through groundbreaking science, inspiring media, and an innovative approach to climate philanthropy. They focus on three pillars of action: energy transition, nature conservation, and regenerative agriculture. By leveraging widely available technologies and community-led efforts, they aim to achieve a just transition to 100% renewable energy, protect and conserve 50% of the world's lands and seas, and achieve net zero food and fiber systems globally through regenerative agricultural practices. One Earth also works to scale resources for on-the-ground climate solutions worldwide and inspire everyone to actively contribute to solving the climate crisis. Through their scientific efforts, they provide a global blueprint and regional roadmaps to drive swift climate action from both top-down and bottom-up approaches. Additionally, they produce diverse content across various platforms to educate, inspire, and mobilize a global community of people advocating for climate solutions.",

  "www.vibrantdatalabs.org": "Vibrant Data Labs combines data science and network theory to create flexible visual tools for addressing systemic social challenges, with a current focus on the climate crisis. Their interactive maps provide a multifaceted view of data, empowering non-data scientists to make evidence-based decisions. They are also developing tools to visualize climate funding flows and support funders and field builders with public and open-source data tools. The team includes experts in data science, complex networks, machine learning, and theoretical ecology, as well as leaders with extensive experience in the nonprofit and philanthropy sector."
}
```

### Summarizing from outputs that were previously scraped
```python
import pandas as pd
from scrape_enrich.scraper.scrape_websites import scrape_websites

from shared_tools.tools.config_utils import get_configuration
from shared_tools.web_summarization.website_summarization import summarize_website

url = "www.vibrantdatalabs.org"
scraped_website = (pd.Series(url))

# In [17]: scraped_website.head()
# Out[17]:
#              type                   source subpath                                               text
# 0  PageType.INDEX  www.vibrantdatalabs.org       /  Combining data science and network theory into...
# 1   PageType.PAGE  www.vibrantdatalabs.org   /cart  info@vibrantdatalabs.org info@vibrantdatalabs....

config = get_configuration()
website_summary = summarize_website(
  website_url=url,
  website_pages=scraped_website,
  configuration=config,
)
```


## How it works

At a high level, the web summarization takes the result of a scraped url from the VDL Web scraper, and returns a single, **generalized** summary for the source url. As shown above, it can also take a list of urls and run the scraping and summarization together.

### Caching

We are utilizing caching using a custom [WebsiteSummarizationCache](./website_summarization_cache.py) built from the `GeneralPromptResponseCache`.

It is nearly the same as the `GeneralPromptResponseCache`, except that it:
* Sets the `namespace_name` directly to `WEB_SUMMARIZER`
* Has a default `prompt_str` when none is given for a prompt that creates a general summary of a website
* Overwrites the unique id method for generating an id that can be created from a url that is "made safe" by replacing special characters.

### Format to OpenAI

(2/29/2024) We are utilizing OpenAI chat completions using the latest GPT3.5 which is `gpt-3.5-turbo-0125`. This allows for a context window of 16K tokens and a completion text of 4K tokens.

We use the system input for the general system prompt and then send a `user` completion for the specific website. This is an example format to the OpenAI api:

{
"model": "gpt-3.5-turbo-1106",
"temperature": 0.2,
"messages": [
{
"role": "system",
"content": "You are an help AI assistant example and you will receive an example text, unfortunately for you, you will never run"
},
{
"role": "user",
"content": "My example text to summarize"
}
],
"max_tokens": 500,
"top_p": 1,
"seed": 7118,
"frequency_penalty": 0.5,
"presence_penalty": 0,
"stop": "Summary:"
}


### Format of Input Texts

For a given url, we use all the pages that:

- Pass the [page title filter](./page_choice/choose_pages.py)
- Have more than 50 characters of scraped text
- Can fit into a context window of ~15K tokens (200-500 for the summary to return and buffer for the other tokens needed for sending messages)

The resulting text looks like the below.

```
URL: https://www.vibrantdatalabs.org/
TEXT: Combining data science and network theory into flexible visual tools to tackle systemic social challenges. We bring contemporary data science tools to the climate finance sector to reveal insights that are invisible to the naked brain...
----
URL: https://www.vibrantdatalabs.org/about-us
TEXT: This would be about us text if it actually existed. Zein made this up =)
---
<As many as appropriate>
```

### Scoring

**WIP - Some ideas**

As we try out different prompts, we want to score them for common errors. We have a [module for scoring](./score_summarization.py) that attempts to do so.

The scorer uses GPT-4 to answer a set of questions and returns the result as a dictionary. Over a random sample of summaries, we can test how well the prompt is doing on readability.

**TO DO**: This does not test for the actual accuracy of the summary. We should also find a scoring mechanism for that.

Common errors seen so far:

- Is the summary not in English?
- Does the summary contain text mentioning their website?
- Does the summary contain copyright text?
- Does the summary ever speak in the first person?

```python
from shared_tools.web_summarization.score_summarization import score_summary
score_summary?
summary = (
"""The North Wessex Downs is a designated Area of Outstanding Natural Beauty (AONB) covering an area straddling four counties and characterized by its remote, rolling downland and picturesque villages. The organization's work focuses on conserving and enhancing the natural beauty of the landscape, its heritage, and its culture. They collaborate with a network of organizations, businesses, farmers, landowners, and local communities to implement the policies and objectives outlined in the AONB Management Plan. Their efforts include managing grants and funding programs, such as the Mend the Gap program aimed at enhancing areas impacted by railway electrification, and promoting dark skies preservation to minimize light pollution. The organization also manages the North Wessex Downs Landscape Trust, which seeks to inspire support for conservation efforts within the AONB. Additionally, they provide resources for visitors interested in exploring the area through walking, cycling, stargazing, and other activities. The team consists of dedicated professionals responsible for various aspects of landscape management, planning, project development, funding coordination, communications, and administrative support.

For more information about their work or specific projects such as dark skies preservation or grants programs, individuals can visit their website or contact them directly.

---
I have provided a comprehensive summary of the North Wessex Downs organization based on the information from their website. If you need further details on any specific aspect of their work or projects mentioned in their website text please let me know!"""
)
score = score_summary(summary)
```

```
{
  "Does the summary contain `---`?": 1,
  "Is the summary not in English?": 0,
  "Does the summary contain text mentioning their website?": 1,
  "Does the summary contain copyright text?": 0,
  "Does the summary ever speak in the first person?": 1
}
```

```
summary = """This is a great summary. It's perfectly in the the third person"""
score = score_summary(score)
```
