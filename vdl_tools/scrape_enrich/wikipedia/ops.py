#### imports
import os
import re
import sys
import time
import json
import datetime
import logging
import pathlib as pl
import numpy as np
import wikipedia
import wikipediaapi

#### filenames
start_time = datetime.datetime.now()  # current datetime as run start time
fmt_time = start_time.strftime(f"%m-%d-%y_%H.%M")  # formatted start time
wd = pl.Path.cwd()  # user's current working directory
pl.Path("logs").mkdir(parents=True, exist_ok=True)  # create logs folder if needed
pl.Path("results").mkdir(parents=True, exist_ok=True)  # create results folder if needed
LOG_FILE = wd / f"logs/page_scraper_errors_{fmt_time}.log"
#### logging
logging.basicConfig(level=logging.ERROR, filename=LOG_FILE)
#### functions
### initialize wikipedia api
def init_wiki(lang=None):
    if lang is not None:
        wiki_lang = wikipediaapi.Wikipedia(lang)
        return wiki_lang
    else:
        wiki_en = wikipediaapi.Wikipedia("en")
        return wiki_en


### search for a keyword
def keyword_search(keywords=None, result_count=10):
    if keywords is None:
        print(f"FatalSearchError --> Keywords list not passed as argument")
    else:
        ## search for keywords and return list
        search_results = []
        for keyword in keywords:
            keyword_results = wikipedia.search(
                keyword, results=result_count, suggestion=False
            )
            search_results = search_results + keyword_results
        return search_results


### get page content from a list
def get_page(page_name=None, wiki_api=None):
    if wiki_api is None:
        wiki_api = init_wiki()
    if page_name is None:
        print(f"FatalGetError --> Page name not passed as argument")
    else:
        ## grab page contents
        page_result = wiki_api.page(page_name)
        if page_result.exists():
            page_url = page_result.fullurl
            title_text = page_result.title
            body_text = page_result.text
            categories_list = list(page_result.categories.keys())
            keyword_obj = {
                "page_url": page_url,
                "page_title": title_text,
                "page_body": body_text,
                "page_categories": categories_list,
            }
            return keyword_obj
        else:
            keyword_obj = {"error": "Could not find this page"}
            return keyword_obj


##  primary scrape function intended to be imported
def get_pages_from_list(page_list=None, wiki_api=None, output_file=None):
    """
    ---------------------
    ------- description : searches Wikipedia and outputs results to a JSON file
    ---------------------
    -------- parameters :
              page_list : array of pages to grab content for
               wiki_api : the Wikipedia API object, which can be obtained by running init_wiki()
            output_file : name of JSON file to write results to
    ---------------------
    ----------- returns : dataframe with new columns from scraping
    ---------------------
    """
    # state checks
    if wiki_api is None:
        wiki_api = init_wiki()
    if page_list is None:
        # throw error if no keywords list
        sys.exit("FatalError --> No page list passed")
    # data init
    page_contents = []
    # specify output filename if not specified
    if output_file is None:
        output_file = f"results/page_scraper_results_{fmt_time}.json"
    # loop through keywords and search/scrape Wikipedia
    for page_name in page_list:
        print(f"... saving {page_name}")
        page_obj = get_page(page_name, wiki_api)
        page_contents.append(page_obj)
    # output results to JSON file
    page_json = json.dumps(page_contents, indent=4)
    with open(output_file, "w") as outfile:
        outfile.write(page_json)
    # print info and exit
    print(f" -> operation finished, see {output_file} for results")
    return page_contents
