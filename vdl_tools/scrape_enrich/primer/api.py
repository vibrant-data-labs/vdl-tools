"""
Infer topics using Primer Engine

Executes Primer API calls asynchronously
"""
import pandas as pd
import pickle
from more_itertools import chunked
import configparser
import pathlib as pl

# Import primer modules
from vdl_tools.scrape_enrich.primer.engines_utils.engines import infer_model_on_docs # from Primer directory

import asyncio
import nest_asyncio  # this allows asyncio to run from in an IDE
nest_asyncio.apply()


### get api from config.ini
CONFIG_FILE = 'config.ini'
CONFIG = configparser.ConfigParser()
CONFIG.read(CONFIG_FILE)
ENGINES_API_KEY = CONFIG['primer']['primerKey']


def get_doclist_topicsdict(df, 
                        outfile_pickle,
                        load_existing=False, 
                        re_run_errors=True):
    # if load_existing = False - prior topics will get over-written

    # infer_topics expects a list of dict with keys id and value text of each document
    documents = [{"id": r['id'], "text": r['text']} for i, r in df.iterrows()] 
    # Initialize empty placeholder for results.
    # Schema is {doc_id: [topic1, topic2... ] }
    topics = {}
    if load_existing:
        # OPTION TO LOAD EXISTING RESULTS AND ONLY RUN NEW DOCS AND THOSE WITH ERRORS
        topics = pd.read_pickle(outfile_pickle)  # read saved results
        print(f"{len(topics)} total topics stored")
        errors = {k: v for k, v in topics.items() if v in ['error','N', 'o', ' ', 'r', 'e', 's', 'u', 'l', 't', ['Request failed. Request could not be processed at this time.', 500]]}
        # trim documents to ones missed or with errors. 
        documents_missed = [d for d in documents if d["id"] not in topics.keys()]
        if re_run_errors:
            documents_errors = [d for d in documents if d["id"] in errors.keys()]
            documents = documents_missed + documents_errors
        else:
            documents = documents_missed
        print(f"{len(documents_missed)} documents missed")
        print(f"{len(documents_errors)} documents with request failed errors")
    print(f"{len(documents)} total docs to run")
    print(f"{len(topics)} total docs in existing topic results")
    return documents, topics



def run_primer_api_engines(documents, 
                        chunk_size, # n docs to run at once
                        model_name, # "abstractive_topics"|"extractive_topics"|"named_entities"|"key_phrases" 
                        topics, # dictionary to hold results
                        results_pickle, # pickle to store results
                        segmented=False # auto segement text
                        ):
    # Infer topics, entities, phrases from text with Primer Engines
    # A loop logic that periodically saves the results so that if something fails we keep the partial results and can attempt a rerun.
    # Processes docs in chunks (n at a time)
    # Stores results in chunk_topics
    # Copies over intermediate results into the overall topics dict
    # Write it to file so we don't lose the data.

    for doc_chunk in chunked(documents, 50):
        print("Doc loop")
        topics_results = infer_model_on_docs(doc_chunk,
                                             model_name=model_name,
                                             api_key=ENGINES_API_KEY,
                                             batch_size=10,
                                             segmented=segmented)
        res = asyncio.run(topics_results)
        topics.update(res)    # add results to topics dictionary
        print(f"Collected topics for {len(topics)} documents")
        # Save
        with open(results_pickle, "wb") as f:
            pickle.dump(topics, f)

    # flag errors
    errors = {k: v for k, v in topics.items() if v in 
            ['N', 'o', ' ', 'r', 'e', 's', 'u', 'l', 't', 
            ['Request failed. Request could not be processed at this time.', 500]]}

    print(f"{len(topics)} documents with {model_name} results") 
    print(f"{len(errors)} documents with request failed errors") 
    return topics

def compile_errors(df_orgs, results_pickle):
    # get results dict
    topics = pd.read_pickle(results_pickle)  # read saved results
    errors = {k: v for k, v in topics.items() if v in
          ['N', 'o', ' ', 'r', 'e', 's', 'u', 'l', 't',
           ['Request failed. Request could not be processed at this time.', 500]]}
    df_errors = df_orgs[df_orgs['id'].apply(lambda x: x in list(errors.keys()))]
    return df_errors


def infer_topics_entities(df,
                        model, # "abstractive_topics"|"extractive_topics"|"named_entities"|"key_phrases"
                        results_pickle, # path to store pickle results
                        load_existing, # load existing and get missing and errors
                        re_run_errors=True, # include errors in re-running
                        segmented=False # auto segement text
                        ):
        # GET TOPICS, PHRASES, OR NAMED ENTITIES
        # format doc list and get topic dict for engines
        documents, topics = get_doclist_topicsdict(df,
                                        results_pickle, # path to results
                                        load_existing=load_existing, # load existing and get missing and errors
                                        re_run_errors=re_run_errors # include errors in re-running
                                        )

        # run engines and get topics
        topic_results = run_primer_api_engines(documents, 
                            50, # n docs to run at once
                            model, # "abstractive_topics"|"extractive_topics"|"named_entities"|"key_phrases" 
                            topics, # dictionary to hold results
                            results_pickle, # pickle to store results
                            segmented=segmented
                            )

        return topic_results

def get_topics_errors(df_docs, # df with cols 'id' and 'text'
                    model, # "extractive_topics"|"abstractive_topics"|"named_entities"|"key_phrases"
                    topicspath, # pathlib path of directory to store results
                    outname_root, # prefix of outfile to store results
                    load_existing, # load existing and get missing and errors
                    segmented=False # auto segmeent (use sparingly)
                    ):

    print(f"Running {model} API asynchronously")
    pickle_name = str(topicspath) + outname_root + model + ".p"
    topics = infer_topics_entities(df_docs,
                                            model, 
                                            pickle_name, # path to store pickle results
                                            load_existing, # load existing and get missing and errors
                                            segmented=segmented # auto segmeent (use sparingly)
                                            )
     
    df_errors = compile_errors(df_docs, pickle_name)
    df_errors.to_csv(topicspath/(model + "_errors.csv"))
    return topics, df_errors 

# %%
if __name__ == "__main__":

    ### get api from config.ini
    CONFIG_FILE = 'config.ini'
    CONFIG = configparser.ConfigParser()
    CONFIG.read(CONFIG_FILE)
    ENGINES_API_KEY = CONFIG['primer']['primerKey']

    ### filenames
    wd = pl.Path.cwd()
    prjpath = wd/"projects"/"ClimateLandscape"
    topicspath = prjpath/"data"/"topics"

    infile = topicspath/"orgs_sections_chunked.csv" # all crunchbase and candid descriptions exploded sections < 500 words
    pickle_prefix = "/TEST_orgs_sections_chunked_" # pickle prefix

    # load company descriptions
    df = pd.read_csv(infile)
    df_docs = df[['section_chunk_id', 'section_chunk']]
    df_docs.columns = ['id', 'text'] # must have these cols to format doc dict
    
    
    df_docs = df_docs[~df_docs['text'].isnull()]
    # subset for testing 
    df_docs = df_docs.head(10)
    print(f"{len(df)} text chunks")

    INFER_EXTRACTIVE = True
    INFER_ABSTRACTIVE = False
    INFER_ENTITIES = False 

    if  INFER_EXTRACTIVE:
        topics_extractive, df_errors_extractive = get_topics_errors(df_docs,
                                                "extractive_topics", 
                                                topicspath, # directory (pathlib)
                                                pickle_prefix, # pickle prefix to stor results 
                                                load_existing=False, # load existing and get missing and errors
                                                )

    if  INFER_ABSTRACTIVE:
        topics_abstractive, df_errors_abstractive = get_topics_errors(df_docs,
                                                "abstractive_topics", 
                                                topicspath, # directory (pathlib)
                                                pickle_prefix, # pickle prefix to stor results 
                                                load_existing=False, # load existing and get missing and errors
                                                )
                                   

    if  INFER_ENTITIES:
        entities, df_errors_entities = get_topics_errors(df_docs,
                                                "named_entities", 
                                                topicspath, # directory (pathlib)
                                                pickle_prefix, # pickle prefix to stor results 
                                                load_existing=False, # load existing and get missing and errors
                                                )
