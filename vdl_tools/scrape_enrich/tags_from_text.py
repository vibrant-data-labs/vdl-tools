#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 18:07:35 2020

@author: ericberlow
"""
import regex
import ast
import multiprocessing as mp
import numpy as np
import pandas as pd
import pathlib as pl
from ast import literal_eval
from collections import defaultdict
from vdl_tools.shared_tools.common_functions import create_folders, join_strings_no_missing, write_excel_no_hyper
from vdl_tools.shared_tools.tools.logger import logger


def blacklist_wtd_tags(df, tagcol, blacklist):
    def _normalize_wts(x):
        val_tot = sum([val[1] for val in x])
        return [(val[0], val[1] / val_tot) for val in x]

    # update keyword list
    kwd_wts = df[tagcol].apply(lambda x: [val for val in x if val[0] not in blacklist])
    # update weights by renormalizing
    kwd_wts = kwd_wts.apply(_normalize_wts)
    df[tagcol] = kwd_wts


def parallel_apply_pandas(data, func, n_cores=8):
    """
    apply a function to a Series or DataFrame in parallel
    splits the data into 1/n_cores chunks and calls func on each chunk
    map is synchronous so output data is same order as input data
    returns a DataFrame (concatenated from each call to func)
    """
    if n_cores == -1:
        n_cores = mp.cpu_count()
    split_data = np.array_split(data, n_cores)
    pool = mp.Pool(n_cores)
    results = pd.concat(pool.map(func, split_data))
    pool.close()
    pool.join()
    return results


def _get_ngrams_nltk(text, maxwords=5, remove_conjunctions=False):
    """
    get 1-, 2-,... up to maxwords from text
    keep words with hyphens, apostrophes, and phrases with & or 'and'
    option to clean conjunctions and determinant words
    then removing all unigram stopwords

    params: text = text block
    returns: set of unique ngrams
    """
    from nltk import everygrams, pos_tag
    from nltk.corpus import stopwords

    stop = set(stopwords.words("english"))

    # split into phrases (note: don't split on "-" "&"  "'" or "/")
    phrases = regex.split(
        '[`\=~!@#$%^*()_+\[\]{};\\\:"<,.|<>?]', text.lower()
    )  # [-&'/]
    words = [p.split() for p in phrases]  # list of word lists
    wordlist = sum(words, [])  # combine into list of words
    wordlist = [word.strip() for word in wordlist]  # strip trailing empty spaces

    if remove_conjunctions:
        # remove 'determiner' (the, a, some, most, every, no) and 'conjunction'
        # (and, or, but, if, while and all) parts of speech words
        STOP_TYPES = ["DT", "CC"]
        words_POS = pos_tag(wordlist)  # add part of speech tags
        wordlist = [w for w, wtype in words_POS if wtype not in STOP_TYPES]

    # get list of all 1-, maxwords-grams from cleaned wordlist
    # ngrams = list(set(list((' '.join(w) for w in list(everygrams(wordlist, 1, maxwords))))))
    ngrams = list((" ".join(w) for w in list(everygrams(wordlist, 1, maxwords))))
    # remove all unigrams that are stopwords
    ngrams = [word for word in ngrams if word not in stop]
    ng_counts = defaultdict(int)
    for ng in ngrams:
        ng_counts[ng] += 1
    ng_fracs = np.array(list(ng_counts.values()), dtype=float)
    ng_fracs /= ng_fracs.sum()
    return list(zip(ng_counts.keys(), ng_fracs))  # list of tuples: [(ngram, count)]


def ngrams_from_text(text_column):
    """
    Helper fn for parallelization
    text is a Series
    """
    return text_column.apply(lambda x: _get_ngrams_nltk(x))


def get_tags_from_ngrams(ngrams_col, tagmap_df):
    """
    Helper fn for parallelization
    ngrams is a Series
    """
    return ngrams_col.apply(lambda x: find_tags(x, tagmap_df))


def find_tags(ngrams, tagmap_df, addRelated=True):
    """
    ngrams = list of ngrams (val, count) tuples for each record in the dataframe
    tagmap_df = df 'corpus' to map unique search term to LIST of Master Terms
                and option to add  LIST of broader 'Add Related' terms
    returns: list of master term (and associated related terms) tuples with counts
    """
    # check if master and add-related are lists
    for col in ["master_term", "add_related"]:
        coltype = tagmap_df[col].map(type).mode(0).astype(str) == "<class 'list'>"
        if coltype is False:
            # convert to lists if not already
            tagmap_df[col].fillna("", inplace=True)
            tagmap_df[col] = tagmap_df[col].str.replace(", ", ",")
            # convert from string to list
            tagmap_df[col] = tagmap_df[col].apply(
                lambda x: x.split(",") if x != "" else list()
            )

    tagmap = tagmap_df.set_index("search_term")  # index search term for mapping
    taglist = defaultdict(int)
    for ng_count in ngrams:  # for each ngram in the text for a given recipient
        ngram = ng_count[0]
        cnt = ng_count[1]
        if ngram in tagmap.index:  # find matching search term
            mapto = tagmap.loc[ngram].master_term  # get master term from search term
            for term in mapto:
                taglist[term] += cnt  # add master term match to list of tags
            if addRelated:
                add_ = tagmap.loc[
                    ngram
                ].add_related  # get add_related matches from search term
                if len([add_]) > 0 and add_ == add_:  # append add_related if not empty
                    for term in add_:
                        taglist[term] += cnt
    # list of unique master term and add related term matches as tuples [(term, count)]
    return list(zip(taglist.keys(), taglist.values()))


def string2list(df, cols):
    # for df read from csv or excel that has list columns
    # convert list columns to lists (instead of strings of lists)

    for col in cols:
        df[col].fillna("", inplace=True)
        df[col] = df[col].apply(lambda x: literal_eval(x) if x != "" else [])


def convert_one_to_many_from_one_to_one_mapping(df_tag_dict,
                                                master_term='master_term',
                                                search_terms='search_term',
                                                other_cols=['add_related']
                                                ):
    # group by master term and join search terms and add_related into lists (manage empty cells)
    # expects a file with one search term > one master term and > one add_related
    # returns a dataframe with unique master term and lists of search terms and add_related
    df_tag_dict.fillna("", inplace=True)
    df_tag_dict_agg = (
        df_tag_dict.groupby(master_term)
        .agg(lambda x: x.dropna().tolist())
        .reset_index()
    )
    for col in ([search_terms] + other_cols):
        df_tag_dict[col] = df_tag_dict[col].apply(
            lambda x: list(set(x))
        )  # remove duplicates
        df_tag_dict[col] = df_tag_dict[col].apply(
            lambda x: [term for term in x if term != ""]
        )
    return df_tag_dict_agg


def prepare_tagmap_df(
    df_kwd_grouped,  # master tag, lists of grouped search terms, add_related, unigrams, bigrams
    master_term,  # col name of incoming master term
    search_terms,  # col name of incoming search terms list
    add_related,  # col name of incoming add_related list (must be a list)
    # listcols= ['broad_tags','bigram', 'unigram','search_terms'],
    add_unigrams=True,
    add_bigrams=True,
):
    # convert unique master_term: [list of search terms], ['list of add_related']to
    # unique search_term : [list of master terms], [list of add_related]
    # string2list(df, listcols) # format as lists
    df = df_kwd_grouped.rename(
        columns={
            master_term: "master_term",
            search_terms: "search_term",
            add_related: "add_related",
        }
    )
    # add unigrams and bigrams within longer grams
    if add_unigrams:
        df["add_related"] = df[["add_related", "unigram"]].sum(axis=1)
        df["add_related"] = df["add_related"].apply(lambda x: list(set(x)))
    if add_bigrams:
        df["add_related"] = df[["add_related", "bigram"]].sum(axis=1)
        df["add_related"] = df["add_related"].apply(lambda x: list(set(x)))

    # explode search terms
    df_search_melt = df.explode("search_term")
    # now re-aggregrate by unique search_terms to in case they occur more than once
    df_search_map = (
        df_search_melt.groupby("search_term")
        .agg(
            {
                "master_term": list,
                "add_related": "sum",
            }
        )
        .reset_index()
    )
    # remove dupe terms
    df_search_map["add_related"] = df_search_map["add_related"].apply(lambda x: list(set(x)))
    df_search_map["n_tags"] = df_search_map["master_term"].apply(lambda x: len(x))
    # subset tags that occur in more than one grouped list
    search_dupes = df_search_map[df_search_map["n_tags"] > 1].master_term.tolist()

    if len(search_dupes) > 0:
        logger.info(
            f"{len(search_dupes)} search terms mapped to multiple tags: {search_dupes}"
        )
    return df_search_map


#################################################################################################
# KEYWORD TAGGING - functions to search text and add tags from curated tag corpus
#################################################################################################


def _search_tags(
    df,
    tagcorp_df,
    idCol,
    kwds,
    textcols,
    blacklist,
):
    """
    Search tags.

    From manual tag corpus, search through text ngrams for a match and assign keyword tag if present
    df = dataframe of entities with text columns to search
    tagcorp_df = dataframe of manual tagging dictionary - with 'search_term', 'master_term', 'add_related'
    idCol = unique id or name for merging tag metadata to original data
    kwds = name of new column to create for the keyword tags
    textcols = columns with text to join and use for searching.

    RETURNS =  dataframe with merging id, text, and  added tags
    Also writes this file so it doesn't have to be re-run
    """
    logger.info("getting tags from text")
    # drop columns not in dataframe
    textcols = [col for col in textcols if col in df.columns]
    # trim dataset to id and text block cols
    df[textcols] = df[textcols].fillna("")
    df["text"] = join_strings_no_missing(df, textcols, delim=" / ")
    df["text"] = df["text"].fillna("").astype(str)  # make sure it's a string
    df_text = df[[idCol, "text"]]  # trim dataset to id and text block
    df_text = df_text.reset_index(drop=True)  # reset index
    # get list of ngrams from text for each row
    logger.info("getting ngrams from text (nltk method)")
    df_text["ngrams"] = ngrams_from_text(df_text["text"])

    # get keyword matches for manually curated dictionary
    logger.info("find keyword  matches")
    df_text[kwds] = df_text["ngrams"].apply(
        lambda x: find_tags(x, tagcorp_df, addRelated=True)
    )
    # remove any blacklisted tags
    df_text[kwds] = df_text[kwds].apply(lambda x: [s for s in x if s[0] not in blacklist])

    # clean columns
    df_text.drop(["ngrams"], axis=1, inplace=True)

    return df_text  # dataframe with merging id, text, and tags


def add_kwd_tags(
    df,
    tagcorp_df,
    blacklist=[],
    idCol="id",
    kwds="keywords",
    textcols=["text"],
    format_tagmap=False,
    master_term="tag",  # name col with master term
    search_terms="search_terms",  # name of col with list of search terms
    add_related="broad_tags",  # name of col with manual list of add_related
    add_unigrams=False,  # add unigrams within 2 or more grams.
    add_bigrams=False,  # add bigrams within 3 or more grams
):
    """
    add tags from text using curated keyword dictionary
    if processed file exists just read and merge them, if not search for tags in text
    df : people with metadata file
    tagcorp_df = dataframe of manual tagging dictionary - with 'search_term', 'master_term', 'add_related'
    idCol = unique id or name for merging tags to main dataframe
    kwds = name of new column for tags
    textcols: list of columns with text to search
    explode_tagmap:  if True, explode grouped lists into unique search_terms
    """
    logger.info("\nAdding Keyword Tags")
    if format_tagmap:
        # reformat kwds map  from list of search_terms >> master term
        # to unique search term >> list of master terms
        tagcorp_df = prepare_tagmap_df(
            tagcorp_df,
            master_term,  # master term name
            search_terms,  # list of search terms
            add_related,  # manual add_related list
            add_unigrams=add_unigrams,
            add_bigrams=add_bigrams,
        )

    # search tags from text
    df_tags = _search_tags(
        df,
        tagcorp_df,
        idCol,    
        kwds,
        textcols,
        blacklist,
    )

    # merge tags to main data file
    df_tags.drop(["text"], axis=1, inplace=True)
    return df.merge(df_tags, on=idCol, how="left")


# %%
if __name__ == "__main__":

    # set directories / files
    wd = pl.Path.cwd()
    resultspath = wd / "results"
    topicspath = wd / "data" / "topics"

    # %%

    # get recipients and descriptive text
    df = pd.read_excel(topicspath / "cb_cd_org_terms_all.xlsx", engine="openpyxl")

    keepcols = ["id", "profile_name", "text", "terms", "n_terms"]
    df = df[keepcols]
    string2list(df, ["terms"])  # format as lists
    df["termtext"] = df["terms"].apply(lambda x: ", ".join(x))
    df["text"] = join_strings_no_missing(df, ["text", "termtext"], delim="; ")

    # get tag mapping corpus
    df_tagmap_raw = pd.read_excel(
        topicspath / "climate_selected_terms_cleaned.xlsx", engine="openpyxl"
    )
    df_tagmap = prepare_tagmap_df(
        df_tagmap_raw,
        "tag",  # master term
        "group_terms_thinned",  # search terms list
        "broad_tags",  # add_related list
        listcols=["broad_tags", "bigram", "unigram", "group_terms_thinned"],
    )

    # get set of ngrams from text for each row
    logger.info("getting ngrams from text")
    df["ngrams"] = df["text"].apply(lambda x: _get_ngrams_nltk(x))

    logger.info("find keyword matches")
    df["keywords"] = df["ngrams"].apply(
        lambda x: find_tags(x, df_tagmap, addRelated=True)
    )
    df["new_kwds"] = df.apply(
        lambda x: list(set(x["keywords"]) - set(x["terms"])), axis=1
    )
    df["dropped_terms"] = df["terms"].map(set) - df["keywords"].map(set)

    # clean columns
    df.drop(["ngrams", "termtext"], axis=1, inplace=True)

    df.to_csv("test.csv")
