#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 13:13:00 2021

@author: ericberlow
"""

from ast import literal_eval
from typing import List
import os
import regex
import pathlib as pl
import pandas as pd
import numpy as np
from collections import OrderedDict, Counter
from functools import reduce
from operator import add
from url_normalize import url_normalize
import regex
from vdl_tools.shared_tools.tools.logger import logger


def str_converter(val):
    if type(val) is not str:
        return val
    if val != '':
        return literal_eval(val)
    return []


def string2list(df, cols):
    # for df read from csv or excel that has list columns
    # convert list columns to lists (instead of strings of lists)
    for col in cols:
        df[col] = df[col].fillna('')
        df[col] = df[col].apply(str_converter)


def read_excel(fname, cols, sheet_name=0):
    """
    Read and excel and convert string columns that are python literals

    Parameters
    ----------
    fname : string
        name of an excel file.
    cols : list[str]
        list of columns ot convert.

    Returns
    -------
    df : pandas.DataFrame
        DataFrame read from file and with columns converted.

    """
    df = pd.read_excel(fname, sheet_name=sheet_name)
    string2list(df, cols)
    return df


def read_excel_wLists(fname, sheet_name=0):
    """
    Read and excel, detect string cols that are python literals and convert

    Parameters
    ----------
    fname : string
        name of an excel file.

    Returns
    -------
    df : pandas.DataFrame
        DataFrame read from file and with columns converted.

    """
    df = pd.read_excel(fname, sheet_name=sheet_name)
    list_cols = [col for col in df.columns if all('[' in str(x) for x in df[col])]
    string2list(df, list_cols)
    return df


def rename_tags(x, tag_dict):
    """
    Rename tags in list using a dictionary of new names.

    x is a list of tags
    Rename tags from a renaming dictionary
    If the tag is not in the dictionary, keep the old one
    Returns a new list of renamed tags
    """
    oldtaglist = x if type(x) is list else x.split("|")
    newtaglist = []
    for tag in oldtaglist:
        if tag.strip() in tag_dict:
            newtaglist.append(tag_dict[tag.strip()])
        else:
            newtaglist.append(tag)
    newtags = [t for t in newtaglist if type(t) is str]
    return newtags


def add_related_tags(x, add_tag_dict):
    """
    Add related tags from dictionary.

    x is a list of tags
    if tag is present, add related tags from dictionary
    Returns a new list of original tags with add related tags
    """
    taglist = x if type(x) is list else x.split("|")
    for tag in taglist:
        if tag in add_tag_dict:
            addlist = add_tag_dict[tag].split(
                ", "
            )  # split comma separated list if more than one
            taglist.extend(addlist)
    return taglist


def find_replace_multi_from_dict_col(df, col, replace_dict):
    # dictionary is mapping of existing strings to replacement strings
    # col is the column or strings to search and replace within
    for (
        string1,
        string2,
    ) in replace_dict.items():  # spell corrections dictionary for all TOG
        df[col] = df[col].str.replace(string1, string2)
    return df[col]  # return cleaned spelling


def find_replace_multi_from_dict(x, replace_dict):
    # dictionary is mapping of existing strings to replacement strings
    # x is string in a record
    for (
        string1,
        string2,
    ) in replace_dict.items():  # spell corrections dictionary for all TOG
        if x == string1:
            x = string2  # replace with string 2
    return x  # return cleaned spelling


def split(delimiters, string, maxsplit=0):
    # split on list of many delimeters
    regexPattern = "|".join(map(regex.escape, delimiters))
    return regex.split(regexPattern, string, maxsplit)


def blacklist_tags (df, tagcol, blacklist):
    # remove tags from tag list
    # tagcol = tag list column
    # blacklist = list of tags to remove
    df[tagcol] = df[tagcol].apply(lambda x: [s for s in x if s not in blacklist])   # only keep keywords not in blacklist


def build_tag_hist_df(df, col, delimiter="|", blacklist=[], mincnt=0):
    # generate dictionary of tags and tag counts for a column, exclude rows with no data
    # trim each dataset to tags with more than x nodes
    # blacklist = list of tags to ignore in counting
    # mincnt = min tag frequency to include
    print("generating tag histogram for %s" % col)
    total = len(df)
    tagDict = {}
    df[col] = df[col].fillna("")
    # convert to list and strip empty spaces for each item in each list
    tagLists = df[col].str.split(delimiter).apply(lambda x: [ss.strip(" ") for ss in x])
    # remove any blacklisted tags
    tagLists = tagLists.apply(lambda x: [t for t in x if t not in blacklist])
    tagHist = OrderedDict(
        Counter([t for tags in tagLists for t in tags if t != ""]).most_common()
    )
    tagDict[col] = list(tagHist.keys())
    tagDict["count"] = list(tagHist.values())
    tagdf = pd.DataFrame(tagDict)
    tagdf["percent"] = tagdf["count"].apply(lambda x: np.round((100 * (x / total)), 2))
    tagdf = tagdf[tagdf["count"] > mincnt]  # remove infrequent tags
    return tagdf


def clean_tags(df, tagCol, delimeter="|"):
    # remove empty tags and duplicates reformat as pipe-separated string of unique tags
    # returns tag column
    df[tagCol] = df[tagCol].astype(str).str.split(delimeter)  # convert tag string to list
    df[tagCol] = df[tagCol].apply(
        lambda x: [s.strip() for s in x if len(s) > 0]
    )  # remove spaces and empty elements - list comprehension
    df[tagCol] = df[tagCol].apply(
        lambda x: delimeter.join(list(set(x)))
    )  # get unique tags, and join back into string of tags
    return df[tagCol]


def clean_tag_cols(df, tagCols, delimeter):
    # for a list of multiple tag columns (tagCols)
    # remove empty tags and duplicates reformat as delimiter-separated string of unique tags
    # returns cleaned tag columns
    for col in tagCols:
        df[col] = clean_tags(df, col, delimeter)
    return df[tagCols]


def clean_tag_list(df, tagCol, replace={}):
    # remove empty tags and duplicates reformat as pipe-separated string of unique tags
    # returns new tags
    # remove spaces and empty elements - list comprehension
    tags = df[tagCol].apply(lambda x: [s.strip() for s in list(set(x)) if (s and len(s.strip()) > 0)])
    if len(replace) > 0:
        tags = tags.apply(lambda x: [replace.get(s, s) for s in x])
    return tags

#def merge_dupes(df, groupVars, pickOneCols, tagCols, stringCols):
#    # merge records of duplicate entities
#    # pickOneCols - select first answer
#    # tagCols - join as pipe-separated list of tags
#    # stringCols - join strings with comma
#    print("\nMerging duplicates")

#   # clean a text columsn missing values
#    df[stringCols + tagCols] = df[stringCols + tagCols].fillna("").astype(str)

#    # build aggregation operations
#    agg_data = {
#        col: (lambda x: "|".join(x)) for col in tagCols
#    }  # join as 'tags' separated by pipe
#    agg_data.update({col: "first" for col in pickOneCols})
#    agg_data.update({col: (lambda x: ", ".join(x)) for col in stringCols})
#    agg_data.update({"count": "sum"})
#    # group and aggregate
#    df_merged = df.groupby(groupVars).agg(agg_data).reset_index()
#    # clean tags
#    df_merged[tagCols] = clean_tag_cols(df_merged, tagCols, delimeter="|")
#    df_merged[stringCols] = clean_tag_cols(df_merged, stringCols, delimeter=", ")

#    return df_merged


def write_excel_no_hyper(df, outname):
    '''
    write to excel without converting strings to hyperlinks
    '''
    # make sure folders exist
    create_folders(outname)
    # write to excel without converting strings to hyperlinks
    writer = pd.ExcelWriter(outname, engine="xlsxwriter")  # ,
    # Don't convert url-like strings to urls.
    writer.book.strings_to_urls = False
    df.to_excel(writer, index=False)
    writer.close()


def add_nodata(df, col_list, filltext="no data"):
    # fill empty tags with 'no data'
    for col in col_list:
        df[col] = df[col].fillna("no data")
        df[col] = df[col].apply(lambda x: filltext if x == "" else x)


def rename_strings(df, col, renameDict):
    # find and replace for multiple strings from dictionary
    for string1, string2 in renameDict.items():  # renaming dictionary
        df[col] = df[col].str.replace(string1, string2)
    return df[col]  # return cleaned recipient spelling


def normalize_linkedin_urls(df, li_url, li_fix_dict={}):
    # li_url = column that has linkedin url
    # li_fix_dict = optional manual dictionary of spelling corrections
    # returns columns of cleaned urls

    # remove long suffix
    df[li_url] = df[li_url].apply(lambda x: x.split("?")[0])
    df[li_url] = (
        df[li_url].str.lower().str.strip()
    )  # make all lower case, remove any trailing space
    # normlize url format
    df[li_url] = df[li_url].apply(
        lambda x: url_normalize(x)
    )  # automated url normalizer
    df[li_url] = df[li_url].fillna("")
    df[li_url] = df[li_url].apply(
        lambda x: ("https://www.linkedin" + x.split("linkedin")[-1]) if x != "" else ""
    )  # standardize prefixc
    df[li_url] = df[li_url].str.replace(
        "https://linkedin", "https://www.linkedin", regex=True
    )  # add www for consistency
    df[li_url] = df[li_url].str.lower()  # make all lowercase
    df[li_url] = df[li_url].str.replace(
        "company/company", "company", regex=True
    )  # remove duplicates (candid data error)
    df[li_url] = df[li_url].apply(lambda x: x.split("mycompany")[0])
    df[li_url] = df[li_url].apply(
        lambda x: "" if x == "" else x + "/" if x[-1] != "/" else x
    )  # make sure all end with "/"
    df[li_url] = df[li_url].str.replace(
        "/about/", "/", regex=True
    )  # remove 'about' from company urls
    # fix known bad url spellings
    df[li_url] = find_replace_multi_from_dict_col(df, li_url, li_fix_dict)
    # remove mobile app indicator
    df[li_url] = df[li_url].str.replace("mwlite/", "")
    return df[li_url]


def get_keywords_path():
    _dir, _filename = os.path.split(__file__)
    return pl.Path(_dir) / ".." / "keywords"


def create_folders(fname):
    pl.Path(fname).parent.mkdir(parents=True, exist_ok=True)


def join_strings_no_missing(df: pd.DataFrame, cols: List[str], delim="|"):
    # cols is list of string columns to concatenate - and ignore missing values
    # returns a series
    df = df.copy()

    df[cols] = df[cols].fillna("")
    df[cols] = df[cols].replace(
        r"^\s*$", np.nan, regex=True
    )  # replace empty string with nan
    joined_strings = df[cols].apply(
        lambda x: delim.join(x.dropna().astype(str)), axis=1
    )
    return joined_strings


def clean_empty_list_elements(df, listcol):
    # clean list of empty elements
    df[listcol] = df[listcol].apply(lambda x: [s.strip() for s in x if s])  # strip empty spaces
    df[listcol] = df[listcol].apply(lambda x: [s for s in x if s])  # keep if not empty
    df[listcol] = df[listcol].apply(lambda x: [s for s in x if s != ''])  # keep if not empty string
    df[listcol] = df[listcol].apply(lambda x: list(set(x)))  # remove duplicates
    df[listcol] = df[listcol].apply(lambda x: None if x == '' else x)  # replace empty string with none
    return df[listcol]


def keep_tags_w_min_count_list(df, tag_attr, min_count=2):
    """
    Filters tags in a DataFrame column to only include those that occur at least a minimum number of times.

    df : pandas.DataFrame
        The DataFrame containing the tags.
    tag_attr : str
        The name of the column in the DataFrame that contains the tags.
        The minimum number of occurrences for a tag to be kept, by default 2.

    pandas.Series
        A Series with the filtered tags as pipe-separated strings.
    pandas.Series
        A Series with the count of tags for each row.
    """
    logger.info("Keeping tags that occur at least %s times", str(min_count))

    taglists = df[tag_attr]
    # build master histogram of tags that occur at least min count times
    tag_hist = dict([
        item for item in
        Counter([k for kwList in taglists for k in kwList]).most_common()
        if item[1] >= min_count
    ])
    # filter tags in each row to ones that are in this global 'active' tags set
    taglists = taglists.apply(lambda x: [k for k in x if k in tag_hist])

    # update tag counts
    n_tags = taglists.apply(len)
    return taglists, n_tags


def clean_spaces_linebreaks_all(df):
    # remove any line breaks, tabs, carriage returns
    try:
        df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
        df.replace('\s+', ' ', regex=True, inplace=True)  # replace repeated spaces with one
    except Exception as ex:
        print(ex)
        raise ex


def clean_spaces_linebreaks_col(df: pd.DataFrame, col: str):
    # remove any line breaks, tabs, carriage returns
    df[col].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    df[col].replace('\s+', ' ', regex=True, inplace=True)  # replace repeated spaces with one


def write_network_to_excel(ndf, ldf, outname):
    writer = pd.ExcelWriter(outname, engine='openpyxl')
    # Don't convert url-like strings to urls.
    writer.book.strings_to_urls = False
    ndf.to_excel(writer, sheet_name='Nodes', index=False)
    ldf.to_excel(writer, sheet_name='Links', index=False)
    writer.close()


def write_network_to_excel_simple(ndf, ldf, outname):
    writer = pd.ExcelWriter(outname)
    ndf.to_excel(writer, sheet_name='Nodes', index=False)
    ldf.to_excel(writer, sheet_name='Links', index=False)
    writer.close()


def normalized_difference(df, attr):
    # compute normalizd difference relative to the mean
    avg_attr = df[attr].mean()
    normalized_diff = ((df[attr]-avg_attr)/(df[attr]+avg_attr)).round(4)
    return normalized_diff


def max_min_normalize(df, attr):
    max_min = (df[attr]-df[attr].min())/(df[attr].max()-df[attr].min())
    return max_min


def explode_chunks(df, chunk_col, chunk='chunk'):
    # explode chunks into separate rows
    print("exploding chunks")
    df_chunked = df[['id', chunk_col]].explode(chunk_col)
    df_chunked.columns = ['id', chunk]
    empty = ((df_chunked[chunk] == '') | (df_chunked[chunk] == ' ')
             | (df_chunked[chunk] == '  ') | (df_chunked[chunk] == '   '))
    df_chunked = df_chunked[~empty].reset_index(drop=True)
    # add global id and ordered id for each chunk wthin each person
    df_chunked[chunk + '_id'] = range(0, len(df_chunked))  # unique id for each chunk
    df_chunked[chunk + '_order'] = df_chunked.groupby(['id']).cumcount()  # numbered id within each person
    return df_chunked


def generate_sponsor_tuples(df, sponsors):
    # create list of sponsor logos etc
    # as list of tuples [(link_title, icon_url, link_url)]
    # df: spreadsheet with sponsor info
    # sponsors: list of sponsor names
    sponsor_tuple_list = []
    for sponsor in sponsors:
        sponsor_tuple = (df.loc[df['link_title'] == sponsor]['link_title'].tolist()[0],
                         df.loc[df['link_title'] == sponsor]['icon_url'].tolist()[0],
                         df.loc[df['link_title'] == sponsor]['link_url'].tolist()[0])
        sponsor_tuple_list.append(sponsor_tuple)
    return sponsor_tuple_list


def sort_weighted_kwds(df, attr):
    li_attr = attr + '_list'
    wt_attr = attr + '_wts'

    def sort_row(row):
        kwd_idx = np.argsort(np.array(row[wt_attr]))[::-1]
        row[li_attr] = np.array(row[li_attr])[kwd_idx].tolist()
        row[wt_attr] = np.array(row[wt_attr])[kwd_idx].tolist()
    df.apply(sort_row, axis=1)


def threshold_weighted_kwds(df, attr, min_relative_wt=0.1, min_cnt=10):
    li_attr = attr + '_list'
    wt_attr = attr + '_wts'

    def threshold_row(row):
        wts = np.array(row[wt_attr])
        words = np.array(row[li_attr])
        if len(wts) > 0:
            order = wts.argsort()[::-1]
            wts = wts[order]
            words = words[order]
            # keep if bigger than relative threshold
            keep = (wts / wts[0]) > min_relative_wt
            # keep at least min count
            keep[0:min(10, len(keep))] = True
            # mask to get kept values
            row[wt_attr] = wts[keep].tolist()
            row[li_attr] = words[keep].tolist()
        else:
            print(f"{row['name']} has no keywords")
        return row
    if min_relative_wt > 0 and min_cnt > 0:
        df = df.apply(threshold_row, axis=1)
    return df



def aggregate(df,
              groupCols,         # list of columns to grouby
              tagCols=[],        # list of columns to aggregate as tags
              txtCols=[],        # list of text columns to concatenate with " // " delimeter
              pickFirstCols=[],  # list of columns to just pick first one
              sumCols=[],        # list of columns to get sum
              maxCols=[],        # list of columns to get max
              meanCols=[],       # list of columns to get mean
              countCols=[],       # list of columns to get count
              ):
    # build aggregation operations
    agg_data = {col: (lambda x: '|'.join(x)) for col in tagCols}  # join as 'tags' separated by pipe
    agg_data.update({col: (lambda x: ' // '.join(x)) for col in txtCols})  # join as 'tags' separated by pipe
    agg_data.update({col: 'first' for col in pickFirstCols})
    agg_data.update({col: 'sum' for col in sumCols})
    agg_data.update({col: 'max' for col in maxCols})
    agg_data.update({col: 'mean' for col in meanCols})
    agg_data.update({col: 'count' for col in countCols})

    # fill empty tags
    for col in tagCols:
        df[col] = df[col].fillna('')
    # group and aggregate
    df_agg = df.groupby(groupCols).agg(agg_data).reset_index()
    # clean tags
    df_agg[tagCols] = clean_tag_cols(df_agg, tagCols, delimeter="|")
    df_agg[txtCols] = clean_tag_cols(df_agg, txtCols, delimeter=" // ")
    return df_agg


def aggregate_cat_fracs(df,
                        groupCols,  # list of columns to groupby
                        attr,  # column with value compute % (e.g. 'org typ')
                        value,  # value to tally if present (e.g. 'non-profit')
                        ):
    """creates fractions of values occurrence of strings/ categories within columns for groupby summaries"""
    df[attr] = df[attr].fillna('')
    df_count = df.groupby(groupCols).agg(
        {attr: [('Count_total', 'count'), (value + '_Count', lambda x: sum(x.str.contains(value)))]}).reset_index()
    df_count.columns = ['_'.join(col) for col in df_count.columns.values]
    df_count[value + '_frac'] = df_count[attr + '_' + value + '_Count'] / df_count[attr + '_Count_total']
    for i in range(len(groupCols)):
        col = df_count.columns[i]
        df_count.rename(columns={col: col.rstrip(col[-1])}, inplace=True)
    df_count = df_count.drop(columns=[attr + '_' + value + '_Count', attr + '_Count_total'])
    return df_count


def aggregate_and_fracs(df,
                        groupCols,  # list of columns to grouby
                        tagCols=[],  # list of columns to aggregate as tags
                        txtCols=[],  # list of text columns to concatenate with " // " delimeter
                        text_delim=" // ",  # custom delimeter for text concatenation
                        pickFirstCols=[],  # list of columns to just pick first one
                        sumCols=[],  # list of columns to get sum
                        maxCols=[],  # list of columns to get max
                        meanCols=[],  # list of columns to get mean
                        countCols=[],  # list of columns to get count
                        fracList=[],  # list of tuples with attr and value to get fractions
                        wtd_kwds=None  # tag that has associated weights that need to be aggregated
                        ):
    """similar to aggregate, but with added fractions of values occurrence within columns for groupby summaries"""
    # build aggregation operations
    # add elements into a list
    def _agg_lists(x):
        res = []
        for li in x:
            res.extend(li)
        return list(set(res))

    df = df.copy()
    agg_data = {col: _agg_lists for col in tagCols}
    # join as 'tags' separated by custom delimeter
    agg_data.update({col: (lambda x: text_delim.join(x)) for col in txtCols})
    agg_data.update({col: 'first' for col in pickFirstCols})
    agg_data.update({col: 'sum' for col in sumCols})
    agg_data.update({col: 'max' for col in maxCols})
    agg_data.update({col: 'mean' for col in meanCols})
    agg_data.update({col: 'count' for col in countCols})

    for col in tagCols:
        if type(df[col].iloc[0]) is not list:
            df[col] = df[col].apply(lambda x: [x] if type(x) is str else [])

    # group and aggregate
    grps = df.groupby(groupCols[0] if len(groupCols) == 1 else groupCols)
    df_agg = grps.agg(agg_data).reset_index()
    df_agg[txtCols] = clean_tag_cols(df_agg, txtCols, text_delim)

    # add fractions from fracList tuples
    for x in fracList:
        (attr, value) = x[0], x[1]
        df_frac = aggregate_cat_fracs(df, groupCols, attr, value)
        df_agg = df_agg.merge(df_frac, on=groupCols, how='left')

    # aggregate weighted keywords across each group
    def sum_dict_list(li):
        agg_vals = reduce(add, (map(Counter, li)))
        vals = np.array(list(agg_vals.values()))
        norm_vals = vals / vals.sum()
        return list(zip(agg_vals.keys(), norm_vals))

    if wtd_kwds is not None:
        li_attr = wtd_kwds + '_list'
        wt_attr = wtd_kwds + '_wts'
        results = {}
        for idx, _df in grps:
            if len(_df) == 1:
                results[idx] = list(zip(_df[li_attr].iloc[0], _df[wt_attr].iloc[0]))
            else:
                # print(idx)
                kwd_wts = _df.apply(lambda x: dict(zip(x[li_attr], x[wt_attr])), axis=1)
                results[idx] = sum_dict_list(kwd_wts.values.tolist())
        if len(groupCols) > 1:
            df_frac = pd.DataFrame([({gc: k[idx] for idx, gc in enumerate(groupCols)} | {'kwd_wts': v})
                                    for k, v in results.items()])
        else:
            df_frac = pd.DataFrame([{groupCols[0]: k, 'kwd_wts': v} for k, v in results.items()])
        df_frac[li_attr] = df_frac['kwd_wts'].apply(lambda x: [val[0] for val in x])
        df_frac[wt_attr] = df_frac['kwd_wts'].apply(lambda x: [val[1] for val in x])
        df_agg = df_agg.merge(df_frac.drop(columns=['kwd_wts']), on=groupCols, how='left')
        sort_weighted_kwds(df_agg, wtd_kwds)
    return df_agg


def compute_percent_value(
    df,  # dataframe
    group_cols,  # list of column(s) to group by
    attrib,  # the column to compute the fraction for
    value  # the value to tally if present
):
    """
    Compute the percent of entities in each group that have a given value
    :param df: dataframe
    :param group_cols: list of column(s) to group by
    :param attrib: the column to compute the fraction for
    :param value: the value to tally if present
    """
    df_ = df.copy()
    df_[attrib] = df_[attrib] == value
    df_grp = (
        df_
        .groupby(group_cols)[attrib]
        .mean(numeric_only=True)
        .mul(100)
        .round(0)
        .reset_index(name=f'pct_{value}')
    )
    # merge back to original df
    df_ = df_.merge(df_grp, on=group_cols, how='left')
    # return series
    return df_[f'pct_{value}']

# %% COMMON FUNCTIONS TO COMBINE CRUNCHBASE, CANDID, AND LINKEDIN DATA
# NOTE THIS WAS MOVED TO: scrape_enrich/combine_crunchbase_candid_linkedin.py
