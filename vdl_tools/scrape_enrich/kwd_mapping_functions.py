#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 25 06:47:46 2022

@author: ericberlow
"""

import vdl_tools.shared_tools.common_functions as cf # from common: commonly used functions
import vdl_tools.scrape_enrich.params as lp  # from common: commonly used parameters and filepaths
import vdl_tools.scrape_enrich.tags_from_text as tft 
import pandas as pd
import pathlib as pl
from ast import literal_eval

#%%
def reformat_original_tag_mapping(tagmap_name_original, tagmap_name_grouped):
        # convert long format tag mapping of unique search to master etc 
        # to list of search terms for each unique master term
    # load manual search term tag mapping
    df_kwds = pd.read_excel(tagmap_name_original, engine='openpyxl') # terms used in database search
    # aggregate search_terms and broad_terms into lists
    df_kwds['broad_terms'].fillna('', inplace=True)
    df_kwds['broad_terms'] = df_kwds['broad_terms'].apply(lambda x: x.split(",") if x != '' else []) # convert to list
    df_kwds_grouped = df_kwds.groupby('master_term').agg({'search_term': list, 
                                                                    'broad_terms': sum,
                                                                    'equity': max,
                                                                    'strategy': max}).reset_index()
    df_kwds_grouped['broad_terms'] = df_kwds_grouped['broad_terms'].apply(lambda x: list(set(x)))
    # write grouped version of climate kwd mapping
    df_kwds_grouped.to_excel(tagmap_name_grouped, index=False)
    return 
  


def get_suggested_terms_longformat(df_climate_kwds):
    # this is to compile terms from the original long search term > master term file
    # add dummy variable for grouping by all
    df_climate_kwds['dummy'] ='all' 
    # get list of master-terms
    df_master_terms = df_climate_kwds[['dummy', 'master_term']]
    master_terms = df_master_terms.groupby('dummy').agg(",".join).reset_index(drop=True)
    master_terms_list = list(set(master_terms['master_term'][0].split(",")))
    # get list of add-related
    df_add_related = df_climate_kwds[['dummy', 'add_related']]
    df_add_related = df_add_related[~df_add_related.add_related.isnull()]
    add_related = df_add_related.groupby('dummy').agg(",".join).reset_index(drop=True)
    add_related_list = list(set(add_related['add_related'][0].split(",")))
    # combine master terms with add related terms
    all_terms_list = master_terms_list
    all_terms_list.extend(add_related_list)
    all_terms = list(set(all_terms_list))
    # convert to dataframe
    df_suggested_terms = pd.DataFrame({'term': all_terms})
    df_suggested_terms.sort_values('term', inplace=True)
    
    return df_suggested_terms 

def get_suggested_list(climate_kwds_corpus_grouped,
                       master_term = 'master_term', # col name for main tag
                       broad_terms = 'broad_terms', # col name for broader tags
                       prior_only = False):  # only include terms from prior cleaned version
    # get list of suggested terms from kwd mapping corups - master term and broad terms            
    ## load grouped kwd mapping
    df_climate_kwds_grouped = pd.read_excel(climate_kwds_corpus_grouped, engine='openpyxl')
    string2list(df_climate_kwds_grouped, [broad_terms])
    if prior_only:
        priortags = (df_climate_kwds_grouped['prior_vs_new']=='prior tag') | (df_climate_kwds_grouped['prior_vs_new']=='prior missing')
        df_climate_kwds_grouped = df_climate_kwds_grouped[priortags]
    master_terms = df_climate_kwds_grouped[master_term].tolist()
    broad_terms = list(set(df_climate_kwds_grouped[broad_terms].sum()))
    suggested_terms = list(set(master_terms + broad_terms))
    return suggested_terms

def get_suggested_terms_freqs(df_tagged, suggested_terms):
    ## compile tag counts from simple n-gram match to generate primer suggested frequencies
    # build kwd tag counts
    df_climate_kwd_counts = cf.buildTagHistDf(df_tagged, 'climate_kwds')
    df_climate_kwd_counts.columns = ['term', 'count', 'freq']
    df_climate_kwd_counts['freq'] = df_climate_kwd_counts['freq']/100
    df_climate_kwd_counts = df_climate_kwd_counts[['term', 'freq']]
    # add freqs where possible to suggested list - otherwise add min 
    # convert suggested list to dataframe
    df_suggested_terms = pd.DataFrame({'term': suggested_terms})
    df_suggested_terms.sort_values('term', inplace=True)
    df_suggested_terms_freqs = df_suggested_terms.merge(df_climate_kwd_counts, on='term', how='left')
    df_suggested_terms_freqs['freq'].fillna(df_suggested_terms_freqs.freq.min(), inplace=True) 
    df_suggested_terms_freqs['frequency'] = 1 # option to set all to highest freq for suggestion in tagger
    # write results
    #df_suggested_terms_freqs.to_csv(lp.primer_topicspath/"climate_suggested_terms_freqs.csv", index=False)
    return df_suggested_terms_freqs


def get_remove_excluded_terms(df_annotated, excluded_topics_name, keep_groups=True):
    # add new excluded tags and their associated grouped terms to term exclusion file
    # df_selected: annotated file flagging terms and grouped terms for exclusions
    # excluded_topics
    # load selected / grouped terms from primer tagger - with tags flagged for exclusion
    #df_selected = pd.read_excel(selected_terms_name, engine='openpyxl') # selected tags, grouped terms, and manually flagged for exclusion
    #for col in ['grouped_terms']: #, 'suggested_terms']: 
     #   df_selected[col] = df_selected[col].apply(lambda x: literal_eval(x))
    
    # get prior excluded tags file
    df_exclude_prior = pd.read_excel(excluded_topics_name, engine='openpyxl') # custom exclude list from prior run, engine='openpyxl') # terms compiled from "raw_tag_hist.csv" 
    df_exclude_prior['tag'] = df_exclude_prior['tag'].astype(str) # convert any numbers to string
    string2list(df_exclude_prior, ['grouped_terms']) # convert string to list 
        
    # flag grouped terms lists to exclude 
    df_annotated['grouped_terms_exclude']= df_annotated.apply(lambda x: [] if x['excl_tag_only']==1 else x['grouped_terms'], axis=1)
    # remove excluded tags from grouped term list that are not excluded
    df_annotated['grouped_terms'] = df_annotated.apply(lambda x: [term for term in x.grouped_terms if term != x.tag ] if x['excl_tag_only']==1 
                                                           else x['grouped_terms'], axis=1)
    # compile all excluded terms and excluded grouped term lists
    df_exclude_new = df_annotated[df_annotated.exclude==1][['tag', 'grouped_terms_exclude']]
    df_exclude_new.reset_index(drop=True, inplace=True)
    # merge with existing excluded terms
    df_exclude = df_exclude_prior.merge(df_exclude_new, on='tag', how = 'outer')
    # fill empty with empty list
    df_exclude['grouped_terms_exclude'] = df_exclude['grouped_terms_exclude'].fillna('').apply(list)
    df_exclude['grouped_terms'] = df_exclude['grouped_terms'].fillna('').apply(list)
    # add both lists together in case they are a little different
    df_exclude['grouped_terms'] = df_exclude['grouped_terms'] + df_exclude['grouped_terms_exclude']
    df_exclude['grouped_terms'] = df_exclude['grouped_terms'].apply(lambda x: list(set(x))) # remove dupes
    df_exclude.drop('grouped_terms_exclude', axis=1, inplace=True)
    # fill exclude flag col
    df_exclude['exclude'] = 1
    
    # clean excluded tags and grouped terms lists from selected tags file 
    df_annotated_thinned = df_annotated[(df_annotated['exclude'] != 1) | (df_annotated['excl_tag_only'] == 1)]
    df_annotated_thinned = df_annotated_thinned.reset_index(drop=True)

    
    return df_annotated_thinned, df_exclude  # excluded terms added to exclude list and removed from df_selected
    
    
def string2list(df, cols):
    def _convert(val):
        if type(val) == list:
            return val
        if val != '':
            return literal_eval(val)
        return []

    for col in cols:
        df[col].fillna('', inplace=True)
        df[col] = df[col].apply(_convert)
        
def remove_terminal_nouns(df, termcol='tag'):
    removelist = ['program', 'programs', 
                  'initiative', 'initiatives', 
                  'system', 'systems', 
                  'platform', 'platforms', #'software',
                  'provider', 'providers',
                  #'service', 'services',
                  'company', 'project','startup', 
                  'solution', 'solutions',
                  #'product', 'products',
                  'toolkit', 'toolkits',
                  'manufacturer', 'conference'
                  ]
    stripped_term  = df[termcol].apply(lambda x: " ".join(x.split()[:-1]) if x.split()[-1] in removelist else x)
    return stripped_term # series

def get_unigrams_bigrams(df, term_col):
    # add word counts for terms in term_cols
    # get unigrams from tags with 2 or more words
    # get bigtrams from tags with 3 or more words
    # add ngram counts to selected and suggested
    df['n_words'] = df[term_col].apply(lambda x: len(x.split()))
    def get_ngram_list(df, n_words=1):
        ngram_list = df[df.n_words == n_words][term_col].tolist() 
        return ngram_list
    unigram_list = get_ngram_list(df, n_words=1)
    bigram_list = get_ngram_list(df, n_words=2)
    # subset longer n-grams
    df_longrams = df[df.n_words >2].reset_index(drop=True)
    df_bigrams = df[df.n_words >= 2].reset_index(drop=True)
      
    # get lists of selected or suggested unigrams and bigrams in longer tag n-grams
    df_longrams['tag_bigram'] = df_longrams['tag'].apply(lambda x: [bigram for bigram in bigram_list if bigram in x]) # includes substrings
    df_bigrams['tag_unigram'] = df_bigrams['tag'].apply(lambda x: [bigram for bigram in unigram_list if bigram in x.split()])
    ngram_bigram_dict = dict(zip(df_longrams['tag'], df_longrams['tag_bigram']))
    ngram_unigram_dict = dict(zip(df_bigrams['tag'], df_bigrams['tag_unigram']))
    
    # add unigram, bigram suggestions to term curation file
    def add_bigrams_unigrams(df, col, ngram_dict):
        df[col] = df['tag'].map(ngram_dict)
        df[col] = df[col].fillna('').apply(list)
        df["n_"+col] = df[col].apply(lambda x: len(x)) # if x != "" else 0)
    
    add_bigrams_unigrams(df, 'bigram', ngram_bigram_dict)
    add_bigrams_unigrams(df, 'unigram', ngram_unigram_dict)
    return df # with added word counts, unigrams, bigrams, unigram counts and bigram counts

def get_unigrams_bigrams_old(df_selected, suggested_list):
    # add tag word counts
    # get selected and suggested unigrams from tags with 2 or more words
    # get selected and suggested bigtrams from tags with 3 or more words
    df_suggested = pd.DataFrame({'term': suggested_list})
    # add ngram counts to selected and suggested
    df_selected['n_words'] = df_selected['tag'].apply(lambda x: len(x.split()))
    df_suggested['n_words'] = df_suggested.term.apply(lambda x: len(x.split())) 
    def get_ngram_list(df_selected, df_suggested, n_words=1):
        selected_list = df_selected[df_selected.n_words == n_words]['tag'].tolist() 
        suggested_list = df_suggested[df_suggested.n_words == n_words]['term'].tolist() 
        ngram_list = list(set(selected_list + suggested_list))
        return ngram_list
    unigram_list = get_ngram_list(df_selected, df_suggested, n_words=1)
    bigram_list = get_ngram_list(df_selected, df_suggested, n_words=2)
    # subset longer n-grams
    df_longrams = df_selected[df_selected.n_words >2].reset_index(drop=True)
    df_bigrams = df_selected[df_selected.n_words >= 2].reset_index(drop=True)
      
    # get lists of selected or suggested unigrams and bigrams in longer tag n-grams
    df_longrams['tag_bigram'] = df_longrams['tag'].apply(lambda x: [bigram for bigram in bigram_list if bigram in x]) # includes substrings
    df_bigrams['tag_unigram'] = df_bigrams['tag'].apply(lambda x: [bigram for bigram in unigram_list if bigram in x.split()])
    ngram_bigram_dict = dict(zip(df_longrams['tag'], df_longrams['tag_bigram']))
    ngram_unigram_dict = dict(zip(df_bigrams['tag'], df_bigrams['tag_unigram']))
    
    # add unigram, bigram suggestions to term curation file
    def add_bigrams_unigrams(df, col, ngram_dict):
        df[col] = df['tag'].map(ngram_dict)
        df[col] = df[col].fillna('').apply(list)
        df["n_"+col] = df[col].apply(lambda x: len(x)) # if x != "" else 0)
    
    add_bigrams_unigrams(df_selected, 'bigram', ngram_bigram_dict)
    add_bigrams_unigrams(df_selected, 'unigram', ngram_unigram_dict)
    return df_selected # with added word counts, unigrams, bigrams, unigram counts and bigram counts

def clean_prior_collapsed_kwds(df):
    # identify prior  master terms that were collapsed into grouped_terms list
    # add the associated search lists from that master term into the grouped_terms listassociated search lists that 
    # remove these terms from main tags df
    df_prior = df[df.prior_vs_new == 'prior tag']
    prior_dict = dict(zip(df_prior.master_term, df_prior.search_term))
    # add list of prior kwds that are in new grouped terms
    df['collapsed_prior'] = df['grouped_terms'].apply(lambda x: [kwd for kwd in prior_dict.keys() if kwd in x])
    # add list of prior search terms associated with the prior kwds that are in new group
    df['add_list'] = df['grouped_terms'].apply(lambda x: [prior_dict[kwd] for kwd in prior_dict.keys() if kwd in x])
    df['add_list'] = df['add_list'].apply(lambda x: sum(x, []))
    df['grouped_terms'] = df.apply(lambda x: list(set(x['grouped_terms'] + x['add_list'])), axis=1)
    # remove collapsed prior kwds 
    collapsed_prior_kwds = df['collapsed_prior'].sum()
    df = df[~df.tag.apply(lambda x: x in collapsed_prior_kwds)].reset_index(drop=True)
    return df


def flag_tags_in_mult_grouped(df):
    df = df[['tag','grouped_terms']]
    df_melt = df.explode('grouped_terms') # explode grouped term list
    # aggregate by grouped term to get list of tags that occur more than once
    df_reagg = df_melt.groupby('grouped_terms').agg({'tag': list}).reset_index()
    df_reagg['n_tags'] = df_reagg['tag'].apply(lambda x: len(x))
    # subset tags that occur in more than one grouped list
    df_dupes = df_reagg[df_reagg['n_tags']>1]
    # explode mult tags 
    df_dupe_group = df_dupes.explode('tag') 
    # combine mult duped search terms into list by tag
    df_dupe_bytag = df_dupe_group.groupby('tag')['grouped_terms'].agg(list).reset_index()
    tag_dupes_dict = dict(zip(df_dupe_bytag['tag'], df_dupe_bytag['grouped_terms']))
    # add list of duplicated group terms to tag file
    df = df.merge(df_dupe_bytag, on='tag', how='left') 
    # return dict of {tag: [list of grouped terms found elsewhere]}, and summary file by grouped term
    return tag_dupes_dict, df_dupe_group # tags and grouped_term that is shared with other tag 
 
def compile_suggested_grouped_lists(row, tag_grouped_dict):
    # where multiple suggested terms collapsed into new tag - combine search lists of prior suggested terms
    # tag_search_dict is {tag: grouped term list}
    if row['prior_vs_new'] == 'new tag':
        add_grouped_list = []
        for term in row['suggested_terms']:
            if term in tag_grouped_dict.keys():
                add_grouped_list.extend(tag_grouped_dict[term])
    else:
        add_grouped_list = []
    return add_grouped_list
    
    
def combine_with_orig_climate_kwd_corups(climate_kwds_name, df_annotated):
    # merge annotated terms with climate kwd dictionary 
    # df_climate_kwds is the grouped climate kwd corpus ('search_term' is list of terms)
    # df_annotated is the annotated selected terms - stripped of exclude term
    df_climate_kwds = pd.read_excel(climate_kwds_name, engine='openpyxl')
    string2list(df_climate_kwds, ['search_term', 'broad_terms'])
    df = df_annotated.merge(df_climate_kwds, left_on="tag", right_on="master_term", how='outer')
    # fill empty with list
    for col in ['grouped_terms', 'broad_tags', # from annotated tags
                'search_term', 'broad_terms']: # from grouped climate kwd corpus
        df[col] = df[col].fillna("").apply(list)  #fill emptyh with list

    
    # flag differences 
    df[['tag', 'master_term']] = df[['tag', 'master_term']].fillna('')
    df['prior_vs_new'] = df.apply(lambda x: 'prior tag'if x['tag'] == ''  
                                      else 'new tag' if x['master_term'] == ''  
                                      else 'both', axis=1)
        
    # update tag with prior master terms 
    df['tag'] = df.apply(lambda x: x.master_term if x.tag == '' else x.tag, axis = 1)
    # add  update grouped terms with prior 'search terms'
    df['grouped_terms'] = df.apply(lambda x: list(set(x['search_term'] + x['grouped_terms'])), axis=1)
    # update broad tags with prior broad 'add related' terms
    df['broad_tags'] = df.apply(lambda x: list(set(x['broad_tags'] + x['broad_terms'])), axis=1)

    # clean collapsed kwds and search lists
    df = clean_prior_collapsed_kwds(df)
    
    df['new_grouped_terms'] = df.apply(lambda x: list(set(x['grouped_terms'])-set(x['search_term'])), axis=1)
    df['new_broad_tags'] = df.apply(lambda x: list(set(x['broad_tags'])-set(x['broad_terms'])), axis=1)
    for col in ['new_grouped_terms','new_broad_tags' ]:
        df['n_'+col] = df[col].apply(lambda x: len(x))
    
    return df
   
def clean_broad_tags(df_kwd_map):
    # clean manual broad tags:
        # map to master term if already in other search term groups
        # add as master term with no search terms if not already there 
    
    # first get search > master tag dict
    df_search2master = df_kwd_map.explode('grouped_terms')[['tag', 'grouped_terms']]
    df_search2master = df_search2master.groupby('grouped_terms')['tag'].agg('first').reset_index()
    # dictionary of all unique search terms
    search2master = dict(zip(df_search2master['grouped_terms'], df_search2master['tag']))
    # replace broad tag that is in grouped search terms with the mapped master tag
    df_kwd_map['broad_tags'] = df_kwd_map['broad_tags'].apply(lambda x: [term if term not in search2master.keys() else search2master[term] for term in x])
    # find broad tags that don't have a master tag home
    broad_tags =  list(set(df_kwd_map['broad_tags'].sum()))
    tags = df_kwd_map['tag'].tolist()
    grouped_terms = list(set(df_kwd_map['grouped_terms'].sum()))
    new_broad = [tag for tag in broad_tags if (tag not in (grouped_terms) and (tag not in tags))]
    df_new_tags = pd.DataFrame({"tag": new_broad})
    # fill other columns
    df_new_tags['rem_search'] = 1
    # concatenate new tags to main df 
    df_kwd_map_broadclean = pd.concat([df_kwd_map, df_new_tags])
    for col in ['broad_tags', 'grouped_terms']:
        df_kwd_map_broadclean[col] = df_kwd_map_broadclean[col].fillna('').apply(list)
    for col in ['excl_tag_only','split_grp','equity', 'strategy']:
        df_kwd_map_broadclean[col] = df_kwd_map_broadclean[col].fillna(0)
    
    return df_kwd_map_broadclean



####################
# MAIN FUNCTIONS FOR  manual annotationand cleaning of tagger output and main kwd mapping corups  - 

#################### PREPARE TAGGER OUTPUT FOR CLEANING
def prepare_tagger_selected_terms_for_cleaning(climate_kwds_corpus, # name of cleaned kwds corpus
                                               tagger_selected_terms, # name of tagger output file 
                                               tagger_terms_to_clean # name of processed file for annotating
                                               ):
    ## process selected term > grouped term output from tagger 
    ## combine with prior cleaned kwds mapping 
    ## write file for annotating to be cleaned. 
    
    # load cleaned kwd corups (cleaned selected terms, grouped terms etc )
    df_kwds = pd.read_excel(climate_kwds_corpus, engine='openpyxl')
    string2list(df_kwds, ['broad_tags', 'grouped_terms'])
    df_kwds.rename(columns={'tag': 'master_term',
                            'grouped_terms': 'search_terms',
                            }, inplace=True)
    annotation_cols = ['exclude','excl_tag_only','rem_search','split_grp','use_sugg']
    kwd_keepcols = annotation_cols+['master_term', 'search_terms', 'broad_tags', 'equity', 'strategy']
    df_kwds = df_kwds[kwd_keepcols]
 
    # load and clean tagger output of selected terms / group terms
    df_selected_new = pd.read_csv(tagger_selected_terms)
    string2list(df_selected_new, ['suggested_terms','grouped_terms'])
    selected_keepcols= ['tag','grouped_terms']
    df_selected_new = df_selected_new[selected_keepcols]
    
    
    # combine new with prior kwds  
    df_toclean = df_selected_new.merge(df_kwds, left_on="tag", right_on="master_term", how="outer")
    df_toclean = df_toclean.reset_index(drop=True)
    # fill empty with list
    for col in ['grouped_terms',   # from tagger selected 
                'search_terms', # from kwd corpus
                'broad_tags']: # from kwd corpus
        df_toclean[col] = df_toclean[col].fillna("").apply(list)  #fill emptyh with list
    # fill empty annotation cols
    for col in annotation_cols:
        df_toclean[col].fillna(0, inplace=True)
    df_toclean['new_tag'] = '' 
    # flag differences 
    df_toclean[['tag', 'master_term']] = df_toclean[['tag', 'master_term']].fillna('')
    df_toclean['prior_vs_new'] = df_toclean.apply(lambda x: 'prior missing'if x['tag'] == ''  
                                      else 'new tag' if x['master_term'] == ''  
                                      else 'prior tag', axis=1)
    # update tag with prior master terms 
    df_toclean['tag'] = df_toclean.apply(lambda x: x.master_term if x.tag == '' else x.tag, axis = 1)
    
    # update grouped terms with prior 'search terms'
    df_toclean['grouped_terms'] = df_toclean.apply(lambda x: list(set(x['search_terms'] + x['grouped_terms'])), axis=1)
    # remove broad tags from search where flagged
    df_toclean['grouped_terms'] = df_toclean.apply(lambda x: [t for t in x['grouped_terms'] if t != x['tag']] if x['rem_search'] ==1 
                                                               else x['grouped_terms'], axis=1)
    df_toclean['new_grouped_terms'] = df_toclean.apply(lambda x: list(set(x['grouped_terms'])-set(x['search_terms'])), axis=1)
    
 
    # add list of suggested terms in the group
    # get suggesteed terms
    suggested_term_list = get_suggested_list(climate_kwds_corpus, master_term='tag', broad_terms='broad_tags')
    df_toclean['suggested_terms'] = df_toclean['grouped_terms'].apply(lambda x: [term for term in x if term in suggested_term_list])
    df_toclean['prior_sugg_picked'] = df_toclean.apply(lambda x: "No Suggested" if len(x.suggested_terms)==0 
                                                                  else True if (x.tag in x.suggested_terms)
                                                                  else False, axis=1)
    
    # remove prior missing tags if already flagged in suggested term lists
    missed_prior_found = df_toclean[df_toclean['prior_vs_new']=='new tag']['suggested_terms'].sum()
    df_prior_found = df_toclean[df_toclean['tag'].apply(lambda x: x in missed_prior_found)]
    suggested_tag_grouped_dict = dict(zip(df_prior_found['tag'], df_prior_found['grouped_terms']))
    df_toclean = df_toclean[~(df_toclean['tag'].apply(lambda x: x in missed_prior_found))]
    df_toclean['sugg_grouped_toadd'] = df_toclean.apply(lambda x: compile_suggested_grouped_lists(x, suggested_tag_grouped_dict), axis=1)
 
    # add list of tag terms in the group list
    df_toclean['redundant_terms'] = df_toclean.apply(lambda x: [term for term in x['grouped_terms'] if 
                                                                        ((x['tag'] in term) & (x['tag'] != term))], axis=1)
    
    df_toclean['search_terms'] = df_toclean.apply(lambda x: list(set(x['grouped_terms'])-set(x['redundant_terms'])) 
                                                                              if x['rem_search']!=1 
                                                                              else x['grouped_terms'], axis=1)
 
    # remove dups in lists and count terms
    for col in ['broad_tags', 'grouped_terms','suggested_terms', 'new_grouped_terms',
                'redundant_terms', 'search_terms', ]:
        df_toclean[col] = df_toclean[col].apply(lambda x: list(set(x)))
        df_toclean['n_'+col] = df_toclean[col].apply(lambda x: len(x))
    
    
    # flag terms in multiple tag groups for cleaning
    tag_dupe_dict, df_dup_group = flag_tags_in_mult_grouped(df_toclean)
    df_toclean['duped_group_terms']  = df_toclean['tag'].map(tag_dupe_dict)
    # write summary of duped group terms and associate tag
    df_dup_group.to_excel(lp.primer_topicspath/"dup_groups_toclean.xlsx", index=False)

    cleancols = [
            'exclude', # exclude tag
            'excl_tag_only', # keep group even if tag excluded
            'rem_search', # keep tag but remove from  group (search terms)
            'split_grp', # group/tag needs to be split
            'use_sugg', # replace selected tags with suggested term
            'tag',
            'new_tag',
            'suggested_terms', # list of suggested terms in the group
            'broad_tags',
            'grouped_terms',
            'search_terms',
            'redundant_terms',
            'new_grouped_terms',
            'sugg_grouped_toadd',
            'duped_group_terms',
            'prior_vs_new',
            'prior_sugg_picked',
            'n_broad_tags',
            'n_grouped_terms',
            'n_suggested_terms',
            'n_new_grouped_terms',
            'n_redundant_terms',
            'n_search_terms',

            'equity', 
            'strategy',
            ]
    df_toclean = df_toclean[cleancols]
    df_toclean.to_excel(tagger_terms_to_clean, index=False)
    return df_toclean


#################### PROCESS ANNOTATIONS TO GENERATE CLEANED KWD MAPPIGN CORPUS
#################### NOTE - THIS CAN BE ITERATED - IF MARK UP THE OUTPUT FILE AND RE-NAME AS 'ANNOTATED' FILE 
def process_annotated_terms(kwd_map_new_annotated, # name of manually annotated terms  
                                    kwd_map_prior, # name of cleaned kwd map to compare it to                                 
                                    excluded_topics,  # name of file with compiled exclude terms and grouped terms
                                    #excluded_topics_old, # name to save input excluded topics as prior version
                                    kwd_map_new_cleaned, # name of new cleaned kwd corpus for tag mapping
                                    topics_dir, # directory of primer topics and kwd annotations
                                    ):
    print("\nProcessing keyword map annotations")
    # process manually annotated/cleaned selected terms
    
    # load selected terms manually annotated
    df_annotated = pd.read_excel(kwd_map_new_annotated, engine='openpyxl')
    
    # format lists
    string2list(df_annotated, ['suggested_terms','broad_tags', 'grouped_terms']) #'bigram','unigram',])
 
    
    # strip out excluded and add to existing
    # load existing excludedd
    df_exclude = pd.read_excel(excluded_topics, engine='openpyxl')
    #df_exclude.to_excel(excluded_topics_old, index=False ) # save as 'old' to be safe
    string2list(df_exclude, ['grouped_terms'])
    
    # remove excluded tags from group lists // add all new excluded terms and lists to prior exclusion file
    df_annotated_thinned,df_exclude  = get_remove_excluded_terms(df_annotated, excluded_topics, keep_groups=True)
    df_exclude.to_excel(excluded_topics, index=False ) # write over exisitng with updated data
    
    # replace tag with new tag
    df_annotated_thinned['tag'] = df_annotated_thinned['new_tag'].fillna(df_annotated_thinned['tag'])
    # replace tag with suggested term where indicated
    df_annotated_thinned['tag'] = df_annotated_thinned.apply(lambda x: x['suggested_terms'][0] if x['use_sugg']==1 else x['tag'], axis=1)
    
    # strip terminal nouns 
    #df_annotated_thinned['tag'] = remove_terminal_nouns(df_annotated_thinned, termcol = 'tag')
    
    # add updated tag to grouped terms in case there are new ones    
    df_annotated_thinned['grouped_terms'] = df_annotated_thinned.apply(lambda x: list(set(x['grouped_terms']+ [x['tag']])), axis=1)
 
    
    # group by new unique tag and summarize grouped terms, add related terms
    df_annotated_agg = df_annotated_thinned.groupby('tag').agg({
                                                        'broad_tags': sum,
                                                        'grouped_terms': sum,                                                      
                                                        'split_grp': max, # review to manually split group
                                                        'excl_tag_only': max, # exclude main tag (map to new_tag) and keep grouped list
                                                        #'keep_grp': max, # keep group if term excluded
                                                        'rem_search': max,  # keep term but rem from grouped search list 
                                                        'equity': max, # flag for terms related to climate equity
                                                        'strategy': max, # flag for terms that are how not what
                                                        'eq_strat_keep': max, # equity and strategy terms to also keep as main kwd tags. 
                                                        #'new_tag_prior': list
                                                      }).reset_index()
    
    # clean broad tags (replace with common master term if mapped elsewere, add as tag if not already done)
    df_annotated_agg = clean_broad_tags(df_annotated_agg)
    
    # merge with prior cleaned corpus
    # TODO: make this into a function cuz it's repetitive with function: prepare_tagger_selected_terms_for_cleaning
    
    # get suggested terms from prior cleaned corpus
    suggested_terms = get_suggested_list(kwd_map_prior, master_term='tag', broad_terms='broad_tags')  # original suggested
   
    df_annotated_agg['prior_vs_new'] = df_annotated_agg.apply(lambda x: 'prior tag' if x['tag'] in suggested_terms
                                      else 'new tag' if x['tag'] not in suggested_terms
                                      else 'error', axis=1)
    
    # remove terms from search where flagged
    rem_from_search = df_annotated_agg[df_annotated_agg['rem_search'] == 1]['tag'].tolist()
    df_annotated_agg['grouped_terms'] = df_annotated_agg['grouped_terms'].apply(lambda x: [t for t in x if t not in rem_from_search])
    
    # add list of suggested terms in the group
    df_annotated_agg['suggested_terms'] = df_annotated_agg['grouped_terms'].apply(lambda x: [term for term in x if term in suggested_terms])
    # add list of tag terms in the group list
    df_annotated_agg['redundant_terms'] = df_annotated_agg.apply(lambda x: [term for term in x['grouped_terms'] if 
                                                                        ((x['tag'] in tft._get_ngrams_nltk(term)) & (x['tag'] != term))], axis=1)
    
    df_annotated_agg['search_terms'] = df_annotated_agg.apply(lambda x: list(set(x['grouped_terms'])-set(x['redundant_terms'])) 
                                                                              if x['rem_search']!=1 
                                                                              else x['grouped_terms'], axis=1)

    # remove dups in lists and count terms
    for col in ['broad_tags', 'grouped_terms','suggested_terms', 
                'redundant_terms', 'search_terms']:
        df_annotated_agg[col] = df_annotated_agg[col].apply(lambda x: list(set(x)))
        df_annotated_agg['n_'+col] = df_annotated_agg[col].apply(lambda x: len(x))
    
   
    # add unigrams, bigrams and their counts
    df_annotated_agg = get_unigrams_bigrams(df_annotated_agg, 'tag')
    
    # fill remaining manual annotation attributes
    for col in ['split_grp', 'rem_search', 'equity', 'strategy','eq_strat_keep']:
        df_annotated_agg[col].fillna(0, inplace=True)
    df_annotated_agg['use_sugg'] = 0
    df_annotated_agg['exclude'] = 0
    df_annotated_agg['excl_tag_only'] = 0
    df_annotated_agg['new_tag'] = ''

    
    df_annotated_agg['prior_sugg_picked'] = df_annotated_agg.apply(lambda x: "No Suggested" if len(x.suggested_terms)==0 
                                                                  else True if (x.tag in x.suggested_terms)
                                                                  else False, axis=1)
    # flag terms in multiple tag groups for cleaning
    tag_dupe_dict, df_dup_group = flag_tags_in_mult_grouped(df_annotated_agg)
    df_annotated_agg['duped_group_terms']  = df_annotated_agg['tag'].map(tag_dupe_dict)
    # write summary of duped group terms and associate tag
    df_dup_group.to_excel(topics_dir/"dup_groups_cleaned.xlsx", index=False)
     
     
    cols = [
            'exclude', # exclude tag
            'excl_tag_only', # keep group even if tag excluded
            'rem_search', # keep tag but remove from  group (search terms)
            'split_grp', # group/tag needs to be split
            'equity', 
            'strategy',
            'eq_strat_keep',
            'tag',
            'new_tag',
            'grouped_terms',
            'broad_tags',
            'unigram',
            'bigram',
            'search_terms',
            'prior_vs_new', 
            'suggested_terms', # list of suggested terms in the group
            'duped_group_terms',
            'n_grouped_terms',
            'n_search_terms',     
            'n_suggested_terms',
            'n_words',            
            'n_bigram',
            'n_unigram',
            'prior_sugg_picked',
            'redundant_terms',
            'n_redundant_terms',
            'use_sugg', # replace selected tags with suggested term
            ]
    
    # write cleaned annotated tags 
    df_cleaned =  df_annotated_agg[cols]  
    df_cleaned.to_excel(kwd_map_new_cleaned, index=False)
    return df_cleaned
   

    
 #%%   

if __name__ == "__main__":

    pr = pl.Path.cwd()
    topicspath = lp.prjpath/"data"/"topics"

    kwds_rootname = "climate_kwd_map_"
    kwds_version = "2022-08-01"
    suffix = ""

    # hierarchical tagger output file
    tagger_selected_raw = topicspath/"climate_selected_term_counts.csv" # output from tagger

    # kwds mapping filenames
    #climate_kwds_map_orig = cp.common_kwds/"climate_kwd_corpus_grouped_2022.xlsx" # original 2022 tag mapping (grouped into lists)
    climate_kwd_map_versioned = topicspath/(kwds_rootname + kwds_version + suffix + ".xlsx") # latest cleaned climate kwds map search terms > master term > add related

    climate_kwd_map_new_toclean = topicspath/(kwds_rootname + "new_to_clean" + ".xlsx") # new search + grouped terms formatted for annotation/cleaning
    climate_kwd_map_new_annotated = topicspath/(kwds_rootname + "new_annotated" + ".xlsx") # new kwd map manually annotated for cleaning 
    climate_kwd_map_new_cleaned = topicspath/(kwds_rootname + "new_cleaned" + ".xlsx") # new kwd map cleaned     

    # term exclusion files
    topics_exclude = topicspath/"topics_to_exclude.xlsx" # custom exclude list from prior run
    topics_exclude_old = topicspath/"topics_to_exclude_old.xlsx" # saved prior version 
    
    

#%% process and prepare raw tagger selected terms for cleaning
    df_toclean = prepare_tagger_selected_terms_for_cleaning(climate_kwd_map_versioned, # name of latest version of cleaned kwds corpus
                                                   tagger_selected_raw, # name of tagger output file 
                                                   climate_kwd_map_new_toclean # name of processed file for annotating
                                                  )
#%% 

##################################
## manually annotate df_toclean ##
## save as  "climate_selected_terms_annotated.xlsx"  
## process annotated file
## repeat as necessary
##################################

#%% process annotations into new cleaned kwds mapping file
    df_cleaned = process_annotated_terms(climate_kwd_map_new_annotated, # name of manually annotated terms                                     
                                          climate_kwd_map_versioned, # name latest version of cleaned kwd map to compare it to                                 
                                          topics_exclude,  # name of file with compiled exclude terms and grouped terms
                                          topics_exclude_old, # name to save input excluded topics as prior version
                                          climate_kwd_map_new_cleaned, # name of new cleaned kwd corpus for tag mapping
                                        )

#%% testing
    xl = engine='openpyxl'
    df_exclude = pd.read_excel(topics_exclude, engine=xl)
    df_exclude_test = pd.read_excel(topicspath/"topics_to_exclude__.xlsx", engine=xl)
    for df in [df_exclude, df_exclude_test]:
        string2list(df, ['grouped_terms'])
    exclude_tags = df_exclude.tag.tolist()
    exclude_tags_test = df_exclude_test.tag.tolist()
    exclude_in_test = [tag for tag in exclude_tags if tag in exclude_tags_test]
    
    exclude_grps = df_exclude.grouped_terms.sum()
    exclude_grps_test = df_exclude_test.grouped_terms.sum()
    excludegrp_in_testgrp = [tag for tag in exclude_grps if tag in exclude_grps_test]
    
    df_kwdmap = pd.read_excel(climate_kwd_map_new_cleaned, engine = xl)
    string2list(df_kwdmap, ['search_terms'])
    taglist = df_kwdmap.tag.tolist()
    grplist = df_kwdmap.group_terms_thinned.sum()
    tags_inexclude = [tag for tag in taglist if tag in exclude_tags]
    grp_inexclude = [tag for tag in grplist if tag in exclude_grps]
