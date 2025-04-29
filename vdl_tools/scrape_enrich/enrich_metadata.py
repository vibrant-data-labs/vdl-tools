#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 16:07:26 2021

@author: ericberlow
"""

#
#

import os
from vdl_tools.shared_tools.common_functions import (
    join_strings_no_missing,
    build_tag_hist_df,
    clean_empty_tags,
    clean_tags,
)


from vdl_tools.scrape_enrich.geocode import add_geo_lat_long
from vdl_tools.scrape_enrich.process_images import add_profile_images
import vdl_tools.scrape_enrich.tags_from_text as tags
from vdl_tools.scrape_enrich.googletranslate import translate_cols
import vdl_tools.scrape_enrich.params as p
import pandas as pd
from vdl_tools.scrape_enrich.params import english_countries
import pathlib as pl


def write_excel_no_hyper(df, outname):
    # make sure the folder exists
    path = pl.Path(os.path.split(outname)[0])
    path.mkdir(exist_ok=True, parents=True)
    # write to excel without converting strings to hyperlinks
    writer = pd.ExcelWriter(
        outname,
        engine="xlsxwriter",
        engine_kwargs={
            "options": {"strings_to_urls": False},
        },
    )
    df.to_excel(writer, index=False, encoding="utf-8-sig")
    writer.close()


#################################################################################################
# TRANSLATE NON-ENGLISH
#################################################################################################
def translate_non_english_countries(df_w_geo, textcols, translated_filename, wd):
    # df must have geocoded 'country'
    # textcols - columns to translate
    # translated_filename - excel file to store results
    # identify non-english countries
    # translate non-english if not already translated
    # store results
    # returns df_w_geo with english and translated non-english
    print("\nTranslating profiles from non-english countries")
    # get list of id's from english countries
    df_w_geo["country"].fillna("", inplace=True)
    is_english_country = df_w_geo["country"].apply(
        lambda x: (x in english_countries) or (x == "")
    )
    # separate dfs for english vs  non-english countries
    # df_w_geo_english = df_w_geo[is_english_country]
    df_w_geo_nonenglish = df_w_geo[~is_english_country][
        ["id", "country"] + textcols
    ]  # subset the columns to translate
    # check non-english for already translated - keep new ones
    if translated_filename.is_file():
        df_translated = pd.read_excel(translated_filename, engine="openpyxl")
        print("\nLoading - %s prior translated profiles" % str(len(df_translated)))
        # get new ones not already translated
        translated = df_translated["id"].tolist()
        df_to_translate = df_w_geo_nonenglish[
            ~df_w_geo_nonenglish["id"].isin(translated)
        ]  # keep ones not already translated
    else:
        df_to_translate = df_w_geo_nonenglish  # translate all if none stored
    if len(df_to_translate) > 0:
        # if new ones to translate
        # translate new non-english - add and save
        print(
            "\nTranslating %s non-english country profiles" % str(len(df_to_translate))
        )
        df_new_translated = translate_cols(df_to_translate, textcols, target="en")
        if p.linkedIn_orgs_translated(wd).is_file():
            # update existing translated records if they exist
            df_translated = pd.concat(
                [df_translated, df_new_translated], ignore_index=True
            )
        else:  # otherwise store newly translated ones
            df_translated = df_new_translated
        for col in textcols:  # clean empty
            df_translated[col + "_en"].fillna("", inplace=True)
            # remove empty results from google translate
            df_translated[col + "_en"] = df_translated[col + "_en"].apply(
                lambda x: "" if x == "in" else x
            )
        # store translated results
        write_excel_no_hyper(
            df_translated, translated_filename
        )  # write updated translated file

    if len(df_translated) > 0:
        # if translated results exist
        # merge with english with translated
        print("\nAdding %s translated profiles" % str(len(df_translated)))
        dropcols = ["country"] + textcols
        df_translated.drop(dropcols, axis=1, inplace=True)  # remove for merging
        df_w_geo_translated = df_w_geo.merge(df_translated, on=["id"], how="left")
        # replace original text with translated text
        for col in textcols:
            df_w_geo_translated[col + "_en"].fillna("", inplace=True)
            df_w_geo_translated[col] = df_w_geo_translated.apply(
                lambda x: x[col + "_en"] if x[col + "_en"] != "" else x[col], axis=1
            )
            df_w_geo_translated.drop([col + "_en"], axis=1, inplace=True)

    else:  # just return the original if no translated to add
        print("\nNo translated profiles")
        df_w_geo_translated = df_w_geo
    return df_w_geo_translated


#################################################################################################
# ADDITIONAL DEFAULT META-DATA CLEANING / ENRICHMENT
#################################################################################################

# PEOPLE #
def prep_metadata_ppl(df):
    df["top_card"].fillna("", inplace=True)
    df["Location"] = df.top_card.apply(lambda x: x.split("@ ")[-1].replace(";", ""))
    return df


def clean_meta_ppl(df, finalMetaCols_ppl=[], summarize_new_geo=True):

    print("\nCleaning and Adding other custom metadata")
    df.reset_index(drop=True, inplace=True)  # trying to avoid copy of slice error....
    # add "city, state, country" as string for dispay in profile
    df["City_Region"] = join_strings_no_missing(df, ["city", "state"], delim=", ")
    # remove 'state' region if not US city,
    df["City_Region"] = df.apply(
        lambda x: x["City_Region"].split(", ")[0]
        if x["country"] != "United States"
        else x["City_Region"],
        axis=1,
    )
    df["City_Region"] = df.apply(
        lambda x: x.City_Region if x.City_Region != x.country else "", axis=1
    )  # remove city string if no city
    # full city, state (if US), country
    df["City"] = join_strings_no_missing(df, ["City_Region", "country"], delim=", ")
    df["City"] = df.apply(
        lambda x: x.City if x.City != x.country else "", axis=1
    )  # remove city string if no city
    # just US states
    df["State"] = df.apply(
        lambda x: x["state"] if x["country"] == "United States" else "", axis=1
    )  # keep State for US locations
    df["Country"] = df["country"]
    # add location City, Country to Geo_Tags in case it was missed
    df["Geo_Tags"] = join_strings_no_missing(df, ["city", "country"], delim="|")
    df["Geo_Tags"] = clean_tags(
        df, "Geo_Tags", delimeter="|"
    )  # remove duplicate geo tags

    df.drop(
        ["City_Region", "city", "state", "country", "Address"], inplace=True, axis=1
    )

    # add 'skills' tags and counts from linkedin
    df["skills"].fillna("", inplace=True)
    df["skills_list"] = df["skills"].apply(lambda x: x.split("; ") if x != "" else [])
    df["skills"] = df["skills_list"].apply(lambda x: "|".join(x))
    df["n_skills"] = df["skills_list"].apply(lambda x: len(x))
    df.drop(["skills_list"], inplace=True, axis=1)

    if summarize_new_geo:
        # compile new geo place names and save as sep files
        df_newGeo = build_tag_hist_df(df, "new_GeoText", delimiter="|")
        df_newPlaces = build_tag_hist_df(df, "new_PlaceNames", delimiter="|")
        df_newGeo.to_csv("new_geo.csv")
        df_newPlaces.to_csv("new_places.csv")
    df.drop(["new_GeoText", "new_PlaceNames"], axis=1, inplace=True)
    # clean empty tags
    df["keywords"] = clean_empty_tags(df, "keywords")
    df["skills"] = clean_empty_tags(df, "skills")
    df["Geo_Tags"] = clean_empty_tags(df, "Geo_Tags")

    if len(finalMetaCols_ppl) > 0:
        df = df[finalMetaCols_ppl]
    df.reset_index(drop=True, inplace=True)
    return df


# ORGANIZATIONS #
def prep_metadata_orgs(df):
    df["Location"] = df["locations"].fillna(df["Headquarters"])
    return df


def clean_meta_orgs(df, finalMetacols_orgs, summarize_new_geo=True):
    print("\nCleaning and Adding other custom metadata")
    df.reset_index(drop=True, inplace=True)  # trying to avoid copy of slice error....
    # add "city, state, country" as string for dispay in profile
    df["City_Region"] = join_strings_no_missing(df, ["city", "state"], delim=", ")
    # remove 'state' region if not US city,
    df["City_Region"] = df.apply(
        lambda x: x["City_Region"].split(", ")[0]
        if x["country"] != "United States"
        else x["City_Region"],
        axis=1,
    )
    df["City_Region"] = df.apply(
        lambda x: x.City_Region if x.City_Region != x.country else "", axis=1
    )  # remove city string if no city
    # full city, state (if US), country
    df["City"] = join_strings_no_missing(df, ["City_Region", "country"], delim=", ")
    df["City"] = df.apply(
        lambda x: x.City if x.City != x.country else "", axis=1
    )  # remove city string if no city
    # just US states
    df["State"] = df.apply(
        lambda x: x["state"] if x["country"] == "United States" else "", axis=1
    )  # keep State for US locations
    df["Country"] = df["country"]
    # add location City, Country to Geo_Tags in case it was missed
    df["Geo_Tags"] = join_strings_no_missing(df, ["city", "country"], delim="|")
    df["Geo_Tags"] = clean_tags(
        df, "Geo_Tags", delimeter="|"
    )  # remove duplicate geo tags

    df.drop(
        ["City_Region", "city", "state", "country", "Address"], inplace=True, axis=1
    )

    if summarize_new_geo:
        # compile new geo place names and save as sep files
        df_newGeo = build_tag_hist_df(df, "new_GeoText", delimiter="|")
        df_newPlaces = build_tag_hist_df(df, "new_PlaceNames", delimiter="|")
        df_newGeo.to_csv("new_geo.csv")
        df_newPlaces.to_csv("new_places.csv")
    df.drop(["new_GeoText", "new_PlaceNames"], axis=1, inplace=True)

    # clean empty tags
    df["Geo_Tags"] = clean_empty_tags(df, "Geo_Tags")

    df = df[finalMetacols_orgs]
    df.reset_index(drop=True, inplace=True)
    return df


##########################################################################
# MAIN FUNCTIONS TO DO ALL ENRICHMENT - FOR PPL VS ORGS
#################################################################################################
def get_ppl_paths(wd):
    return dict(
        geocode_name=p.geocode_name_ppl(wd),  # file to store geocode results
        geopath=p.geopath(wd),  # directory to store geocode results
        kwdcorp=p.kwd_corpus,  # keyword mappign corpus filepath
        profcorp=None,  # p.prof_corpus, # professions mapping corpus filepath
        geocorp=p.geotag_corpus,  # geotags mapping corpus filepath
        translated_name=p.linkedIn_ppl_translated(
            wd
        ),  # file to store translated results
        kwds_name=p.kwds_name_ppl(wd),  # file to store keywords
        professiontags_name=p.proftags_name_ppl(wd),  # file to store profession tags
        tagspath=p.tagspath(wd),  # directory for storing tag results,
        customcorp=None,  # "None" by default - additional custom tagging corpus filepath
        customtags_name=p.tagspath(wd)
        / (p.custom_tag_col + "_ppl.xlsx"),  # file to store custom tags
        geotags_name=p.geotags_name_ppl(wd),  # file to store geotags PEOPLE
        images_name=p.images_name_ppl(wd),  # file to store image meta PEOPLE
        images_meta_path=p.images_path(wd),  # local directory to store image metadata
        images_directory=str(
            p.images_directory(wd)
        ),  # local directory to store image files (.png's)
        resultspath=p.resultspath(wd),  # path to store processed results
        enriched_w_tags_ppl=p.enriched_w_tags_ppl(wd),  # output file of enriched data
        enriched_no_tags_ppl=p.enriched_no_tags_ppl(
            wd
        ),  # output file of people with no keywords
    )


#################################################################################################
# ORGS


def get_org_paths(wd):
    return dict(
        geocode_name=p.geocode_name_orgs(wd),  # file to store geocode results
        geopath=p.geopath(wd),  # directory to store geocode results
        translated_name=p.linkedIn_orgs_translated(
            wd
        ),  # file to store translated results
        kwdcorp=p.kwd_corpus(),  # keyword mappign corpus filepath
        geocorp=p.geotag_corpus(),  # geotags mapping corpus filepath
        kwds_name=p.kwds_name_orgs(wd),  # file to store keywords
        tagspath=p.tagspath(wd),  # directory for storing tag results
        customcorp=None,  # additional custom tagging corpus filepath
        customtags_name=p.tagspath / (p.custom_tag_col + "_orgs.xlsx"),
        geotags_name=p.geotags_name_orgs(wd),  # file to store geotags
        images_name=p.images_name_orgs(wd),  # file to store image meta
        images_meta_path=p.images_path(wd),  # local directory to store image metadata
        images_directory=str(
            p.images_directory(wd)
        ),  # local directory to store image files (.png's)
        resultspath=p.resultspath(wd),  # path to store processed results
        enriched_w_tags_orgs=p.enriched_w_tags_orgs(wd),  # output file of enriched data
        enriched_no_tags_orgs=p.enriched_no_tags_orgs(
            wd
        ),  # output file of orgs with no keywords
    )


def add_metadata_orgs(
    df,
    wd,
    geocode_name=None,  # file to store geocode results
    geopath=None,  # directory to store geocode results
    addresscol="Location",  # default column to find location for geocoding
    translated_name=None,  # file to store translated results
    kwdcorp=None,  # keyword mappign corpus filepath
    geocorp=None,  # geotags mapping corpus filepath
    textcols=p.textcols_orgs,  # columns to search for keyword tags
    kwds_name=None,  # file to store keywords
    tagspath=None,  # directory for storing tag results,
    customcorp=None,  # additional custom tagging corpus filepath
    customtagcol=p.custom_tag_col,  # name of custom tag column
    customtags_name=None,  # file to store custom tags
    tag_blacklist=[],  # tags to blacklist
    geotags_name=None,  # file to store geotags
    images_name=None,  # file to store image meta
    images_meta_path=None,  # local directory to store image metadata
    images_directory=None,  # local directory to store image files (.png's)
    images_bucket=p.images_bucket,  # s3 bucket to store images
    resultspath=None,  # path to store processed results
    finalMetaCols=p.finalMetaCols_orgs,  # final columns to keep
    enriched_w_tags_orgs=None,  # output file of enriched data
    tag_attr=p.tag_attr,  # attribute used for linking
    mintags=0,  # min tags to keep
    enriched_no_tags_orgs=None,  # output file of orgs with no keywords
    loadExisitng_geocode=False,  # False = run/overwrite geocode for lat/long, city, state
    translate_non_english=False,  # True = run Google Translate for non-english countries
    loadExisting_tags=False,  # False = run/overwrite keyword tagging
    loadExisting_geotags=False,  # False = run/overwrite geotagging
    loadExisting_images=False,  # False = run/overwrite image processing
):
    """
    Add organization metadata.

    Main function to enrich the raw linkedin ORGANIZATIONS scrape with
    > geocoding lat/long, and structured city, state, country tags
    > translate profiles from non-english countries (before tagging)
    > keywords from social impact / professions corpus
    > other tags from custom corpus
    > geo tags - searching geographic place names in text
    > profile images
    > custom enrichment/cleaning for orgs
    -------
    kwdcorp : keyword corpus
    syscorp: systems tags
    Returns - id with linkedin metadata, also writes to a file
    """
    # pre-clean org metadata
    # df = prep_metadata_orgs(df) 

    # add geo (lat/longs, city, state, country from address) if processed file exists, otherwise geocode addresses
    df_w_geo = add_geo_lat_long(
        df, geocode_name, geopath, loadexisting=loadExisitng_geocode
    )

    if translate_non_english:
        # translate profiles for non-english countries if not already translated
        df_w_geo = translate_non_english_countries(df_w_geo, textcols, translated_name)

    # add keywords, profession tags from text
    # keywords
    if kwdcorp is not None:
        kwdcorp_df = pd.read_excel(kwdcorp, engine="openpyxl")  # keyword corpus
        print("\nAdding keywords")
        df_w_geo_kwds = tags.add_kwd_tags(
            df_w_geo,
            kwdcorp_df,
            kwds_name,
            tagspath,
            blacklist=tag_blacklist,
            kwds="keywords",
            textcols=textcols,
            loadexisting=loadExisting_tags,
        )

    # custom tags
    if customcorp is not None:
        customcorp_df = pd.read_excel(customcorp, engine="openpyxl")  # custom corpus
        print("\nAdding %s tags" % customtagcol)
        df_w_geo_kwds = tags.add_kwd_tags(
            df_w_geo_kwds,
            customcorp_df,
            customtags_name,
            tagspath,
            blacklist=tag_blacklist,
            kwds=customtagcol,
            textcols=textcols,
            loadexisting=loadExisting_tags,
        )

    # add profile images if processed file exists, otherwise scrape/process them from raw url
    df_w_geo_kwds_geotag_images = add_profile_images(
        df_w_geo_kwds,
        images_name,
        images_meta_path=images_meta_path,  # local directory to store image metadata
        idCol="id",  # column for merging
        nameCol="id",  # use for image filename
        imageURL="image_url",  # image source url
        image_directory=images_directory,  # local directory to store images
        bucket=images_bucket,  # s3 bucket to store images
        grayscale=False,  # covert to BW image
        loadExisting=loadExisting_images,
    )

    # clean raw organization metadata
    df_meta_final = clean_meta_orgs(
        df_w_geo_kwds_geotag_images, finalMetaCols, summarize_new_geo=False
    )

    # Get/remove profiles where linking tag attribute has less than min tags - write results to review/fix
    df_no_tags = df_meta_final[
        df_meta_final["n_" + tag_attr] < mintags
    ]  # save profiles with no tags
    df_no_tags = df_no_tags.reset_index(drop=True)
    df_meta_final = df_meta_final[
        df_meta_final["n_" + tag_attr] >= mintags
    ]  # remove profiles with no tags
    df_meta_final = df_meta_final.reset_index(drop=True)

    # write meta summary files
    print(
        "\nWriting file of %s orgs with metadata and linking tags"
        % str(len(df_meta_final))
    )
    resultspath.mkdir(exist_ok=True)  # create directory if it doesn't exist
    write_excel_no_hyper(
        df_meta_final, enriched_w_tags_orgs
    )  # save file of good profiles with tags
    print("%d profiles with less than %d linking tags" % (len(df_no_tags), mintags))
    write_excel_no_hyper(
        df_no_tags, enriched_no_tags_orgs
    )  # write file of thin profiles no tags

    return df_meta_final


# %%
if __name__ == "__main__":

    # set directories / files
    wd = pl.Path.cwd()
    resultspath = wd / "results"
    topicspath = wd / "data" / "topics"

    # get recipients and descriptive text
    df = pd.read_excel(topicspath / "cb_cd_org_terms_all.xlsx", engine="openpyxl")

    keepcols = ["id", "profile_name", "text", "terms", "n_terms"]
    df = df[keepcols]
    tags.string2list(df, ["terms"])  # format as lists
    df["termtext"] = df["terms"].apply(lambda x: ", ".join(x))
    df["text"] = join_strings_no_missing(df, ["text", "termtext"], delim="; ")

    # get tag mapping corpus
    df_tagmap_raw = pd.read_excel(
        topicspath / "climate_selected_terms_cleaned.xlsx", engine="openpyxl"
    )
    df_tagmap = tags.prepare_tagmap_df(
        df_tagmap_raw,
        "tag",  # master term
        "group_terms_thinned",  # search terms list
        "broad_tags",  # add_related list
        listcols=["broad_tags", "bigram", "unigram", "group_terms_thinned"],
    )
