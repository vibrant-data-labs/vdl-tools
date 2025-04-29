#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 16:09:43 2021

@author: ericberlow
"""

import os
import pathlib as pl  # path library

# network parameters
tag_attr = "keywords"  # tag attrib to use for linking
linksPer = 8
tag_blacklist = []

# DIRECTORIES
wd = pl.Path.cwd()  # current working directory
datapath = wd / "data"
resultspath = wd / "results"
scrapedpath = wd / "data" / "scraping"
images_path = wd / "data" / "images"  # logos data


def datapath(wd):
    return wd / "data"


def resultspath(wd):
    return wd / "results"


def scrapedpath(wd):
    return wd / "data" / "scraping"


def images_path(wd):
    return wd / "data" / "images"  # logos data


def geopath(wd):
    return wd / "data" / "geocode"  # geo data


def tagspath(wd):
    return wd / "data" / "tagging"  # tags data


def tagmap_path():
    return (
        pl.Path(os.path.split(__file__)[0]) / "keywords"
    )  # kwd and geo tag mapping corpus


# FILENAMES

# CRUNCHBASE DATA
def cb_data(wd):
    return datapath(wd) / "crunchbase"


def cb_company_searches(wd):
    return cb_data(wd) / "companies/*"


def cb_funding_searches(wd):
    return cb_data(wd) / "funding/*"


def cb_companies_cleaned(wd):
    return cb_data(wd) / ("cb_companies_cleaned.xlsx")


def cb_companies_cleaned_li(wd):
    return cb_data(wd) / ("cb_companies_cleaned_li.xlsx")


## METADTA ENRICHMENT / TAGGING
rootname = ""


def enriched_w_tags_ppl(wd):
    return resultspath(wd) / (
        rootname + "enriched_tags_ppl.xlsx"
    )  # people scraped, translated auto-enriched with metadata -  kwds, geo, images, etc


def enriched_w_tags_orgs(wd):
    return resultspath(wd) / (
        rootname + "enriched_tags_orgs.xlsx"
    )  # orgs, scraped, auto-enriched w metadata - kwds, geo, images, etc


# NO TAGS (after enrichment)
def enriched_no_tags_ppl(wd):
    return resultspath(wd) / (
        rootname + "enriched_no_tags_ppl.xlsx"
    )  # people scraped, translated auto-enriched with metadata -  kwds, geo, images, etc


def enriched_no_tags_orgs(wd):
    return resultspath(wd) / (
        rootname + "enriched_no_tags_orgs.xlsx"
    )  # orgs, scraped, auto-enriched w metadata - kwds, geo, images, etc



## TAG MAPPING DICTIONARIES
def kwd_corpus():
    return tagmap_path() / "kwd_corpus.xlsx"  # keyword dictionary


def prof_corpus():
    return tagmap_path() / "professions_corpus.xlsx"  # professions dictionary


custom_corpus = None  # add local path to custom keyword mapping file
custom_tag_col = "my_custom_tags"  # name of column for custom tags


def geotag_corpus():
    return tagmap_path() / "geo_tag_mapping.xlsx"  # geotag mapping dict


# Individual metadata files for kwds, geocode, geotags, images
# PEOPLE
def kwds_name_ppl(wd):
    return tagspath(wd) / "kwds_ppl.xlsx"  # processed tags file (keywords)


def proftags_name_ppl(wd):
    return tagspath(wd) / "professions_ppl.xlsx"  # processed tags file (professions)


def geocode_name_ppl(wd):
    return (
        geopath(wd) / "geocode_ppl.xlsx"
    )  # processed geocode file (lat/long, hq city, state, country)


def geotags_name_ppl(wd):
    return (
        tagspath(wd) / "geo_tags_ppl.xlsx"
    )  # processed geotags file (geographic places)


def images_name_ppl(wd):
    return (
        images_path(wd) / "profile_images_ppl.xlsx"
    )  # processed images with s3 urls file


# ORGS
def kwds_name_orgs(wd):
    return tagspath(wd) / "kwds_orgs.xlsx"  # processed tags file (keywords) - ORGS


def geocode_name_orgs(wd):
    return (
        geopath(wd) / "geo_orgs.xlsx"
    )  # processed geo file (lat/long, hq city, state, country) - ORGS


def geotags_name_orgs(wd):
    return (
        tagspath(wd) / "geo_tags_orgs.xlsx"
    )  # processed geotags file (geographic places) - ORGS


def images_name_orgs(wd):
    return (
        images_path(wd) / "profile_images_orgs.xlsx"
    )  # processed images with s3 urls file - ORGS


# metadata enrichment parameters
textcols_ppl = [
    "title",
    "about",
    "experiences",
    "accomplishments",
    "skills",
]  # ppl cols to translate and/or search for tags
textcols_orgs = [
    "profile_name",
    "about",
    "sector",
]  # default org cols to translate and/or search for tags
professionCols = ["title", "about", "experiences"]  # ppl cols to search for professions
idCol = "id"  # id or unique name for merging metdadata etc.

# image prcoessing params
nameCol = "profile_name"  # must be unique (use id?)
imageURL = "image_url"
# images_directory = images_path/"image_files"  # directory to store image files
images_bucket = "test-images"  # images s3 bucket


# fix known linkedin url errors:
li_fix_dict = {}
li_blacklist = [
    "https://wwww.linkedin.com/in/dontuselinkedin/",
    "https://wwww.nolinkedin.com/in/",
    "https://wwww.linkedin.com/in/",
    "https://wwww.linkedin.com/feed/",
]  # list of known bad urls

# default final ppl columns after metadata enrichment
finalMetaCols_ppl = [
    "profile_name",
    "title",
    "organization",
    "school",
    "top_card",
    "about",
    "experiences",
    "accomplishments",
    "keywords",
    "professions",
    "skills",
    "Geo_Tags",
    "City",
    "Country",
    "n_keywords",
    "n_skills",
    "LinkedIn_Profile",
    "Image_URL",  # processed image S3 bucket url
    "image_source_url",  # linkedIn original image url
    "Latitude",
    "Longitude",
    "id",
    # 'geo_manual',
    # 'new_GeoText',
    # 'new_PlaceNames',
]

# default final organization columns after metadata enrichment
finalMetaCols_orgs = [
    "profile_name",
    "about",
    "Type",
    "employee_count",
    "Company size",
    "keywords",
    "sector",
    "Industry",
    "Geo_Tags",
    "City",  # full city, state (if US), country
    "State",  # US State
    "Country",
    "n_keywords",
    "LinkedIn_Profile",
    "Website",
    "Image_URL",  # processed image S3 bucket url
    "image_source_url",  # linkedIn original image url
    "Latitude",
    "Longitude",
    "id",
]

# list of major countries with english as official language - or spoken by a majority
english_countries = [
    "Australia",
    "Canada",
    "Ireland",
    "England",
    "Northern Ireland",
    "Scotland",
    "Ghana",
    "Kenya",
    "Nigeria",
    "Rwanda",
    "South Africa",
    "Uganda",
    "Zimbabwe",
    "India",
    "Pakistan",
    "Israel",
    "New Zealand",
    "United Kingdom",
    "United States",
]
