#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: ericlberlow

This script gets latitude and longitude for addresses  using the Google API geocoder
It requires a google api key - which is in the .bash_profile file
To use the .bash_profile you need to launch spyder from the terminal (not Anaconda)
You can either use an auto-rate limiter - or loop through the rows one-by-one with manual rate limiter
It returns the original datafraem with added lat/long columns.
Addresses that failed lookup have empty lat,long.

"""

# %% ###########
# ## libraries and file paths
import pathlib as pl
import pandas as pd
import numpy as np
from vdl_tools.shared_tools.common_functions import (
    build_tag_hist_df,
    join_strings_no_missing,
    write_excel_no_hyper,
    clean_tag_list
)

# import os
from vdl_tools.scrape_enrich.geocode_cache import GeocodeCache, get_component
from geopy.geocoders import GoogleV3  # , Nominatim
from geopy.extra.rate_limiter import RateLimiter
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.unique_ids import make_uuid
import json


geo_rename_dict = {
    "San Francisco Bay Area": "San Francisco, CA, United States",
    "Ireland": "Ireland, United Kingdom",
    "City of Johannesburg": "Johannesburg",
    "Greater Vancouver Metropolitan Area": "Vancouver, British Colombia",
    "Greater Oxford Area": "Oxford, United Kingdom",
    "Greater Brighton and Hove Area": "Brighton, United Kingdom",
    "Greater Cheshire West and Chester Area": "Chester, United Kingdom",
    "Greater Sydney Area": "Sydney, Australia",
    "Halifax, Nova Scotia, Canada": "Halifax Regional Municipality, Nova Scotia",
    "Brooklyn, New York, United States": "New York City, New York, United States",
    "The Randstad, Netherlands": "Amsterdam, Netherlands",
    "Berlin Metropolitan Area": "Berlin, Germany",
    "North Vancouver": "Vancouver",
    "Bellaire, Texas": "Houston, Texas",
    "Metropolitan Area": "",
}


def get_api_info():
    cfg = get_configuration()
    user = cfg["geocode"]["user"]
    key = cfg["geocode"]["key"]
    return user, key


def geocode_addresses(df, address, test=None, use_cached_result=True):
    """
    get lat long from address
    also extract city, state, and country
    df : dataframe with address
    address : column name of address
    test : sample size for testing

    Returns: df with added columns of latitude, longitude, city, state, country
    """
    cache = GeocodeCache()

    if test is not None:
        print("subset %d for testing" % test)
        df = df.head(test).copy()
    # only compute on facilities that have address info
    df = df.reset_index(drop=True)
    df[address].fillna("", inplace=True)
    for string1, string2 in geo_rename_dict.items():  # spell corrections for geocoding
        df.loc[:, address] = df[address].str.replace(string1, string2)
    # remove records with no address
    df_w_geo: pd.DataFrame = df[df[address].apply(lambda x: x != "")]  # .copy()
    df_w_geo = df_w_geo.reset_index(drop=True)  # trying to avoid slice error...

    # Sometime the address is too long for the filesystem, --create a unique id for the address
    def _shorten_address(x):
        if len(x) > 100:
            hashed_address = make_uuid(x, "geocode", True)
            return f"hashed_{hashed_address}"
        return x

    def load_cache_item(x):
        x = _shorten_address(x)
        cache_item = cache.get_cache_item(x)
        if cache_item:
            return json.loads(cache_item)
        return None

    # split dataframe and fill cached data
    if use_cached_result:
        df_w_geo['location'] = df_w_geo[address].apply(load_cache_item)
    else:
        df_w_geo['location'] = None

    has_missing_items = len(df_w_geo[df_w_geo['location'].isnull()]) > 0

    if has_missing_items:
        print("set up google api")
        USER, KEY = get_api_info()

        geolocator = GoogleV3(user_agent=USER, api_key=KEY)

        geocodeRL = RateLimiter(
            geolocator.geocode, min_delay_seconds=1 / 50
        )  # auto rate limiter

        def geocode_query(x):
            if x['location']:
                return x['location']

            data = geocodeRL(x[address])
            if not data:
                cache.save_as_error(x[address])
                return None

            location_data = {
                'latitude': data.latitude,
                'longitude': data.longitude,
                'city': get_component(data, "locality"),
                'state': get_component(data, "administrative_area_level_1"),
                'country': get_component(data, "country"),
                'raw': data.raw
            }
            cache.store_item(_shorten_address(x[address]), json.dumps(location_data))
            return location_data

        print("getting lat/longs with geocode auto rate limiter and S3 cache")
        index_to_geocode = {}
        for i, row in df_w_geo.iterrows():
            try:
                # Set the value of the 'location' column to the geocode_query result
                index_to_geocode[i] = geocode_query(row)
            except KeyboardInterrupt:
                print("(Geocode) Received Keyboard Interrupt, exiting gracefully...")
                break

        df_w_geo['location'] = df_w_geo.index.map(index_to_geocode)

    print("getting address components")
    df_w_geo["Latitude"] = df_w_geo["location"].apply(
        lambda x: x['latitude'] if x else ""
    )
    df_w_geo["Longitude"] = df_w_geo["location"].apply(
        lambda x: x['longitude'] if x else ""
    )

    df_w_geo["city"] = df_w_geo["location"].apply(
        lambda x: x['city'] if x else None
    )
    df_w_geo["state"] = df_w_geo["location"].apply(
        lambda x: x['state'] if x else None
    )
    df_w_geo["country"] = df_w_geo["location"].apply(
        lambda x: x['country'] if x else None
    )

    df_w_geo.drop("location", axis=1, inplace=True)
    return df_w_geo


def reverse_geocode_addresses(df, latitude, longitude, test=None):
    """
    get address, city, state, country from latitude and longitude
    df : dataframe with address
    latitude : column name of latitude
    longitude : column name of longitude
    test : sample size for testing

    Returns: df with added columns of full address, city, state, country
    """

    if test is not None:
        print("subset %d for testing" % test)
        df = df.head(test).copy()

    print("set up google api")
    USER, KEY = get_api_info()

    geolocator = GoogleV3(user_agent=USER, api_key=KEY)

    reverseRL = RateLimiter(geolocator.reverse, min_delay_seconds=1 / 50)

    print("getting address components with reverse geocode auto rate limiter")
    df["address"] = df.apply(lambda x: reverseRL((x[latitude], x[longitude])), axis=1)
    df["city"] = df["address"].apply(lambda x: get_component(x, "locality"))
    df["state"] = df["address"].apply(
        lambda x: get_component(x, "administrative_area_level_1")
    )
    df["country"] = df["address"].apply(lambda x: get_component(x, "country"))

    return df


#################################################################################################
# GEOCODE - functions to get lat/long, city, statey, country tags from address
#################################################################################################


def get_lat_long(
    df,
    idCol,
    address,
):
    """
    Get lat lon.

    # parse location from linkedIn top card
    # idCol: column to use for merging
    # geoname: pathname of processed lat/long file
    # geopath: directory for geocode results
    """
    df["Address"] = df[address]
    # get lat long and city, state, country from final addresses
    print("geocoding addresses")
    df_geo = df[[idCol, "Address"]]
    df_geo = geocode_addresses(df_geo, "Address")
    df.drop(["Address"], axis=1, inplace=True)  # remove origional address for merging
    return df_geo


def add_geo_lat_long(
    df,
    idCol="id",  # unique id column
    address="Location",  # column with address
):
    """
    Add geo lat lon to a dataframe with an address column

    add lat/long, hq city, state, country from address to recipient metadata
    if processed file exists
    df :  metadata file, must have [idCol, address] columns
    idCol : unique id for merging metadata
    geo_name : pathname of processed geocode file
    geopath: direcotry of processed geocode file
    """
    print("\nAdding Lat/Long, City, Region, Country")
    df_geo = get_lat_long(
        df,
        idCol,
        address,
    )

    # merge geo data to main file
    if address == "Address":
        df.drop(["Address"], axis=1, inplace=True)  # drop for merging with new
    df_w_geo = df.merge(df_geo, on=idCol, how="left")
    return df_w_geo


def __clean_city_region(row):
    if type(row['country']) == str and row['country'] != 'United States':
        return row["City_Region"].split(", ")[0]
    return row["City_Region"]


def _join_list_no_missing(df, cols):
    # cols is list of string columns to concatenate - and ignore missing values
    # returns a series
    df[cols] = df[cols].fillna("")
    df[cols] = df[cols].replace(
        r"^\s*$", np.nan, regex=True
    )  # replace empty string with nan
    joined_list = df[cols].apply(
        lambda x: list(x.dropna()), axis=1
    )  # join as a list without missing values
    return joined_list


def clean_geo(df, summarize_new_geo=False):
    """
    clean city state country tags from geocode
    add location to geotags
    summarize new place names found in text
    """
    print("\nCleaning geo tags")
    df.reset_index(drop=True, inplace=True)  # trying to avoid copy of slice error....
    # add "city, state, country" as string for dispay in profile
    df["City_Region"] = join_strings_no_missing(df, ["city", "state"], delim=", ")
    # remove 'state' region if not US city,
    df["City_Region"] = df.apply(__clean_city_region, axis=1)
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
    if "Geo_Tags" in df.columns:
        df.Geo_Tags = df.Geo_Tags + _join_list_no_missing(df, ["city", "state", "country"])
        df["Geo_Tags"] = clean_tag_list(df, "Geo_Tags", replace={"USA": "United States"})
    df.drop(
        ["City_Region", "city", "state", "country", "Address"], inplace=True, axis=1
    )
    # compile new geo place names and save as sep files
    if summarize_new_geo:
        df_newGeo = build_tag_hist_df(df, "new_GeoText", delimiter="|")
        df_newPlaces = build_tag_hist_df(df, "new_PlaceNames", delimiter="|")
        df_newGeo.to_csv("new_geo.csv")
        df_newPlaces.to_csv("new_places.csv")
    # df.drop(["new_GeoText", "new_PlaceNames"], axis=1, inplace=True)
    return df


# %%
if __name__ == "__main__":
    import shutil

    # copy project-specific config.ini to the current working directory (the data root)
    wd = pl.Path.cwd()  # current working directory
    prjpath = wd / "projects/ClimateLandscape"
    shutil.copy(prjpath / "config.ini", wd)

    # df = pd.read_excel('test.xlsx', engine='openpyxl')[['Recipient', 'HQ_City/State','HQ_Country','HQ Address']]
    # df = geocode_addresses(df, "HQ Address", test=None)
    df = pd.read_excel("test.xlsx")
    df = geocode_addresses(df, "Address")
