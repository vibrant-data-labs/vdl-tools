#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Geocoding v2 — Postgres-backed cache.

Drop-in replacement for `geocode.py` with an identical public API
(`geocode_addresses`, `get_lat_long`, `add_geo_lat_long`) and output columns, but
backed by the Postgres `geocode` table instead of the S3 + local-file cache.

Instead of one S3 GET/PUT per address, it issues a single bulk SQL read for every
address up front, geocodes only the misses (rate-limited Google calls), and bulk
upserts the results. See `geocode_cache_sql.GeocodeCache`.

Cache-independent helpers (`clean_geo`, `reverse_geocode_addresses`, `get_component`,
`geo_rename_dict`, `get_api_info`) are unchanged from v1 and re-exported here so
callers only need to import from `geocode_v2`.
"""
from vdl_tools.scrape_enrich.geocode import (
    geo_rename_dict,
    get_api_info,
    clean_geo,
    reverse_geocode_addresses,
)
from vdl_tools.scrape_enrich.geocode_cache import get_component
from vdl_tools.scrape_enrich.geocode_cache_sql import GeocodeCache
from vdl_tools.shared_tools.database_cache.database_utils import get_session
from vdl_tools.shared_tools.tools.logger import logger


__all__ = [
    "geocode_addresses",
    "get_lat_long",
    "add_geo_lat_long",
    "clean_geo",
    "reverse_geocode_addresses",
    "get_component",
    "geo_rename_dict",
    "get_api_info",
]


def geocode_addresses(df, address, test=None, use_cached_result=True):
    """
    get lat long from address
    also extract city, county, state, and country
    df : dataframe with address
    address : column name of address
    test : sample size for testing

    Returns: df with added columns of latitude, longitude, city, county, state, country
    """
    if test is not None:
        logger.info("subset %d for testing" % test)
        df = df.head(test).copy()
    # only compute on facilities that have address info
    df = df.reset_index(drop=True)
    df[address] = df[address].fillna("")
    for string1, string2 in geo_rename_dict.items():  # spell corrections for geocoding
        df.loc[:, address] = df[address].str.replace(string1, string2)
    # remove records with no address
    df_w_geo = df[df[address].apply(lambda x: x != "")]
    df_w_geo = df_w_geo.reset_index(drop=True)  # trying to avoid slice error...

    # Bulk: read the cache for every address at once, then geocode only the misses.
    USER, KEY = get_api_info()
    unique_addresses = df_w_geo[address].unique().tolist()
    logger.info("getting lat/longs with postgres geocode cache")
    with get_session() as session:
        cache = GeocodeCache(session, USER, KEY)
        address_to_location = cache.bulk_get_cache_or_run(
            unique_addresses,
            use_cached_result=use_cached_result,
        )

    # `.get` -> None for addresses that errored / returned no result (NaN-safe)
    df_w_geo['location'] = df_w_geo[address].apply(lambda a: address_to_location.get(a))

    logger.info("getting address components")
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
    # County (US) / second-level admin area. Google omits it where the city is
    # itself the county equivalent (DC, Baltimore city, Virginia's independent
    # cities), so it is None for those — fall back to `city` when full
    # coverage matters.
    df_w_geo["county"] = df_w_geo["location"].apply(
        lambda x: x['county'] if x else None
    )

    df_w_geo.drop("location", axis=1, inplace=True)
    return df_w_geo


def get_lat_long(
    df,
    idCol,
    address,
    use_cached_result=True
):
    """
    Get lat lon.

    # parse location from linkedIn top card
    # idCol: column to use for merging
    """
    df["Address"] = df[address]
    # get lat long and city, state, country from final addresses
    logger.info("geocoding addresses")
    df_geo = df[[idCol, "Address"]]
    df_geo = geocode_addresses(
        df_geo,
        "Address",
        use_cached_result=use_cached_result,
    )
    df.drop(["Address"], axis=1, inplace=True)  # remove origional address for merging
    return df_geo


def add_geo_lat_long(
    df,
    idCol="id",  # unique id column
    address="Location",  # column with address
    use_cached_result=True
):
    """
    Add geo lat lon to a dataframe with an address column

    add lat/long, hq city, state, country from address to recipient metadata
    df :  metadata file, must have [idCol, address] columns
    idCol : unique id for merging metadata
    """
    logger.info("\nAdding Lat/Long, City, Region, Country")
    df_geo = get_lat_long(
        df,
        idCol,
        address,
        use_cached_result=use_cached_result,
    )

    # merge geo data to main file
    if address == "Address":
        df.drop(["Address"], axis=1, inplace=True)  # drop for merging with new
    df_w_geo = df.merge(df_geo, on=idCol, how="left")
    return df_w_geo
