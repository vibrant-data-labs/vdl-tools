# -*- coding: utf-8 -*-
import pathlib as pl
import datetime as dt
import pandas as pd
import vdl_tools.shared_tools.tools.config_utils as config_tools
from vdl_tools.shared_tools.cache import Cache

S3_DEFAULT_BUCKET_NAME = 'geocode-cache'
S3_DEFAULT_BUCKET_REGION = 'us-east-1'
DEFAULT_FILE_CACHE_DIRECTORY = '.cache/geocode'

# %%
def get_component(gc_data, component_type):
    """Get component from geocode data block."""
    if gc_data is not None:
        for component in gc_data.raw["address_components"]:
            if component_type in component["types"]:
                return component["long_name"]
    return None

class GeocodeCache(Cache):
    _cached_keys = []
    _local_cache_keys = []

    def __init__(
        self,
        config: dict = None,
        bucket_name: str = None,
        aws_region: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        file_cache_directory: str = None,
    ):
        self.config = config or config_tools.get_configuration()
        bucket_name = (
            bucket_name or
            config_tools.get_configuration_value(
                self.config, 'aws.geocode_cache_bucket', S3_DEFAULT_BUCKET_NAME)
        )
        aws_region = (
            aws_region or
            config_tools.get_configuration_value(
                self.config, 'aws.region', S3_DEFAULT_BUCKET_REGION)
        )
        aws_access_key_id = (
            aws_access_key_id or
            config_tools.get_configuration_value(self.config, 'aws.access_key_id')
        )
        aws_secret_access_key = (
            aws_secret_access_key or
            config_tools.get_configuration_value(self.config, 'aws.secret_access_key')
        )
        file_cache_directory = (
            file_cache_directory or
            config_tools.get_configuration_value(
                self.config, 'geocode.geocode_cache_dir', DEFAULT_FILE_CACHE_DIRECTORY)
        )
        super().__init__(
            bucket_name,
            aws_region,
            aws_access_key_id,
            aws_secret_access_key,
            file_cache_directory,
            cache_validity_days=365
        )

    def create_search_key(self, id: str) -> str:
        return f'{id}.json'

# %%
if __name__ == "__main__":
    val = [
        {
            "address": "asd",
            "lat": 10,
            "lon": 20,
            "locality": "somecity",
            "administrative_area_level_1": "somestate",
            "country": "somecountry",
        }
    ]
    df = pd.DataFrame(val).set_index("address")

    cache = GeocodeCache(df)
