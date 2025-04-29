import boto3
import os

from vdl_tools.shared_tools.tools import log_utils as log
from vdl_tools.shared_tools.cache import Cache
import vdl_tools.shared_tools.tools.config_utils as config_tools


S3_DEFAULT_BUCKET_NAME = 'vdl-website-cache'
S3_DEFAULT_BUCKET_REGION = 'us-east-1'
DEFAULT_FILE_CACHE_DIRECTORY = '.cache/websites'


def get_configuration_from_config(config):
    result_config = dict()
    if 'aws' in config:
        aws_config = config['aws']
        result_config['region'] = aws_config.get(
            'region', S3_DEFAULT_BUCKET_REGION)
        result_config['access_key_id'] = aws_config.get('access_key_id', '')
        result_config['secret_access_key'] = aws_config.get(
            'secret_access_key')
        cache_bucket = aws_config.get('website_cache_bucket', None)
        if cache_bucket:
            result_config['website_cache_bucket'] = cache_bucket
        else:
            result_config['website_cache_bucket'] = S3_DEFAULT_BUCKET_NAME

    if 'website_scraping' in config:
        website_config = config['website_scraping']
        website_cache_dir = website_config.get('website_cache_dir', None)
        if website_cache_dir:
            result_config['website_cache_dir'] = website_cache_dir
    else:
        result_config['website_cache_dir'] = DEFAULT_FILE_CACHE_DIRECTORY

    return result_config


def get_configuration():
    return get_configuration_from_config(config_tools.get_configuration())


class WebsiteCache(Cache):
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

        self.config = (
            get_configuration_from_config(config) if config
            else get_configuration()
        )

        bucket_name = (
            bucket_name or
            config_tools.get_configuration_value(
                self.config, 'website_cache_bucket', S3_DEFAULT_BUCKET_NAME)
        )
        aws_region = (
            aws_region or
            config_tools.get_configuration_value(
                self.config, 'region', S3_DEFAULT_BUCKET_REGION)
        )
        aws_access_key_id = (
            aws_access_key_id or
            config_tools.get_configuration_value(self.config, 'access_key_id')
        )
        aws_secret_access_key = (
            aws_secret_access_key or
            config_tools.get_configuration_value(
                self.config, 'secret_access_key')
        )
        file_cache_directory = (
            file_cache_directory or
            config_tools.get_configuration_value(
                self.config, 'website_cache_dir', file_cache_directory)
        )
        super().__init__(
            bucket_name,
            aws_region,
            aws_access_key_id,
            aws_secret_access_key,
            file_cache_directory
        )

    def create_search_key(self, id: str) -> str:
        return f'{id}.html'

    def delete_by_extracted_website(self, extracted_website: str):
        self.delete_s3_prefix_by_extracted_website(extracted_website)
        self.delete_local_prefix_by_extracted_website(extracted_website)

    def delete_local_prefix_by_extracted_website(self, extracted_website: str):
        local_filepath = self._get_file_path(extracted_website)
        if local_filepath.exists():
            log.info(f"Item {extracted_website} exists locally, deleting")
            # Remove all files inside:
            for filename in os.listdir(local_filepath):
                (local_filepath / filename).unlink()
            local_filepath.rmdir()
        else:
            log.info(f"Item {extracted_website} doesn't exist locally")

    def delete_s3_prefix_by_extracted_website(self, extracted_website: str):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.bucket)
        log.info(f"Delete all objects with prefix {extracted_website}")
        bucket.objects.filter(Prefix=extracted_website).delete()
