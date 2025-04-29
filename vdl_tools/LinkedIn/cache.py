import vdl_tools.shared_tools.tools.log_utils as log
import vdl_tools.shared_tools.tools.config_utils as config_tools
from vdl_tools.shared_tools.cache import Cache


S3_DEFAULT_BUCKET_NAME = 'linkedin-cache'
S3_DEFAULT_BUCKET_REGION = 'us-east-1'
DEFAULT_FILE_CACHE_DIRECTORY = '.cache/linkedin'


def get_configuration_from_config(config):
    result_config = dict()
    if 'aws' in config:
        aws_config = config['aws']
        result_config['region'] = aws_config.get(
            'region', S3_DEFAULT_BUCKET_REGION)
        result_config['access_key_id'] = aws_config.get('access_key_id', '')
        result_config['secret_access_key'] = aws_config.get(
            'secret_access_key')
        cache_bucket = aws_config.get('linkedin_cache_bucket', None)
        if cache_bucket:
            result_config['linkedin_cache_bucket'] = cache_bucket
        else:
            result_config['linkedin_cache_bucket'] = S3_DEFAULT_BUCKET_NAME

    if 'linkedin' in config:
        linkedin_config = config['linkedin']
        linkedin_cache_dir = linkedin_config.get('linkedin_cache_dir', None)
        if linkedin_cache_dir:
            result_config['linkedin_cache_dir'] = linkedin_cache_dir
        result_config['linkedin_cookie'] = linkedin_config.get('linkedin_cookie', None)
        
        coresignal_api_key = linkedin_config.get('coresignal_api_key', None)
        if coresignal_api_key:
            result_config['coresignal_api_key'] = coresignal_api_key
    else:
        result_config['linkedin_cache_dir'] = DEFAULT_FILE_CACHE_DIRECTORY

    return result_config


def get_configuration():
    return get_configuration_from_config(config_tools.get_configuration())


class LinkedInCache(Cache):
    _cached_keys = []
    _local_cache_keys = []
    TOTAL_KEY = '__total__'

    def __init__(
        self,
        item_type: str = 'profile', # organization
        config: dict = None,
        bucket_name: str = None,
        aws_region: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        file_cache_directory: str = None,
    ):
        self.item_type = item_type

        self.config = (
            get_configuration_from_config(config) if config
            else get_configuration()
        )

        bucket_name = (
            bucket_name or
            config_tools.get_configuration_value(
                self.config, 'linkedin_cache_bucket', S3_DEFAULT_BUCKET_NAME)
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
            config_tools.get_configuration_value(self.config, 'secret_access_key')
        )
        file_cache_directory = (
            file_cache_directory or
            config_tools.get_configuration_value(
                self.config, 'linkedin_cache_dir', DEFAULT_FILE_CACHE_DIRECTORY)
        )
        super().__init__(
            bucket_name,
            aws_region,
            aws_access_key_id,
            aws_secret_access_key,
            file_cache_directory
        )

    def create_search_key(self, id: str) -> str:
        return f'json/{self.item_type}/{id}.json'


class LinkedInRawDataCache(LinkedInCache):
    TOTAL_KEY = '__total__'

    def __init__(
        self,
        item_type: str = 'profile', # organization
        config: dict = None,
        bucket_name: str = None,
        aws_region: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        file_cache_directory: str = None,
    ):
        super().__init__(
            item_type,
            config,
            bucket_name,
            aws_region,
            aws_access_key_id,
            aws_secret_access_key,
            file_cache_directory
        )

    def create_search_key(self, id: str) -> str:
        if id == self.TOTAL_KEY:
            return f'raw/{self.item_type}/{self.TOTAL_KEY}.json'

        return f'raw/{self.item_type}/{id}.html'
