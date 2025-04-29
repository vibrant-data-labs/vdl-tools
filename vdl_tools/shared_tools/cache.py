from datetime import datetime, timezone, timedelta
from logging import getLogger
import os
import pathlib as pl
from typing import TypeVar

import boto3

from vdl_tools.shared_tools.tools.logger import logger

getLogger("botocore").setLevel("WARNING")
getLogger("boto3").setLevel("WARNING")
getLogger("urllib3").setLevel("WARNING")


T = TypeVar("T")


S3_DEFAULT_BUCKET_REGION = 'us-east-1'
_cache_validity_days = 90


class Cache:
    config: dict = None
    _cached_keys = []
    _local_cache_keys = []

    def __init__(
        self,
        bucket_name: str,
        aws_region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        file_cache_directory: str,
        cache_validity_days=_cache_validity_days,
        error_counter_enabled: bool = False,
        error_counter_threshold: int = 5
    ):
        today = datetime.now(timezone.utc)
        last_valid_date = today - timedelta(days=cache_validity_days)
        self.cache_validity_days = cache_validity_days
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket_name
        self.file_cache_directory = file_cache_directory
        self.error_counter_enabled = error_counter_enabled
        self.error_counter_threshold = error_counter_threshold

        self.cur_path = pl.Path.cwd()

        self.client = self._get_boto_client()
        self._check_create_bucket(self.bucket)

        self._cached_keys = set(self.get_cached_list(last_valid_date))

    def _check_create_bucket(self, bucket):
        try:
            self.client.head_bucket(Bucket=bucket)
        except self.client.exceptions.ClientError:
            logger.warn('Bucket "%s" does not exist, creating...', bucket)
            self.client.create_bucket(Bucket=bucket)

    def _get_boto_client(self):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )
        return self.client

    def store_item(self, id: str, item: str, force: bool = False):
        '''
        Stores item in the cache, first saves to file system, then tries to upload to s3
        '''
        key = self.create_search_key(id)
        if force or (id not in self._local_cache_keys):
            self.save_to_file(key, item)

        try:
            if not force and (key in self._cached_keys):
                return

            logger.info('Saving to s3 bucket "%s"', self.bucket)
            self.save_to_s3(key, item)
        except Exception as e:
            logger.error(e)
            logger.error('Failed to store item for %s.', id)

    def save_to_s3(self, path: str, body: str):
        self.client.put_object(Body=body, Bucket=self.bucket, Key=path)
        self._cached_keys.add(path)

    def save_to_file(self, path: str, body: str):
        cache_dir = self.cur_path / self.file_cache_directory
        save_path = cache_dir / path

        os.makedirs(save_path.parent, exist_ok=True)

        with open(save_path, 'w+', encoding="utf-8") as f:
            f.write(body)

    def is_error(self, id: str) -> bool:
        '''
        Checks if the id is saved as the id with error
        '''
        search_key = f'errors/{id}'
        cache_item = self.get_cache_item(search_key)
        if self.error_counter_enabled and cache_item:
            counter = int(cache_item)
            return counter >= self.error_counter_threshold

        return cache_item is not None

    def delete_by_id(self, id: str):
        self.delete_s3_item_by_id(id)
        self.delete_local_item_by_id(id)

    def delete_local_item_by_id(self, id: str):
        search_key = self.create_search_key(id)
        local_filepath = self._get_file_path(search_key)
        if local_filepath.exists():
            logger.info("Item %s exists locally, deleting", search_key)
            local_filepath.unlink()
            if search_key in self._local_cache_keys:
                self._local_cache_keys.remove(search_key)
        else:
            logger.info(f"Item %s doesn't exist locally", search_key)

    def delete_s3_item_by_id(self, id: str):
        search_key = self.create_search_key(id)
        exists = False

        try:
            self.client.get_object(Bucket=self.bucket, Key=search_key)
            exists = True
        except Exception:
            exists = False

        if exists:
            logger.info("Item %s exists in S3, deleting", search_key)
            self.client.delete_object(
                Bucket=self.bucket,
                Key=search_key,
            )
            if search_key in self._cached_keys:
                self._cached_keys.remove(search_key)
        else:
            logger.info("Item %s doesn't exist in S3", search_key)

    def save_as_error(self, id: str):
        '''
        By default the cache only marks the item as errored item

        Having `error_counter_enabled=True` allows tracking how many times the item has failed
        and until the `error_counter_threshold` is reached, the item is not treated as an error
        '''
        search_key = f'errors/{id}'
        if self.error_counter_enabled:
            cache_item = self.get_cache_item(search_key)
            counter = int(cache_item) + 1 if cache_item else 1
            self.store_item(search_key, str(counter))
        else:
            self.store_item(search_key, '')

    def create_search_key(self, id: str) -> str:
        return NotImplementedError()

    def get_cache_item(self, id: str) -> str:
        '''
        Attempts to load the item from the cache, first tries to get it from file, then from s3
        '''
        search_key = self.create_search_key(id)
        today = datetime.now(timezone.utc)
        last_valid_date = today - timedelta(days=self.cache_validity_days)

        try:
            logger.debug("Getting item %s from file system", id)
            is_valid, data = self.get_file_item(search_key, last_valid_date)
            if is_valid:
                logger.debug("Item %s is taken from local cache", search_key)
                self._local_cache_keys.append(search_key)
                return data
        except Exception as e:
            logger.warning("Failed to get the item %s from file system.", search_key)

        try:
            logger.info(
                'Getting item %s from s3 bucket "%s"',
                search_key,
                self.bucket,
            )
            is_valid, data = self.get_s3_item(search_key)
            if is_valid:
                # if item is present at s3, but missed locally, update item locally
                self.save_to_file(search_key, data)
                logger.warning("Item %s is taken from S3 cache", search_key)
                return data
        except Exception as e:
            logger.warning("Failed to fetch the item %s from bucket.", search_key)

        return None

    def get_cached_list(self, last_valid_date: datetime):

        res = []
        try:
            marker = ''
            while True:
                object_data = self.client.list_objects(
                    Bucket=self.bucket,
                    Marker=marker
                )
                if 'Contents' not in object_data:
                    break

                res.extend(
                    [
                        x['Key'] for x in object_data.get('Contents', [])
                        if x['LastModified'] > last_valid_date
                    ]
                )
                if object_data['IsTruncated']:
                    marker = res[-1]
                else:
                    break
            return res
        except Exception as e:
            logger.error('Can not fetch items from the bucket')
            raise e

    def get_s3_item(self, search_path: str) -> (bool, str):
        try:
            if search_path not in self._cached_keys:
                return False, None

            obj = self.client.get_object(
                Bucket=self.bucket,
                Key=search_path,
            )
            return True, obj['Body'].read().decode('utf-8')
        except:
            logger.info("Key: %s does not exist in the bucket", search_path)

        return False, None

    def _get_file_path(self, search_path):
        return self.cur_path / self.file_cache_directory / search_path

    def get_file_item(
            self,
            search_path: str,
            last_valid_date: datetime) -> (bool, str):
        fp = self._get_file_path(search_path)

        if fp.exists():
            mtime = datetime.fromtimestamp(fp.stat().st_mtime, tz=timezone.utc)
            if mtime < last_valid_date:
                return False, None

            with open(fp, 'r', encoding="utf-8") as f:
                return True, f.read()

        return False, None
