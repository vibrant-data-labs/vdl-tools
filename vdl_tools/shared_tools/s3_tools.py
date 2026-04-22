"""General-purpose S3 helpers for arbitrary files (xlsx, images, raw JSON, etc.).

.. note::
    **For DataFrames, use** :mod:`vdl_tools.shared_tools.parquet_cache` instead.
    ``parquet_cache`` writes typed, compressed Parquet, handles dict/list
    columns, and caches remote reads locally with ETag validation — a better
    fit than ``write_pandas_to_s3`` + ``to_json`` for any DataFrame workload.
    This module is the right tool when you need arbitrary file I/O (Excel,
    images, bulk JSON blobs, etc.) or when you want boto3-level control.
"""

import boto3
import pathlib as pl
import tempfile
import os
import json
from contextlib import contextmanager
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from vdl_tools.shared_tools.tools.logger import logger


def __get_default_policy(bucket_name):
    return '''{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::%s/*"
 
            ]
        }
    ]
}''' % bucket_name


def get_s3_client():
    config = get_configuration()
    REGION = config['aws']['region']
    ACCESS_KEY = config['aws']['access_key_id']
    SECRET_KEY = config['aws']['secret_access_key']
    return boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )


def create_bucket(s3_client, bucket_name):
    # create public bucket if it doesn't exist
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration={
        "BlockPublicPolicy": False
    })

    s3_client.put_bucket_policy(
        Bucket=bucket_name,
        Policy=__get_default_policy(bucket_name)
    )


def list_bucket_contents(s3_client, bucket_name):

    res = []
    try:
        marker = ''
        while True:
            object_data = s3_client.list_objects(
                Bucket=bucket_name,
                Marker=marker
            )
            if 'Contents' not in object_data:
                break

            res.extend(
                [
                    x['Key'] for x in object_data.get('Contents', [])
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


def bucket_exists(bucket_name):
    """
    Check if an S3 bucket exists and is accessible.
    
    Args:
        bucket_name: Name of the S3 bucket
        
    Returns:
        bool: True if bucket exists and is accessible, False otherwise
    """
    try:
        s3_client = get_s3_client()
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except Exception:
        return False


def save_file_to_s3(bucket_name, full_name):
    s3_client = get_s3_client()
    # Check if bucket exists, create if it doesn't
    if not bucket_exists(bucket_name):
        s3_client.create_bucket(Bucket=bucket_name)
    path = pl.Path(full_name)
    if path.is_dir():
        raise ValueError(f"Path {full_name} is a directory")
    if path.parts[0] == '..':
        path = pl.Path(*path.parts[1:])
    key = path.as_posix()
    s3_client.upload_file(full_name, bucket_name, key)
    return key


def download_file_from_s3(bucket_name, full_name):
    s3_client = get_s3_client()
    # Check if bucket exists
    if not bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist")

    path = pl.Path(full_name)
    if path.is_dir():
        raise ValueError(f"Path {full_name} is a directory")
    if path.parts[0] == '..':
        path = pl.Path(*path.parts[1:])
    key = path.as_posix()
    s3_client.download_file(bucket_name, key, full_name)
    return full_name


@contextmanager
def s3_file(
    s3_path,
    bucket_name='shared-data-clone',
    mode='r',
    auto_upload=False,
    cleanup=False,
):
    """
    Context manager for working with S3 files as if they were local.

    Args:
        s3_path: Path to file in S3 (and local path to use)
        bucket_name: S3 bucket name (default: 'shared-data-clone')
        mode: 'r' for read (download), 'w' for write (upload), 'rw' for both
        auto_upload: If True and mode includes 'r', upload the file on exit even in read mode

    Usage:
        # Read from S3
        with s3_file('data/input.json', mode='r') as local_path:
            df = pd.read_json(local_path)

        # Write to S3
        with s3_file('data/output.json', mode='w') as local_path:
            df.to_json(local_path)

        # Read and write (download, modify, upload)
        with s3_file('data/file.json', mode='rw') as local_path:
            df = pd.read_json(local_path)
            df = df * 2
            df.to_json(local_path)
    """
    local_path = s3_path
    # if using Pathlib, the s3 path might have the full path
    cwd = s3_path.cwd()
    s3_path = s3_path.relative_to(cwd)
    downloaded = False
    try:
        # Download if reading
        if mode in ('r', 'rw'):
            try:
                download_file_from_s3(bucket_name, s3_path)
                downloaded = True
            except Exception as e:
                logger.warning(f"Could not download {s3_path} from S3: {e}")
                if mode == 'r' and not auto_upload:
                    raise
        yield local_path

        # Upload if writing or if auto_upload is enabled
        if mode in ('w', 'rw') or (downloaded and auto_upload):
            save_file_to_s3(bucket_name, s3_path)
    finally:
        # Optionally clean up local file after upload (for temp files)
        if cleanup and os.path.exists(local_path):
            os.remove(local_path)


def read_s3_to_pandas(
    s3_path, reader_func,
    bucket_name='shared-data-clone',
    cleanup=False,
    **kwargs,
):
    """
    Download a file from S3 and read it with a pandas function.
    
    Args:
        s3_path: Path to file in S3
        reader_func: Pandas reading function (e.g., pd.read_json, pd.read_csv)
        bucket_name: S3 bucket name (default: 'shared-data-clone')
        cleanup: Whether to delete the local file after reading
        **kwargs: Additional arguments to pass to the reader function
    
    Returns:
        pandas DataFrame
    
    Usage:
        df = read_s3_to_pandas('data/input.json', pd.read_json)
        df = read_s3_to_pandas('data/input.csv', pd.read_csv, sep=';')
    """
    with s3_file(s3_path, bucket_name=bucket_name, mode='r', cleanup=cleanup) as local_path:
        return reader_func(local_path, **kwargs)


def write_pandas_to_s3(
    df,
    s3_path,
    writer_method_name,
    bucket_name='shared-data-clone',
    cleanup=False,
    **kwargs,
):
    """
    Write a pandas DataFrame to S3.
    
    Args:
        df: pandas DataFrame to write
        s3_path: Path to save file in S3
        writer_method_name: Name of the pandas writer method (e.g., 'to_json', 'to_csv')
        bucket_name: S3 bucket name (default: 'shared-data-clone')
        cleanup: Whether to delete the local file after uploading
        **kwargs: Additional arguments to pass to the writer method

    Returns:
        S3 key of uploaded file

    Usage:
        write_pandas_to_s3(df, 'data/output.json', 'to_json', orient='records')
        write_pandas_to_s3(df, 'data/output.csv', 'to_csv', index=False)
    """
    # Ensure the directory exists
    local_path = pl.Path(s3_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.parts[0] == '..':
        local_path = pl.Path(*local_path.parts[1:])

    with s3_file(s3_path, bucket_name=bucket_name, mode='w', cleanup=cleanup) as local_path:
        # Call the writer method on the dataframe
        writer_method = getattr(df, writer_method_name)
        writer_method(local_path, **kwargs)

    # Return the S3 key (which is the path)
    return pl.Path(s3_path).as_posix()


def read_s3_json(
    s3_path,
    bucket_name='shared-data-clone',
    cleanup=False,
    **kwargs,
):
    """
    Download a JSON file from S3 and load it.

    Args:
        s3_path: Path to JSON file in S3
        bucket_name: S3 bucket name (default: 'shared-data-clone')
        cleanup: Whether to delete the local file after reading
        **kwargs: Additional arguments to pass to json.load

    Returns:
        Parsed JSON data (dict, list, etc.)

    Usage:
        data = read_s3_json('data/config.json')
    """
    with s3_file(s3_path, bucket_name=bucket_name, mode='r', cleanup=cleanup) as local_path:
        with open(local_path) as f:
            return json.load(f, **kwargs)


def write_json_to_s3(
    data,
    s3_path,
    bucket_name='shared-data-clone',
    cleanup=True,
    **kwargs,
):
    """
    Write JSON data to S3.
    
    Args:
        data: Data to write (dict, list, etc.)
        s3_path: Path to save file in S3
        bucket_name: S3 bucket name (default: 'shared-data-clone')
        cleanup: Whether to delete the local file after uploading
        **kwargs: Additional arguments to pass to json.dump (e.g., indent=2)

    Returns:
        S3 key of uploaded file

    Usage:
        write_json_to_s3({'key': 'value'}, 'data/output.json', indent=2)
    """
    # Ensure the directory exists
    local_path = pl.Path(s3_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with s3_file(s3_path, bucket_name=bucket_name, mode='w', cleanup=cleanup) as local_path:
        with open(local_path, 'w') as f:
            json.dump(data, f, **kwargs)
    
    # Return the S3 key
    return pl.Path(s3_path).as_posix()


def _quick_test():
    import pandas as pd
    import pathlib as pl
    from vdl_tools.shared_tools.s3_tools import (
        write_pandas_to_s3,
        read_s3_to_pandas,
    )

    CLEANUP = False
    paths = {
        'input_path': pl.Path('../new-shared-data/test_data/test_data_s3.json'),
        'output_path': pl.Path('../new-shared-data/test_data/test_data_s3_updated.json'),
        'path_xlsx': pl.Path('../new-shared-data/test_data/test_data_s3.xlsx'),
    }

    original_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    write_pandas_to_s3(
        original_df,
        s3_path=paths['input_path'],
        cleanup=CLEANUP,
        # Example with using the name
        writer_method_name='to_json',
        orient='records'
    )
    # Example with using the method directly
    downloaded_df = read_s3_to_pandas(paths['input_path'], pd.read_json, cleanup=CLEANUP)

    assert original_df.equals(downloaded_df)

    updated_df = original_df.copy()
    updated_df['c'] = updated_df['a'] + updated_df['b']
    write_pandas_to_s3(
        updated_df,
        s3_path=paths['output_path'],
        cleanup=CLEANUP,
        # Example with using the method
        writer_method_name='to_json',
        orient='records'
    )
    downloaded_df = read_s3_to_pandas(paths['output_path'], pd.read_json, cleanup=CLEANUP)
    assert updated_df.equals(downloaded_df)


    write_pandas_to_s3(
        updated_df,
        s3_path=paths['path_xlsx'],
        cleanup=CLEANUP,
        writer_method_name='to_excel',
        index=False
    )
    downloaded_df = read_s3_to_pandas(paths['path_xlsx'], pd.read_excel, cleanup=CLEANUP)
    assert updated_df.equals(downloaded_df)

