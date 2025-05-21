import configparser
import os
from typing import Dict
from pathlib import Path
import boto3
import vdl_tools.shared_tools.s3_tools as s3_tools
from vdl_tools.shared_tools.tools.config_utils import get_configuration

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

def has_google_analytics(data: str) -> bool:
    return "googletagmanager.com" in data


def has_publish_tags(data: str) -> bool:
    return "<title>openmappr | network exploration tool</title>" not in data


def s3_worker(path: Path, bucket_name: str):
    _file_mapping: Dict[str, str] = {
        "html": "text/html",
        "json": "application/json",
        "sh": "text/x-shellscript",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "gif": "image/gif",
        "svg": "image/svg+xml",
    }
    ### Config Setup ###
    config = get_configuration()
    # load AWS settings from config file
    REGION = config["aws"]["region"]
    ACCESS_KEY = config["aws"]["access_key_id"]
    SECRET_KEY = config["aws"]["secret_access_key"]
    S3_CLIENT = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )
    
    # create bucket if it doesn't exist
    s3_tools.create_bucket(S3_CLIENT, bucket_name)

    # Create the configuration for the website
    website_configuration = {
        "ErrorDocument": {"Key": "error.html"},
        "IndexDocument": {"Suffix": "index.html"},
    }
    # Set the new policy on the selected bucket
    S3_CLIENT.put_bucket_website(
        Bucket=bucket_name, WebsiteConfiguration=website_configuration
    )

    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)

    try:
        bucket.objects.filter(Prefix='data/').delete()
    except Exception as e:
        print(f"Error cleaning 'data/' folder in the bucket: {e}")

    data_path = str(path)
    for subdir, _, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, "rb") as data:
                ext = full_path.split(".")[-1]
                object_key = (
                    full_path[len(data_path) + 1 :]
                    if subdir == data_path
                    else "%s/%s"
                    % (os.path.basename(subdir), os.path.basename(full_path))
                )

                print(f"Putting {object_key} to {bucket_name}")
                if 'DS_Store' in ext:
                    continue
                bucket.put_object(
                    Key=object_key,
                    Body=data,
                    ContentType=_file_mapping[ext],
                )

    print(
        "\nUpload complete. To view your map, go to http://%s.s3-website-%s.amazonaws.com/"
        % (bucket_name, REGION)
    )

    return {"bucket": bucket_name, "region": REGION}
