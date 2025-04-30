import boto3
import sys
import pathlib as pl

from vdl_tools.shared_tools.tools.config_utils import get_configuration

from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
)

config = get_configuration()
wd = pl.Path.cwd()

REGION = 'us-east-2'
ACCESS_KEY = config['aws']['access_key_id']
SECRET_KEY = config['aws']['secret_access_key']
BUCKET = 'fine-tuned-vdl-models'
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION
)


def put_file(key, filename):
    s3_client.upload_file(filename, BUCKET, key)


def list_all_files(prefix: str):
    return [x['Key'] for x in s3_client.list_objects(Bucket=BUCKET, Prefix=prefix)['Contents']]


def s3_file(key, filename):
    """
    Yields a file object from the filename at fine-tuned-vdl-models/{key}

    Args:
        key (str): Relative path from the base of your bucket, including the filename and extension of the object to be retrieved.
    """
    meta_data = s3_client.head_object(Bucket=BUCKET, Key=key)
    total_length = int(meta_data.get('ContentLength', 0))
    downloaded = 0

    def progress(chunk):
        nonlocal downloaded
        downloaded += chunk
        done = int(50 * downloaded / total_length)
        percentage = round(downloaded / total_length * 100, 0)
        sys.stdout.write("\r[%s%s] %s" % ('=' * done, ' ' * (50-done), f'{percentage}%') )
        sys.stdout.flush()

    sys.stdout.write('\n')
    print(f'Downloading {key}')
    s3_client.download_file(BUCKET, key, filename, Callback=progress)



def s3_fileobj(key, f):
    """
    Yields a file object from the filename at fine-tuned-vdl-models/{key}

    Args:
        key (str): Relative path from the base of your bucket, including the filename and extension of the object to be retrieved.
    """
    meta_data = s3_client.head_object(Bucket=BUCKET, Key=key)
    total_length = int(meta_data.get('ContentLength', 0))
    downloaded = 0

    def progress(chunk):
        nonlocal downloaded
        downloaded += chunk
        done = int(50 * downloaded / total_length)
        percentage = round(downloaded / total_length * 100, 0)
        sys.stdout.write("\r[%s%s] %s" % ('=' * done, ' ' * (50-done), f'{percentage}%') )
        sys.stdout.flush()

    sys.stdout.write('\n')
    print(f'Downloading {key}')
    s3_client.download_fileobj(BUCKET, key, f, Callback=progress)


def __create_model(model_folder: pl.Path):
    local_model = AutoModelForSequenceClassification.from_pretrained(model_folder, num_labels=3)
    tokenizer = AutoTokenizer.from_pretrained(model_folder)
    return local_model, tokenizer


def load_model(path_to_model):
    root_model_folder: pl.Path = wd / 'models'
    model_folder: pl.Path = wd / 'models' / path_to_model

    model_loaded = model_folder.exists()

    if model_loaded:
        print(f'Model {path_to_model} found, loading...')
        return __create_model(model_folder)
    
    model_folder.mkdir(parents=True, exist_ok=True)

    model_files = list_all_files(path_to_model)
    for file_name in model_files:
        p = root_model_folder / file_name
        with open(p, 'wb+') as f:
            s3_fileobj(file_name, f)

    return __create_model(model_folder)

if __name__ == "__main__":
    model, tokenizer = load_model('cwf_adaptation')