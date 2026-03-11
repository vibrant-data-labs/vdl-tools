import boto3
from vdl_tools.shared_tools.tools.config_utils import get_configuration
from bs4 import BeautifulSoup
from pathlib import Path

config = get_configuration()

REGION = config['aws']['region']
ACCESS_KEY = config['aws']['access_key_id']
SECRET_KEY = config['aws']['secret_access_key']

PLAYER_MAIN_BUCKET = "mappr-player"

s3 = boto3.client('s3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
)

response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets'] if bucket['Name'] != PLAYER_MAIN_BUCKET]

with open(Path(__file__).parent.parent / '_templates/index.html', 'r', encoding='utf-8') as f:
    template_html = f.read()

index_template = BeautifulSoup(template_html, 'html.parser')

print(f'Found {len(buckets)} buckets')
for bucket in buckets:
    print(f'Processing bucket: {bucket}')
    index_key = 'index.html'
    try:
        index_contents = s3.get_object(Bucket=bucket, Key=index_key)['Body'].read().decode('utf-8')
    except Exception as e:
        print(f'Index file not found in bucket: {bucket}, skipping')
        continue
    print(f'Index file found in bucket: {bucket}, updating')
    with open(Path(__file__).parent.parent / '_templates/index_remote.html', 'w+', encoding='utf-8') as f:
        f.write(index_contents)

    index_contents = BeautifulSoup(index_contents, 'html.parser')
    
    # update all "body script" elements
    for script in index_contents.select('body script'):
        script.decompose()

    for script in index_template.select('body script'):
        index_contents.body.append(script)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=index_key,
            Body=index_contents.prettify(),
            ACL='public-read',
            ContentType='text/html')
    except Exception as e:
        # some old buckets was created using object ACL instead of bucket ACL
        s3.put_object(
            Bucket=bucket,
            Key=index_key,
            Body=index_contents.prettify(),
            ContentType='text/html')

    print(f'Index file scripts updated in bucket: {bucket}')
