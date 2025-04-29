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
