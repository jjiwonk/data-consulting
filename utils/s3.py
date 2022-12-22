import os

import boto3

from utils.const import DEFAULT_S3_PUBLIC_BUCKET, DEFAULT_S3_PRIVATE_BUCKET
from utils.path_util import get_tmp_path


def download_file(s3_path: str, s3_bucket: str = DEFAULT_S3_PRIVATE_BUCKET, local_path: str = get_tmp_path()) -> str:
    s3 = boto3.client("s3")
    if os.path.isdir(get_tmp_path()):
        local_path = local_path.rstrip("/") + "/" + s3_path.split("/")[-1]
    try:
        s3.download_file(s3_bucket, s3_path, local_path)
    except Exception as e:
        print(e)
        raise e
    return local_path


def upload_file(local_path: str, s3_path: str, s3_bucket: str = DEFAULT_S3_PUBLIC_BUCKET) -> str:
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_path, s3_bucket, s3_path)
    except Exception as e:
        print(e)
        raise e
    return f"https://{s3_bucket}.s3.ap-northeast-2.amazonaws.com/{s3_path}"


def delete_file(s3_bucket: str, s3_path: str):
    s3 = boto3.client("s3")
    s3.delete_object(Bucket=s3_bucket, Key=s3_path)
