import os
import boto3
from datetime import datetime, date
import time
import calendar
from utils.const import DEFAULT_S3_PUBLIC_BUCKET, DEFAULT_S3_PRIVATE_BUCKET
from utils.path_util import get_tmp_path


def download_file(s3_path: str, s3_bucket: str = DEFAULT_S3_PUBLIC_BUCKET, local_path: str = get_tmp_path()) -> str:
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
    if '.png' in local_path :
        content_type = 'image/png'
    else :
        content_type = 'application/octet-stream'
    try:
        s3.upload_file(local_path, s3_bucket, s3_path, ExtraArgs={'ContentType' : content_type})
    except Exception as e:
        print(e)
        raise e
    url = f"https://{s3_bucket}.s3.ap-northeast-2.amazonaws.com/{s3_path}"
    return url


def delete_file(s3_bucket: str, s3_path: str):
    s3 = boto3.client("s3")
    s3.delete_object(Bucket=s3_bucket, Key=s3_path)


def get_info_from_s3(owner_id, product_id):
    s3_path = 'job_info/owner_id={}/{}.txt'.format(owner_id, product_id)
    local_path = get_tmp_path()
    info_dir = download_file(s3_path=s3_path, s3_bucket=DEFAULT_S3_PRIVATE_BUCKET, local_path=local_path)
    f = open(info_dir, 'r', encoding='utf-8-sig')
    info = eval(f.read())
    f.close()
    os.remove(info_dir)
    return info


def build_partition_s3(default_s3_path, standard_date: datetime=datetime.now(), s3_bucket=DEFAULT_S3_PUBLIC_BUCKET):
    s3 = boto3.client('s3')
    year = standard_date.strftime('%Y')
    month = standard_date.strftime('%m')
    num_days = calendar.monthrange(standard_date.year, standard_date.month)[1]
    days = [date(standard_date.year, standard_date.month, day) for day in range(1, num_days + 1)]
    for day in days:
        day = day.strftime('%d')
        for hour in list(range(0, 24, 1)):
            hour = str(hour).zfill(2)
            for minute in list(range(0, 60, 5)):
                minute = str(minute).zfill(2)
                directory_name = f"{default_s3_path}/year={year}/month={month}/day={day}/hour={hour}/minute={minute}"
                try:
                    s3.put_object(Bucket=s3_bucket, Key=(directory_name + '/'))
                except Exception as e:
                    time.sleep(30)
                    s3.put_object(Bucket=s3_bucket, Key=(directory_name + '/'))
                    pass
