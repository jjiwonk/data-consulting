from utils.const import DEFAULT_S3_PRIVATE_BUCKET
from utils.s3 import build_partition_s3
from datetime import datetime

# s3 폴더 경로 생성
solution = 'keyword_monitoring'
owner_id = '29cm'
channel = '네이버SA'
start_date = '2023-03-03'
default_s3_path = f'{solution}/owner_id={owner_id}/channel={channel}'

# 파티션 생성 함수 실행
date = datetime.strptime(start_date, "%Y-%m-%d")
build_partition_s3(default_s3_path, date, s3_bucket=DEFAULT_S3_PRIVATE_BUCKET, start_day=date.day)
