###########################################################################################################
# README (To do before use)                                                                               #
# 1. 아래 코드 활용 이전에 GoogleAdsClient import 하기 위한 라이브러리 설치 필요                                   #
#    참고: https://developers.google.com/google-ads/api/docs/client-libs/python/installation?hl=ko         #
# 2. yaml 파일 s3 업로드 필요                                                                                #
#    경로: data-consulting-private/job_info/owner_id={광고주명}/google_auth.txt 형식                         #
###########################################################################################################
import os
from utils.s3 import get_info_from_s3
from google.ads.googleads.client import GoogleAdsClient


def get_google_client(owner_id):
    get_yaml = get_info_from_s3(owner_id, "google_auth")
    for key in get_yaml.keys():
        env_key = "GOOGLE_ADS_" + key.upper()
        os.environ[env_key] = get_yaml[key]
    client = GoogleAdsClient.load_from_env()
    return client
