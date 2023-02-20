import os
import json
import time
import requests
import hashlib
import hmac
import base64
import pandas as pd
from utils.google_drive import GoogleDrive
from worker.const import ResultCode
from solution.DCT649_keyword_monitoring_solution import KeywordMonitoring


class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)

        hash.hexdigest()
        return base64.b64encode(hash.digest())


class AutoBidSolution(KeywordMonitoring):
    def __init__(self, job_name = None):
        super().__init__(job_name)
        self.api_key = os.getenv('NAVER_AD_API')
        self.secret_key = os.getenv('NAVER_AD_SECRET')
        self.madup_account = "wooddd12@madit.kr"
        self.customer_id = None
        self.bid_adjust_df = None

    def get_header(self, method, uri, api_key, secret_key, customer_id):
        timestamp = str(round(time.time() * 1000))
        signature = Signature.generate(timestamp, method, uri, secret_key)

        return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp,
                'X-API-KEY': api_key, 'X-Customer': str(customer_id), 'X-Signature': signature}

    def get_results(self, uri, method, params, data=None):
        BASE_URL = 'https://api.searchad.naver.com'
        API_KEY = self.api_key
        SECRET_KEY = self.secret_key
        CUSTOMER_ID = self.customer_id
        r = None
        if method == 'GET':
            r = requests.get(BASE_URL + uri, params=params,
                             headers=self.get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
        elif method == 'PUT':
            r = requests.put(BASE_URL + uri, params=params, json=data,
                             headers=self.get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
        return r

    def get_current_bid_amount(self, spread_sheet_url, keyword_sheet):
        gd = GoogleDrive()
        sheet = gd.get_work_sheet(spread_sheet_url, keyword_sheet)
        setting_df = gd.sheet_to_df(sheet)
        keywords_df = setting_df.drop(setting_df.loc[setting_df.iloc[:, 0] == ''].index)  # 빈행 제거
        ids = keywords_df.loc[:, '키워드 ID'].tolist()

        uri = '/ncc/keywords'
        method = 'GET'
        params = {'ids': ids}
        adgroup_res = self.get_results(uri, method, params)
        adgroup_json = json.loads(adgroup_res.text)
        id_list = []
        bid_list = []

        for j in adgroup_json:
            id_list.append(j['nccKeywordId'])
            bid_list.append(j['bidAmt'])
        bid_amount_df = pd.DataFrame({'키워드 ID': id_list, '현재 입찰가': bid_list})
        bid_amount_df = keywords_df.merge(bid_amount_df, how='left', on='키워드 ID')
        return bid_amount_df

    def do_work(self, info: dict, attr: dict):
        try:
            # 상속받은 키워드 모니터링 솔루션 실행
            super().do_work(info, attr)
            rank_df = pd.DataFrame()
            rank_df = pd.concat([rank_df, self.result_df])
            # 현재 순위 모니터링 결과 받아오기
            rank_df = rank_df.rename(
                columns={'ad_keyword': '키워드', 'pc_mobile_type': '디바이스', 'ad_rank': '현재 순위', 'date': '일시'})
            rank_df = rank_df.loc[:, ['키워드', '디바이스', '현재 순위', '일시']]
            # 현재 키워드 입찰가 받아오기
            self.customer_id = info.get("customer_id")
            spread_sheet_url = info.get("spread_sheet_url")
            keyword_sheet = info.get("keyword_sheet")
            bid_amount_df = self.get_current_bid_amount(spread_sheet_url, keyword_sheet)
            # 키워드 순위와 입찰가 정보 머징
            self.bid_adjust_df = bid_amount_df.merge(rank_df, how='left', on=['키워드', '디바이스'])
            result_msg = "AutoBidSolution is ready"
        except Exception as e:
            self.logger.info(e)
            raise e

        return {
            "result_code": ResultCode.SUCCESS,
            "msg": result_msg,
            "result_df": self.bid_adjust_df
        }


# for id in adgroup_list:
#     uri = f'/ncc/adgroups/{id}/restricted-keywords'
#     method = 'GET'
#     params = {}
#
#     temp_res = get_results(uri, method, params)
#     temp_json = json.loads(temp_res.text)
#
#
# ### 입찰가 변경 테스트
# uri = '/ncc/keywords'
# method = 'PUT'
# params = {'fields': 'bidAmt'}
# data = [{'nccKeywordId': 'nkw-a001-01-000005118934433',
#          'nccAdgroupId': 'grp-a001-01-000000032024509',
#          'bidAmt': 700, 'useGroupBidAmt': 0},
#         {'nccKeywordId': 'nkw-a001-01-000005118934434',
#          'nccAdgroupId': 'grp-a001-01-000000032024509',
#          'bidAmt': 600, 'useGroupBidAmt': 0}
#         ]
# change_result = get_results(uri, method, params, data)
# change_json = json.loads(change_result.text)
#
#
# ### 키워드 노출 순위
# uri = '/stats'
# method = 'GET'
# params = {'id': 'nkw-a001-01-000005001031101', 'fields': '["impCnt", "avgRnk", "recentAvgRnk"]',
#           'timeRange': '{"since": "2023-01-25", "until": "2023-01-27"}'}
# params = {'id': 'nkw-a001-01-000001569480144', 'fields': '["avgRnk"]',
#           'timeIncrement': 'allDays',
#           'datePreset': 'today', 'breakdown': 'pcMblTp'}
# stat_result = get_results(uri, method, params)
# stat_json = json.loads(stat_result.text)
#
#
#
# ### 키워드 실시간 입찰가 확인
# uri = '/ncc/keywords'
# method = 'GET'
# params = {'ids': ['nkw-a001-01-000004694765496']}
# stat_result = get_results(uri, method, params)
# stat_json = json.loads(stat_result.text)