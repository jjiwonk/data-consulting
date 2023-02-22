import os
import sys
import json
import time
import requests
import hashlib
import hmac
import base64
import pandas as pd
from utils.google_drive import GoogleDrive
from worker.const import ResultCode
from utils.s3 import download_file, upload_file
from utils.const import DEFAULT_S3_PRIVATE_BUCKET
from solution.DCT649_keyword_monitoring_solution import KeywordMonitoring


class Signature:
    @staticmethod
    def generate(timestamp, method, uri, secret_key):
        message = "{}.{}.{}".format(timestamp, method, uri)
        if sys.version_info.major == 3:
            hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
        else:
            hash = hmac.new(bytes(secret_key, "Latin - 1"), bytes(message, "Latin - 1"), hashlib.sha256)

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
        self.s3_log_path = ''
        self.data_list = []
        self.result = None

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
        use_gbamt_list = []

        for j in adgroup_json:
            id_list.append(j['nccKeywordId'])
            bid_list.append(j['bidAmt'])
            use_gbamt_list.append(j['useGroupBidAmt'])
        bid_amount_df = pd.DataFrame({'키워드 ID': id_list, '현재 입찰가': bid_list, '그룹예산 사용여부':use_gbamt_list})
        bid_amount_df = keywords_df.merge(bid_amount_df, how='left', on='키워드 ID')
        return bid_amount_df

    def get_bid_amt_info(self, owner_id, channel):
        max_bid_msg = [f"## 최대 입찰가 조정 필요 ##"]
        min_bid_msg = [f"## 최소 입찰가 조정 필요 ##"]
        for index, row in self.bid_adjust_df.iterrows():
            keyword = row['키워드']
            keyword_id = row['키워드 ID']
            group_id = row['광고그룹 ID']
            goal_rank = int(row['목표 순위'])
            min_bid = int(row['최소 입찰가'])
            max_bid = int(row['최대 입찰가'])
            bid_degree = float(row['입찰 강도'])
            cur_bid = int(row['현재 입찰가'])
            cur_rank = row['현재 순위']
            use_gbamt = row['그룹예산 사용여부']
            if cur_rank == '-':
                cur_rank = 30
            else:
                cur_rank = int(cur_rank)
            if goal_rank < cur_rank:
                # 현재 순위가 목표 순위 보다 낮은 경우
                if cur_bid >= max_bid:
                    max_bid_msg.append(f"   {group_id} {keyword}: {cur_rank}위 / 최대 {max_bid}원, 현재 {cur_bid}원")
                    bid_amt = cur_bid
                elif cur_bid < max_bid:
                    bid_amt = int(round(cur_bid * (1 + bid_degree) / 10) * 10)
                    if bid_amt > max_bid:
                        bid_amt = max_bid
                    elif bid_amt < min_bid:
                        bid_amt = min_bid
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
            elif goal_rank > cur_rank:
                # 현재 순위가 목표 순위 보다 높은 경우
                if cur_bid <= min_bid:
                    min_bid_msg.append(f"   {group_id} {keyword}: {cur_rank}위 / 최소 {min_bid}원, 현재 {cur_bid}원")
                    bid_amt = cur_bid
                elif cur_bid > min_bid:
                    bid_amt = int(round(cur_bid * (1 - bid_degree) / 10) * 10)
                    if bid_amt < min_bid:
                        bid_amt = min_bid
                    elif bid_amt > max_bid:
                        bid_amt = max_bid
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
            else:
                # 현재 순위가 목표 순위와 같은 경우
                bid_amt = cur_bid
            self.bid_adjust_df.loc[index, '조정 입찰가'] = bid_amt
        result_msg = [f"{owner_id} {channel} 입찰가 조정 결과"]
        result_msg = result_msg + max_bid_msg + min_bid_msg
        return result_msg

    def adjust_bid_amt(self):
        uri = '/ncc/keywords'
        method = 'PUT'
        params = {'fields': 'bidAmt'}
        data = self.data_list
        change_result = self.get_results(uri, method, params, data)
        return change_result

    def auto_bid_result_s3_update(self, owner_id, channel):
        self.s3_log_path = self.s3_folder + "/" + f"owner_id={owner_id}/channel={channel}/year={self.year}/month={self.month}/day={self.day}_auto_bid_log.csv"
        previous_file_path = None
        try:
            previous_file_path = download_file(s3_path=self.s3_log_path, local_path=self.tmp_path,
                                               s3_bucket=DEFAULT_S3_PRIVATE_BUCKET)
        except:
            self.logger.info(f"no previous file on {self.s3_log_path}")
            pass
        if previous_file_path:
            # s3 저장
            previous_df = pd.read_csv(previous_file_path, encoding='utf-8-sig')
            previous_df = previous_df.astype(str)
            total_df = pd.concat([previous_df, self.bid_adjust_df], axis=0, ignore_index=True)
            total_df = total_df.drop_duplicates(keep='last')
            total_df.to_csv(previous_file_path, encoding='utf-8-sig', index=False)
            upload_file(local_path=previous_file_path, s3_path=self.s3_log_path, s3_bucket=DEFAULT_S3_PRIVATE_BUCKET)
            os.remove(previous_file_path)
        else:
            total_df = self.bid_adjust_df
            result_path = self.tmp_path + f'/day={self.day}.csv'
            total_df.to_csv(result_path, encoding='utf-8-sig', index=False)
            # s3 저장
            upload_file(local_path=result_path, s3_path=self.s3_log_path, s3_bucket=DEFAULT_S3_PRIVATE_BUCKET)
            os.remove(result_path)

    def do_work(self, info: dict, attr: dict):
        try:
            # 상속받은 키워드 모니터링 솔루션 실행
            if info.get("s3_folder"):
                self.s3_folder = info.get("s3_folder")
            else:
                self.s3_folder = 'auto_bid'
            super().do_work(info, attr)
            rank_df = pd.DataFrame()
            rank_df = pd.concat([rank_df, self.result_df])

            # 현재 키워드 순위 모니터링 결과 받아오기
            rank_df = rank_df.rename(
                columns={'ad_keyword': '키워드', 'pc_mobile_type': '디바이스', 'ad_rank': '현재 순위', 'date': '일시'})
            rank_df = rank_df.loc[:, ['키워드', '디바이스', '현재 순위', '일시']]

            # 현재 키워드 입찰가 받아오기
            self.customer_id = info.get("customer_id")
            spread_sheet_url = info.get("spread_sheet_url")
            keyword_sheet = info.get("keyword_sheet")
            bid_amount_df = self.get_current_bid_amount(spread_sheet_url, keyword_sheet)

            # 현재 키워드 순위와 현재 입찰가 정보 머징
            self.bid_adjust_df = bid_amount_df.merge(rank_df, how='left', on=['키워드', '디바이스'])

            # 입찰가 조정
            owner_id = attr.get("owner_id")
            channel = attr.get("channel")
            result_msg = self.get_bid_amt_info(owner_id, channel)
            self.result = self.adjust_bid_amt()
            if self.result.status_code != 200:
                raise self.result.text

            # 입찰가 조정 결과 로그 s3 업데이트
            self.auto_bid_result_s3_update(owner_id, channel)

        except Exception as e:
            self.logger.info(e)
            raise e

        if self.result.status_code == 200:
            return {
            "result_code": ResultCode.SUCCESS,
            "msg": "\n".join(result_msg),
            "result_df": self.data_list
        }
        else:
            return {
                "result_code": ResultCode.ERROR,
            }
