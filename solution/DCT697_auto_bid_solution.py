import os
import sys
import json
import time
import requests
import hashlib
import hmac
import base64
import gspread
import pandas as pd
from utils.google_drive import GoogleDrive
from worker.const import ResultCode
import boto3
from utils.s3 import download_file, upload_file, build_partition_s3
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


class Response:
    status_code = None
    reason = None
    dict = {}


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
        self.result_msg = ''
        self.result = Response()
        self.bid_downgrade = True

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
        RETRY_CNT = 3
        WAITING_TIME = 60
        gd = GoogleDrive()
        for i in range(RETRY_CNT):
            try:
                sheet = gd.get_work_sheet(spread_sheet_url, keyword_sheet)
                break
            except gspread.exceptions.APIError as e:
                error_code = e.args[0].get("code")
                if error_code == 429 and i != RETRY_CNT - 1:
                    # Call Limit으로 인한 Quota 초과인 경우 대기 후 RETRY_CNT만큼 재시도.
                    time.sleep(WAITING_TIME)
                    continue
                raise e
        setting_df = gd.sheet_to_df(sheet)
        setting_df = setting_df.iloc[:, :11]
        keywords_df = setting_df.drop(setting_df.loc[setting_df.iloc[:, 0] == ''].index)  # 빈행 제거
        keywords_df = keywords_df.rename(columns={'키워드': 'ad_keyword', '디바이스': 'pc_mobile_type',
                                                  '캠페인명': 'campaign_name', '광고그룹명': 'adgroup_name',
                                                  '캠페인 ID': 'campaign_id', '광고그룹 ID': 'adgroup_id',
                                                  '키워드 ID': 'ad_keyword_id', '목표 순위': 'goal_rank',
                                                  '최소 입찰가': 'min_bid', '최대 입찰가': 'max_bid',
                                                  '입찰 강도': 'bid_degree'})
        keywords_df['min_bid'] = keywords_df['min_bid'].apply(lambda x: x.replace(',', ''))
        keywords_df['max_bid'] = keywords_df['max_bid'].apply(lambda x: x.replace(',', ''))
        ids = keywords_df.loc[:, 'ad_keyword_id'].tolist()

        uri = '/ncc/keywords'
        method = 'GET'
        params = {'ids': ids}
        adgroup_res = self.get_results(uri, method, params)
        adgroup_json = json.loads(adgroup_res.text)
        id_list = []
        bid_list = []
        use_gbamt_list = []
        if type(adgroup_json) == dict:
            return str(adgroup_json)
        for j in adgroup_json:
            id_list.append(j['nccKeywordId'])
            bid_list.append(j['bidAmt'])
            use_gbamt_list.append(j['useGroupBidAmt'])
        bid_amount_df = pd.DataFrame({'ad_keyword_id': id_list, 'cur_bid': bid_list, 'use_groupbid': use_gbamt_list})
        bid_amount_df = keywords_df.merge(bid_amount_df, how='left', on='ad_keyword_id')
        return bid_amount_df

    def get_bid_amt_info(self, owner_id, channel):
        max_bid_msg = [f"## 최대 입찰가 상향 필요 ##"]
        min_bid_msg = [f"## 최소 입찰가 하향 필요 ##"]
        for index, row in self.bid_adjust_df.iterrows():
            campaign = row['campaign_name']
            adgroup = row['adgroup_name']
            keyword = row['ad_keyword']
            keyword_id = row['ad_keyword_id']
            group_id = row['adgroup_id']
            goal_rank = int(row['goal_rank'])
            min_bid = int(row['min_bid'])
            max_bid = int(row['max_bid'])
            bid_degree = float(row['bid_degree'])
            cur_bid = int(row['cur_bid'])
            cur_rank = row['ad_rank']
            use_gbamt = row['use_groupbid']
            if cur_rank == '-':
                cur_rank = 30
            else:
                cur_rank = int(cur_rank)
            if goal_rank < cur_rank:
                # 현재 순위가 목표 순위 보다 낮은 경우
                if cur_bid > max_bid:
                    if cur_rank == 30:
                        cur_rank = '-'
                    max_bid_msg.append(f"{keyword}: {cur_rank}위({goal_rank}위) / 현재 {cur_bid}원, 최대 {max_bid}원 /// {campaign}, {adgroup}")
                    bid_amt = int(round(max_bid / 10) * 10)
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
                elif cur_bid == max_bid:
                    if cur_rank == 30:
                        cur_rank = '-'
                    max_bid_msg.append(f"{keyword}: {cur_rank}위({goal_rank}위) / 현재 {cur_bid}원, 최대 {max_bid}원 /// {campaign}, {adgroup}")
                    bid_amt = cur_bid
                else:
                    bid_amt = int(round(cur_bid * (1 + bid_degree) / 10) * 10)
                    if bid_amt > max_bid:
                        bid_amt = int(round(max_bid / 10) * 10)
                    elif bid_amt < min_bid:
                        bid_amt = int(round(min_bid / 10) * 10)
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
            elif goal_rank > cur_rank:
                if self.bid_downgrade is False:
                    bid_amt = cur_bid
                # 현재 순위가 목표 순위 보다 높은 경우
                else:
                    if cur_bid < min_bid:
                        min_bid_msg.append(f"{keyword}: {cur_rank}위({goal_rank}위) / 현재 {cur_bid}원, 최소 {min_bid}원 /// {campaign}, {adgroup}")
                        bid_amt = int(round(min_bid / 10) * 10)
                        data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                        self.data_list.append(data)
                    elif cur_bid == min_bid:
                        min_bid_msg.append(f"{keyword}: {cur_rank}위({goal_rank}위) / 현재 {cur_bid}원, 최소 {min_bid}원 /// {campaign}, {adgroup}, ")
                        bid_amt = cur_bid
                    else:
                        bid_amt = int(round(cur_bid * (1 - bid_degree) / 10) * 10)
                        if bid_amt < min_bid:
                            bid_amt = int(round(min_bid / 10) * 10)
                        elif bid_amt > max_bid:
                            bid_amt = int(round(max_bid / 10) * 10)
                        data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                        self.data_list.append(data)
            else:
                # 현재 순위가 목표 순위와 같은 경우
                bid_amt = cur_bid
                if bid_amt > max_bid:
                    bid_amt = int(round(max_bid / 10) * 10)
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
                elif bid_amt < min_bid:
                    bid_amt = int(round(min_bid / 10) * 10)
                    data = dict(nccKeywordId=keyword_id, nccAdgroupId=group_id, bidAmt=bid_amt, useGroupBidAmt=use_gbamt)
                    self.data_list.append(data)
            self.bid_adjust_df.loc[index, 'next_bid'] = bid_amt
        result_msg = [f"{owner_id} {channel} 입찰가 조정 결과"]
        result_msg = result_msg + max_bid_msg + ['\n'] + min_bid_msg
        return result_msg

    def adjust_bid_amt(self):
        uri = '/ncc/keywords'
        method = 'PUT'
        params = {'fields': 'bidAmt'}
        data = self.data_list
        result = Response()
        if len(data) > 0:
            total_result = self.get_results(uri, method, params, data)
            if total_result.status_code != 200:
                text = json.loads(total_result.text)
                if text['title'] == "The target keyword you requested does not exist.":
                    result.status_code = text['status']
                    text_temp = []
                    for i in data:
                        time.sleep(self.searching_waiting_time)
                        change_result = self.get_results(uri, method, params, [i])
                        if change_result.status_code != 200:
                            text_temp.append("Request error: " + str(i))
                            result.dict[i['nccKeywordId']] = 'Failed'
                        else:
                            result.dict[i['nccKeywordId']] = 'Success'
                    result.reason = '\n'.join(text_temp)
                else:
                    total_result.dict = {}
                    for i in data:
                        total_result.dict[i['nccKeywordId']] = 'Failed'
                    result = total_result
                    result.reason = str(text)
            else:
                total_result.dict = {}
                for i in data:
                    total_result.dict[i['nccKeywordId']] = 'Success'
                result = total_result
            return result
        else:
            if len(self.result_msg) > 4:
                result.status_code = 200
                result.reason = '\n'.join(self.result_msg)
            else:
                result.status_code = 200
                result.reason = '입찰 조정할 사항이 없습니다.'
            result.dict = {}
            for i in data:
                result.dict[i['nccKeywordId']] = 'Skip'
            return result

    def auto_bid_result_s3_update(self, owner_id, channel):
        self.s3_log_path = self.s3_folder + "/" + f"owner_id={owner_id}/channel={channel}/year={self.year}/month={self.month}" \
                                                  f"/day={self.day}/hour={self.hour}/minute={self.minute}/auto_bid_log.csv"
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
            super().do_work(info, attr)
            rank_df = pd.DataFrame()
            rank_df = pd.concat([rank_df, self.result_df])
            # 현재 키워드 순위 모니터링 결과 받아오기
            rank_df = rank_df.loc[:, ['ad_keyword', 'pc_mobile_type', 'ad_rank', 'date']]

            # 입찰가 조정 로그 저장폴더 변경
            if info.get("s3_folder"):
                self.s3_folder = info.get("s3_folder")
            else:
                self.s3_folder = 'auto_bid'
            # 현재 키워드 입찰가 받아오기
            self.customer_id = info.get("customer_id")
            spread_sheet_url = info.get("spread_sheet_url")
            keyword_sheet = info.get("keyword_sheet")
            bid_amount_df = self.get_current_bid_amount(spread_sheet_url, keyword_sheet)
            if type(bid_amount_df) == str:
                return {
                    "result_code": ResultCode.ERROR,
                    "msg": bid_amount_df
                }

            # 현재 키워드 순위와 현재 입찰가 정보 머징
            self.bid_adjust_df = bid_amount_df.merge(rank_df, how='left', on=['ad_keyword', 'pc_mobile_type'])
            if len(self.bid_adjust_df.loc[self.bid_adjust_df['cur_bid'].isnull()]) > 0:
                check_list = list(self.bid_adjust_df.loc[self.bid_adjust_df['cur_bid'].isnull(), 'ad_keyword'])
                return {
                    "result_code": ResultCode.ERROR,
                    "msg": f"Check keyword status {check_list}"
                }

            # 입찰가 조정
            owner_id = attr.get("owner_id")
            channel = attr.get("channel")
            self.bid_downgrade = info.get("bid_downgrade")
            self.result_msg = self.get_bid_amt_info(owner_id, channel)
            self.result = self.adjust_bid_amt()
            self.bid_adjust_df.insert(0, 'customer_id', self.customer_id)
            # 입찰가 조정 결과 머징
            self.bid_adjust_df['result'] = self.bid_adjust_df.ad_keyword_id.apply(lambda x: self.result.dict[x] if x in self.result.dict.keys() else 'Skip')
            # 당월 폴더 없는 경우 파티션 신규 생성
            default_path = self.s3_folder + "/" + f"owner_id={owner_id}/channel={channel}"
            directory_name = f"{default_path}/year={self.year}/month={self.month}"
            s3 = boto3.client("s3")
            res = s3.list_objects_v2(Bucket=DEFAULT_S3_PRIVATE_BUCKET, Prefix=directory_name, MaxKeys=1)
            if 'Contents' not in res:
                build_partition_s3(default_s3_path=default_path, standard_date=self.now_time, s3_bucket=DEFAULT_S3_PRIVATE_BUCKET)
            # 입찰가 조정 결과 s3 업데이트
            self.auto_bid_result_s3_update(owner_id, channel)
            if self.result.status_code != 200:
                self.result_msg = self.result.reason

        except Exception as e:
            self.logger.info(e)
            raise e

        if self.result.status_code == 200:
            if len(self.result_msg) > 4:
                return {
                    "result_code": ResultCode.SUCCESS,
                    "msg": "\n".join(self.result_msg),
                }
            else:
                return {
                    "result_code": ResultCode.SUCCESS,
                    "msg": "\n".join(self.result_msg),
                    "result_df": self.data_list
                }
        else:
            return {
                "result_code": ResultCode.ERROR,
                "msg": self.result_msg,
                "data_list": self.data_list
            }
