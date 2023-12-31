import os
import pandas as pd
import random
from ast import literal_eval
import time
from datetime import datetime
from pytz import timezone

from utils.google_drive import (
    GoogleDrive,
)
import requests
from bs4 import BeautifulSoup
import math
from utils.s3 import upload_file
from worker.const import ResultCode
from utils.const import DEFAULT_S3_PRIVATE_BUCKET, DEFAULT_S3_PUBLIC_BUCKET
from utils.path_util import get_tmp_path
from worker.abstract_worker import Worker


class Key:
    WEEK_DAYS = ["월", "화", "수", "목", "금", "토", "일"]
    WEEKDAYS_SIZE = 5

    REQUESTS_TIMEOUT = 5
    CRAWLING_RETRY_CNT = 5

    # requests에 headers로 넣는 정보
    # https://deviceatlas.com/blog/list-of-user-agent-strings 참고
    USER_AGENTS = {
        0: {
            # Windows 10-based PC using Edge browser
            "PC": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
            # Apple iPhone 8 Plus
            "MO": "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) "
                  "AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A5370a Safari/604.1",
        },
        1: {
            # Chrome OS-based laptop using Chrome browser (Chromebook)
            "PC": "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/51.0.2704.64 Safari/537.36",
            # Apple iPhone SE (3rd generation)
            "MO": "Mozilla/5.0 (iPhone14,6; U; CPU iPhone OS 15_4 like Mac OS X) "
                  "AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/19E241 Safari/602.1",
        },
        2: {
            # Mac OS X-based computer using a Safari browser
            "PC": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/35.0.1916.47 Safari/537.36",
            # iPhone 13 Pro Max
            "MO": "Mozilla/5.0 (iPhone14,3; U; CPU iPhone OS 15_0 like Mac OS X) "
                  "AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/19A346 Safari/602.1",
        },
        3: {
            # Windows 7-based PC using a Chrome browser
            "PC": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/47.0.2526.111 Safari/537.36",
            # iPhone 12
            "MO": "Mozilla/5.0 (iPhone13,2; U; CPU iPhone OS 14_0 like Mac OS X) "
                  "AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/15E148 Safari/602.1",
        },
        4: {
            "PC": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/92.0.8896.9453 Safari/537.36",
            # Apple iPhone XS (Chrome)
            "MO": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/69.0.3497.105 Mobile/15E148 Safari/605.1",
        }
    }
    KST = timezone("Asia/Seoul")
    UTC = timezone("UTC")
    USE_HEADLESS = False
    S3_BUCKET = DEFAULT_S3_PRIVATE_BUCKET


def is_ad(tag_element, ad_names):
    # 해당 광고가 입력 받은 광고의 광고주명, URL인지 확인하여 참 거짓 반환.
    tag_text = tag_element.text.split("\n")
    for tag_t in tag_text:
        if any(ad_name.lower() in tag_t.lower() for ad_name in ad_names):
            return True
    return False


def to_int(value: str, none_is_zero: bool = False) -> int:
    if value is None and none_is_zero:
        return 0
    try:
        return int(float(value.replace(',', '')))
    except Exception as e:
        raise Exception(f"[{value}] can not convert integer")


class Device:
    def __init__(self, device_type, keyword_column, search_infos):

        self.device_type: str = device_type  # PC or MO
        search_info = search_infos.get(device_type)
        self.search_url: str = search_info.get("search_url")
        self.selector: str = search_info.get("selector")
        self.ad_name_path: str = search_info.get("ad_info_path")  # 회사명 또는 URL을 포함하는 위치
        self.ads_in_page: int = search_info.get("ads_in_page")  # 검색한 페이지에 존재하는 광고의 개수

        # 키워드에 해당하는 컬럼명을 잡인포로 받지 않거나 시트에 존재하지 않으면, run 함수 내에서 첫 번 째 컬럼으로 지정.
        self.keyword_column: str = keyword_column
        self.keywords: list = []


class KeywordMonitoring(Worker):
    def __init__(self, job_name):
        super().__init__(job_name=job_name)
        self.searching_waiting_time = 1
        self.driver = None
        self.now_time = datetime.now(Key.KST)
        self.year = self.now_time.strftime('%Y')
        self.month = self.now_time.strftime('%m')
        self.day = self.now_time.strftime('%d')
        self.hour = self.now_time.strftime('%H')
        if self.now_time.minute % 5 != 0 and self.now_time.minute % 10 != 0 and self.now_time.minute % 10 < 5:
            self.minute = str(math.floor(self.now_time.minute / 10) * 10).zfill(2)
        elif self.now_time.minute % 5 != 0 and self.now_time.minute % 10 != 0 and self.now_time.minute % 10 > 5:
            self.minute = str(math.ceil(self.now_time.minute / 10) * 10).zfill(2)
        else:
            self.minute = self.now_time.strftime('%M')
        self.date = self.now_time.strftime('%Y-%m-%d')
        self.row = {
            'collected_at': self.date,
            'pc_mobile_type': "",
            'weekday': Key.WEEK_DAYS[self.now_time.weekday()],
            'date': "",
            'ad_keyword': "",
            'ad_rank': "-",
        }
        self.s3_path = ''
        self.s3_folder = 'keyword_monitoring'
        self.tmp_path = ''
        self.result_df = pd.DataFrame(columns=['collected_at', 'pc_mobile_type', 'weekday', 'ad_keyword', 'ad_rank', 'date'])

    def get_search_infos(self, media_info) -> dict:
        if media_info == '네이버SA':
            search_infos = {
                "PC": {
                    "search_url": "https://ad.search.naver.com/search.naver?where=ad&query=",
                    "selector": "#content > div > ol",
                    "ad_info_path": ("div", "url_area"),
                    "ads_in_page": 25,
                },
                "MO": {
                    "search_url": "https://m.ad.search.naver.com/search.naver?sm=&where=m_expd&query=",
                    "selector": "#contentsList",
                    "ad_info_path": ("span", "url_link"),
                    "ads_in_page": 15,
                },
            }
        else:
            search_infos = {}
        return search_infos

    def save_screenshot(self, filename):
        local_screenshot_path = self.tmp_path + filename
        s3_screenshot_path = '/'.join(self.s3_path.split('.')[0].split('/')[:-1]) + filename
        self.driver.save_screenshot(local_screenshot_path)
        screenshot_url = upload_file(local_screenshot_path, s3_screenshot_path, DEFAULT_S3_PUBLIC_BUCKET)
        os.remove(local_screenshot_path)
        return screenshot_url

    def crawling_ads(self, device: Device, keyword: str, ad_names: list, time_stamp: datetime):
        # 최대 5번 크롤링을 시도함.
        crawling_exception = Exception()
        for i in range(Key.CRAWLING_RETRY_CNT):
            try:
                url = device.search_url + keyword
                res = requests.get(url)
                if res.status_code == 200:
                    html_source = res.text
                    soup = BeautifulSoup(html_source, 'html.parser')
                else:
                    raise crawling_exception
                # selector를 사용해 상품에 해당하는 부분을 선택함.
                ads = soup.select_one(device.selector)
                if ads is None:
                    return
                url_tag, url_class = device.ad_name_path
                # 위의 상품에 해당하는 부분에서 url_tag와 url_class를 포함하는 건들(상품의 회사명에 해당하는 부분)을 모두 찾아 해당하는 광고주의 광고가 있는지 찾음.
                for index, ad_element in enumerate(ads.find_all(url_tag, url_class)[:15]):
                    # ad_element가 광고주의 광고라면 row에 등수를 기록함.
                    if ad_element and is_ad(ad_element, ad_names):
                        self.row['ad_rank'] = str(index + 1)
                        break
                return
            except Exception as e:
                self.logger.warning(f"크롤링 {i + 1}/{Key.CRAWLING_RETRY_CNT}번 시도 중 오류 발생 - {e}")
                crawling_exception = e
                time.sleep(self.searching_waiting_time)
        raise crawling_exception

    def do_work(self, info: dict, attr: dict):
        owner_id = attr.get("owner_id")
        channel = attr.get("channel")
        Key.USE_HEADLESS = info.get("use_headless")
        if info.get("s3_folder"):
            self.s3_folder = info.get("s3_folder")
        self.tmp_path = get_tmp_path() + "/" + self.s3_folder + "/" + owner_id + "/" + channel
        os.makedirs(self.tmp_path, exist_ok=True)
        self.s3_path = self.s3_folder + "/" + f"owner_id={owner_id}/channel={channel}/year={self.year}/month={self.month}" \
                                              f"/day={self.day}/hour={self.hour}/minute={self.minute}/keyword_monitoring.csv"
        spread_sheet_url = info.get("spread_sheet_url")
        keyword_sheet = info.get("keyword_sheet")
        ad_names = literal_eval(info.get("ad_names", "[]"))
        media_info = info.get("media_info", "")
        search_infos = self.get_search_infos(media_info)

        pc = Device(
            "PC",
            info.get("keyword_column", ""),
            search_infos,
        )
        mo = Device(
            "MO",
            info.get("keyword_column", ""),
            search_infos,
        )
        devices = [pc, mo]

        result_msg = [f"{media_info} 키워드 검색 순위 모니터링 결과"]
        try:
            gd = GoogleDrive()
            sheet = gd.get_work_sheet(spread_sheet_url, keyword_sheet)
            setting_df = gd.sheet_to_df(sheet)
            setting_df = setting_df.iloc[:, :11]
            keywords_df = setting_df.drop(setting_df.loc[setting_df.iloc[:, 0] == ''].index) # 빈행 제거
            if len(keywords_df) == 0:
                return {
                    "result_code": ResultCode.SUCCESS,
                    "msg": "No keywords on the list.",
                }
            column_names = setting_df.columns.values

            for device in devices:
                # 키워드 컬럼을 잡 인포로 받지 않았거나 입력 받은 값이 존재하지 않는 경우, 첫 번째 컬럼을 공통 키워드로 사용.
                if device.keyword_column not in column_names:
                    device.keyword_column = column_names[0]
                # device 별로 키워드를 입력.
                keywords = keywords_df.loc[keywords_df['디바이스'] == device.device_type, device.keyword_column].unique().tolist()
                if keywords:
                    device.keywords = keywords

            for device in devices:
                # 키워드 별로 검색 후에 순위를 확인함.
                for keyword in device.keywords:
                    # 키워드 별로 랭킹 초기화. 랭킹이 없는 경우 "-"를 반환.
                    self.row['pc_mobile_type'] = device.device_type
                    self.row['ad_rank'] = "-"
                    self.row['ad_keyword'] = keyword
                    try:
                        time_stamp = datetime.now()
                        self.crawling_ads(device, keyword, ad_names, time_stamp)
                        self.row['date'] = f"{time_stamp.strftime('%Y-%m-%d %H:%M:%S')}"
                        result_msg.append(
                            f"{device.device_type} - {keyword}: " f"{self.row['ad_rank']}"
                        )
                        # 해당 키워드에 해당하는 등수를 찾은 후에, 결과 데이터프레임에 행 추가
                        temp = pd.DataFrame([self.row])
                        self.result_df = pd.concat([self.result_df, temp], axis=0)

                    except Exception as e:
                        error_msg = f"{device.device_type} - {keyword}: 오류 발생. error_msg: {e}"
                        self.logger.warning(error_msg)
                        result_msg.append(error_msg)
                    # 키워드마다 대기 시간을 줌.
                    time.sleep(self.searching_waiting_time)
                # self.driver.quit()
                self.result_df = self.result_df.reset_index(drop=True)
                self.logger.info("크롬 브라우저 종료")
                self.logger.info(f"{device.device_type} 키워드 검색 완료.")
            self.logger.info(f"{media_info} 모니터링 완료")

        except Exception as e:
            # self.driver.quit()
            self.logger.error(e)
            raise e

        if not info.get("send_result_msg", True):
            result_msg = ['Keyword Monitoring Job complete.']

        return {
            "result_code": ResultCode.SUCCESS,
            "msg": "\n".join(result_msg),
            "result_df": self.result_df
        }
