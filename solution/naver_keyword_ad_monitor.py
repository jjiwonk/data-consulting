import csv
import datetime
import os
import shutil
from functools import wraps
import time
import requests

from utils import s3
from utils.google_drive import (
    GoogleDrive,
)
from worker.abstract_worker import Worker
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from utils.selenium_util import get_chromedriver
from utils.path_util import get_tmp_path

BASE_URL = {
    "PC": "https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query=",
    "MO": "https://m.search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query=",
}
AD_TYPE = {"powerlink": "파워링크", "shopping": "쇼핑검색"}
S3_FOLDER = "bb_job"


def retry_func(function):
    @wraps(function)
    def wrapper(self, driver, *args):
        retry_cnt = 0
        while True:
            if retry_cnt == 2:
                return None
            try:
                return function(self, driver, *args)
            except Exception as e:
                self.logger.warning(e)
                self.logger.info(f"retrying... {retry_cnt}")
                driver.refresh()
                time.sleep(2)
                retry_cnt += 1

    return wrapper


class Key:
    USE_HEADLESS = True
    keyword = "keyword"
    slack_webhook_url = "slack_webhook_url"
    powerlink_ad_cnt_pc = "powerlink_ad_cnt_pc"
    powerlink_ad_cnt_mo = "powerlink_ad_cnt_mo"
    spreadsheet_url = "spreadsheet_url"
    spreadsheet_credential = "spreadsheet_credential"


class NaverKeywordAdMonitor(Worker):
    def do_work(self, info: dict, attr: dict):
        self.logger.info(f"NaverSmartstoreQnaMonitor job info: [{info}]")

        keyword = info.get(Key.keyword).strip()
        powerlink_ad_cnt_pc = int(info.get(Key.powerlink_ad_cnt_pc, "5"))
        powerlink_ad_cnt_mo = int(info.get(Key.powerlink_ad_cnt_mo, "3"))
        ad_cnt_dic = {
            "powerlink": {"PC": powerlink_ad_cnt_pc, "MO": powerlink_ad_cnt_mo},
            "shopping": {"PC": 2, "MO": 2},
        }
        slack_webhook_url = info.get(Key.slack_webhook_url)
        spreadsheet_url = info.get("spreadsheet_url")
        schedule_date = attr["schedule_time"].split(" ")[0]
        schedule_time = attr["schedule_time"].split(" ")[1].split(".")[0]
        owner_id = attr["owner_id"]
        product_id = attr["product_id"]
        year, month, day = schedule_date.split("-")

        tmp_path = get_tmp_path() + "/" + "/".join(["naver_ad", keyword, str(datetime.datetime.now().timestamp())]) + "/"
        os.makedirs(tmp_path, exist_ok=True)

        powerlink_s3_path = S3_FOLDER + "/" + f"owner_id={owner_id}/product_id={product_id}/channel=powerlink_ad/year={year}/month={month}/day={day}/{keyword}.csv"
        shopping_s3_path = S3_FOLDER + "/" + f"owner_id={owner_id}/product_id={product_id}/channel=shopping_ad/year={year}/month={month}/day={day}/{keyword}.csv"
        s3_path_dic = {"powerlink": powerlink_s3_path, "shopping": shopping_s3_path}

        # 크롬 브라우저 생성
        driver = get_chromedriver(headless=Key.USE_HEADLESS)
        slack_msg = f"{schedule_date} {schedule_time}\n"

        try:
            for device_type in ["PC", "MO"]:
                ## 네이버 포털에 키워드 검색
                keyword_search_url = BASE_URL[device_type] + keyword
                driver.get(keyword_search_url)
                driver.implicitly_wait(3)
                self.logger.info(f"{device_type} 수집 시작")

                for ad_type in ["powerlink", "shopping"]:
                    slack_msg += "\n"

                    # s3 기존 파일 체크
                    ad_tmp_path = os.path.join(tmp_path, ad_type)
                    os.makedirs(ad_tmp_path, exist_ok=True)
                    previous_file_path = None
                    try:
                        previous_file_path = s3.download_file(s3_path=s3_path_dic[ad_type], local_path=ad_tmp_path)
                    except:
                        self.logger.info(f"no previous file on {s3_path_dic[ad_type]}")
                        pass

                    slack_msg += f"*{keyword} - {AD_TYPE[ad_type]} {device_type} 광고 현황*\n"
                    self.logger.info(f"{keyword} - {ad_type} {device_type} 광고 현황 수집 시작")

                    ad_elements = self.get_ad_status(driver, ad_type, device_type, ad_cnt_dic[ad_type][device_type])
                    if not ad_elements:
                        slack_msg += f"{keyword} {AD_TYPE[ad_type]} {device_type} 광고 현황 수집 에러 발생\n"
                    else:
                        sheet_name = f"{keyword} {AD_TYPE[ad_type]} {device_type}"
                        slack_msg = self.update_spreadsheet_and_upload_to_s3(
                            driver,
                            spreadsheet_url,
                            sheet_name,
                            previous_file_path,
                            ad_tmp_path,
                            s3_path_dic[ad_type],
                            ad_type,
                            device_type,
                            keyword,
                            ad_cnt_dic[ad_type][device_type],
                            ad_elements,
                            schedule_date,
                            schedule_time,
                            slack_msg,
                        )

        except Exception as e:
            raise e

        finally:
            shutil.rmtree(tmp_path)
            driver.quit()

        response = requests.post(slack_webhook_url, json={"text": slack_msg})

        if response.status_code == 200:
            return "NaverKeywordAdMonitor Job complete."
        else:
            return "NaverKeywordAdMonitor Job FAILED."

    def _get_text_from_element(self, element, selector: str):
        try:
            text = element.find_element(By.CSS_SELECTOR, selector).text
        except NoSuchElementException:
            text = ""

        return text

    @retry_func
    def get_ad_status(self, driver, ad_type, device_type, ad_cnt):
        # 파워링크
        if ad_type == "powerlink":
            if device_type == "PC":
                ad_elements = driver.find_elements(By.CSS_SELECTOR, "#power_link_body > ul > li")[:ad_cnt]
            else:
                ad_elements = driver.find_elements(By.CSS_SELECTOR, "#power_link_body > li")[:ad_cnt]
        # 쇼핑
        else:
            if device_type == "PC":
                ad_elements = driver.find_elements(By.CSS_SELECTOR, ".sc_new.sp_nshop .shop_list_divide .box._ad")[
                    :ad_cnt
                ]
            else:
                ad_elements = driver.find_elements(By.CSS_SELECTOR, ".shop_default_group ._ad")[:ad_cnt]

        return ad_elements

    def update_spreadsheet_and_upload_to_s3(
        self,
        driver,
        sheet_url,
        sheet_name,
        previous_file,
        ad_tmp_path,
        s3_path,
        ad_type,
        device_type,
        keyword,
        ad_cnt,
        ad_elements,
        schedule_date,
        schedule_time,
        msg,
    ):
        COLUMN_LIST = {
            "powerlink": ["date", "time", "rank", "title", "sub_title", "link", "desc"],
            "shopping": ["date", "time", "rank", "title", "price", "brand"],
        }
        gss_client = GoogleDrive()
        sheet = gss_client.get_work_sheet(sheet_url, sheet_name)
        sheet_data = gss_client.get_all_rows(sheet, COLUMN_LIST[ad_type])

        total_rowcnt = len(sheet_data)

        if previous_file:
            report_path = previous_file
            self.logger.info(f"{ad_type} - {device_type} previous file exists.")
            f = open(report_path, "a")
            csv_writer = csv.writer(f, delimiter="\t")

        else:
            report_path = ad_tmp_path + f"/{keyword}.csv"
            f = open(report_path, "a")
            csv_writer = csv.writer(f, delimiter="\t")
            csv_writer.writerow(COLUMN_LIST[ad_type] + ["device"])

        for i in range(ad_cnt):
            rank = i + 1
            ad_values = self.get_ad_element_values(driver, ad_elements[i], ad_type, device_type)
            total_values = [schedule_date, schedule_time, rank] + ad_values

            sheet.insert_row(
                values=total_values,
                index=total_rowcnt + (i + 2),
            )

            self.logger.info(f"{ad_type} {device_type} - {i + 1}번째 행 시트 입력 완료")

            # csv 파일에 입력
            csv_writer.writerow(total_values + [device_type])

            self.logger.info(f"{i + 1}번째 행 csv 파일 입력 완료")

            if ad_type == "powerlink":
                title, sub_title, _, desc = ad_values
                msg += f"{str(rank)}. {title} / {sub_title} / {desc}\n"
            else:
                title, price, brand = ad_values
                msg += f"{str(rank)}. {brand} - {title}, {price}\n"

        f.close()

        # s3 upload
        s3.upload_file(local_path=report_path, s3_path=s3_path)
        self.logger.info(f"{keyword}.csv s3 업로드 완료 - {s3_path}")
        os.remove(report_path)

        return msg

    @retry_func
    def get_ad_element_values(self, driver, ad_element, ad_type, device_type) -> list:
        if ad_type == "powerlink":
            if device_type == "PC":
                title = self._get_text_from_element(ad_element, ".lnk_tit").strip().replace("\t", " ")
                sub_title = self._get_text_from_element(ad_element, ".sub_tit").strip().replace("\t", " ")
                link = self._get_text_from_element(ad_element, ".url_area .lnk_url").strip()
                desc = self._get_text_from_element(ad_element, ".ad_dsc .ad_dsc_inner").strip().replace("\t", " ")
            else:
                title = self._get_text_from_element(ad_element, ".tit_area .tit").strip().replace("\t", " ")
                sub_title = self._get_text_from_element(ad_element, ".tit_area .tit_sub").strip().replace("\t", " ")
                link = self._get_text_from_element(ad_element, ".url_area .url").strip()
                desc = self._get_text_from_element(ad_element, ".desc").strip().replace("\t", " ")

            values = [title, sub_title, link, desc]

        else:
            if device_type == "PC":
                title = self._get_text_from_element(ad_element, ".product_info a").strip().replace("\t", " ")
                price = self._get_text_from_element(ad_element, ".product_info .price").strip().replace(",", "")
                brand = self._get_text_from_element(ad_element, ".product_info .store_area .name").strip()
            else:
                title = self._get_text_from_element(ad_element, ".product_info a").strip().replace("\t", " ")
                price = self._get_text_from_element(ad_element, ".product_info .price").strip().replace(",", "")
                brand = (
                    self._get_text_from_element(ad_element, ".product_info .store")
                    .split("\n")[-1]
                    .replace("광고", "")
                    .strip()
                )

            values = [title, price, brand]

        return values

