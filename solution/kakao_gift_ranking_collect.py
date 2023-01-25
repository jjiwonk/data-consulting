import os

import pandas as pd
import requests
import time
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    ElementClickInterceptedException,
)
from utils.google_drive import (
    GoogleDrive,
)
from utils.selenium_util import get_chromedriver
from utils.path_util import get_tmp_path
from worker.abstract_worker import Worker
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By

from utils import s3


class Key:
    USE_HEADLESS = True
    WEB_DRIVER_NAME = "chromedriver"
    URL = "https://gift.kakao.com/ranking/review"
    brand_name = "brand_name"
    search_keywords = "search_keywords"  # 랭킹에서 찾을 단어 (comma separated)
    slack_webhook_url = "slack_webhook_url"
    slack_mention_id = "slack_mention_id"
    tmp_path = None
    spreadsheet_url = "spreadsheet_url"
    spreadsheet_credential = "spreadsheet_credential"
    sheet_name = "sheet_name"


def wait_for_element(self, driver, element, css_selector):
    retry_cnt = 0
    while True:
        if retry_cnt == 2:
            raise StaleElementReferenceException
        try:
            elements = WebDriverWait(
                element,
                5,
            ).until(expected_conditions.presence_of_all_elements_located((By.CSS_SELECTOR, css_selector)))
            break
        except StaleElementReferenceException or TimeoutException as e:
            self.logger.warning(e)
            retry_cnt += 1
            driver.refresh()

    if len(elements) == 1:
        return elements[0]
    else:
        return elements


def click_and_check_if_element_is_selected(element, wait_time=5):
    element_selected = False
    element.click()

    start_time = time.time()
    while time.time() - start_time <= wait_time:
        if element.get_attribute("class") == "on":
            element_selected = True
            break

    return element_selected


class KakaoGiftRankingCollect(Worker):
    def do_work(self, info: dict, attr: dict):
        self.logger.info(f"KakaoGiftRankingCollect job info: [{info}]")
        result_msg = ""

        owner_id, product_id = attr["owner_id"], attr["product_id"]
        schedule_date = attr["schedule_time"].split(" ")[0]
        year, month, day = schedule_date.split("-")
        brand_name = info.get(Key.brand_name, product_id).strip()
        search_keywords = [keyword.strip() for keyword in info.get(Key.search_keywords).split(",")]
        slack_webhook_url = info.get(Key.slack_webhook_url)
        slack_mention_id = info.get(Key.slack_mention_id, "")
        spreadsheet_url = info.get("spreadsheet_url")
        sheet_name = info.get("sheet_name")
        if slack_mention_id:
            slack_mention_id = f"<@{slack_mention_id}>"

        Key.tmp_path = get_tmp_path() + "/kakaogiftranking/" + owner_id + "/" + product_id + "/"
        os.makedirs(Key.tmp_path, exist_ok=True)

        msg_header = f"*{brand_name} 카카오선물하기 랭킹 모니터링*\n{schedule_date}\n\n"
        msg_body = ""
        try:
            driver = get_chromedriver(headless=Key.USE_HEADLESS)

            driver.get(Key.URL)
            driver.implicitly_wait(5)
            self.logger.info("카카오선물하기 웹사이트 접속 완료")

            dic = {"날짜": [], "카테고리": [], "금액대": [], "상품": [], "순위": []}
            product_ranked_occasion_list = []
            # 상황별 랭킹
            occasion_tag_cnt = len(wait_for_element(self, driver, driver, ".area_tag .tag_findkeyword > a"))
            for i in range(occasion_tag_cnt):
                occasion_tag, price_tag, product_name, rank, review = None, None, None, None, None
                tag = wait_for_element(self, driver, driver, ".area_tag .tag_findkeyword > a")[i]
                occasion_tag = tag.text.strip()
                retry_cnt = 0
                while True:
                    if retry_cnt == 2:
                        raise Exception(f"{occasion_tag} tab not clickable at this point")
                    try:
                        tag.click()
                        break
                    except ElementClickInterceptedException as e:
                        retry_cnt += 1
                        time.sleep(2)

                self.logger.info(f"[{occasion_tag}] 탭 클릭")
                time.sleep(1)

                # 가격대
                price_range_cnt = len(wait_for_element(self, driver, driver, ".list_price > li"))
                for j in range(price_range_cnt):
                    retry_cnt = 0
                    while True:
                        if retry_cnt == 2:
                            raise Exception
                        try:
                            price_range = wait_for_element(self, driver, driver, ".list_price > li")[j]
                            price_tag = price_range.text.strip()
                            clicked = click_and_check_if_element_is_selected(price_range)
                            break
                        except Exception as e:
                            retry_cnt += 1
                            driver.refresh()

                    if clicked:
                        self.logger.info(f"- <{price_tag}> 가격대 조회")
                        time.sleep(1.5)
                    else:
                        raise Exception(f"- <{price_tag}> 가격대 클릭 실패")

                    product_element_cnt = len(wait_for_element(self, driver, driver, ".wrap_rankitem .area_rank"))
                    for k in range(product_element_cnt):
                        product_element = None
                        retry_cnt = 0
                        while True:
                            if retry_cnt == 2:
                                self.logger.warning(f"{k}번째 상품 이름 접근 실패")
                                break
                            try:
                                product_element = wait_for_element(self, driver, driver, ".wrap_rankitem .area_rank")[k]
                                product_name = wait_for_element(self, driver, product_element, ".txt_prdname").text
                                break
                            except IndexError:
                                break
                            except StaleElementReferenceException or TimeoutException as e:
                                retry_cnt += 1

                        # 상품명에 입력한 키워드가 포함되어 있을 경우 자사 브랜드 제품으로 간주
                        if all(keyword in product_name for keyword in search_keywords):
                            if occasion_tag not in product_ranked_occasion_list:
                                product_ranked_occasion_list.append(occasion_tag)
                                msg_body += f"*[{occasion_tag}]*\n"

                            msg_body += f"- *{price_tag}*\n"
                            rank = wait_for_element(self, driver, product_element, ".num_rank").text.strip()
                            review = wait_for_element(self, driver, product_element, ".desc_prd").text.strip()
                            self.logger.info(f"{rank}위 - [{product_name}]")
                            msg_body += f"{rank}위 - [{product_name}]\n리뷰: {review}\n\n"

                            dic["날짜"].append(schedule_date)
                            dic["카테고리"].append(occasion_tag)
                            dic["금액대"].append(price_tag)
                            dic["상품"].append(product_name)
                            dic["순위"].append(rank)

                        else:
                            continue

            if len(product_ranked_occasion_list) > 0:
                msg_header += f"*{', '.join(product_ranked_occasion_list)}* 탭에 {brand_name} 상품이 랭킹되었습니다.\n\n"
                df = pd.DataFrame(dic)
                df.to_csv(os.path.join(Key.tmp_path, "data.csv"), index=False)
                # df = pd.read_csv("C:/Github/data-consulting/tmp/kakaogiftranking/bb/1950/data.csv")

                # s3 업로드
                s3_path = "bb_job/"\
                           + f"owner_id={owner_id}/product_id={product_id}/channel=kakaogiftranking" \
                           + f"/year={year}/month={month}/day={day}/data.csv"
                s3.upload_file(os.path.join(Key.tmp_path, "data.csv"), s3_path)
                self.logger.info("s3 업로드 완료")

                # 구글 스프레드시트 입력
                gss_client = GoogleDrive()
                sheet = gss_client.get_work_sheet(spreadsheet_url, sheet_name)
                sheet_data = gss_client.get_all_rows(sheet, list(df.columns))

                total_rowcnt = len(sheet_data)
                if total_rowcnt:
                    for i in df.index:
                        row_values = list(df.loc[i])
                        row_values = [str(v) for v in row_values]
                        row_values[-1] = int(row_values[-1])
                        sheet.insert_row(
                            values=row_values,
                            index=total_rowcnt + (i + 2),
                        )
                        gss_client.update_cell(sheet, cell=sheet.cell(total_rowcnt + (i + 2), 1),
                                               value=f"=DATE({year}, {month}, {day})",
                                               value_input_option="USER_ENTERED",)

                    self.logger.info("스프레드시트 입력 완료")

            else:
                msg_header += f"{brand_name} 상품이 랭킹되지 않았습니다.\n\n"

            if slack_mention_id:
                msg_body = msg_header + slack_mention_id + "\n" + msg_body
            else:
                msg_body = msg_header + msg_body
            response = requests.post(slack_webhook_url, json={"text": msg_body})

            if response.status_code != 200:
                result_msg += "카카오선물하기 랭킹 현황 슬랙 전송에 실패하였습니다."

            return result_msg

        except Exception as e:
            self.logger.exception(e)
            result_msg += str(e)
            raise e

        finally:
            driver.quit()

