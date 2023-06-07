import logging
import os
import shutil
import time
from distutils.util import strtobool
from datetime import datetime
from typing import List
from zipfile import ZipFile
import pandas as pd
import requests
from utils.google_drive import (
    GoogleDrive,
    GSS_ROW,
)
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoAlertPresentException, ElementClickInterceptedException
from worker.const import ResultCode
from utils.selenium_util import get_chromedriver, click_and_find_downloaded_filename, selenium_error_logging
from utils.path_util import get_tmp_path
import utils.os_util as os_util
from worker.abstract_worker import Worker
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By


class Key:
    LOGIN_URL = "https://eclogin.cafe24.com/Shop/"
    USE_HEADLESS = True
    cafe24_id = "cafe24_id"
    cafe24_pw = "cafe24_pw"
    monitor_detail = "monitor_detail"
    slack_webhook_url = "slack_webhook_url"
    slack_mention_id = "slack_mention_id"
    store_name = "store_name"
    tmp_path = None
    spreadsheet_url = "spreadsheet_url"
    spreadsheet_credential = "spreadsheet_credential"
    raw_sheet_name = "raw_sheet_name"
    sales_sheet_name = "sales_sheet_name"


def wait_for_element(driver, css_selector, by=By.CSS_SELECTOR):
    max_retry_cnt = 5
    while max_retry_cnt >= 0:
        try:
            elements = WebDriverWait(
                driver,
                10,
            ).until(expected_conditions.presence_of_all_elements_located((by, css_selector)))
            if len(elements) == 1:
                return elements[0]
            else:
                return elements
        except StaleElementReferenceException or TimeoutException as e:
            logging.warning(e)
            if max_retry_cnt > 0:
                max_retry_cnt -= 1
                driver.refresh()
                time.sleep(1)
            else:
                raise e


class Cafe24SalesMonitor(Worker):
    def do_work(self, info: dict, attr: dict):
        self.logger.info(f"Cafe24SalesMonitor job info: [{info}]")

        schedule_date = attr["schedule_time"].split(" ")[0]
        schedule_time = attr["schedule_time"].split(" ")[1].split(".")[0]
        owner_id = attr["owner_id"]
        product_id = attr["product_id"]
        store_name = info.get(Key.store_name, product_id)
        slack_webhook_url = info.get(Key.slack_webhook_url)
        slack_mention_id = info.get(Key.slack_mention_id, "")
        if slack_mention_id:
            slack_mention_id = f"<@{slack_mention_id}>"
        cafe24_id = info.get(Key.cafe24_id)
        cafe24_pw = info.get(Key.cafe24_pw)
        monitor_detail = strtobool(info.get(Key.monitor_detail, "true"))
        spreadsheet_url = info.get("spreadsheet_url")
        raw_sheet_name = info.get("raw_sheet_name")
        sales_sheet_name = info.get("sales_sheet_name")
        screenshot_file_name = f'Error Screenshot_{owner_id}_{product_id}_{schedule_time.replace(":", "")}.png'
        page_source_file_name = f'Error PageSource_{owner_id}_{product_id}_{schedule_time.replace(":", "")}.txt'
        n_products = info.get("n_products")

        # tmp 폴더에 소스 폴더 생성 -> 리포트 임시저장
        Key.tmp_path = get_tmp_path() + "/cafe24/" + owner_id + "/" + product_id + "/" + store_name + "/"
        os.makedirs(Key.tmp_path, exist_ok=True)


        # 크롬 브라우저 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
        else:
            download_dir = Key.tmp_path
        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=download_dir)

        slack_msg = f"*{store_name} cafe24 매출 모니터링*\n{schedule_date} {schedule_time}\n\n"

        try:
            # 로그인
            driver.get(Key.LOGIN_URL)
            driver.implicitly_wait(3)
            self.logger.info(f"로그인페이지 접속: {Key.LOGIN_URL}")

            driver.find_element(By.XPATH, "//input[@placeholder='아이디를 입력해 주세요.']").send_keys(cafe24_id)
            driver.find_element(By.XPATH, "//input[@placeholder='비밀번호를 입력해 주세요.']").send_keys(cafe24_pw)
            driver.find_element(By.XPATH, "//input[@placeholder='비밀번호를 입력해 주세요.']").send_keys(Keys.ENTER)
            driver.implicitly_wait(3)
            self.logger.info("로그인 완료")

            max_retry_cnt = 5
            while max_retry_cnt >= 0:
                try:
                    sales_manage_tab = driver.find_element(By.CLASS_NAME, "link.order")
                    sales_manage_tab.click()
                    break
                except ElementClickInterceptedException:
                    driver.find_element(By.CSS_SELECTOR, ".btnClose.eClose").click()
                    sales_manage_tab = driver.find_element(By.CLASS_NAME, "link.order")
                    sales_manage_tab.click()
                    break
                except Exception as e:
                    if max_retry_cnt > 0:
                        max_retry_cnt -= 1
                        driver.refresh()
                        time.sleep(1)
                    else:
                        raise e

            side_menu_found = False
            side_menus = '-'
            retry_cnt = 0
            while type(side_menus) != list:
                if retry_cnt == 2:
                    break
                side_menus = wait_for_element(driver, ".subMenu .depthList .depth2 > li")
                retry_cnt += 1
                time.sleep(2)

            if monitor_detail:
                side_menu_item = "전체주문조회"
            else:
                side_menu_item = "주문대시보드"

            for side_menu in side_menus:
                if side_menu_item in side_menu.text.replace(" ", ""):
                    side_menu.click()
                    side_menu_found = True
                    break

            if not side_menu_found:
                self.logger.warning(f"{side_menu_item} 탭을 찾지 못했습니다.")
                raise Exception(f"{side_menu_item} 탭을 찾지 못했습니다.")
            else:
                self.logger.info(f"{side_menu_item} 탭 클릭")

            if "service-api" in driver.current_url:
                driver.back()
                driver.implicitly_wait(3)

            if not monitor_detail:
                cur_sales_amt = "-"
                retry_cnt = 0
                while cur_sales_amt == "-":
                    if retry_cnt == 2:
                        break
                    cur_sales_amt = wait_for_element(driver, "today_payed_price", By.ID).text
                    retry_cnt += 1
                    time.sleep(2)

                self.logger.info(f"실시간 매출현황: {cur_sales_amt}원")
                slack_msg += f"실시간 매출현황: *{cur_sales_amt}원*"

            else:
                # 기간 설정
                date_elements = wait_for_element(driver, ".btnDate")
                if len(date_elements) == 0:
                    self.logger.warning("기간 설정 엘리먼트를 찾지 못했습니다.")
                    raise Exception("기간 설정 엘리먼트를 찾지 못했습니다.")
                for date_element in date_elements:
                    if date_element.text.strip() == "오늘":
                        driver.execute_script("window.scrollTo(0,0)")
                        date_element.click()
                        self.logger.info("'오늘'자로 기간 설정")
                        break

                driver.find_element(By.ID, "search_button").click()
                self.logger.info("검색 버튼 클릭")
                time.sleep(2)

                driver.execute_script("window.scrollTo(1920,0)")
                driver.find_element(By.CSS_SELECTOR, ".mState .gRight #eExcelDownloadBtn").click()
                self.logger.info("엑셀 다운로드 버튼 클릭")
                driver.implicitly_wait(2)

                driver.switch_to.window(driver.window_handles[-1])
                self.logger.info("다운로드 탭으로 이동")

                driver.find_element(By.CSS_SELECTOR, "#aManagesList #리포트").click()  # 양식선택 - 리포트
                self.logger.info("리포트 양식 선택")
                driver.find_element(By.ID, "Password").send_keys(cafe24_pw)
                driver.find_element(By.ID, "PasswordConfirm").send_keys(cafe24_pw)
                driver.find_element(By.CLASS_NAME, "excelSubmit").click()
                self.logger.info("비밀번호 입력 후 조회")
                time.sleep(2)

                # 얼럿 창 제거
                try:
                    alert = driver.switch_to.alert
                    alert.dismiss()
                    self.logger.info("얼럿 창 종료")
                    time.sleep(2)
                except NoAlertPresentException:
                    time.sleep(2)
                    pass

                button = driver.find_elements(By.CSS_SELECTOR, ".center tr")[0].find_element(By.TAG_NAME, "a")
                for c in range(0, 5):
                    driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
                    time.sleep(1)
                button.click()
                driver.find_element(By.ID, "password").send_keys(cafe24_pw)
                driver.find_element(By.ID, "reason_for_download").send_keys("다운로드")
                self.logger.info("리포트 다운로드 시작...")

                report_zipfile = click_and_find_downloaded_filename(
                    clickable_btn=driver.find_element(By.ID, "excel_download"),
                    download_dir=Key.tmp_path,
                    download_file_ext="zip",
                    wait_sec=300,
                )
                self.logger.info("리포트 다운로드 완료")

                # zip file 열기
                with ZipFile(os.path.join(Key.tmp_path, report_zipfile)) as zf:
                    filename = zf.filelist[0].filename
                    zf.extractall(path=Key.tmp_path, pwd=bytes(cafe24_pw, "utf-8"))

                filepath = os.path.join(Key.tmp_path, filename)
                sales_report = pd.read_csv(filepath).reset_index(drop=True)

                # 다운로드 시 오늘자 선택이 안됐을 경우를 대비해 주문일시로 재필터링
                sales_report["주문날짜"] = pd.to_datetime(sales_report["주문일시"]).apply(lambda x: x.date())
                # fmt:off
                sales_report = sales_report[
                    sales_report["주문날짜"] == datetime.today().date()
                    ].drop("주문날짜", axis=1).reset_index(drop=True)
                # fmt:on
                num_sales = len(sales_report)
                self.logger.info(f"오늘 주문 - {str(num_sales)}건")

                # 옵션명이 비어있는 경우 상품명을 가져옴
                sales_report["상품옵션"] = sales_report["상품옵션"].fillna(sales_report["상품명(한국어 쇼핑몰)"])
                sales_report["상품옵션"] = sales_report["상품옵션"].apply(lambda x: "'" + x if x.startswith("+") else x)

                # 중복 주문건 처리
                compare_columns = sales_report.columns[:9]
                unique_orders = list(
                    sales_report[sales_report[compare_columns].duplicated()][compare_columns]
                    .to_dict(orient="index")
                    .values()
                )
                self.logger.info(f"동일한 주문자의 주문 - {len(unique_orders)}행")

                for order_num, unique_order in enumerate(unique_orders):
                    compare_data = sales_report.copy()
                    for key, val in unique_order.items():
                        compare_data = compare_data[compare_data[key] == val]
                    self.logger.info(f"{str(order_num + 1)} 주문자의 주문 건수: {str(len(compare_data))}건")
                    product_columns = ["상품명(한국어 쇼핑몰)", "상품옵션"]
                    unique_products = compare_data[product_columns].drop_duplicates()[product_columns].values
                    self.logger.info(f"{str(order_num+1)} 주문자가 주문한 상품 종류: {str(len(unique_products))}개")
                    if len(unique_products) == 1:
                        continue
                    else:
                        coupon_dc_values = compare_data["쿠폰 할인금액"].unique()
                        if len(coupon_dc_values) == len(compare_data):
                            self.logger.info("쿠폰 할인금액이 중복되지 않음")

                        elif len(coupon_dc_values) == 1:
                            if coupon_dc_values[0] == 0:
                                self.logger.info("쿠폰 할인금액이 없음")
                            else:
                                self.logger.info("쿠폰 할인금액이 중복되므로 첫행만 남기고 삭제")
                                for index in list(compare_data.index)[1:]:
                                    sales_report.loc[index, "쿠폰 할인금액"] = 0

                        mileage_values = compare_data["사용한 적립금액(최초)"].unique()
                        if len(mileage_values) == len(compare_data):
                            self.logger.info("사용한 적립금액이 중복되지 않음")
                            continue
                        elif len(mileage_values) == 1:
                            if mileage_values[0] == 0:
                                self.logger.info("사용한 적립금액이 없음")
                                continue
                            self.logger.info("사용한 적립금액이 중복되므로 첫행만 남기고 삭제")
                            for index in list(compare_data.index)[1:]:
                                sales_report.loc[index, "사용한 적립금액(최초)"] = 0

                # 구글 스프레드시트 입력
                gss_client = GoogleDrive()
                self.logger.info("구글 스프레드시트 연결")

                sheet = gss_client.get_work_sheet(spreadsheet_url, raw_sheet_name)
                sheet_data: List[GSS_ROW] = gss_client.get_all_rows(sheet)
                last_row = max(len(sheet_data), num_sales) + 1

                # raw 매출 데이터 업데이트
                update_cell_list = []
                cell_list = sheet.range(f"A{2}:W{str(last_row)}")

                for row_num in range(last_row):
                    row_cells = cell_list[row_num * 23 : row_num * 23 + 23]

                    if row_num >= num_sales:
                        for cell in row_cells:
                            cell.value = ""
                            update_cell_list.append(cell)
                        self.logger.info(f"{str(row_num + 1)}행 삭제 완료")
                    else:
                        sales_values = sales_report.loc[row_num]
                        for col_num, cell in enumerate(row_cells):
                            cell.value = str(sales_values[col_num])
                            update_cell_list.append(cell)
                        self.logger.info(f"{str(row_num + 1)}행 입력 완료")

                sheet.update_cells(update_cell_list, value_input_option="USER_ENTERED")

                # 상품별 매출 슬랙 전송
                sheet = gss_client.get_work_sheet(spreadsheet_url, sales_sheet_name)
                sheet_data: List[GSS_ROW] = gss_client.get_all_rows(sheet)

                last_row = len(sheet_data)
                sales_df = pd.DataFrame()
                row_num_list = list(range(3, last_row + 1))
                total_sales_cnt, total_sales_amt = 0, 0
                for row_num in row_num_list:
                    cells = sheet.range(f"B{str(row_num)}:D{str(row_num)}")
                    product_name, sales_cnt, sales_amt = [cell.value for cell in cells]
                    if int(sales_cnt) == 0:
                        continue

                    sales_df.loc[row_num, "상품명"] = product_name
                    sales_df.loc[row_num, "주문건수"] = int(sales_cnt)
                    sales_df.loc[row_num, "매출"] = int(sales_amt.replace(",", ""))

                    slack_msg += f"*{product_name}*\n"
                    slack_msg += f"- 주문: {sales_cnt}건\n"
                    slack_msg += f"- 매출: {sales_amt}원\n\n"
                    total_sales_cnt += int(sales_cnt)
                    total_sales_amt += int(sales_amt.replace(",", ""))

                slack_msg += "---------------------------------\n\n"
                slack_msg += f"*TOTAL 주문 {str(total_sales_cnt)}건, 매출 {'{:,}'.format(total_sales_amt)}원*"

                # 로컬 폴더 삭제
                shutil.rmtree(Key.tmp_path)
                self.logger.info("tmp 폴더 삭제")

        except Exception as e:
            self.logger.warning(str(e))
            selenium_error_logging(driver, download_dir[:-1], screenshot_file_name, page_source_file_name)
            raise e

        finally:
            driver.quit()
            self.logger.info("크롬 브라우저 종료")

        # return "Process end."
        if slack_mention_id:
            slack_msg = slack_mention_id + "\n" + slack_msg
        response = requests.post(slack_webhook_url, json={"text": slack_msg})

        if response.status_code == 200:
            return {
                "result_code": ResultCode.SUCCESS,
                "msg": "Cafe24SalesMonitor Job complete."
            }

        else:
            return {
                "result_code": ResultCode.ERROR,
                "msg": "Cafe24SalesMonitor Job FAILED."
            }
