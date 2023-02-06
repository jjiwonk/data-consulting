from selenium.webdriver.common.by import By

from utils.selenium_util import get_chromedriver, wait_for_element
from utils.path_util import get_tmp_path
from utils import dropbox_util
import utils.os_util as os_util
from worker.abstract_worker import Worker

import os
import pandas as pd
import datetime

class Key:
    LOGIN_URL = "https://center.shopping.naver.com/login"
    USE_HEADLESS = True
    tmp_path = None
    USE_LOGGING = True
    login_id = None
    login_pw = None
    id_input_value = 'normal_login_username'
    pw_input_value = 'normal_login_password'
    login_btn_value = '#root > div > div > div > div > form > div:nth-child(4) > div > div > span > button.ant-btn.btn_main_login.ant-btn-primary'
    service_item_value = '#content > div.tab2 > ul > li:nth-child(2) > a'
    total_item_value = '#content > div.prdt_status_lst > h4 > a > span'
    file_name = None
    file_path = None
    upload_path = None

class SpcDownload(Worker):
    def Key_initiallize(self, owner_id, product_id, login_id, login_pw, schedule_time):
        Key.tmp_path = get_tmp_path() + "/spc_download/" + owner_id + "/" + product_id + "/"
        Key.login_id = login_id
        Key.login_pw = login_pw

        yearmonth = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S').strftime('%Y%m')

        Key.file_name = f'{owner_id}_EP_item_list_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.file_name

    def spc_login_action(self, driver):
        id_input = driver.find_element(by=By.ID, value=Key.id_input_value)
        id_input.send_keys(Key.login_id)

        pw_input = driver.find_element(by=By.ID, value=Key.pw_input_value)
        pw_input.send_keys(Key.login_pw)

        login_click = driver.find_element(by=By.CSS_SELECTOR, value=Key.login_btn_value)
        login_click.click()

        msg = "로그인 완료"
        if Key.USE_LOGGING == True:
            self.logger.info(msg)
        else:
            print(msg)

        driver.implicitly_wait(time_to_wait=5)

    def get_download_number(self, driver):
        service_item = driver.find_element(by=By.CSS_SELECTOR, value=Key.service_item_value)
        service_item.click()

        # 상품수 계산
        total_item = driver.find_element(by=By.CSS_SELECTOR, value=Key.total_item_value)
        total_item = total_item.text.replace(',', '')
        down_num = int((int(total_item) / 1000)) + 1

        msg = f"상품 수 : {total_item} / 다운로드 횟수 : {down_num}"
        if Key.USE_LOGGING == True:
            self.logger.info(msg)
        else:
            print(msg)

        return down_num

    def selenium_download(self):
        os.makedirs(Key.tmp_path, exist_ok=True)

        # 크롬 브라우저 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
        else:
            download_dir = Key.tmp_path

        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=download_dir)
        driver.get(Key.LOGIN_URL)

        # 로그인하기
        self.spc_login_action(driver)

        # 상품 탭 이동 후 다운로드 횟수 계산
        down_num = self.get_download_number(driver)

        # 엑셀 다운받기
        excel_down_btn = driver.find_element(by=By.CSS_SELECTOR, value='#excelDown > a')
        excel_down_btn.click()

        total_excel_down = driver.find_element(by=By.CSS_SELECTOR, value='#excelDown > div > ul > li:nth-child(1) > a')
        total_excel_down.click()

        # 팝업창으로 바꿔주기

        driver.switch_to.window(driver.window_handles[-1])

        for i in range(down_num):
            download_btn = wait_for_element(driver=driver, by=By.CSS_SELECTOR,
                                            value=f'#downloadList > tr:nth-child({i + 1}) > td.last > a')
            n = 0
            max_download_try = 3
            while n < max_download_try:
                try:
                    download_btn.click()
                except:
                    alert_message = driver.find_element(by=By.CLASS_NAME, value='swal-button-container')
                    alert_message.click()
                n += 1

            progress_bar = wait_for_element(driver=driver, by=By.CSS_SELECTOR,
                                            value=f'#downloadList > tr:nth-child({i + 1}) > td:nth-child(3) > span > span')
            progress = progress_bar.get_attribute('style')

            n = 0
            max_wait_try = 3
            while n < max_wait_try:
                if progress == 'width: 100%;':
                    break
                else:
                    progress = progress_bar.get_attribute('style')
                    driver.implicitly_wait(time_to_wait=5)
                    continue
                n += 1

        driver.quit()

        msg = "다운로드 완료"
        if Key.USE_LOGGING == True:
            self.logger.info(msg)
        else:
            print(msg)

    def file_concat(self):
        files = os.listdir(Key.tmp_path)

        item_df = pd.DataFrame()

        for f in files:
            df = pd.read_excel(Key.tmp_path + '/' + f, header=1)
            item_df = pd.concat([item_df, df]).fillna('')

        item_df.to_csv(Key.file_path, index=False, encoding='utf-8-sig')

        for f in files:
            os.remove(Key.tmp_path + '/' + f)

        msg = "파일 병합 완료"
        if Key.USE_LOGGING == True:
            self.logger.info(msg)
        else:
            print(msg)

    def file_deliver(self, upload_path):
        dropbox_path = upload_path + '/' + Key.file_name

        # 드롭박스 업로드로 대체
        dropbox_util.upload_v2(file_path=Key.file_path, dropbox_path=dropbox_path)

        msg = '드롭박스 업로드 완료'
        if Key.USE_LOGGING == True:
            self.logger.info(msg)
        else:
            print(msg)

        os.remove(Key.file_path)
    def do_work(self, info:dict, attr:dict):
        owner_id = attr['owner_id']
        product_id = attr['product_id']
        schedule_time = attr['schedule_time']
        login_id = info['id']
        login_pw = info['pw']
        upload_path = info['upload_path']

        self.Key_initiallize(owner_id, product_id, login_id, login_pw, schedule_time)
        self.selenium_download()
        self.file_concat()
        self.file_deliver(upload_path)

        return "Shopping Partner Center EP Data Download Success"


