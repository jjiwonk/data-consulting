from selenium.webdriver.common.by import By

from utils.selenium_util import get_chromedriver, wait_for_element
from utils.path_util import get_tmp_path
from utils import dropbox
import utils.os_util as os_util
from worker.abstract_worker import Worker

from setting import directory as dr
import os
import pandas as pd
import datetime

class Key:
    LOGIN_URL = "https://center.shopping.naver.com/login"
    USE_HEADLESS = False
    tmp_path = None
    USE_LOGGING = False
    id_input_value = 'normal_login_username'
    pw_input_value = 'normal_login_password'
    login_btn_value = '#root > div > div > div > div > form > div:nth-child(4) > div > div > span > button.ant-btn.btn_main_login.ant-btn-primary'
    service_item_value = '#content > div.tab2 > ul > li:nth-child(2) > a'
    total_item_value = '#content > div.prdt_status_lst > h4 > a > span'

def spc_login_action(self, driver, login_id, login_pw, use_logging = Key.USE_LOGGING) :
    id_input = driver.find_element(by=By.ID, value = Key.id_input_value)
    id_input.send_keys(login_id)

    pw_input = driver.find_element(by=By.ID, value = Key.pw_input_value)
    pw_input.send_keys(login_pw)

    login_click = driver.find_element(by=By.CSS_SELECTOR,value = Key.login_btn_value)
    login_click.click()

    if use_logging == True :
        self.logger.info("로그인 완료")

    driver.implicitly_wait(time_to_wait=5)

def get_download_number(self, driver, use_logging = Key.USE_LOGGING):
    service_item = driver.find_element(by=By.CSS_SELECTOR, value=Key.service_item_value)
    service_item.click()

    # 상품수 계산
    total_item = driver.find_element(by=By.CSS_SELECTOR, value=Key.total_item_value)
    total_item = total_item.text.replace(',', '')
    down_num = int((int(total_item) / 1000)) + 1
    if use_logging == True :
        self.logger.info(f"상품 수 : {total_item} / 다운로드 횟수 : {down_num}")

    return down_num


def selenium_download(self, owner_id, product_id, login_id, login_pw):
    Key.tmp_path = get_tmp_path() + "/spc_download/" + owner_id + "/" + product_id + "/"
    os.makedirs(Key.tmp_path, exist_ok=True)

    # 크롬 브라우저 생성
    if os_util.is_windows_os():
        download_dir = Key.tmp_path.replace('/', '\\')
    else:
        download_dir = Key.tmp_path

    driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=download_dir)
    driver.get(Key.LOGIN_URL)

    # 로그인하기
    spc_login_action(self, driver, login_id, login_pw, logging = False)

    # 상품 탭 이동 후 다운로드 횟수 계산
    down_num = get_download_number()

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
        while n < max_download_try :
            try :
                download_btn.click()
            except :
                alert_message = driver.find_element(by=By.CLASS_NAME, value='swal-button-container')
                alert_message.click()
            n+=1

        progress_bar = wait_for_element(driver=driver, by=By.CSS_SELECTOR,
                                        value=f'#downloadList > tr:nth-child({i + 1}) > td:nth-child(3) > span > span')
        progress = progress_bar.get_attribute('style')

        n = 0
        max_wait_try = 3
        while n < max_wait_try:
            if progress == 'width: 100%;':
                break
            else :
                progress = progress_bar.get_attribute('style')
                driver.implicitly_wait(time_to_wait=5)
                continue
            n += 1

    driver.quit()
    self.logger.info("다운로드 완료")


def file_concat(self, schedule_time, owner_id, upload_dir):
    file_dir = Key.tmp_path
    files = os.listdir(file_dir)

    item_df = pd.DataFrame()

    for f in files:
        df = pd.read_excel(file_dir + '/' + f, header = 1)
        item_df = pd.concat([item_df,df]).fillna('')

    yearmonth = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S').strftime('%Y%m')
    file_name = f'{owner_id}_EP_item_list_{yearmonth}.csv'
    file_path = file_dir + file_name
    item_df.to_csv(file_path, index=False, encoding='utf-8-sig')

    upload_dir = '/광고사업부/데이터컨설팅/데이터 솔루션/쇼핑파트너센터 다운 자동화'
    dropbox_path = upload_dir + '/' + file_name

    # 드롭박스 업로드로 대체
    dropbox.upload_file(file_path = file_path, dropbox_path = dropbox_path, token = 'rEtlXqnPweAAAAAAAAAVtlY2vRHQ-LT6nsHXomwgDNZXNWXNDzJEb8N_C3NYb3W4')
    os.remove(file_path)

    for f in files:
        os.remove(file_dir + '/' + f)








class SpcDownload(Worker):
    def do_work(self, info:dict, attr:dict):
        owner_id = attr['owner_id']
        product_id = attr['product_id']
        schedule_time = attr['schedule_time']
        login_id = info['id']
        login_pw = info['pw']

        selenium_download(self, owner_id, product_id, login_id, login_pw)
        file_concat(self, schedule_time, owner_id)


