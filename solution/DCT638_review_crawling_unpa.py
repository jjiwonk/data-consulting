import urllib.parse

from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils.selenium_util import get_chromedriver, wait_for_element, selenium_error_logging
from utils.google_drive import GoogleDrive
from utils import dropbox_util
from utils import s3
from utils import const

from worker.abstract_worker import Worker

from selenium.webdriver.common.by import By
from selenium.webdriver import Keys

import datetime
import os
import pandas as pd
import time

from bs4 import BeautifulSoup
from urllib import parse

class Key:
    USE_HEADLESS = False
    tmp_path = None
    table_name = None
    file_name = None
    file_path = None
    upload_path = None
    screenshot_file_name = None
    page_source_file_name = None
    s3_path = None
    file_upload_checker = True
    review_df_list = []
    site_category = '언니의 파우치'
    target_keyword_list = []
    max_expand_num = None
    review_data_columns = ['review_url','review_id','제조사','제품명','rating', 'image_list', 'num_of_image',  'review_text', 'recommend_num', 'review_date','user_name']

class parse_data :
    def extract_review_data(self, driver):
        review_url = driver.current_url
        review_id = review_url.split("/")[-1]

        brand = driver.find_element(by=By.CLASS_NAME, value='brand-name').text
        product = driver.find_element(by=By.CLASS_NAME, value='product-name').text
        rating = float(driver.find_element(by=By.CLASS_NAME, value='rating-value').text)

        content = driver.find_element(by=By.CLASS_NAME, value='content')

        image_url_list = []
        try:
            image_element = content.find_elements(by=By.TAG_NAME, value='img')

            for elem in image_element:
                image_url = elem.get_attribute('src')
                image_url_list.append(image_url)
        except:
            pass
        num_of_image = len(image_url_list)

        try :
            review_text_desc_list = content.find_elements(by = By.CLASS_NAME, value = 'description')
            review_text_desc = '\n'.join([desc.text for desc in review_text_desc_list])
        except :
            review_text_desc = ''
        try :
            review_text_main = content.find_element(by=By.CLASS_NAME, value='text').text
        except :
            review_text_main = ''
        review_text = review_text_desc + '\n' + review_text_main

        recommend = int(driver.find_element(by=By.CLASS_NAME, value='like-button').text)
        review_date = driver.find_element(by=By.CLASS_NAME, value='time').text.replace(".", "-")
        try :
            user_name = driver.find_element(by=By.CLASS_NAME, value='unpa-orange.unpa-font-weight-medium').text
        except :
            user_name = '-'
        return [review_url, review_id, brand, product, rating, image_url_list, num_of_image, review_text, recommend, review_date, user_name]

class UnpaCrawling(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, target_keyword_list, max_expand_num):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"
        Key.target_keyword_list = target_keyword_list

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        yearmonth = schedule_date.strftime('%Y%m')
        time_str = schedule_date.strftime('%Y%m%d%H%M%S')

        Key.table_name = f'{owner_id}_Unpa_Review_Data.csv'
        Key.file_name = f'{owner_id}_Unpa_Review_Data_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.table_name

        Key.max_expand_num = max_expand_num

        Key.s3_path = f'review_data/owner_id={owner_id}/category_id={Key.site_category}/{Key.table_name}'

        Key.screenshot_file_name = f'Error Screenshot_{owner_id}_{product_id}_{time_str}.png'
        Key.page_source_file_name = f'Error PageSource_{owner_id}_{product_id}_{time_str}.txt'





    def selenium_download(self):
        self.logger.info('셀레니움 다운로드를 시작합니다.')

        os.makedirs(Key.tmp_path, exist_ok=True)

        # 크롬 브라우저 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
            Key.file_upload_checker = False
        else:
            download_dir = Key.tmp_path


        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=Key.tmp_path)

        try :
            for keyword in Key.target_keyword_list:
                url = f"https://www.unpa.me/search?verb=review&q={parse.quote(keyword)}"
                driver.get(url)

                dropdown_btn = wait_for_element(driver=driver, by=By.CSS_SELECTOR, value='#dLabel')
                dropdown_btn.click()

                recent_sort_btn = wait_for_element(driver=driver, by=By.PARTIAL_LINK_TEXT, value='최신순')
                recent_sort_btn.click()

                # 리뷰 버튼 클릭 후 딜레이 넣어야 첫번째 페이지 정상적으로 불러옴
                time.sleep(5)

                num = 0
                while num < Key.max_expand_num :
                    try :
                        next_page_btn = driver.find_element(by=By.CSS_SELECTOR,
                                                            value='#unpa-body > div.container.none > div > button')
                        next_page_btn.click()
                    except :
                        break
                    num += 1
                    time.sleep(1)

                review_list = driver.find_elements(by=By.CLASS_NAME, value='unpa-card.unpa-card-box-shadow.unpa-review')
                review_url_list = [card.get_attribute('data-unpa-url') for card in review_list]

                data = []
                for review_url in review_url_list :
                    driver.get('https://unpa.me' + review_url)

                    # 요청 많이 보내면 500 error 발생하여 회피 코드 추가
                    n = 0
                    while n < 3 :
                        try:
                            data.append(parse_data().extract_review_data(driver))
                        except:
                            driver.refresh()
                        n += 1

                    time.sleep(3)

                df_concat = pd.DataFrame(data, columns = Key.review_data_columns)
                df_concat['검색 키워드'] = keyword
                df_concat['제조사 구분'] = df_concat['제조사'].apply(lambda x : '자사' if x == '메디힐' else '경쟁사')

                Key.review_df_list.append(df_concat)
                time.sleep(5)

            if len(Key.review_df_list) > 0 :
                final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
                final_df.to_csv(Key.file_path, index = False, encoding = 'utf-8-sig')
                s3.upload_file(Key.file_path, Key.s3_path, const.DEFAULT_S3_PRIVATE_BUCKET)
            else :
                Key.file_upload_checker = False

            driver.quit()
            self.logger.info("크롤링 완료")

        except Exception as e :
            print(e)
            self.logger.info(e)
            final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
            final_df.to_csv(download_dir + '/temp_df.csv', index=False, encoding='utf-8-sig')
            selenium_error_logging(driver, download_dir, Key.screenshot_file_name, Key.page_source_file_name)
            raise e

    def file_deliver(self, upload_path):
        dropbox_path = upload_path + '/' + Key.file_name

        # 드롭박스 업로드로 대체
        dropbox_util.upload_v2(file_path=Key.file_path, dropbox_path=dropbox_path)

        msg = '드롭박스 업로드 완료'
        self.logger.info(msg)

        os.remove(Key.file_path)

    def do_work(self, info:dict, attr:dict):
        owner_id = attr['owner_id']
        product_id = attr['product_id']
        schedule_time = attr['schedule_time']
        upload_path = info['upload_path']
        target_keyword_list = info['target_keyword_list']

        self.Key_initiallize(owner_id, product_id, schedule_time, target_keyword_list, 10)
        self.selenium_download()
        if Key.file_upload_checker == True :
            self.file_deliver(upload_path)

        return "Unpa Review Data Crawling Success"