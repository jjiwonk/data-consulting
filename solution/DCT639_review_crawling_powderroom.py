import urllib.parse

from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils.selenium_util import get_chromedriver, wait_for_element, selenium_error_logging
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
    USE_HEADLESS = True
    tmp_path = None
    table_name = None
    file_name = None
    file_path = None
    upload_path = None
    screenshot_file_name = None
    page_source_file_name = None
    s3_path = None
    file_upload_checker = True
    schedule_time = None
    review_df_list = []
    site_category = '파우더룸'
    max_expand_num = 8
    target_keyword_list = []
    review_data_columns = ['review_url','review_id', 'user_name', 'user_tag','is_offer','rating', 'review_date', '제조사','제품명', '제조사 구분', 'review_text', 'review_image', 'recommend_num']

class parse_data :
    def extract_review_data(self, review):
        review_url = review.find_element(by =By.CLASS_NAME, value = 'relative-position.block').get_attribute('href')
        review_id = review_url.split("/")[-1]

        user_name = review.find_element(by=By.CLASS_NAME, value = 'col-auto.nickname.ellipsis').text
        user_tag = review.find_element(by=By.CLASS_NAME, value = 'col-12.tag.ellipsis').text.split('#')[1:]

        try :
            review.find_element(by=By.CLASS_NAME, value='col-auto.block.review-badge.OFFER')
            is_offer = True
        except :
            is_offer = False

        rating = int(review.find_element(by = By.CLASS_NAME, value = 'rating.col').get_attribute('score'))

        review_date = review.find_element(by=By.CLASS_NAME, value='col-auto.right.review-time').text

        if '시간' in review_date :
            review_date = Key.schedule_time - datetime.timedelta(hours=int(review_date.split('시간')[0]))
            review_date = review_date.strftime('%Y-%m-%d')
        else :
            review_date = review_date.replace('.', '-')

        brand = review.find_element(by = By.CLASS_NAME, value = 'brand-name.col-auto').text
        product_name = review.find_element(by = By.CLASS_NAME, value = 'product-name.col').text
        if brand == '메디힐' :
            brand_category = '자사'
        else :
            brand_category = '경쟁사'

        title = review.find_element(by = By.CLASS_NAME, value = 'title.ellipsis-2-lines').text
        content = review.find_element(by=By.CLASS_NAME, value='content.ellipsis-2-lines').text

        review_text = title + '\n' + content

        try :
            review_img = review.find_elements(by=By.CLASS_NAME, value = 'q-img__image.absolute-full')[2]
            review_img_url = review_img.get_attribute('style').split('url')[-1].split('"')[1]
        except :
            review_img_url = ''

        recommend = int(review.find_elements(by = By.CLASS_NAME, value = 'like-count.inline-block')[0].text)

        return [review_url, review_id, user_name, user_tag, is_offer, rating, review_date, brand, product_name, brand_category, review_text, review_img_url, recommend]

class PowderRoomCrawling(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, target_keyword_list, max_expand_num = 9):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"
        Key.target_keyword_list = target_keyword_list

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        Key.schedule_time = schedule_date
        yearmonth = schedule_date.strftime('%Y%m')
        time_str = schedule_date.strftime('%Y%m%d%H%M%S')

        Key.table_name = f'{owner_id}_PowderRoom_Review_Data.csv'
        Key.file_name = f'{owner_id}_PowderRoom_Review_Data_{yearmonth}.csv'
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



        keyword = Key.target_keyword_list[1]

        try :
            for keyword in Key.target_keyword_list:
                driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=Key.tmp_path)
                url = f"https://www.powderroom.co.kr/search?keyword={parse.quote(keyword)}&tab=motd"
                driver.get(url)

                review_tab_btn = driver.find_elements\
                    (by = By.CLASS_NAME, value = 'q-tab__content.self-stretch.flex-center.relative-position.q-anchor--skip.non-selectable.column')[2]
                review_tab_btn.click()

                time.sleep(5)

                # recent_sort_btn = driver.find_elements(by=By.TAG_NAME, value = 'option')[-1]
                # recent_sort_btn.click()

                body = driver.find_element(by=By.CSS_SELECTOR, value = 'body')

                num = 0
                while num < Key.max_expand_num :
                    body.send_keys(Keys.END)
                    num += 1
                    time.sleep(1)

                review_list = driver.find_elements(by=By.CLASS_NAME, value='col-12.board-item-box')
                len(review_list)

                data = []
                for review in review_list :
                    data.append(parse_data().extract_review_data(review))

                df_concat = pd.DataFrame(data, columns = Key.review_data_columns)
                df_concat['검색 키워드'] = keyword

                Key.review_df_list.append(df_concat)
                driver.quit()
                time.sleep(5)

            if len(Key.review_df_list) > 0 :
                final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
                final_df.to_csv(Key.file_path, index = False, encoding = 'utf-8-sig')
                s3.upload_file(Key.file_path, Key.s3_path, const.DEFAULT_S3_PRIVATE_BUCKET)
            else :
                Key.file_upload_checker = False


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

        return "PowderRoom Review Data Crawling Success"