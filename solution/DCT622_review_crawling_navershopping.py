from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils.selenium_util import get_chromedriver, wait_for_element, selenium_error_logging
from utils.google_drive import GoogleDrive
from utils import dropbox_util
from utils import s3
from utils import const

from worker.abstract_worker import Worker

from selenium.webdriver.common.by import By
import datetime
import os
import pandas as pd
import time

from bs4 import BeautifulSoup
from urllib import parse
import hashlib
import hmac
import re
import math
import numpy as np

class Key:
    USE_HEADLESS = True
    tmp_path = None
    table_name = None
    file_name = None
    file_path = None
    upload_path = None
    screenshot_file_name = None
    page_source_file_name = None
    spread_url = None
    sheet_name = None
    is_init = False
    s3_path = None
    file_upload_checker = True
    target_date_trigger = False
    target_date = None
    review_df_list = []
    site_category = '네이버 쇼핑'
    review_data_columns = ['rating', 'buy_from', 'user_name', 'review_date', 'option', 'review_text', 'review_img']
    extra_data_columns = ['제품 URL', 'product_id', 'page_num', 'platform', 'review_id']

class parse_data :
    def generate_review_id(self, product_id, user_name, review_date):
        message = "{}.{}.{}".format(Key.site_category, user_name, review_date)
        hash = hmac.new(bytes(product_id, "utf-8-"), bytes(message, "utf-8-"), hashlib.sha256)
        result = hash.hexdigest()
        return result

    def extract_review_data(self, review, url, pid, num):
        etc_area = review.find('div', {'class' : re.compile('reviewItems_etc_area')})
        rating = int(etc_area.find('span', {'class' : re.compile('reviewItems_average')}).text.replace('평점', ''))
        etc_list = etc_area.find_all('span', {'class' : re.compile('reviewItems_etc')})

        buy_from = etc_list[0].text
        user_name = etc_list[1].text
        review_date = datetime.datetime.strptime(etc_list[2].text, '%y.%m.%d.').strftime('%Y-%m-%d')
        try :
            option = etc_list[3].text
        except :
            option = ''

        review_area = review.find('div', {'class' : re.compile('reviewItems_review')})
        review_text = review_area.find('p', {'class' : re.compile('reviewItems_text')}).text

        try :
            review_img = review_area.find('img')['src']
        except :
            review_img = ''

        review_id = parse_data().generate_review_id(pid, user_name, review_date)

        return [rating, buy_from, user_name, review_date, option, review_text, review_img] + [url, pid, num, Key.site_category, review_id]

class sheet :
    def get_download_sheet(self):
        sheet = GoogleDrive().get_work_sheet(url= Key.spread_url, sheet_name= Key.sheet_name)
        data = GoogleDrive().sheet_to_df(sheet)
        data = data.loc[(data['사이트 구분']==Key.site_category) & (data['수집 구분']=='TRUE')]
        data = data.loc[data['제품 URL'].str.len()>0]
        return data

class NaverShoppingCrawling(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, spread_url, sheet_name, target_date_range):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"
        Key.spread_url = spread_url
        Key.sheet_name = sheet_name

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        yearmonth = schedule_date.strftime('%Y%m')
        time_str = schedule_date.strftime('%Y%m%d%H%M%S')

        Key.table_name = f'{owner_id}_NaverShopping_Review_Data.csv'
        Key.file_name = f'{owner_id}_NaverShopping_Review_Data_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.table_name

        Key.s3_path = f'review_data/owner_id={owner_id}/category_id={Key.site_category}/{Key.table_name}'

        Key.screenshot_file_name = f'Error Screenshot_{owner_id}_{product_id}_{time_str}.png'
        Key.page_source_file_name = f'Error PageSource_{owner_id}_{product_id}_{time_str}.txt'

        if target_date_range > 0 :
            Key.target_date = (datetime.date.today() - datetime.timedelta(target_date_range)).strftime('%Y-%m-%d')
            Key.target_date_trigger = True



    def selenium_download(self):
        self.logger.info('셀레니움 다운로드를 시작합니다.')

        os.makedirs(Key.tmp_path, exist_ok=True)

        # 크롬 브라우저 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
            Key.file_upload_checker = False
        else:
            download_dir = Key.tmp_path

        # 최초 실행이 아닌 경우에는 s3에서 원본 데이터 불러옴
        # 각 제품별로 리뷰 데이터 탐색 시, 가져온 모든 리뷰 ID가 기존에 포함되어 있었다면 break, 그렇지 않다면 계속 진행
        if Key.is_init == False:
            table_csv = s3.download_file(s3_path = Key.s3_path, s3_bucket = const.DEFAULT_S3_PRIVATE_BUCKET, local_path= Key.tmp_path)
            mother_table = pd.read_csv(table_csv)
            mother_review_id_list = set(mother_table['review_id'])
            os.remove(table_csv)
        else :
            mother_table = pd.DataFrame()

        download_sheet = sheet().get_download_sheet()
        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=Key.tmp_path)

        # test용
        #url = list(download_sheet['제품 URL'])[1]
        try :
            # 시트에서 URL 하나씩 가져오기
            for url in download_sheet['제품 URL']:
                driver.get(url)
                pid = parse.urlparse(url).path.split('/')[-1]

                time.sleep(10)

                html = driver.page_source
                bs = BeautifulSoup(html, "html.parser")

                ul_list = bs.find('ul', {'role' : 'tablist'}).find_all('a')
                target_idx = 0
                for idx, ul in enumerate(ul_list) :
                    if '쇼핑몰리뷰' in ul.text:
                        target_idx = idx
                        break

                floating_btn = driver.find_element(by= By.CSS_SELECTOR,value = f'#snb > ul > li:nth-child({target_idx+1}) > a')
                floating_btn.click()

                final_page_num = math.ceil(int(''.join(re.compile('\d').findall(floating_btn.text)))/20)

                sort_btn_class = bs.find('a', {'role' : 'button', 'class' : re.compile('filter_sort'), 'data-nclick' : 'N=a:rev.rec'}).get('class')[0]
                recent_sort_btn = driver.find_elements(by = By.CLASS_NAME, value = sort_btn_class)[1]
                recent_sort_btn.click()

                # 리뷰 버튼 클릭 후 딜레이 넣어야 첫번째 페이지 정상적으로 불러옴
                time.sleep(5)

                # page_list = wait_for_element(driver=driver, by = By.CLASS_NAME, value = 'pageing')
                # page_btn_list = list(page_list.find_elements(by = By.TAG_NAME, value = 'a'))

                num = 1
                page_cursor = 1

                btn_class_name = bs.find('div', {'class': re.compile('pagination_pagination')}).get('class')[0]

                while num <= final_page_num :
                    html = driver.page_source
                    bs = BeautifulSoup(html, "html.parser")
                    review_list = bs.find('ul', {'class': re.compile('reviewItems_list')}).find_all('li')

                    data = []
                    for review in review_list:
                        row = parse_data().extract_review_data(review, url, pid, num)
                        data.append(row)

                    df_concat = pd.DataFrame(data, columns = Key.review_data_columns + Key.extra_data_columns)

                    if Key.is_init == False :
                        id_list = set(df_concat['review_id'])
                        compare_id = id_list - mother_review_id_list
                        if len(compare_id) == 0 :
                            self.logger.info(f'{url}에서 더 이상 새로운 리뷰를 탐색하지 못하였습니다.')
                            print(f'{url}에서 더 이상 새로운 리뷰를 탐색하지 못하였습니다.')
                            break

                    if Key.target_date_trigger == True :
                        min_date = np.min(df_concat['review_date'])
                        if min_date < Key.target_date :
                            self.logger.info(f'{url}에서 목표 기간의 데이터까지 탐색하였습니다.')
                            print(f'{url}에서 목표 기간의 데이터까지 탐색하였습니다.')
                            break

                    #Key.review_df_list = []
                    Key.review_df_list.append(df_concat)

                    # 페이지 넘기는 부분부터 다시 시작
                    if num != final_page_num :
                        next_page_btn = wait_for_element(driver=driver, by = By.CSS_SELECTOR,
                                                         value = f'#section_review > div.{btn_class_name} > a:nth-child({page_cursor+1})', max_retry_cnt=5)
                        next_page_btn.click()
                        html = driver.page_source
                        bs = BeautifulSoup(html, "html.parser")

                    if num < 10  :
                        page_cursor += 1
                    else :
                        if num == 10 :
                            page_cursor = 2
                        elif page_cursor == 11 :
                            page_cursor = 2
                        else :
                            page_cursor += 1

                    num += 1

                    time.sleep(5)

            if len(Key.review_df_list) > 0 :
                final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
                final_df_merge = download_sheet.merge(final_df, on = '제품 URL')

                final_df_merge_concat = pd.concat([mother_table, final_df_merge], sort = False, ignore_index=True)
                final_df_merge_concat = final_df_merge_concat.loc[final_df_merge_concat['제품 URL'].str.len()>0]
                final_df_merge_concat = final_df_merge_concat.drop_duplicates('review_id')
                final_df_merge_concat.to_csv(Key.file_path, index = False, encoding = 'utf-8-sig')
                s3.upload_file(Key.file_path, Key.s3_path, const.DEFAULT_S3_PRIVATE_BUCKET)
            else :
                Key.file_upload_checker = False

            driver.quit()
            self.logger.info("크롤링 완료")

        except Exception as e :
            print(e)
            self.logger.info(e)
            # final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
            # final_df_merge = download_sheet.merge(final_df, on='제품 URL')
            #
            # final_df_merge_concat = pd.concat([mother_table, final_df_merge], sort=False, ignore_index=True)
            # final_df_merge_concat = final_df_merge_concat.loc[final_df_merge_concat['제품 URL'].str.len() > 0]
            # final_df_merge_concat = final_df_merge_concat.drop_duplicates('review_id')
            # final_df_merge_concat.to_csv(download_dir + '/temp_df.csv', index=False, encoding='utf-8-sig')
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
        spread_url = info['spread_url']
        sheet_name = info['sheet_name']
        upload_path = info['upload_path']
        target_date_range = info['target_date_range']

        self.Key_initiallize(owner_id, product_id, schedule_time, spread_url, sheet_name, target_date_range)
        self.selenium_download()
        if Key.file_upload_checker == True :
            self.file_deliver(upload_path)

        return "Oliveyoung Review Data Crawling Success"