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
    site_category = '올리브영'
    user_data_columns = ['user_name', 'user_rank', 'user_tag']
    review_data_columns = ['rating', 'review_date', 'is_offline', 'review_text', 'image_list', 'num_of_image', 'recommend_num']

class parse_data :
    def get_user_data(self, user_infos):
        result = []
        for info in user_infos :
            user_property = info.find_element(by=By.CLASS_NAME, value='info_user').text
            property_split = user_property.split('\n')

            user_name = property_split[0]
            if 'TOP' in user_property:
                user_rank = int(property_split[-1].split(' ')[-1])
            else :
                user_rank = 'unranked'

            user_tag = info.find_elements(by=By.TAG_NAME, value='em')
            user_tag = [tag.text for tag in user_tag]

            row = [user_name, user_rank, user_tag]
            result.append(row)

        return result

    def get_contents_data(self, review_contents):
        result = []

        for review in review_contents:
            score_area = review.find_element(by = By.CLASS_NAME, value = 'score_area')
            rating = score_area.find_element(by = By.CLASS_NAME, value = 'point').text
            rating = float(rating.split(' ')[-1].replace('점', ''))
            review_date = score_area.find_element(by = By.CLASS_NAME, value = 'date').text
            review_date = datetime.datetime.strptime(review_date, '%Y.%m.%d').strftime('%Y-%m-%d')

            try :
                score_area.find_element(by = By.CLASS_NAME, value = 'ico_offlineStore')
                is_offline = True
            except :
                is_offline = False

            txt_inner = review.find_element(by = By.CLASS_NAME, value = 'txt_inner')
            review_text = txt_inner.text

            try :
                review_image_container = review.find_element(by = By.CLASS_NAME, value = 'rw-photo-slide')
                image_list = review_image_container.find_elements(by = By.TAG_NAME, value = 'img')
                image_list = [img.get_attribute('src') for img in image_list]
                num_of_image = len(image_list)
            except :
                image_list = []
                num_of_image = 0

            recommend_btn = review.find_element(by = By.CLASS_NAME, value = 'btn_recom')
            recommend_num = int(recommend_btn.find_element(by = By.CLASS_NAME, value = 'num').text)

            row = [rating, review_date, is_offline, review_text, image_list, num_of_image, recommend_num]
            result.append(row)
        return result

    def generate_review_id(self, row):
        platform = row['platform']
        product_id = row['product_id']
        user_name = row['user_name']
        review_date = row['review_date']
        message = "{}.{}.{}".format(platform, user_name, review_date)
        hash = hmac.new(bytes(product_id, "utf-8-"), bytes(message, "utf-8-"), hashlib.sha256)
        result = hash.hexdigest()
        return result


class OliveyoungCrawling(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, spread_url, sheet_name, target_date_range):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"
        Key.spread_url = spread_url
        Key.sheet_name = sheet_name

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        yearmonth = schedule_date.strftime('%Y%m')
        time_str = schedule_date.strftime('%Y%m%d%H%M%S')

        Key.table_name = f'{owner_id}_Oliveyoung_Review_Data.csv'
        Key.file_name = f'{owner_id}_Oliveyoung_Review_Data_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.table_name

        Key.s3_path = f'review_data/owner_id={owner_id}/category_id={Key.site_category}/{Key.table_name}'

        Key.screenshot_file_name = f'Error Screenshot_{owner_id}_{product_id}_{time_str}.png'
        Key.page_source_file_name = f'Error PageSource_{owner_id}_{product_id}_{time_str}.txt'

        if target_date_range > 0 :
            Key.target_date = (datetime.date.today() - datetime.timedelta(target_date_range)).strftime('%Y-%m-%d')
            Key.target_date_trigger = True

    def get_download_sheet(self, site_category):
        sheet = GoogleDrive().get_work_sheet(url= Key.spread_url, sheet_name= Key.sheet_name)
        data = GoogleDrive().sheet_to_df(sheet)
        data = data.loc[(data['사이트 구분']==site_category) & (data['수집 구분']=='TRUE')]
        data = data.loc[data['제품 URL'].str.len()>0]
        return data

    def generate_review_df(self, driver, url, num):
        review_list_wrap = wait_for_element(driver=driver, by=By.CLASS_NAME, value='review_list_wrap')
        inner_list = review_list_wrap.find_element(by=By.CLASS_NAME, value='inner_list')

        user_infos = inner_list.find_elements(by=By.CLASS_NAME, value='info')
        user_data = parse_data().get_user_data(user_infos)
        user_data_df = pd.DataFrame(user_data, columns=Key.user_data_columns)

        review_contents = inner_list.find_elements(by=By.CLASS_NAME, value='review_cont')
        review_data = parse_data().get_contents_data(review_contents)
        review_data_df = pd.DataFrame(review_data, columns=Key.review_data_columns)

        df_concat = pd.concat([user_data_df, review_data_df], axis=1)
        df_concat['제품 URL'] = url
        df_concat['product_id'] = parse.parse_qs(parse.urlparse(url).query)['goodsNo'][0]
        df_concat['page_num'] = num
        df_concat['platform'] = Key.site_category
        df_concat['review_id'] = df_concat.apply(parse_data().generate_review_id, axis=1)
        return df_concat

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

        download_sheet = self.get_download_sheet(Key.site_category)
        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=Key.tmp_path)

        try :
            # 시트에서 URL 하나씩 가져오기
            for url in download_sheet['제품 URL']:
                driver.get(url)

                review_tab_btn = wait_for_element(driver = driver, by=By.CSS_SELECTOR, value='#reviewInfo > a')
                final_page_num = math.ceil(int(''.join(re.compile('\d').findall(review_tab_btn.text)))/10)
                if final_page_num > 100 :
                    final_page_num = 100

                review_tab_btn.click()

                recent_sort_btn = wait_for_element(driver = driver, by = By.CSS_SELECTOR, value = '#gdasSort > li:nth-child(3) > a')
                recent_sort_btn.click()

                # 리뷰 버튼 클릭 후 딜레이 넣어야 첫번째 페이지 정상적으로 불러옴
                time.sleep(5)

                # page_list = wait_for_element(driver=driver, by = By.CLASS_NAME, value = 'pageing')
                # page_btn_list = list(page_list.find_elements(by = By.TAG_NAME, value = 'a'))

                num = 1
                page_cursor = 1

                while num <= final_page_num :
                    df_concat = self.generate_review_df(driver, url, num)

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

                    if num != final_page_num :
                        next_page_btn = wait_for_element(driver=driver, by = By.CSS_SELECTOR,
                                                         value = f'#gdasContentsArea > div > div.pageing > a:nth-child({page_cursor+1})',max_retry_cnt=5)
                        # 다음 페이지 버튼 클릭이 잘 안되어서 3번까지 클릭 시도
                        n=0
                        while n < 3:
                            try :
                                next_page_btn.click()
                            except :
                                n+=1
                                continue

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
            final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
            final_df_merge = download_sheet.merge(final_df, on='제품 URL')

            final_df_merge_concat = pd.concat([mother_table, final_df_merge], sort=False, ignore_index=True)
            final_df_merge_concat = final_df_merge_concat.loc[final_df_merge_concat['제품 URL'].str.len() > 0]
            final_df_merge_concat = final_df_merge_concat.drop_duplicates('review_id')
            final_df_merge_concat.to_csv(download_dir + '/temp_df.csv', index=False, encoding='utf-8-sig')
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