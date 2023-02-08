from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils.selenium_util import get_chromedriver, wait_for_element
from utils.google_drive import GoogleDrive
from utils import s3
from utils import dropbox_util
from utils import const

from worker.abstract_worker import Worker

from selenium.webdriver.common.by import By
import datetime
import os
import pandas as pd
import time
class Key:
    USE_HEADLESS = True
    tmp_path = None
    file_name = None
    file_path = None
    upload_path = None
    screenshot_file_name = None
    page_source_file_name = None
    spread_url = None
    sheet_name = None
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



class OliveyoungCrawling(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, spread_url, sheet_name):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"
        Key.spread_url = spread_url
        Key.sheet_name = sheet_name

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        yearmonth = schedule_date.strftime('%Y%m')
        time_str = schedule_date.strftime('%Y%m%d%H%M%S')

        Key.file_name = f'{owner_id}_Oliveyoung_Review_Data_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.file_name

        Key.screenshot_file_name = f'Error Screenshot_{owner_id}_{product_id}_{time_str}.png'
        Key.page_source_file_name = f'Error PageSource_{owner_id}_{product_id}_{time_str}.txt'

    def get_download_sheet(self, site_category):
        sheet = GoogleDrive().get_work_sheet(url= Key.spread_url, sheet_name= Key.sheet_name)
        data = GoogleDrive().sheet_to_df(sheet)
        data = data.loc[(data['사이트 구분']==site_category) & (data['수집 구분']=='TRUE')]
        data = data.loc[data['제품 URL'].str.len()>0]
        return data
    def selenium_download(self):
        self.logger.info('셀레니움 다운로드를 시작합니다.')

        os.makedirs(Key.tmp_path, exist_ok=True)

        # 크롬 브라우저 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
        else:
            download_dir = Key.tmp_path

        download_sheet = self.get_download_sheet(Key.site_category)
        driver = get_chromedriver(headless=Key.USE_HEADLESS, download_dir=download_dir)

        try :
            # 시트에서 URL 하나씩 가져오기
            for url in download_sheet['제품 URL']:
                driver.get(url)

                review_tab_btn = wait_for_element(driver = driver, by=By.CSS_SELECTOR, value='#reviewInfo > a')
                review_tab_btn.click()

                # 리뷰 버튼 클릭 후 딜레이 넣어야 첫번째 페이지 정상적으로 불러옴
                time.sleep(5)

                # page_list = wait_for_element(driver=driver, by = By.CLASS_NAME, value = 'pageing')
                # page_btn_list = list(page_list.find_elements(by = By.TAG_NAME, value = 'a'))

                #final_page_num = int(page_btn_list[-2].text)
                final_page_num = 10
                num = 1

                while num <= final_page_num :
                    review_list_wrap = wait_for_element(driver=driver, by = By.CLASS_NAME, value = 'review_list_wrap')
                    inner_list = review_list_wrap.find_element(by = By.CLASS_NAME, value = 'inner_list')

                    user_infos = inner_list.find_elements(by = By.CLASS_NAME, value = 'info')
                    user_data = parse_data().get_user_data(user_infos)
                    user_data_df = pd.DataFrame(user_data, columns = Key.user_data_columns)

                    review_contents = inner_list.find_elements(by = By.CLASS_NAME, value = 'review_cont')
                    review_data = parse_data().get_contents_data(review_contents)
                    review_data_df = pd.DataFrame(review_data, columns = Key.review_data_columns)

                    df_concat = pd.concat([user_data_df, review_data_df], axis = 1)
                    df_concat['제품 URL'] = url
                    df_concat['page_num'] = num
                    Key.review_df_list.append(df_concat)

                    if num != final_page_num :
                        next_page_btn = wait_for_element(driver=driver, by = By.CSS_SELECTOR, value = f'#gdasContentsArea > div > div.pageing > a:nth-child({num+1})')

                        # 다음 페이지 버튼 클릭이 잘 안되어서 3번까지 클릭 시도
                        n=0
                        while n < 3:
                            try :
                                next_page_btn.click()
                            except :
                                n+=1
                                continue
                    num+=1

                    time.sleep(5)

            final_df = pd.concat(Key.review_df_list, sort=False, ignore_index=True)
            final_df_merge = download_sheet.merge(final_df, on = '제품 URL')
            final_df_merge.to_csv(download_dir + '/' + Key.file_name, index = False, encoding = 'utf-8-sig')

            driver.quit()
            self.logger.info("크롤링 완료")

        except Exception as e :
            print(e)
            self.logger.info(e)

            local_screenshot_path = download_dir + '/' + Key.screenshot_file_name
            driver.get_screenshot_as_png()
            driver.save_screenshot(download_dir + '/' + Key.screenshot_file_name)
            s3.upload_file(local_path = local_screenshot_path,s3_path='screenshot/'+Key.screenshot_file_name, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET)
            os.remove(local_screenshot_path)

            page_source_path = download_dir + '/' + Key.page_source_file_name
            page_source = driver.page_source
            f = open(page_source_path, 'w')
            f.write(page_source)
            f.close()
            s3.upload_file(local_path = page_source_path, s3_path='page_source/' + Key.page_source_file_name, s3_bucket = const.DEFAULT_S3_PRIVATE_BUCKET)
            os.remove(page_source_path)

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

        self.Key_initiallize(owner_id, product_id, schedule_time, spread_url, sheet_name)
        self.selenium_download()
        self.file_deliver(upload_path)

        return "Oliveyoung Review Data Crawling Success"