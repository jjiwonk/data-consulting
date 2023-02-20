import re

import numpy as np

from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils.selenium_util import get_chromedriver, wait_for_element, selenium_error_logging
from utils import dropbox_util
from utils import s3
from utils import const

from worker.abstract_worker import Worker

import datetime
import os
import pandas as pd
import time

from pykospacing import Spacing
from hanspell import spell_checker
from konlpy.tag import Okt
from collections import Counter


class Key:
    tmp_path = None
    file_name = None
    file_path = None
    upload_path = None
    result_s3_path = None
    rd_s3_path = None
    file_name_dict = {
        '올리브영' : 'Oliveyoung_Review_Data.csv',
        '네이버 쇼핑' : 'NaverShopping_Review_Data.csv',
        '언니의 파우치' : 'Unpa_Review_Data.csv'
    }
    file_upload_checker = True
    category_list = []

class word_preprocessor :
    okt = Okt()
    def remain_korean_words(self, text):
        korean_pat = re.compile('[^가-힣]')
        result = korean_pat.sub(' ', text)
        return result

    def fix_spell(self, text):
        spelled_sent = spell_checker.check(text)
        hanspell_sent = spelled_sent.checked

        return hanspell_sent

    def text_normalize(self, text):
        return self.okt.normalize(text)

    def nouns_extraction(self, text):
        return self.okt.nouns(text)


    STOPWORDS = ['후', '때', '날', '편', '다음', '사용', '정도', '일', '경우', '이상', '전', '생각', '요즘', '요새',
                 '사람', '자리', '이번', '중', '게', '구매', '마다', '동안', '제품', '구입', '감사', '상품', '주문',
                 '대비', '물건', '후기', '너무', '아주', '매우', '완전', '약간', '조금', 'ㅎㅎ', 'ㅋㅋ', 'ㅎㅎㅎ', 'ㅋㅋㅋ', '배송', '일단',
                 '그리고', '정말', '근데', '이거', '엄청', '많이', '그냥', '느낌', '계속', '현재', '아무래도', '일반', '마침',
                 '얘기', '진짜']
    WORD_LIST = [" "]
    REPLACE_WORDS = {"조아": "좋아"}
    pos_tags = ["NNG", "VV", "VA", "VCN", "VCP", "XR", "IC"]

class prep :
    def naver_shopping(self, data):
        target_columns = ['사이트 구분', '제조사 구분', '제조사', '제품명', 'rating', 'user_name', 'review_date', 'review_text', 'review_id']
        data = data[target_columns]
        return data

    def oliveyoung(self, data):
        target_columns = ['사이트 구분', '제조사 구분', '제조사', '제품명', 'rating', 'user_name', 'review_date', 'review_text', 'review_id']
        data = data[target_columns]
        return data

    def unpa(self, data):
        data['사이트 구분'] = '언니의 파우치'

        target_columns = ['사이트 구분', '제조사 구분', '제조사', '제품명', 'rating', 'user_name', 'review_date', 'review_text', 'review_id']
        data = data[target_columns]
        return data


class ReviewDataReport(Worker):
    def Key_initiallize(self, owner_id, product_id, schedule_time, category_list):
        Key.tmp_path = get_tmp_path() + "/" + owner_id + "/" + product_id + "/"

        schedule_date = datetime.datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        yearmonth = schedule_date.strftime('%Y%m')

        Key.file_name = f'{owner_id}_Review_Data_Report_{yearmonth}.csv'
        Key.file_path = Key.tmp_path + Key.file_name

        Key.category_list = category_list
        Key.result_s3_path = f'review_data_report/owner_id={owner_id}/{Key.file_name}'
        Key.rd_s3_path = f'review_data/owner_id={owner_id}/category_id=/{owner_id}_'

    def data_concat(self):
        self.logger.info('리뷰 리포트 생성을 시작합니다.')

        os.makedirs(Key.tmp_path, exist_ok=True)

        # 다운로드 디렉토리 생성
        if os_util.is_windows_os():
            download_dir = Key.tmp_path.replace('/', '\\')
            Key.file_upload_checker = False
        else:
            download_dir = Key.tmp_path

        df_list = []
        for category in Key.category_list:
            s3_path = Key.rd_s3_path.replace('category_id=', f'category_id={category}')
            s3_path = s3_path + Key.file_name_dict.get(category)

            s3_result_dir = s3.download_file(s3_path=s3_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET,
                                             local_path=download_dir)

            df = pd.read_csv(s3_result_dir)

            if category == '올리브영' :
                df = prep().oliveyoung(df)
            elif category == '네이버 쇼핑' :
                df = prep().naver_shopping(df)
            elif category == '언니의 파우치' :
                df = prep().unpa(df)

            df_list.append(df)
            os.remove(s3_result_dir)

        df_concat = pd.concat(df_list, sort= False, ignore_index= True)
        df_concat = df_concat.drop_duplicates('review_id')

        df_concat['sentiment'] = df_concat['rating'].apply(lambda x : '긍정' if float(x) >= 4.0 else '부정' if float(x) < 3 else '중립')

        word_prep = word_preprocessor()
        df_concat['review_text_prep'] = df_concat['review_text'].apply(lambda x : word_prep.remain_korean_words(x))
        df_concat['review_text_prep'] = df_concat['review_text_prep'].apply(lambda x : word_prep.text_normalize(x))

        df_concat['nouns_list'] = df_concat['review_text_prep'].apply(lambda x : word_prep.nouns_extraction(x))

        review_id_array = np.array(df_concat['review_id'])
        nouns_list_array = np.array(df_concat['nouns_list'])

        count_df_list = []
        for idx, nouns_list in enumerate(nouns_list_array):
            review_id = review_id_array[idx]

            count_df = pd.DataFrame(dict(Counter(nouns_list)), index=['-', 'count']).transpose()
            count_df.drop('-', axis=1, inplace=True)
            count_df.reset_index(inplace=True)
            count_df['review_id'] = review_id

            count_df_list.append(count_df)

        total_count_df = pd.concat(count_df_list)

        df_count_merge = df_concat.merge(total_count_df, on = 'review_id', how = 'left')
        df_count_merge = df_count_merge.rename(columns = {'index' : 'keyword'})
        df_count_merge['review_length'] = df_count_merge['review_text_prep'].str.len()
        df_count_merge = df_count_merge.loc[df_count_merge['keyword'].str.len()>1]
        df_count_merge.to_csv(download_dir + '/review_report_sample.csv', index=False, encoding = 'utf-8-sig')







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
        category_list = info['category_list']

        self.Key_initiallize(owner_id, product_id, schedule_time, category_list)
        self.selenium_download()
        if Key.file_upload_checker == True :
            self.file_deliver(upload_path)

        return "Unpa Review Data Crawling Success"