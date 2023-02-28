from utils.path_util import get_tmp_path
import utils.os_util as os_util
from utils import dropbox_util
from utils import s3
from utils import const

from worker.abstract_worker import Worker

import datetime
import os
import pandas as pd
import re
import numpy as np

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
        '언니의 파우치' : 'Unpa_Review_Data.csv',
        '파우더룸' : 'PowderRoom_Review_Data.csv'
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

    def stop_words(self, text):

        STOPWORDS = ['다음', '사용', '정도', '일', '경우', '이상', '생각', '요즘', '요새','사람', '자리', '이번', '구매', '마다', '동안', '제품', '구입', '감사', '상품', '주문','대비', '물건',
                     '너무', '아주', '매우', '완전', '약간', '조금','일단','그리고', '정말', '근데', '이거', '엄청', '많이','그냥', '느낌', '계속', '현재', '아무래도', '일반', '마침', '얘기',
                     '진짜', '우리', '저희', '따라', '의해', '으로', '에게', '뿐이다','기준으로', '예를 들면', '저희', '지말고', '하지마', '하지마라', '다른', '물론', '또한', '그리고', '뿐만 아니라',
                     '관계없이','그치지 않고','그러나', '그런데', '하지만','든간에', '따지지 않고', '설사', '비록', '더라도', '아니면', '불문하고', '틈타', '제외하고', '이 외에', '이 밖에', '한다면 몰라도',
                     '외에도','이곳', '여기', '부터', '따라서', '할 생각이다', '하려고하다', '하지만', '일때', '할때', '앞에서', '중에서', '보는데서', '으로써', '로써', '까지', '반드시',
                     '할수있어', '한다면', '등등','겨우', '단지', '다만', '할뿐', '대해서', '훨씬', '얼마나', '얼마만큼', '얼마큼','남짓', '얼마간', '약간', '다소', '조금', '다수', '얼마', '지만', '하물며', '또한',
                     '그러나', '그렇지만','하지만', '이외에도', '대해 말하자면', '다음에', '반대로', '만약', '그렇지않으면', '각각', '여러분', '각종', '각자', '제각기', '그러므로', '그래서', '고로','하기 때문에', '거니와',
                     '이지만', '대하여', '관하여', '관한', '과연', '아니나다를까','생각한대로', '진짜로', '거바','어째서', '무엇때문에', '무슨', '어디', '어느곳', '하물며', '어느때', '언제','그래도', '그리고', '바꾸어말하면',
                     '할지라도', '일지라도', '지든지', '거의', '하마터면', '이젠', '된바에야','된이상', '만큼', '어찌됏든', '그위에','게다가', '점에서 보아','고려하면','사면','사려','중이','역시']

        result = [x for x in text if not x in STOPWORDS]

        return result

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

    def powderroom(self, data):
        data['사이트 구분'] = '파우더룸'

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
            elif category == '파우더룸' :
                df = prep().powderroom(df)

            df_list.append(df)
            os.remove(s3_result_dir)

        df_concat = pd.concat(df_list, sort= False, ignore_index= True)
        df_concat = df_concat.drop_duplicates('review_id')
        df_concat['sentiment'] = df_concat['rating'].apply(lambda x : '긍정' if float(x) >= 4.0 else '부정' if float(x) < 3 else '중립')

        word_prep = word_preprocessor()
        df_concat['review_text_prep'] = df_concat['review_text'].apply(lambda x : word_prep.remain_korean_words(x))
        df_concat['review_text_prep'] = df_concat['review_text_prep'].apply(lambda x : word_prep.text_normalize(x))

        df_concat['nouns_list'] = df_concat['review_text_prep'].apply(lambda x : word_prep.nouns_extraction(x))
        df_concat['nouns_list'] = df_concat['nouns_list'].apply(lambda x: word_prep.stop_words(x))

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
        df_count_merge.to_csv(Key.file_path, index=False, encoding = 'utf-8-sig')
        s3.upload_file(Key.file_path, Key.result_s3_path, const.DEFAULT_S3_PRIVATE_BUCKET)
        os.remove(Key.file_path)

        return df_count_merge



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
        self.data_concat()
        if Key.file_upload_checker == True :
            self.file_deliver(upload_path)

        return "Review Data Report Success"