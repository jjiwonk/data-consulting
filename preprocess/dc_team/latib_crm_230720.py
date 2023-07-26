import numpy as np
import pandas as pd
from setting import directory as dr
import datetime
from workers import func
from workers import bigquery

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/Latib'

bigquery_prep = bigquery.BigQueryPrep()

data1 = bigquery_prep.read_data(raw_dir, 'query_data.json')
data2 = bigquery_prep.read_data(raw_dir, 'query_data2.json')
data = pd.concat([data1, data2], sort=False, ignore_index=True)
del data1, data2

data = bigquery_prep.timestamp_to_kst(data)
data = data.drop_duplicates(['event_timestamp', 'event_name', 'user_pseudo_id'])
data.index = range(len(data))
data['event_id'] = data.index
data['Cnt'] = 1

data['time_gap'] = data.groupby('user_pseudo_id')['event_date_kst'].diff()
data['time_gap'] = data['time_gap'].apply(lambda x: x.total_seconds())

checkout = bigquery_prep.event_param_parser(data, 'begin_checkout')
checkout_naver = checkout.loc[checkout['transaction_id'].str.len()>0]

data.loc[data['event_id'].isin(checkout_naver['event_id']), 'event_name'] = 'begin_checkout_naver'

### 비 전환 유저
user_transaction_pivot = data.pivot_table(index = 'user_pseudo_id',columns= 'event_name', values = 'Cnt', aggfunc = 'sum').reset_index()
user_transaction_pivot = user_transaction_pivot.fillna(0)
non_conversion_user = user_transaction_pivot.loc[(user_transaction_pivot['begin_checkout']==0) &
                                                 (user_transaction_pivot['begin_checkout_naver']==0) &
                                                 (user_transaction_pivot['purchase']==0), 'user_pseudo_id']

### 인스타그램 유입 유저
insta_user_data = bigquery_prep.event_param_parser(data, 'session_start')
insta_user_data = insta_user_data.loc[insta_user_data['page_location'].str.contains('utm_source=instagram')]
insta_user_data_non_conversion = insta_user_data.loc[(insta_user_data['user_pseudo_id'].isin(non_conversion_user))]
insta_user_session = list(insta_user_data_non_conversion['ga_session_id'])

salad_url_1 = '/product/detail.html?product_no=15'
salad_url_2 = '/product/salad-juice/15'

### 샐러드 주스 페이지 뷰
salad_page_view = bigquery_prep.event_param_parser(data, 'view_item')
salad_page_view = salad_page_view.loc[(salad_page_view['page_location'].str.contains(salad_url_1, regex=False)) |
                                      (salad_page_view['page_location'].str.contains(salad_url_2))]
salad_page_view_with_insta = salad_page_view.loc[(salad_page_view['ga_session_id'].isin(insta_user_session))]

# 가드레일 지표 : 간편 결제 전환 수
checkout_naver = bigquery_prep.event_param_parser(data, 'begin_checkout_naver')
checkout_naver_with_salad = checkout_naver.loc[checkout_naver['ga_session_id'].isin(salad_page_view_with_insta['ga_session_id'])]

# 체류 시간
engagement = bigquery_prep.event_param_parser(data, 'user_engagement')
engagement = engagement.loc[engagement['ga_session_id'].isin(insta_user_session)]
engagement['engagement_time_msec'] = pd.to_numeric(engagement['engagement_time_msec'])

# 데이터 취합
basic_pivot = salad_page_view_with_insta.drop_duplicates('user_pseudo_id', keep='first')
basic_pivot = salad_page_view_with_insta.pivot_table(index = 'event_date', values = 'Cnt', aggfunc='sum')
basic_pivot.rename(columns = {'Cnt' : 'Users'}, inplace=True)

conversion_pivot = checkout_naver_with_salad.drop_duplicates('user_pseudo_id', keep = 'first')
conversion_pivot = checkout_naver_with_salad.pivot_table(index = 'event_date', values = 'Cnt', aggfunc='sum')
conversion_pivot.rename(columns = {'Cnt' : 'Conversions'}, inplace=True)

engagement_time_pivot = engagement.pivot_table(index = ['event_date', 'user_pseudo_id'], values = 'engagement_time_msec', aggfunc='sum').reset_index()
engagement_time_pivot = engagement_time_pivot.pivot_table(index = 'event_date', values = 'engagement_time_msec', aggfunc='mean')
engagement_time_pivot.rename(columns = {'engagement_time_msec' : 'Engagement Time'}, inplace=True)
engagement_time_pivot['Engagement Time'] = engagement_time_pivot['Engagement Time']/1000

result_data = pd.concat([basic_pivot, conversion_pivot, engagement_time_pivot], axis=1)
result_data['Conversions'] = result_data['Conversions'].fillna(0)
result_data.to_csv(raw_dir + '/latib_salad_insta_user.csv', encoding='utf-8-sig')