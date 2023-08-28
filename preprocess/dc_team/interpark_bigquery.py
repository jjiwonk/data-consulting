from setting import directory as dr
from workers import read_data
import pandas as pd
import pyarrow as pa
import os
from urllib.parse import urlparse, parse_qs
import datetime

raw_dir = dr.download_dir + '/06. 공유자료'
ga_files = os.listdir(raw_dir)

dtypes = {'Date' : pa.string(),
          'Part_Main' : pa.string(),
          'Part_Sub' : pa.string(),
          'fullVisitorId': pa.string(),
          'visitId': pa.string(),
          'userId': pa.string(),
          'visitNumber': pa.string(),
          'visitStartTime': pa.string(),
          'TrafficSource_source': pa.string(),
          'TrafficSource_medium': pa.string(),
          'pagePath': pa.string(),
          'page_hostname' : pa.string(),
          'page_type' : pa.string(),
          'action_type': pa.string(),
          'transactionId': pa.string(),
          'productRevenue': pa.string(),
          'productQuantity': pa.string(),
          'eventInfo_eventCategory': pa.string(),
          'eventInfo_eventAction': pa.string(),
          'eventInfo_eventLabel': pa.string(),
          'eventInfo_eventValue': pa.string()}

data = read_data.pyarrow_csv(directory=raw_dir, dtypes=dtypes,file_list=ga_files[:1])

# pagepath에 utm_source는 facebook으로 로깅된 이력이 있는 유저들
facebook = data.loc[data['pagePath'].str.contains('utm_source=facebook')]
facebook['fbclid'] = facebook['pagePath'].apply(lambda x :
                                                parse_qs(urlparse(x).query)['fbclid'][0] if 'fbclid' in x else '') # 광고로 유입된 유저만 필터링
facebook = facebook.loc[facebook['fbclid']!='']
facebook = facebook.sort_values('visitStartTime')
facebook = facebook.drop_duplicates('fbclid', keep = 'first')
facebook['Cnt'] = 1
#facebook_pivot = facebook.pivot_table(index = ['Part_Main', 'Part_Sub', 'TrafficSource_source'], values = 'Cnt', aggfunc = 'sum').reset_index()

# 기여 소스가 페이스북으로 인정된 유입 이력이 있는 유저
facebook_user_list = facebook.loc[(facebook['TrafficSource_source']=='facebook'), 'fullVisitorId']

# 페이스북으로 인정된 유입 이력이 있는 유저의 전체 로그를 불러옴
facebook_user_log = data.merge(facebook_user_list, on = 'fullVisitorId', how = 'inner')

# 그 중에서도 페이스북이 아닌 소스로 인정된 이력이 있는 유저 리스트를 다시 탐색
facebook_user_log = facebook_user_log.loc[facebook_user_log['TrafficSource_source']!='facebook']
target_user_list = facebook_user_log['fullVisitorId']

# 페이스북으로 소스가 인정된 이력도 있고, 다른 소스로도 인정된 이력이 있는 유저들의 전체 데이터를 가져옴
target_user_journey = data.merge(target_user_list, on = ['fullVisitorId'], how = 'inner')
target_user_journey = target_user_journey.sort_values(['fullVisitorId', 'visitStartTime']) # 방문 시간 순서대로 정렬
target_user_journey['VisitTimestamp'] = target_user_journey['visitStartTime'].apply(lambda x : datetime.datetime.utcfromtimestamp(int(x)))
target_user_journey.to_csv(dr.download_dir + '/interpark_target_user_log.csv', index=False, encoding = 'utf-8-sig')

target_user_journey_dedup = target_user_journey.drop_duplicates(['TrafficSource_source', 'fullVisitorId', 'visitStartTime'], keep = 'first')
target_user_journey_dedup['time_gap'] = target_user_journey_dedup.groupby('fullVisitorId')['VisitTimestamp'].diff()
target_user_journey_dedup['time_gap'] = target_user_journey_dedup['time_gap'].apply(lambda x: x.total_seconds())
target_user_journey_dedup.to_csv(dr.download_dir + '/interpark_target_user_log_unique.csv', index=False, encoding = 'utf-8-sig')


target_user_jouney_non_facebook = target_user_journey_dedup.loc[target_user_journey['TrafficSource_source']!='facebook']
target_user_jouney_non_facebook_pivot = target_user_jouney_non_facebook.pivot_table(index = 'TrafficSource_source', values = 'time_gap', aggfunc = 'mean')
target_user_jouney_non_facebook['fullVisitorId']
target_user_jouney_non_facebook['TrafficSource_source'].value_counts()
