from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info
from setting import directory as dr
import pandas as pd
import datetime
import numpy as np

raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                         from_date = info.from_date,
                         to_date = info.to_date)

raw_df_exception = prep.campaign_name_exception(raw_df)

raw_data = raw_df_exception.copy()
raw_data['CTET'] = (raw_data['event_time'] - raw_data['attributed_touch_time']).apply(lambda x : x.total_seconds()/86400)
raw_data['CTIT'] = (raw_data['install_time'] - raw_data['attributed_touch_time']).apply(lambda x : x.total_seconds()/86400)
raw_data['ITET'] = (raw_data['event_time'] - raw_data['install_time']).apply(lambda x : x.total_seconds()/86400)
raw_data = raw_data.loc[raw_data['platform']=='android']
raw_data['Cnt'] = 1


loan_data = raw_data.loc[raw_data['event_name'] == 'loan_contract_completed']
loan_data = loan_data.loc[(loan_data['CTIT']<7)&(loan_data['ITET']<30)]
loan_data = loan_data.loc[loan_data['attributed_touch_type']!='impression']
loan_data['transaction_id'] = loan_data.index
loan_data.to_csv(dr.download_dir + '/loan_user_data.csv', index=False, encoding = 'utf-8-sig')

loan_users = list(loan_data['appsflyer_id'].unique())

cost_data = prep.get_campaign_cost_df(from_date=info.from_date, to_date=info.to_date)
cost_data = cost_data.loc[cost_data['OS']=='AOS']
cost_data['campaign'] = cost_data['campaign'].str.lower()
cost_pivot = cost_data.pivot_table(index=['날짜', 'media_source', 'campaign'], values='cost',
                                             aggfunc='sum').reset_index()

click_data = raw_data.loc[pd.notnull(raw_data['attributed_touch_time'])]
click_data = click_data.loc[click_data['attributed_touch_time'].dt.date>info.from_date]
click_data = click_data.drop_duplicates(['appsflyer_id', 'attributed_touch_time'])
click_data['is_loan'] = False
click_data.loc[click_data['appsflyer_id'].isin(loan_users), 'is_loan'] = True
click_data['campaign'] = click_data['campaign'].str.lower()
click_data['Cnt'] = 1

click_pivot = click_data.pivot_table(index = ['click_date', 'media_source', 'campaign'], values = ['Cnt', 'is_loan'], aggfunc='sum').reset_index()
click_pivot = click_pivot.rename(columns = {'click_date' : '날짜'})

cost_click_merge = cost_pivot.merge(click_pivot, on = ['날짜', 'media_source', 'campaign'], how = 'left')
cost_click_merge['CVR'] = cost_click_merge['is_loan'] / cost_click_merge['Cnt']
cost_click_merge = cost_click_merge.loc[cost_click_merge[['cost', 'Cnt', 'is_loan']].values.sum(axis=1)>0]
cost_click_merge['weekday'] = pd.to_datetime(cost_click_merge['날짜']).dt.weekday
cost_click_merge.to_csv(dr.download_dir + '/cost_click_merge.csv', index=False, encoding = 'utf-8-sig')

cost_click_merge_pivot = cost_click_merge.pivot_table(index = 'weekday', values = ['cost', 'Cnt', 'is_loan'], aggfunc='sum').reset_index()
cost_click_merge_pivot['CPE'] = cost_click_merge_pivot['cost'] / cost_click_merge_pivot['Cnt']

# # campaign_cost - raw_data merge
# raw_pivot = raw_df_exception.pivot_table(index=['click_date', 'media_source', 'campaign'], columns='event_name',
#                                          values='event_time', aggfunc='count').reset_index().fillna(0)
# raw_pivot = raw_pivot.rename(columns={'click_date': '날짜'})
# raw_pivot = raw_pivot.loc[raw_pivot['날짜'] >= info.from_date]







raw_df_loan_user_filter = raw_df_exception.loc[raw_df_exception['appsflyer_id'].isin(loan_users)]
raw_df_loan_user_filter = raw_df_loan_user_filter.sort_values('event_time')

# install data
install_data = raw_df_loan_user_filter.loc[raw_df_loan_user_filter['event_name']=='install']
install_data = install_data.drop_duplicates('appsflyer_id')
install_data['install_date'] = install_data['install_time'].dt.date
install_data['install_hour'] = install_data['install_time'].dt.hour
install_data = install_data.rename(columns = {'install_time' : 'real_install_time'})
install_data = install_data[['real_install_time','install_date', 'install_hour', 'appsflyer_id']]

# 대출 실행 유저 기준 중복 이벤트 제거
target_event = ['Viewed LA Home', 'loan_contract_completed']
target_event_data = raw_df_loan_user_filter.loc[raw_df_loan_user_filter['event_name'].isin(target_event)]
target_event_data = target_event_data.merge(install_data, on = 'appsflyer_id', how = 'left')
target_event_data = target_event_data.sort_values(['appsflyer_id', 'event_time'])

# 첫 이벤트 여부 확인 컬럼 생성
first_target_event_data = target_event_data.drop_duplicates(['event_name', 'appsflyer_id'], keep='first')
first_target_event_data = first_target_event_data.loc[first_target_event_data['event_time']>=first_target_event_data['real_install_time']]
first_target_event_data = first_target_event_data[['appsflyer_id', 'event_name', 'event_time']]
first_target_event_data = first_target_event_data.rename(columns = {'event_time' : 'first_event_time'})

target_event_data_featured = target_event_data.merge(first_target_event_data, on = ['appsflyer_id', 'event_name'], how = 'left')
target_event_data_featured.loc[(target_event_data_featured['event_time']==target_event_data_featured['first_event_time']), 'is_first_event'] = True
target_event_data_featured['is_first_event'] = target_event_data_featured['is_first_event'].fillna(False)

first_viewed_la = target_event_data_featured.loc[(target_event_data_featured['event_name']=='Viewed LA Home')&(target_event_data_featured['is_first_event']==True)]
first_viewed_la = first_viewed_la[['appsflyer_id', 'event_time']]
first_viewed_la = first_viewed_la.rename(columns = {'event_time' : 'first_viewed_la_time'})

target_event_data_featured = target_event_data_featured.merge(first_viewed_la, on = ['appsflyer_id'], how = 'left')
target_event_data_featured.to_csv(dr.download_dir + '/loan_user_events.csv', encoding='utf-8-sig', index=False)