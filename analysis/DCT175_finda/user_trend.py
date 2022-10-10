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

loan_users = list(raw_df_exception.loc[raw_df_exception['event_name'] == 'loan_contract_completed', 'appsflyer_id'].unique())
raw_df_loan_user_filter = raw_df_exception.loc[raw_df_exception['appsflyer_id'].isin(loan_users)]
raw_df_loan_user_filter = raw_df_loan_user_filter.sort_values('event_time')

# install data
install_data = raw_df_loan_user_filter.loc[raw_df_loan_user_filter['event_name']=='install']
install_data = install_data.drop_duplicates('appsflyer_id')
install_data['install_date'] = install_data['install_time'].dt.date
install_data['install_hour'] = install_data['install_time'].dt.hour
install_data = install_data[['install_date', 'install_hour', 'appsflyer_id']]

# 대출 실행 유저 기준 중복 이벤트 제거
target_event = ['Viewed LA Home', 'loan_contract_completed']
target_event_data = raw_df_loan_user_filter.loc[raw_df_loan_user_filter['event_name'].isin(target_event)]
target_event_data = target_event_data.merge(install_data, on = 'appsflyer_id', how = 'left')
target_event_data = target_event_data.sort_values(['appsflyer_id', 'event_time'])

# 첫 이벤트 여부 확인 컬럼 생성
first_target_event_data = target_event_data.drop_duplicates(['event_name', 'appsflyer_id'], keep='first')
first_target_event_data = first_target_event_data[['appsflyer_id', 'event_name', 'event_time']]
first_target_event_data = first_target_event_data.rename(columns = {'event_time' : 'first_event_time'})

target_event_data_featured = target_event_data.merge(first_target_event_data, on = ['appsflyer_id', 'event_name'], how = 'left')
target_event_data_featured.loc[(target_event_data_featured['event_time']==target_event_data_featured['first_event_time']), 'is_first_event'] = True
target_event_data_featured['is_first_event'] = target_event_data_featured['is_first_event'].fillna(False)
target_event_data_featured.to_csv(dr.download_dir + '/loan_user_events.csv', encoding='utf-8', index=False)
