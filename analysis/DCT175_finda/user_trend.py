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
raw_df_exception['campaign'] = raw_df_exception['campaign'].str.lower()

campaign_cost_df = prep.get_campaign_cost_df(info.raw_dir, from_date = info.from_date, to_date = info.to_date)
campaign_cost_df['campaign'] = campaign_cost_df['campaign'].str.lower()
cost_df_pivot = campaign_cost_df.pivot_table(index = ['media_source', 'campaign'], values = 'cost', aggfunc = 'sum').reset_index()

# campaign_cost - raw_data merge
raw_pivot = raw_df_exception.pivot_table(index=['media_source','campaign'], columns='event_name', values='event_time', aggfunc='count').reset_index().fillna(0)
merged_df = pd.merge(cost_df_pivot, raw_pivot, how='outer', on=['media_source', 'campaign'])

# 대출 실행 유저 기준 중복 이벤트 제거
loan_user_events = raw_df_exception.loc[raw_df['appsflyer_id'].isin(raw_df.loc[raw_df['event_name']=='loan_contract_completed'].appsflyer_id.unique())].sort_values(by=['appsflyer_id','event_time']).drop_duplicates(['appsflyer_id','event_name'], keep='first')
loan_user_events.to_csv(dr.download_dir + '/loan_user_events.csv', encoding='utf-8', index=False)
