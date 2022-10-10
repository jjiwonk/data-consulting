from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info
from setting import directory as dr
import pandas as pd
import setting.holidays as hol
import datetime
import numpy as np

raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                         from_date = info.from_date,
                         to_date = info.to_date)

raw_df_exception = prep.campaign_name_exception(raw_df)

# 영업일 구분컬럼 추가
raw_df_exception['business_day'] = raw_df_exception['event_weekday'].apply(lambda x: '주중' if x < 5 else '휴일')
holiday_list = hol.get_holidays(info.from_date, info.to_date)
raw_df_exception.loc[raw_df_exception['event_date'].isin(holiday_list), 'business_day'] = '휴일'

# 요일별 성과 볼륨
week_pivot = raw_df_exception.loc[raw_df_exception['event_name']=='Viewed LA Home'].pivot_table(index='event_weekday', values='event_name', aggfunc='count').reset_index()
week_pivot = week_pivot.rename(columns={'event_name':'counts'})
total = week_pivot.counts.sum()
week_pivot['percent'] = week_pivot['counts']/total*100

# 주중-휴일 평균 성과 비율
holiday_handicap = np.mean(week_pivot.loc[week_pivot['event_weekday'] >= 5].counts)/np.mean(week_pivot.loc[week_pivot['event_weekday'] < 5].counts)

# 시간대 성과 비율
hour_pivot = raw_df_exception.loc[raw_df_exception['event_name']=='Viewed LA Home'].pivot_table(index='event_hour', values='event_name', aggfunc='count').reset_index()
hour_pivot = hour_pivot.rename(columns={'event_name':'counts'})
total = hour_pivot.counts.sum()
hour_pivot['percent'] = hour_pivot['counts']/total*100

