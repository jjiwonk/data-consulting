from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info

import pandas as pd
import datetime
import numpy as np

def merge_on_click_date():
    raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                             from_date = info.from_date,
                             to_date = info.to_date)

    raw_df_exception = prep.campaign_name_exception(raw_df)
    raw_df_exception['campaign'] = raw_df_exception['campaign'].str.lower()

    campaign_cost_df = prep.get_campaign_cost_df(from_date = info.from_date, to_date = info.to_date)
    campaign_cost_df['campaign'] = campaign_cost_df['campaign'].str.lower()
    cost_df_pivot = campaign_cost_df.pivot_table(index = ['날짜', 'media_source', 'campaign'], values = 'cost', aggfunc = 'sum').reset_index()

    # campaign_cost - raw_data merge
    raw_pivot = raw_df_exception.pivot_table(index=['click_date', 'media_source','campaign'], columns='event_name', values='event_time', aggfunc='count').reset_index().fillna(0)
    raw_pivot = raw_pivot.rename(columns = {'click_date' : '날짜'})
    raw_pivot = raw_pivot.loc[raw_pivot['날짜']>=info.from_date]

    merged_df = pd.merge(cost_df_pivot, raw_pivot, how='outer', on=['날짜','media_source', 'campaign'])
    merged_df.to_csv(info.result_dir + '/cost_data_merge.csv', index=False, encoding = 'utf-8-sig')

    # cost_df_pivot2 = campaign_cost_df.pivot_table(index=['media_source', 'campaign'], values='cost',
    #                                              aggfunc='sum').reset_index()
    #
    # # campaign_cost - raw_data merge
    # raw_pivot2 = raw_df_exception.pivot_table(index=['media_source', 'campaign'], columns='event_name',
    #                                          values='event_time', aggfunc='count').reset_index().fillna(0)
    #
    # merged_df2 = pd.merge(cost_df_pivot2, raw_pivot2, how='outer', on=['media_source', 'campaign'])
    #
    # # campaign_cost_df - raw_df 비교
    # temp1 = merged_df2.loc[(merged_df2['cost']>0)&(pd.isnull(merged_df2['Viewed LA Home']))]
    # temp1 = temp1.sort_values('campaign')
    #
    #
    # temp2 = merged_df2.loc[(merged_df2['Viewed LA Home']>0)&(pd.isnull(merged_df2['cost']))]
    # temp2 = temp2.sort_values('loan_contract_completed', ascending=False)
    #
    # temp2 = temp2[['media_source', 'campaign', 'loan_contract_completed']]
    #
    # null_data = raw_df.loc[(raw_df['media_source']=='')&(raw_df['is_organic']==False)]
    # null_data['event_date'].value_counts()

def merge_on_click_hour():
    cost_hour_df = prep.get_campaign_cost_hour_df(from_date=info.from_date, to_date=info.to_date)
    cost_hour_df['campaign'] = cost_hour_df['campaign'].str.lower()
    cost_hour_df['campaign'] = cost_hour_df['campaign'].str.replace('(미운영)', '')
    cost_hour_df_pivot = cost_hour_df.pivot_table(index=['날짜', 'click_hour','media_source', 'campaign'], values='cost',  aggfunc='sum').reset_index()
    campaign_list = list(cost_hour_df_pivot['campaign'].unique())

    raw_df = prep.raw_data_concat(
        media_filter=['Facebook', 'Facebook Ads', 'Facebook_RE_2207', 'Facebook_MD_2206', 'Facebook_onelink'],
        from_date=info.from_date,
        to_date=info.to_date)

    raw_df_exception = prep.campaign_name_exception(raw_df)
    raw_df_exception['campaign'] = raw_df_exception['campaign'].str.lower()
    raw_df_exception = raw_df_exception.loc[raw_df_exception['is_organic']==False]
    raw_df_exception = raw_df_exception.loc[raw_df_exception['campaign'].isin(campaign_list)]

    raw_pivot = raw_df_exception.pivot_table(index=['click_date', 'click_hour', 'media_source', 'campaign'], columns='event_name',
                                             values='event_time', aggfunc='count').reset_index().fillna(0)
    raw_pivot = raw_pivot.rename(columns={'click_date': '날짜'})

    merged_df = cost_hour_df_pivot.merge(raw_pivot, on = ['날짜', 'click_hour', 'media_source', 'campaign'], how = 'outer').fillna(0)

    merged_df_copy = merged_df.copy()
    merged_df_copy = merged_df_copy.set_index(['날짜', 'click_hour', 'media_source', 'campaign'])
    merged_df_copy = merged_df_copy.stack().reset_index()
    merged_df_copy = merged_df_copy.rename(columns= {'level_4' : 'Metric', 0 : 'Value'})
    merged_df_copy.to_csv(info.result_dir + '/cost_hourly_data_merge.csv', index=False, encoding='utf-8-sig')

merge_on_click_date()
merge_on_click_hour()