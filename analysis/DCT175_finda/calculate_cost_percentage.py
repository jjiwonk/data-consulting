from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info
from setting import directory as dr
import pandas as pd
import setting.holidays as hol
import datetime
import numpy as np

def get_cost_mix_table(raw_data, target_event):
    # 영업일 구분컬럼 추가
    # raw_df_exception['business_day'] = raw_df_exception['event_weekday'].apply(lambda x: '주중' if x < 5 else '휴일')
    # holiday_list = hol.get_holidays(info.from_date, info.to_date)
    # raw_df_exception.loc[raw_df_exception['event_date'].isin(holiday_list), 'business_day'] = '휴일'

    target_event_data = raw_data.loc[raw_data['event_name']==target_event]
    target_event_data['Cnt'] = 1

    raw_pivot = target_event_data.pivot_table(index = ['event_weekday', 'event_hour'], values = 'Cnt', aggfunc='sum').reset_index()
    cnt_before = pd.concat([raw_pivot['Cnt'][-1:], raw_pivot['Cnt'][0:-1]])
    cnt_before.index = raw_pivot.index
    raw_pivot['Cnt_Before'] = cnt_before
    raw_pivot['delta'] = (raw_pivot['Cnt'] - raw_pivot['Cnt_Before']) / raw_pivot['Cnt_Before']
    raw_pivot.loc[raw_pivot['event_hour']==0, 'delta'] = 0

    hour_array = np.array(raw_pivot['event_hour'])
    delta_array = np.array(raw_pivot['delta'])

    cost_amount_array = np.array(raw_pivot['delta'])

    for i, hour in enumerate(hour_array) :
        delta = delta_array[i]
        if hour == 0:
            cost_amount = 1
        else :
            cost_amount = float(cost_amount * (1+delta))
        cost_amount_array[i] = cost_amount

    raw_pivot['cost_amount'] = cost_amount_array

    daily_total_amount = raw_pivot.pivot_table(index = ['event_weekday'], values = 'cost_amount', aggfunc='sum').reset_index()
    daily_total_amount = daily_total_amount.rename(columns = {'cost_amount' : 'total_amount'})

    raw_pivot_merge= raw_pivot.merge(daily_total_amount, on = 'event_weekday', how = 'left')
    raw_pivot_merge['cost_rate'] = raw_pivot_merge['cost_amount'] / raw_pivot_merge['total_amount']

    return raw_pivot_merge

def get_test_table(raw_data, media_source, campaign_name, event_list):
    campaign_name = str(campaign_name).lower()

    cost_hour_df = prep.get_campaign_cost_hour_df(from_date=info.from_date, to_date=info.to_date)
    cost_hour_df['campaign'] = cost_hour_df['campaign'].str.lower()
    cost_hour_df['campaign'] = cost_hour_df['campaign'].str.replace('(미운영)', '')
    cost_hour_df = cost_hour_df.loc[(cost_hour_df['campaign']==campaign_name)&(cost_hour_df['media_source']==media_source)]

    cost_hour_df_pivot = cost_hour_df.pivot_table(index=['날짜', 'click_hour', 'media_source', 'campaign'], values='cost',
                                                  aggfunc='sum').reset_index()
    cost_hour_df_pivot = cost_hour_df_pivot.sort_values(['날짜', 'click_hour'])
    cost_hour_df_pivot = cost_hour_df_pivot.rename(columns = {'click_hour' : 'event_hour'})

    raw_data['campaign'] = raw_data['campaign'].str.lower()
    target_campaign_event_data = raw_data.loc[(raw_data['event_name'].isin(event_list))&
                                              (raw_data['media_source']==media_source)&
                                              (raw_data['campaign']==campaign_name)]
    target_campaign_event_data = target_campaign_event_data.loc[target_campaign_event_data['is_organic'] == False]

    event_pivot = target_campaign_event_data.pivot_table(index=['event_date', 'event_hour', 'media_source', 'campaign'],
                                             columns='event_name',
                                             values='event_time', aggfunc='count').reset_index().fillna(0)
    event_pivot = event_pivot.rename(columns={'event_date': '날짜'})

    merged_df = cost_hour_df_pivot.merge(event_pivot, on=['날짜', 'event_hour', 'media_source', 'campaign'],
                                         how='outer').fillna(0)
    merged_df_pivot = merged_df.pivot_table(index = '날짜', values = 'cost', aggfunc = 'sum').reset_index()
    merged_df_pivot = merged_df_pivot.rename(columns = {'cost' : 'total_cost'})

    final_data = merged_df.merge(merged_df_pivot, on = '날짜', how = 'left')
    final_data = final_data.loc[final_data['total_cost']>0]

    final_data['날짜'] = pd.to_datetime(final_data['날짜'])
    final_data['event_weekday'] = final_data['날짜'].dt.weekday
    return final_data

def get_predict_table(raw_data):
    predict_table = test_table_merge.pivot_table(index=['event_weekday', 'event_hour'], values=event_list + ['cost'],
                                                 aggfunc='sum').reset_index()
    predict_table['CVR'] = predict_table['loan_contract_completed'] / predict_table['Viewed LA Home']
    predict_table['CPA'] = predict_table['cost'] / predict_table['Viewed LA Home']

    naive_predict_table = test_table_merge.pivot_table(index=['event_hour'], values=event_list + ['cost'],
                                                       aggfunc='sum').reset_index()
    naive_predict_table['CVR_naive'] = naive_predict_table['loan_contract_completed'] / naive_predict_table[
        'Viewed LA Home']
    naive_predict_table['CPA_naive'] = naive_predict_table['cost'] / naive_predict_table['Viewed LA Home']

    predict_table = predict_table.merge(naive_predict_table[['event_hour', 'CVR_naive', 'CPA_naive']], on='event_hour',
                                        how='left')
    predict_table.loc[predict_table['CVR'] == 0, 'CVR'] = predict_table['CVR_naive']
    predict_table.loc[predict_table['Viewed LA Home'] == 0, 'CPA'] = predict_table['CPA_naive']

    predict_table_for_merge = predict_table[['event_weekday', 'event_hour', 'CVR', 'CPA']]
    return predict_table_for_merge

# 요일별 성과 볼륨

raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                         from_date = info.from_date,
                         to_date = info.to_date)

raw_df_exception = prep.campaign_name_exception(raw_df)

campaign_name= 'Madit_GG-AC_LOAN_NU_AOS_AEO-VIEWED_220805'
media_source = 'googleadwords_int'
event_list = ['Viewed LA Home', 'loan_contract_completed']
target_event = 'Viewed LA Home'

cost_mix_table = get_cost_mix_table(raw_df_exception, target_event = target_event)
cost_rate = cost_mix_table[['event_weekday', 'event_hour', 'cost_rate']]

test_table = get_test_table(raw_df_exception, media_source = media_source, campaign_name = campaign_name, event_list = event_list)

test_table_merge = test_table.merge(cost_rate, on=['event_weekday', 'event_hour'], how = 'left')

# 성과 예측용 테이블 만들기
predict_table = get_predict_table(test_table_merge)
test_table_merge = test_table_merge.merge(predict_table, on =['event_weekday', 'event_hour'], how = 'left')

test_table_merge['cost(new)'] = test_table_merge['total_cost'] * test_table_merge['cost_rate']
test_table_merge['la_home(new)'] = test_table_merge['cost(new)'] / test_table_merge['CPA']
test_table_merge['loan(new)'] = test_table_merge['la_home(new)'] * test_table_merge['CVR']

np.sum(test_table_merge['loan_contract_completed'] )
np.sum(test_table_merge['loan(new)'])

test_table_merge.to_csv(info.result_dir + '/test_table.csv', encoding = 'utf-8-sig', index=False)

