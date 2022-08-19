import setting.directory as dr
import setting.report_date as rdate

import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime

jk_raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/ADID, IDFA'
am_raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW'
# result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/잡코리아/result'
result_dir = dr.download_dir

# 잡코리아

def jobkorea_organic_read():
    raw_dir = jk_raw_dir + f'/organic_{rdate.month_name}'
    jk_raw_files = os.listdir(raw_dir)
    organic_files = [f for f in jk_raw_files if 'prism' not in f]

    dtypes = {
        'AppsFlyer ID': pa.string(),
        'Event Time': pa.string(),
        'Media Source': pa.string(),
        'Platform': pa.string(),
        'Device Model': pa.string(),
        'Country Code': pa.string(),
        'Language': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)

    table_list = []
    for f in organic_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)

    print('오가닉 데이터 Read 완료')

    table = pa.concat_tables(table_list)
    organic_df = table.to_pandas()
    organic_df['Media Source'] = 'Organic'
    organic_df = organic_df.rename(columns={
        'AppsFlyer ID': 'appsflyer_id',
        'Event Time': 'event_time',
        'Media Source': 'media_source',
        'Platform': 'platform',
        'Device Model': 'device_model',
        'Country Code': 'country_code',
        'Language': 'language'
    })
    return organic_df

def jobkorea_paid_read(columns):
    raw_dir = jk_raw_dir + f'/organic_{rdate.month_name}'
    jk_raw_files = os.listdir(raw_dir)
    prism_files = [f for f in jk_raw_files if 'prism' in f]
    ### paid
    paid_df = pd.read_csv(raw_dir + '/' + prism_files[0], usecols=columns)
    print('Paid 데이터 Read 완료')

    return paid_df

def jobkorea_active_user(organic_df, paid_df) :
    jk_total_df = pd.concat([organic_df, paid_df], sort=False, ignore_index=True)
    jk_total_df['event_time'] = pd.to_datetime(jk_total_df['event_time'])
    jk_total_df = jk_total_df.sort_values(['appsflyer_id', 'event_time'])
    jk_total_df['Date'] = jk_total_df['event_time'].dt.date
    jk_total_df = jk_total_df.drop_duplicates(['appsflyer_id', 'Date'], keep='first')
    def device_check(df):
        if 'samsung' in df['device_model'] or 'lge' in df['device_model'] or 'lenovo' in df['device_model']:
            return 1
        else:
            return 0
    jk_total_df['device_not_fraud'] = jk_total_df.apply(device_check, axis=1).astype(bool)

    def country_check(data):
        if data == 'KR':
            return 1
        else:
            return 0
    jk_total_df['region_not_fraud'] = jk_total_df['country_code'].map(country_check).astype(bool)

    def language_check(data):
        if data == '한국어':
            return 1
        else:
            return 0
    jk_total_df['language_not_fraud'] = jk_total_df['language'].map(language_check).astype(bool)
    jk_total_df = jk_total_df.drop(['device_model', 'country_code', 'language'], axis=1)
    def fraud_check(df):
        if df['device_not_fraud'] == 1 and df['region_not_fraud'] == 1 and df['language_not_fraud'] == 1:
            return 1
        else:
            return 0
    jk_total_df['is_not_fraud'] = jk_total_df.apply(fraud_check, axis=1).astype(bool)
    jk_total_df['DAU'] = 1
    jk_total_df['MAU'] = 0

    id_arr = np.array(jk_total_df['appsflyer_id'])
    mau_arr = np.array(jk_total_df['MAU'])

    for i, id in enumerate(id_arr):
        if i == 0 :
            before_id = id
            mau = 1
        else :
            if id != before_id :
                mau = 1
            else :
                mau = 0
            before_id = id
        mau_arr[i] = mau
    jk_total_df['MAU'] = mau_arr

    jk_total_pivot = jk_total_df.pivot_table(index = ['Date', 'platform', 'media_source', 'device_not_fraud', 'region_not_fraud', 'language_not_fraud', 'is_not_fraud'],
                                             values = ['DAU','MAU'], aggfunc='sum')
    jk_total_pivot = jk_total_pivot.reset_index()
    jk_total_pivot['Business'] = '잡코리아'
    jk_total_pivot.to_csv(result_dir + f'/jk_mau_data_fraud_check_{rdate.yearmonth}.csv', index=False, encoding = 'utf-8-sig')

    return jk_total_pivot

def albamon_adjust_read():
    ### 알바몬
    am_raw_files = os.listdir(am_raw_dir + f'/{rdate.month_name}')
    folders = [f for f in am_raw_files if len(f) == 9]

    dtypes = {
        '{adid}': pa.string(),
        '{created_at}': pa.string(),
        '{network_name}': pa.string(),
        '{app_name}': pa.string(),
        '{device_name}': pa.string(),
        '{device_manufacturer}':  pa.string(),
        '{region}': pa.string(),
        '{language}': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)

    table_list = []
    for period in folders:
        daily_dir = am_raw_dir + f'/{rdate.month_name}/' + period
        daily_files = os.listdir(daily_dir)

        for f in daily_files:
            temp = pacsv.read_csv(daily_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)

    table = pa.concat_tables(table_list)
    adjust_df = table.to_pandas()
    return adjust_df

def albamon_active_user(adjust_df):
    adjust_df.info()
    adjust_df = adjust_df.sort_values(['{adid}', '{created_at}'])
    adjust_df = adjust_df.loc[adjust_df['{adid}'] != '']
    def device_check(df):
        if df['{device_name}'] != 'Xiaomi' and df['{device_manufacturer}'] not in ['Xiaomi','Blackshark','TCL','Huawei']:
            return 1
        else:
            return 0
    adjust_df['device_not_fraud'] = adjust_df.apply(device_check, axis=1).astype(bool)
    def region_check(data):
        if data == 'kr':
            return 1
        else:
            return 0
    adjust_df['region_not_fraud'] = adjust_df['{region}'].map(region_check).astype(bool)

    def language_check(data):
        if data == 'ko':
            return 1
        else:
            return 0
    adjust_df['language_not_fraud'] = adjust_df['{language}'].map(language_check).astype(bool)
    adjust_df = adjust_df.drop(['{device_name}','{device_manufacturer}','{region}','{language}'], axis=1)
    adjust_df[['{adid}', '{network_name}', '{app_name}']] = adjust_df[['{adid}', '{network_name}', '{app_name}']].astype('category')
    def unixtime(x):
        return datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d')
    adjust_df['Date'] = adjust_df['{created_at}'].apply(unixtime)
    del adjust_df['{created_at}']
    adjust_df['Month'] = pd.to_datetime(adjust_df['Date'].values).month
    adjust_df['Month'] = adjust_df['Month'].astype('int8')
    adjust_df = adjust_df.loc[adjust_df['Month'] == rdate.day_1.month]
    adjust_df['Date'] = adjust_df['Date'].astype('category')

    adjust_df.info()
    adjust_df = adjust_df.drop_duplicates(['{adid}', 'Date'], keep='first')
    adjust_df['DAU'] = 1
    adjust_df['MAU'] = 0

    id_arr = np.array(adjust_df['{adid}'])
    mau_arr = np.array(adjust_df['MAU'])

    for i, id in enumerate(id_arr):
        if i == 0:
            before_id = id
            mau = 1
        else:
            if id != before_id:
                mau = 1
            else:
                mau = 0
            before_id = id
        mau_arr[i] = mau
    adjust_df['MAU'] = mau_arr

    def fraud_check(df):
        if df['device_not_fraud'] == 1 and df['region_not_fraud'] == 1 and df['language_not_fraud'] == 1:
            return 1
        else:
            return 0
    adjust_df['is_not_fraud'] = adjust_df.apply(fraud_check, axis=1).astype(bool)
    adjust_df_pivot = adjust_df.pivot_table(index=['Date', '{app_name}', '{network_name}', 'device_not_fraud', 'region_not_fraud', 'language_not_fraud', 'is_not_fraud'],
                                            values=['DAU', 'MAU'],
                                            aggfunc='sum')
    adjust_df_pivot = adjust_df_pivot.reset_index()
    adjust_df_pivot = adjust_df_pivot.rename(columns={'{network_name}': 'media_source', '{app_name}': 'platform'})
    adjust_df_pivot['platform'] = adjust_df_pivot['platform'].apply(
        lambda x: 'android' if x == 'com.albamon.app' else 'ios')
    adjust_df_pivot['Business'] = '알바몬'
    adjust_df_pivot.to_csv(result_dir + f'/am_mau_data_fraud_check_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')

    return adjust_df_pivot

def file_concat():
    jk_data = pd.read_csv(result_dir + f'/jk_mau_data_fraud_check_{rdate.yearmonth}.csv')
    am_data = pd.read_csv(result_dir + f'/am_mau_data_fraud_check_{rdate.yearmonth}.csv')
    total_data = pd.concat([jk_data, am_data], sort=False, ignore_index= True)

    total_data.to_csv(result_dir + '/total_mau_data_fraud_check.csv', index=False, encoding = 'utf-8-sig')

organic_df = jobkorea_organic_read()
paid_df = jobkorea_paid_read(columns = organic_df.columns)
# 로드에 약 1분 소요
jk_active_df = jobkorea_active_user(organic_df, paid_df)
# 처리에 약 2분 소요

adjust_df = albamon_adjust_read()
# 로드에 약 3분 소요
albamon_active_user(adjust_df)
# 처리에 약 36분 소요

file_concat()