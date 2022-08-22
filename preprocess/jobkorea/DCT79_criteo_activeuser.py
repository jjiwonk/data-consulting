import setting.directory as dr
import setting.report_date as rdate

import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime

jk_raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/ADID, IDFA'
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/잡코리아/result'

# 잡코리아

def jobkorea_organic_read():
    raw_dir = jk_raw_dir + f'/organic_{rdate.month_name}'
    jk_raw_files = os.listdir(raw_dir)
    organic_files = [f for f in jk_raw_files if 'prism' not in f]


    dtypes = {
        'AppsFlyer ID': pa.string(),
        'Event Time': pa.string(),
        'Media Source': pa.string(),
        'Campaign' : pa.string(),
        'Platform': pa.string()
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
        'Campaign' : 'campaign',
        'Platform': 'platform'
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

def jobkorea_criteo_active_user(organic_df, paid_df) :
    jk_total_df = pd.concat([organic_df, paid_df], sort=False, ignore_index=True)
    jk_total_df['event_time'] = pd.to_datetime(jk_total_df['event_time'])
    jk_total_df = jk_total_df.sort_values(['appsflyer_id', 'event_time'])
    jk_total_df['Date'] = jk_total_df['event_time'].dt.date
    jk_total_df = jk_total_df.drop_duplicates(['appsflyer_id', 'Date'], keep='first')
    jk_total_df['DAU'] = 1
    jk_total_df['MAU'] = 0
    jk_total_df.index = range(0, len(jk_total_df))

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

    criteo_active_user_raw = jk_total_df.loc[jk_total_df['media_source']=='criteonew_int']
    non_criteo_active_user = set(criteo_active_user_raw.loc[criteo_active_user_raw['MAU']!=1, 'appsflyer_id'])

    jk_total_df = jk_total_df.loc[jk_total_df['appsflyer_id'].isin(non_criteo_active_user)]

    return jk_total_df


organic_df = jobkorea_organic_read()
paid_df = jobkorea_paid_read(columns = organic_df.columns)

criteo_data = jobkorea_criteo_active_user(organic_df, paid_df)
criteo_data.to_csv(result_dir +'/criteo_non_activeuser_log.csv', index=False, encoding = 'utf-8-sig')


concat_data = pd.concat([organic_df, paid_df],sort=False)
criteo_user = set(concat_data.loc[concat_data['media_source']=='criteonew_int', 'appsflyer_id'])

concat_data = concat_data.loc[concat_data['appsflyer_id'].isin(criteo_user)]
concat_data_unique = concat_data.drop_duplicates(['media_source','appsflyer_id'])
concat_data_unique['cnt']=1
concat_data_unique_pivot = concat_data_unique.pivot_table(index = ['media_source'], values = 'cnt', aggfunc='sum')
concat_data_unique_pivot = concat_data_unique_pivot.reset_index()
concat_data_unique_pivot = concat_data_unique_pivot.sort_values('cnt', ascending=False)
concat_data_unique_pivot['rate'] = concat_data_unique_pivot['cnt'] / len(criteo_user)
concat_data_unique_pivot.to_csv(result_dir + '/criteo_user_pivot.csv', index=False, encoding = 'utf-8-sig')
