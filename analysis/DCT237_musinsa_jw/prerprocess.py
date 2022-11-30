from analysis.DCT237_musinsa_jw import info

import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np

def get_paid_df():
    dtypes = {
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'media_source': pa.string(),
        'adset' : pa.string(),
        'ad' : pa.string(),
        'campaign': pa.string(),
        'appsflyer_id': pa.string()
    }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df = raw_df.loc[raw_df['attributed_touch_type'] == 'click']
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(lambda x: pd.to_datetime(x))
    raw_df['organic'] = 'N'
    return raw_df

paid_df = get_paid_df()

def get_organic_df():
    dtypes = {
        'Attributed Touch Time': pa.string(),
        'Attributed Touch Type': pa.string(),
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Media Source': pa.string(),
        'Adset' : pa.string(),
        'Ad' : pa.string(),
        'Campaign': pa.string(),
        'AppsFlyer ID': pa.string()
    }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.organic_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df[['Install Time','Event Time']] = raw_df[['Install Time','Event Time']].apply(lambda x: pd.to_datetime(x))
    raw_df.rename(columns = {
        'Attributed Touch Time': 'attributed_touch_time',
        'Attributed Touch Type': 'attributed_touch_type',
        'Install Time': 'install_time',
        'Event Time': 'event_time',
        'Event Name': 'event_name',
        'Media Source': 'media_source',
        'Adset' : 'adset',
        'Ad' : 'ad',
        'Campaign': 'campaign',
        'AppsFlyer ID': 'appsflyer_id'
    })
    raw_df['organic'] = 'Y'
    return raw_df

organic_df = get_organic_df()


def get_purchase_df():
    dtypes = {
        'event_time': pa.string(),
        'appsflyer_id': pa.string()
    }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    raw_df = pacsv.read_csv(info.raw_dir + '/purchase/fp_2020-01_2022_09.csv' , convert_options=convert_ops, read_options=ro)
    raw_df = raw_df.to_pandas()

    return raw_df

purchase_df = get_purchase_df()



## 컨버전 데이터 기준으로 유저 정리하기




paid_conv = paid_df.loc[paid_df['event_name'].isin(['install','re-engagement','re-attribution'])]
paid_pc = paid_df.loc[paid_df['event_name'].isin(['af_purchase','first_purchase'])]
organic_conv = organic_df.loc[organic_df['Event Name'].isin(['install','re-engagement','re-attribution'])]
organic_pc = organic_df.loc[organic_df['Event Name'].isin(['af_purchase','first_purchase'])]

# 근데 이제 합치기 전에... 리인게이지먼트 기여를 언제, 어디로 할지 정해야함
# 기준 , 인스톨 TO 리인게이지먼트 갭이 7일 이내일땐 최초 설치 타임, 미디어 소스로 하기!!
# 그럴라면... 먼저 최초 인스톨 데이터를 구해줘야함 !





