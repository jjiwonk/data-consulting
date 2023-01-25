from analysis.DCT512_wconcept import info
import pandas as pd
import numpy as np
import re
import datetime
import pyarrow as pa
import pyarrow.csv as pacsv
import os
import json
import setting.directory as dr

file_dir = info.paid_dir
#file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/W컨셉/리포트 자동화/appsflyer/~ 2204'

def data_read():
    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'original_url': pa.string(),
        'attributed_touch_time': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'ad' : pa.string(),
        'event_value': pa.string(),
        'appsflyer_id':pa.string() }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(file_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(file_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)

    raw_df = table.to_pandas()

    return raw_df

df = data_read()

df['order_id'] = df['event_value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').replace('\\','').split(',')[0] if x.find(
    'af_order_no') != -1 else '-' )

df = df.loc[df['order_id'] != '-']
df = df.loc[df['order_id'] != '']

df = df.sort_values(by = ['appsflyer_id','event_time'])
df2 = df.drop_duplicates('appsflyer_id',keep='first')

df2 = df2[['event_time','appsflyer_id','order_id']]

df2.to_csv(dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/DCT512/raw/첫구매_orderid.csv', index= False , encoding= 'utf-8-sig')

