import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import json
import pandas as pd
import datetime

file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/DCT477'

def data_read():
    dtypes = {
        'event_time': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'event_name': pa.string(),
        'event_value': pa.string()}
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

df = df.loc[df['event_name'] == 'item_payment']
df = df.loc[df['media_source'].isin(['facebook', 'KAKAOBIZBOARD', 'NAVERGFA', 'Twitter'])]
df = df.loc[df['campaign'].isin(['biz_promotion_traffic_aos','biz_promotion_traffic_ios','ig_promotion_traffic_aos','ig_promotion_traffic_aos_dpa','ig_promotion_traffic_ios','ig_promotion_traffic_ios_dpa','promotion_traffic_aos','promotion_traffic_ios'])]

#df['event_value'] = df['event_value'].apply(lambda x : json.loads(x))
#df['item_code'] = df['event_value'].apply(lambda x : x['af_item_code'] if 'af_item_code' in x.keys() else '')

df['item_code'] = df['event_value'].apply(lambda x:x.split('af_item_code')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').replace('\\','').split(',')[0] if x.find(
    'af_item_code') != -1 else 0 )

df.to_csv(dr.download_dir+'/w컨셉_itemcode추출.csv', index = False)