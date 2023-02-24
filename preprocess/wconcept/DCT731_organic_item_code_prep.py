import datetime
import os
from workers import read_data
import setting.directory as dr
import pyarrow as pa
import pandas as pd

file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/DCT512/raw/organic'
file_list = os.listdir(file_dir)

organic_dtypes = {
        'event_time': pa.string(),
        'event_value': pa.string(),
        'appsflyer_id':pa.string() }

organic_df = read_data.pyarrow_csv(dtypes= organic_dtypes, directory= file_dir, file_list=file_list)

organic_df = organic_df.sort_values('event_time')
organic_df['event_time'] = pd.to_datetime(organic_df['event_time'])

df = organic_df.loc[organic_df['event_time'] < datetime.datetime(year=2022, month= 2, day= 23)]

df['item_code'] = df['event_value'].apply(lambda x:x.split('af_item_code')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').replace('\\','').split(',')[0] if x.find(
    'af_item_code') != -1 else 0 )

df.to_csv(dr.download_dir+'/w컨셉_오가닉_itemcode추출.csv', index = False)