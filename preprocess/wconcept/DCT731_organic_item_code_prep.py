import datetime
import os
from workers import read_data
import setting.directory as dr
import pyarrow as pa
import pandas as pd
import re
import numpy as np
from collections import Counter

file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/DCT731'
file_list = os.listdir(file_dir)

organic_dtypes = {
        'Event Time': pa.string(),
        'Event Value': pa.string(),
        'AppsFlyer ID':pa.string() }

organic_df = read_data.pyarrow_csv(dtypes= organic_dtypes, directory= file_dir, file_list=file_list)
organic_df = organic_df.sort_values('Event Time')

organic_df['item_code_num'] = organic_df['Event Value'].apply(lambda x:x.count('af_item_code'))
pat = re.compile('"af_item_code":"\d{9,9}"')
organic_df['item_code_pat'] = organic_df['Event Value'].apply(lambda x: re.findall(pat,x) if x.find('af_item_code') != -1 else '-' )
organic_df['pat_num'] = organic_df['item_code_pat'].apply(lambda x:str(x).count('af_item_code'))
organic_df['key'] = organic_df['Event Time'] + organic_df['AppsFlyer ID']

no = organic_df.loc[organic_df['item_code_num'] != organic_df['pat_num']]

key_array = np.array(organic_df['key'])
item_list_array = np.array(organic_df['item_code_pat'])

item_list = []
item_df = pd.DataFrame()

for idx, item_list in enumerate(item_list_array):
    key = key_array[idx]

    df = pd.DataFrame(item_list)
    df = df.reset_index().drop(columns='index').rename(columns = {0:'item_code'})
    df['key'] = key

    item_df = pd.concat([item_df,df])

item_df.to_csv(dr.download_dir+'/w컨셉_itemcode추출_원본.csv', index = False)

key_df = organic_df[['Event Time','AppsFlyer ID','key']].drop_duplicates()

merge_df = pd.merge(item_df,key_df, on = 'key',how='left')
merge_df['item_code'] = merge_df['item_code'].apply(lambda x:x.replace('"', '').replace('af_item_code', '').replace(':', ''))
merge_df = merge_df.drop(columns='key')

merge_df.to_csv(dr.download_dir+'/w컨셉_오가닉_itemcode추출_2023.csv', index = False, encoding ='utf-8-sig')
