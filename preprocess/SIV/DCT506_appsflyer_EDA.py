import os
from setting import directory as dr
from workers import read_data
import pandas as pd
import json
import datetime
import pyarrow as pa
import pyarrow.csv as pacsv
import numpy as np

event_file = dr.download_dir + '/04e2fd41-2115-4453-9fd3-434db6148e64.csv'
dtypes = {'event_name' : pa.string(),
         'event_value' : pa.string(),
         'platform' : pa.string(),
         'app_id' : pa.string(),
         'app_name' : pa.string()}
convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=dtypes.keys())
ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')

data = pacsv.read_csv(event_file, convert_options=convert_ops, read_options=ro)
data = data.to_pandas()


app_id_dict = {'id1581919728' : 'DU',
               'id1555398916' : 'SIBEAUTY',
               'com.sivillage.du' : 'DU',
               'com.sivillage.beauty' : 'SIBEAUTY',
               'id1081793605' : 'JAJU',
               'com.jaju.mobile': 'JAJU',
               'id1081778027' : 'SIV',
               'com.si.simembers': 'SIV'}

data['cnt'] = 1
data['part'] = data['app_id'].apply(lambda x : app_id_dict.get(x))
data['event_value'] = data['event_value'].fillna('{}')
data['event_value'] = data['event_value'].apply(lambda x : eval(x))
data['event_value'][0]['af_content_list_id']

event_pivot = data.pivot_table(index = ['platform','part'], values = 'cnt', columns = 'event_name', aggfunc = 'sum')
event_pivot = event_pivot.reset_index()
event_pivot.to_csv(dr.download_dir + '/event_pivot.csv', index=False, encoding = 'utf-8-sig')


event_list = list(data['event_name'].unique())

def get_event_params(arr) :
    key_list = []
    for event in arr :
        keys = event.keys()
        for key in keys :
            if key in key_list :
                pass
            else :
                key_list.append(key)
    return key_list



for event in event_list :
    temp_df = data.loc[data['event_name']==event, ['part', 'app_id', 'platform', 'event_name', 'event_value']]
    event_keys = get_event_params(np.array(temp_df['event_value']))

    for key in event_keys :
        temp_df[key] = temp_df['event_value'].apply(lambda x : x.get(key))

    temp_df.to_csv(dr.download_dir + f'/{event}_parse_data.csv', index=False, encoding = 'utf-8-sig')

data['event_name'].value_counts()