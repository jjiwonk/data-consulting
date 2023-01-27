from analysis.DCT512_wconcept import info
import pandas as pd
import numpy as np
import os
import json
from workers import read_data
import time

def object_to_eval(data):
    try :
        return eval(data)
    except :
        return '{}'

file_dir = info.paid_dir
file_list = os.listdir(file_dir)

df = read_data.pyarrow_csv(dtypes = info.dtypes, directory = info.paid_dir, file_list = file_list)

vec = np.vectorize(object_to_eval)
value_arr = np.array(df['event_value'])
df['event_value(dict)'] = vec(value_arr)

df['order_id'] = df['event_value(dict)'].apply(lambda x : '' if (type(x) != dict) else x.get('af_order_no'))



df['order_id'] = df['event_value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').replace('\\','').split(',')[0] if x.find(
    'af_order_no') != -1 else '-' )

df = df.loc[df['order_id'] != '-']
df = df.loc[df['order_id'] != '']

df = df.sort_values(by = ['appsflyer_id','event_time'])
df2 = df.drop_duplicates('appsflyer_id',keep='first')

df2 = df2[['event_time','appsflyer_id','order_id']]

df2.to_csv(info.raw_dir + '/첫구매_orderid.csv', index= False , encoding= 'utf-8-sig')

