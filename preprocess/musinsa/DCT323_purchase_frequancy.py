from setting import directory as dr
import pandas as pd
import numpy as np
import json
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime


raw_dir = dr.download_dir
columns = ['appsflyer_id', 'event_time', 'event_value']

data1 = pd.read_csv(raw_dir + '/5d7ff61b-c36d-43bf-825c-447be8d33b32.csv', usecols = columns)
data2 = pd.read_csv(raw_dir + '/c85216f6-9485-4037-ba53-afe2a6a93573.csv', usecols = columns)

dtypes = {
    'AppsFlyer ID': pa.string(),
    'Event Time': pa.string(),
    'Event Value': pa.string()
}

convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=dtypes.keys())
ro = pacsv.ReadOptions(block_size=10 << 20)
table_list = []

organic_dir = dr.download_dir + '/무신사 오가닉_raw'
organic_files = os.listdir(organic_dir)
organic_list = []

for f in organic_files:
    temp = pacsv.read_csv(organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
    organic_list.append(temp)
table = pa.concat_tables(organic_list)
data3 = table.to_pandas()
data3.columns = [col.lower().replace(' ', '_') for col in list(data3.columns)]

raw_data = pd.concat([data1, data2, data3], sort=False, ignore_index=True)
del data1, data2, data3, table, temp

raw_data['event_time'] = pd.to_datetime(raw_data['event_time'])
raw_data = raw_data.loc[(raw_data['event_time'].dt.date >= datetime.date(2022, 5, 13)) & (raw_data['event_time'].dt.date <= datetime.date(2022,11,9))]
raw_data = raw_data.sort_values('event_time')
raw_data['event_value'] = raw_data['event_value'].apply(lambda x : json.loads(x))
raw_data['order_id'] = raw_data['event_value'].apply(lambda x : x['af_order_id'] if 'af_order_id' in x.keys() else '')

raw_data_dedup = raw_data.drop_duplicates('order_id')
del raw_data
raw_data_dedup['event_revenue'] = raw_data_dedup['event_value'].apply(lambda x : x['af_revenue'] if 'af_revenue' in x.keys() else 0)

raw_data_non_error = raw_data_dedup.loc[(raw_data_dedup['event_revenue']!=0) & (raw_data_dedup['order_id']!='')]
del raw_data_dedup
raw_data_non_error = raw_data_non_error.sort_values(['appsflyer_id', 'event_time'])

raw_data_non_error['before_user'] = raw_data_non_error['appsflyer_id'].shift(1)
raw_data_non_error['before_time'] = raw_data_non_error['event_time'].shift(1)
raw_data_non_error['time_gap'] = raw_data_non_error['event_time'] - raw_data_non_error['before_time']

raw_data_non_error.to_csv(dr.download_dir + '/total_purchase.csv')

raw_data_non_error = pd.read_csv(raw_dir + '/total_purchase.csv')
raw_data_non_error['is_re_purchase'] = raw_data_non_error['before_user'] == raw_data_non_error['appsflyer_id']

re_purchase = raw_data_non_error.loc[raw_data_non_error['is_re_purchase'] == True]
re_purchase['time_gap'] = pd.to_timedelta(re_purchase['time_gap'])
re_purchase = re_purchase.loc[re_purchase['time_gap']>=datetime.timedelta(1)]
re_purchase['time_gap'] = re_purchase['time_gap'].apply(lambda x : x.total_seconds())

re_purchase_pivot = re_purchase.pivot_table(index = 'appsflyer_id', values = 'time_gap', aggfunc = 'mean')
re_purchase_pivot = re_purchase_pivot.reset_index()
re_purchase_pivot['time_gap'] = re_purchase_pivot['time_gap'].apply(lambda x : x/86400)


user_data = raw_data_non_error.copy()
user_data['Cnt'] = 1
user_data_pivot = user_data.pivot_table(index = 'appsflyer_id', values = ['Cnt', 'event_revenue'], aggfunc = 'sum')
user_data_pivot = user_data_pivot.reset_index()

user_data_pivot_merge = user_data_pivot.merge(re_purchase_pivot, on = 'appsflyer_id', how = 'left')
user_data_pivot_merge['ATV'] = user_data_pivot_merge['event_revenue'] / user_data_pivot_merge['Cnt']

atv_mean = np.mean(user_data_pivot_merge['ATV'])
atv_std = np.std(user_data_pivot_merge['ATV'])

user_data_pivot_merge['ATV_norm'] = user_data_pivot_merge['ATV'].apply(lambda x: (x - atv_mean)/atv_std)

rev_mean = np.mean(user_data_pivot_merge['event_revenue'])
rev_std = np.std(user_data_pivot_merge['event_revenue'])

user_data_pivot_merge['Rev_norm'] = user_data_pivot_merge['event_revenue'].apply(lambda x: (x - rev_mean)/rev_std)


last_purchase_time = raw_data_non_error.drop_duplicates('appsflyer_id', keep = 'last')
last_purchase_time = last_purchase_time[['appsflyer_id', 'event_time']]
last_purchase_time['Recency'] = datetime.date(2022,11,9) - pd.to_datetime(last_purchase_time['event_time']).dt.date
last_purchase_time['Recency'] = last_purchase_time['Recency'].apply(lambda x: x.days)
last_purchase_time.rename(columns = {'event_time' : 'last_purchase_time'}, inplace = True)

user_data_pivot_merge = user_data_pivot_merge.merge(last_purchase_time, on = 'appsflyer_id', how = 'left')
user_data_pivot_merge.to_csv(raw_dir + '/user_report.csv', index=False, encoding = 'utf-8-sig')