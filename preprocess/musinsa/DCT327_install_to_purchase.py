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
    'Install Time' : pa.string(),
    'Event Time': pa.string(),
    'Event Value' : pa.string()
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
data3['구분'] = 'Organic'
data3 = data3.sort_values('event_time')
data3 = data3.drop_duplicates('appsflyer_id')

organic_user_list = list(data3['appsflyer_id'].unique())

install_data = pd.read_csv(raw_dir + '/3c9dae2e-237f-487d-92f9-a402148c73ea.csv')
install_data = install_data.sort_values('event_time')
install_data = install_data.drop_duplicates('appsflyer_id')
install_data = install_data[['appsflyer_id', 'install_time']]

paid_purchase = pd.concat([data1, data2], sort=False, ignore_index=True)
paid_purchase = paid_purchase.loc[~paid_purchase['appsflyer_id'].isin(organic_user_list)]
paid_purchase = paid_purchase.sort_values('event_time')
paid_purchase = paid_purchase.drop_duplicates('appsflyer_id')

paid_purchase_merge = paid_purchase.merge(install_data, on = 'appsflyer_id', how = 'inner')
paid_purchase_merge['구분'] = 'Paid'

total_purchase = pd.concat([data3, paid_purchase_merge], sort = False, ignore_index=True)
total_purchase['install_time'] = pd.to_datetime(total_purchase['install_time'])
total_purchase['event_time'] = pd.to_datetime(total_purchase['event_time'])

total_purchase = total_purchase.loc[(total_purchase['install_time'].dt.date >= datetime.date(2022,5,13))&(total_purchase['install_time'].dt.date <= datetime.date(2022,11,9))]
total_purchase = total_purchase.loc[total_purchase['event_time'].dt.date <= datetime.date(2022,11,9)]
total_purchase['event_value'] = total_purchase['event_value'].apply(lambda x : json.loads(x))
total_purchase['order_id'] = total_purchase['event_value'].apply(lambda x : x['af_order_id'] if 'af_order_id' in x.keys() else '')

total_purchase = total_purchase.sort_values('event_time')
total_purchase_dedup = total_purchase.drop_duplicates('order_id')
total_purchase_dedup = total_purchase_dedup.loc[total_purchase_dedup['order_id']!='']
total_purchase_dedup['install_to_purchase'] = total_purchase_dedup['event_time'] - total_purchase_dedup['install_time']
total_purchase_dedup = total_purchase_dedup.loc[total_purchase_dedup['install_to_purchase'] >= datetime.timedelta(0)]
total_purchase_dedup['install_to_purchase'] = total_purchase_dedup['install_to_purchase'].apply(lambda x: x.total_seconds()/86400)

user_report_data = pd.read_csv(raw_dir + '/user_report.csv')
total_purchase_merge = total_purchase_dedup.merge(user_report_data, on ='appsflyer_id', how = 'left')
total_purchase_merge['유저 분류'] = '일반'
total_purchase_merge.loc[total_purchase_merge['ATV']>=500000, '유저 분류'] = '50만원 이상'
total_purchase_pivot = total_purchase_merge.pivot_table(index = ['구분', '유저 분류'], values = 'install_to_purchase', aggfunc='mean').reset_index()

total_purchase_merge.to_csv(raw_dir + '/install_to_purcahse_data.csv', index=False, encoding = 'utf-8-sig')

