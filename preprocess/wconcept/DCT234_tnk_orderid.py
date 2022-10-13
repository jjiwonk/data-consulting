import setting.directory as dr
import setting.report_date as rdate
import pandas as pd
import os
import datetime

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/tnk_raw'
result_dir = raw_dir

file_path = raw_dir
file_list = os.listdir(file_path)
raw_data = pd.DataFrame()
for file in file_list:
    file_df = pd.read_csv(file_path + '/' + file, header=0, encoding='utf-8-sig')
    raw_data = pd.concat([raw_data, file_df])

raw_data = raw_data.loc[raw_data['media_source']=='tnk_int']
raw_data = raw_data.loc[raw_data['event_name']=='item_payment']
raw_data['order_id'] = raw_data['event_value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']','').replace(':', '').split(',')[0] if x.find('af_order_no') != -1 else x)

#ctit,itet 계산

raw_data['attributed_touch_time'] = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in raw_data['attributed_touch_time']]
raw_data['install_time'] = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in raw_data['install_time']]
raw_data['event_time'] = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in raw_data['event_time']]

raw_data = raw_data.loc[(raw_data['install_time'] - raw_data['attributed_touch_time']) <= datetime.timedelta(days=1)]
raw_data = raw_data.loc[(raw_data['event_time'] - raw_data['install_time']) <= datetime.timedelta(days=1)]

tnk_orderid = raw_data['order_id']

tnk_orderid.to_csv(result_dir + f'/wconcept_orderid_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')
