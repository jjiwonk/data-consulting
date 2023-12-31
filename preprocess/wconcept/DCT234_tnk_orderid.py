import setting.directory as dr
import setting.report_date as rdate
import pandas as pd
import os
import datetime
import json

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/tnk_raw'
result_dir = dr.download_dir

file_path = raw_dir
file_list = os.listdir(file_path)
raw_data = pd.DataFrame()
for file in file_list:
    file_df = pd.read_csv(file_path + '/' + file, header=0, encoding='utf-8-sig')
    raw_data = pd.concat([raw_data, file_df])

raw_data = raw_data.loc[raw_data['Media Source']=='tnk_int']
#raw_data.to_csv(result_dir + f'/wconcept_tnkraw_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')
#raw_data = raw_data.loc[raw_data['event_name']=='item_payment']
#raw_data['order_id'] = raw_data['event_value'].apply(lambda x:json.loads(x)['af_order_no'])
raw_data['주문번호'] = raw_data['Event Value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').split(',')[0] if x.find(
    'af_order_no') != -1 else x )
raw_data.loc[raw_data['Event Name'] != 'item_payment','주문번호'] = '-'

#ctit,itet 계산
raw_data['Attributed Touch Time'] = pd.to_datetime(raw_data['Attributed Touch Time'])
raw_data['Install Time'] = pd.to_datetime(raw_data['Install Time'])
raw_data['Event Time'] = pd.to_datetime(raw_data['Event Time'])

raw_data = raw_data.loc[(raw_data['Install Time'] - raw_data['Attributed Touch Time']) < datetime.timedelta(days=1)]
raw_data = raw_data.loc[(raw_data['Event Time'] - raw_data['Install Time']) < datetime.timedelta(days=1)]

#raw_data = raw_data.drop_duplicates(subset = '주문번호', keep='first')

#tnk_orderid = raw_data[['event_time','order_id']]
#tnk_orderid = tnk_orderid.sort_values(by=['event_time'] ,ascending= True)
#tnk_orderid = tnk_orderid.drop_duplicates(subset = 'order_id', keep='first')

raw_data.to_csv(result_dir + f'/wconcept_orderid_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')