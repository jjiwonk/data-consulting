import os
import setting.directory as dr
import pandas as pd
import datetime

file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/레이지나잇/4. 리포트 자동화/Appsflyer_prism/raw'
file_list = os.listdir(file_dir)

raw_data = pd.DataFrame()

for file in file_list:
    file_df = pd.read_csv(file_dir + '/' + file, header=0, encoding='utf-8-sig')
    raw_data = pd.concat([raw_data, file_df])

raw_data = raw_data.loc[raw_data['attributed_touch_type'] == 'click']
raw_data['event_time'] = pd.to_datetime(raw_data['event_time'])
raw_data['install_time'] = pd.to_datetime(raw_data['install_time'])
raw_data['attributed_touch_time'] = pd.to_datetime(raw_data['attributed_touch_time'])

event_data = raw_data.loc[raw_data['event_name'].isin(['af_content_view','af_add_to_cart','af_login','af_complete_registration','first_purchase','af_purchase' ])]
conv_data = raw_data.loc[raw_data['event_name'].isin(['install','re-attribution'])]

conv_data = conv_data.loc[conv_data['install_time'] - conv_data['attributed_touch_time'] <= datetime.timedelta(days=1)]
event_data = event_data.loc[event_data['event_time'] - event_data['install_time'] <= datetime.timedelta(days=7)]

result_data = pd.concat([conv_data,event_data])
result_data = result_data.sort_values('event_time')

result_data.to_csv(dr.download_dir + '/lazynight_appsflyer_prep.csv', index = False, encoding= 'utf-8-sig')
