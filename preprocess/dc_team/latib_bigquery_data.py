import numpy as np
import pandas as pd
from setting import directory as dr
import datetime
import json
import time
from dateutil import tz


raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/Latib'
# session_file_name= 'bquxjob_565609be_188b8f07f00.csv'
#
# session_df = pd.read_csv(raw_dir + '/' + session_file_name)
#
# def get_attribute(x, attribute, key, key_name, value,value_name):
#     array = json.loads(x)[attribute]
#
#     target_item = None
#     for item in array :
#         if item[key] == key_name :
#             target_item = item
#             break
#
#     return target_item[value][value_name]
#
# event_param_array = np.array(session_df['event_params'])
#
# result_array = list(range(len(event_param_array)))
# for i, event_param in enumerate(event_param_array) :
#     result_array[i] = get_attribute(event_param, 'event_params', 'key', 'page_location', 'value','string_value')
#
# session_df['page_location'] = result_array
#
# for col in ['name', 'source', 'medium']:
#     session_df[col] = session_df['traffic_source'].apply(lambda x : json.loads(x)['traffic_source'][col])
#
# session_df_comp = session_df[['event_date', 'event_timestamp', 'event_name', 'event_params',
#                               'user_pseudo_id','user_first_touch_timestamp','device',  'platform', 'event_dimensions',
#                               'page_location', 'name', 'source', 'medium']]
# temp = session_df_comp.loc[session_df_comp['page_location'].str.contains('utm')]
# temp = temp.loc[temp['page_location'].str.contains('naver_bs')]
# temp = temp.loc[temp['source']!='naver_bs']
# temp_user_list = temp['user_pseudo_id'].unique()
#
# session_df_comp_user_filter = session_df_comp.loc[session_df_comp['user_pseudo_id'].isin(temp_user_list)]

log1 = pd.read_json(raw_dir + '/bq-results-20230614-085528-1686733050757.json', lines=True)
log2 = pd.read_json(raw_dir + '/bq-results-20230614-093849-1686735542547.json', lines=True)

total_log_data = pd.concat([log1, log2], ignore_index=True)
# total_log_data['event_timestamp_parsing'] = total_log_data['event_timestamp'].apply(lambda x : float(str(x)[:10] + "." + str(x)[-6:]))
# total_log_data['event_date_utc'] = total_log_data['event_timestamp_parsing'].apply(lambda x : datetime.datetime.fromtimestamp(x, ))
# total_log_data['event_date_utc'] = total_log_data['event_date_utc'].apply(lambda x : x + datetime.timedelta(hours = 9))
# total_log_data['event_date_kst'] = total_log_data['event_date_utc'].dt.date

#total_log_data_kst = total_log_data.loc[total_log_data['event_date_kst']==datetime.date(2023,6,12)]
total_log_data_kst = total_log_data.loc[total_log_data['event_date']==20230612]


for col in ['name', 'source', 'medium']:
    total_log_data_kst[col] = total_log_data_kst['traffic_source'].apply(lambda x : x.get(col))
total_log_data_kst['cnt'] = 1

for col in ['manual_source', 'manual_medium'] :
    total_log_data_kst[col] = total_log_data_kst['collected_traffic_source'].apply(lambda x : x.get(col) if pd.notnull(x) else 'none')

log_pivot = total_log_data_kst.pivot_table(index = ['event_name', 'source', 'medium', 'manual_source', 'manual_medium'], values = 'cnt', aggfunc='sum').reset_index()
log_pivot_selected = log_pivot.loc[((log_pivot['source']=='instagram')|(log_pivot['manual_source']=='instagram'))&
                                   (log_pivot['event_name']=='session_start')]