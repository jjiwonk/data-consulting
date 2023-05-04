import analysis.DCT1088_thehandsome.data_prep as prep
from workers import func

import pandas as pd
import numpy as np
import json

# raw 데이터 로드 및 가공
total_df = prep.get_total_raw_data()
total_df.loc[total_df['event_value'] == ''] = '{}'
total_df['member_id'] = prep.get_event_from_values(np.array(total_df['event_value']), 'af_member_id')

user_id_dict = func.user_identifier(total_df, 'appsflyer_id', 'member_id')
total_df['unique_user_id'] = total_df['appsflyer_id'].apply(lambda x : user_id_dict.get(x))

total_df_identified = total_df.loc[(total_df['unique_user_id'].str.len()>0)]
total_df_identified = total_df_identified.sort_values(['unique_user_id', 'event_time'])
total_df_identified['event_name'].value_counts()

purchase_df = total_df_identified.loc[total_df_identified['event_name'].isin(['af_purchase','af_first_purchase'])]
purchase_df['order_id'] = purchase_df['event_value'].apply(lambda x: json.loads(x)['af_order_id'] if 'af_order_id' in json.loads(x).keys() else '-')
purchase_df_dedup = purchase_df.drop_duplicates('order_id')

non_purchase_df = total_df_identified.loc[~total_df_identified['event_name'].isin(['af_purchase','af_first_purchase'])]
non_purchase_df_dedup = non_purchase_df.drop_duplicates(['event_name', 'event_time'])

paid_user_list = list(total_df_identified.loc[total_df_identified['is_paid']==True, 'unique_user_id'])
concat_df = pd.concat([purchase_df_dedup, non_purchase_df_dedup], sort=False, ignore_index=True)
concat_df = concat_df.sort_values(['unique_user_id', 'event_time'])
concat_df = concat_df.loc[concat_df['unique_user_id'].isin(paid_user_list)]
concat_df.index = range(len(concat_df))

compare_df = concat_df.iloc[1:].append(concat_df.iloc[0])
compare_df = compare_df[['unique_user_id', 'event_time']]
compare_df = compare_df.rename(columns = {'unique_user_id' : 'unique_user_id_comp', 'event_time' : 'event_time_comp'})
compare_df.index = range(len(compare_df))

concat_df_compare = pd.concat([concat_df, compare_df], axis = 1)
concat_df_compare['time_gap'] = pd.to_datetime(concat_df_compare['event_time_comp']) - pd.to_datetime(concat_df_compare['event_time'])
concat_df_compare['time_gap'] = concat_df_compare['time_gap'].apply(lambda x : x.total_seconds())

user_arr = np.array(concat_df_compare['unique_user_id'])
event_arr = np.array(concat_df_compare['event_name'])
event_time_arr= np.array(concat_df_compare['event_time'])
time_gap_arr = np.array(concat_df_compare['time_gap'])

session_generator = func.SessionDataGenerator(user_array = user_arr, event_array = event_arr,
                                         event_time_array = event_time_arr, time_gap_array = time_gap_arr)
session_generator.do_work()
session_data = session_generator.data











