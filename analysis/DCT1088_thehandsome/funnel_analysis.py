import analysis.DCT1088_thehandsome.data_prep as prep
from workers import func

import pandas as pd
import numpy as np
import json
import setting.directory as dr

# raw 데이터 로드 및 가공
total_df = prep.get_total_raw_data()
total_df['member_id'] = prep.get_event_from_values(np.array(total_df['event_value']), 'af_member_id')
total_df['event_name'].unique() 

user_id_dict = func.user_identifier(total_df, 'appsflyer_id', 'member_id')
total_df['unique_user_id'] = total_df['appsflyer_id'].apply(lambda x : user_id_dict.get(x))

total_df_identified = total_df.loc[(total_df['unique_user_id'].str.len()>0)]
total_df_identified = total_df_identified.sort_values(['unique_user_id', 'event_time'])
total_df_identified['event_name'].value_counts()

purchase_df = total_df_identified.loc[total_df_identified['event_name'].isin(['af_purchase','af_first_purchase'])]
purchase_df['order_id'] = purchase_df['event_value'].apply(lambda x: json.loads(x)['af_order_id'] if 'af_order_id' in json.loads(x).keys() else '-')
purchase_df['event_name'] = 'purchase'
purchase_df_dedup = purchase_df.drop_duplicates('order_id')

non_purchase_df = total_df_identified.loc[~total_df_identified['event_name'].isin(['af_purchase', 'af_first_purchase'])]
non_purchase_df.loc[non_purchase_df['event_name']=='장바구니 담기', 'event_name'] = 'af_add_to_cart'
non_purchase_df.loc[non_purchase_df['event_name']=='상세페이지 조회', 'event_name'] = 'af_content_view'
non_purchase_df.loc[non_purchase_df['event_name']=='찜한 목록 추가', 'event_name'] = 'af_add_to_wishlist'
non_purchase_df.loc[non_purchase_df['event_name']=='상품 검색', 'event_name'] = 'af_search'
non_purchase_df['event_revenue'] = 0
non_purchase_df_dedup = non_purchase_df.drop_duplicates(['event_name', 'event_time'])

first_purchase_user_list = list(total_df.loc[total_df['event_name']=='af_first_purchase', 'unique_user_id'])
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
concat_df_compare['event_name'].value_counts()
concat_df_compare['event_name'].unique()

user_arr = np.array(concat_df_compare['unique_user_id'])
event_arr = np.array(concat_df_compare['event_name'])
event_time_arr= np.array(concat_df_compare['event_time'])
time_gap_arr = np.array(concat_df_compare['time_gap'])
value_arr = np.array(concat_df_compare['event_revenue'])

session_generator = func.SessionDataGenerator(user_array = user_arr, event_array = event_arr,
                                         event_time_array = event_time_arr, time_gap_array = time_gap_arr, value_array = value_arr)
session_generator.do_work()
session_data = session_generator.data
session_data['Cnt'] = 1
session_data['session_sequence_string'] = session_data['session_sequence'].apply(lambda x: ' > '.join(x))

sankey = func.SankeyModeling(raw_data = session_data.copy(),
                        funnel_list=['install', 'af_content_view', 'af_add_to_cart', 'purchase'],
                        end_sequence='session_end',
                        sequence_column_name='session_sequence_string',
                        destination=dr.download_dir,
                        file_name = 'sankey_data.xlsx')
sankey.do_work()

sankey_data = sankey.data
sankey_data['first_purchase_user'] = sankey_data['user_id'].apply(lambda x : True if x in first_purchase_user_list else False)
sankey_data = sankey_data.sort_values(['user_id', 'first_session_time'])

sankey_purchase = sankey_data.loc[sankey_data['Step 4']=='purchase']
sankey_purchase = sankey_purchase.drop_duplicates('user_id')
sankey_purchase['first_purchase_in_period'] = True
sankey_purchase = sankey_purchase[['session_id', 'first_purchase_in_period']]

sankey_data_merge = sankey_data.merge(sankey_purchase, on = 'session_id', how = 'left')
sankey_data_merge['first_purchase_in_period'] = sankey_data_merge['first_purchase_in_period'].fillna(False)

# 진성유저 매핑
purchase_df_dedup[['attributed_touch_time', 'install_time', 'event_time']] = purchase_df_dedup[['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
purchase_df_dedup[['event_revenue', 'event_revenue_krw']] = purchase_df_dedup[['event_revenue', 'event_revenue_krw']].astype(float)
rfm_segment_df = prep.prep_rfm_segment_df(purchase_df_dedup)
quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'unique_user_id'])
sankey_data_merge['quality_user'] = sankey_data_merge['user_id'].apply(lambda x: True if x in quality_user_list else False)


sankey.data = sankey_data_merge
sankey.data = sankey.data.rename(columns = {'cnt' : 'Cnt'})
sankey.sankey_to_excel()






