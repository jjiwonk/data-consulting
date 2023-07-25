import numpy as np
import pandas as pd
from setting import directory as dr
import datetime
from workers import func
from workers import bigquery

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/Latib'


bigquery_prep = bigquery.BigQueryPrep()

data = bigquery_prep.read_data(raw_dir, 'query_data.json')
data = bigquery_prep.timestamp_to_kst(data)

period_data = data.loc[(data['event_date']>=datetime.date(2023,5,4))&
                       (data['event_date']<=datetime.date(2023,7,1))]
period_data.index = range(len(period_data))

# 세션 시작 이벤트
session_start = bigquery_prep.event_param_parser(period_data, 'session_start')

# 네이버 결제 시작 이벤트 발라내기
checkout = bigquery_prep.event_param_parser(period_data, 'begin_checkout')
checkout_naver = checkout.loc[checkout['transaction_id'].str.len()>0]

period_data.loc[period_data['event_id'].isin(checkout_naver['event_id']), 'event_name'] = 'begin_checkout_naver'
period_data['event_name'].value_counts()
period_data = period_data.loc[period_data['event_name']!='user_engagement']

len(checkout_naver['user_pseudo_id'].unique())

# scroll 데이터 EDA
# scroll = bigquery_prep.event_param_parser(period_data, 'scroll')
# scroll_10 = scroll.loc[scroll['Scroll_Depth']=='10']
# scroll['Scroll_Depth'].value_counts()

period_data = period_data.sort_values(['user_pseudo_id', 'event_timestamp'])
period_data['event_date_kst'] = pd.to_datetime(period_data['event_date_kst'])
# 날짜 차이 계산 (shift 연산)

period_data['time_gap'] = period_data.groupby('user_pseudo_id')['event_date_kst'].diff()
period_data['time_gap'] = period_data['time_gap'].apply(lambda x: x.total_seconds())

user_array = np.array(period_data['user_pseudo_id'])
event_array = np.array(period_data['event_name'])
event_time_array = np.array(period_data['event_date_kst'])
time_gap_array = np.array(period_data['time_gap'])
value_array = np.zeros(shape = len(period_data))

session_gen = func.SessionDataGenerator(user_array, event_array, event_time_array, time_gap_array, value_array)
session_gen.do_work()

session_data = session_gen.data.copy()
session_data['Cnt'] = 1
session_data['session_sequence_string'] = session_data['session_sequence'].apply(lambda x: ' > '.join(x))

session_id_list = []
for i in range(len(session_data)) :
    repeat = len(session_data['session_sequence'][i]) - 1
    session_id_list += [f'session {i}'] * repeat

period_data['session_id'] = session_id_list

scroll_data = bigquery_prep.event_param_parser(period_data, 'scroll')
scroll_data['Scroll_Depth'] = scroll_data['Scroll_Depth'].astype('int')
scroll_data_max = scroll_data.pivot_table(index = ['session_id', 'page_title'], values = 'Scroll_Depth', aggfunc = 'max').reset_index()

item = bigquery_prep.event_param_parser(period_data, 'view_item')
last_item = item.drop_duplicates('session_id', keep = 'last')
last_item = last_item[['session_id', 'page_title']]

first_item = item.drop_duplicates('session_id', keep = 'first')
first_item = first_item[['session_id', 'page_title']]

last_item_max_scroll = last_item.merge(scroll_data_max, on = ['session_id', 'page_title'], how = 'left')
last_item_max_scroll = last_item_max_scroll.rename(columns = {'page_title' : 'last_item',
                                                              'Scroll_Depth' : 'last_item_scroll_depth'})

first_item_max_scroll = first_item.merge(scroll_data_max, on = ['session_id', 'page_title'], how = 'left')
first_item_max_scroll = first_item_max_scroll.rename(columns = {'page_title' : 'first_item',
                                                                'Scroll_Depth' : 'first_item_scroll_depth'})

session_data_merge = session_data.merge(last_item_max_scroll, on = 'session_id', how = 'left')
session_data_merge = session_data_merge.merge(first_item_max_scroll, on = 'session_id', how = 'left')

session_data_merge.to_csv(raw_dir + '/session_data.csv', index=False, encoding = 'utf-8-sig')

raw_data = session_data_merge
funnel_list = ['view_item', 'page_view','scroll', 'begin_checkout_naver']
end_sequence = 'session_end'
sequence_column_name = 'session_sequence_string'
destination = raw_dir
file_name = 'latib_sankey_data.xlsx'

Sankey = func.SankeyModeling(raw_data, funnel_list, end_sequence, sequence_column_name, destination, file_name)
Sankey.do_work()
Sankey.sankey_to_excel()

####

naver_session_data = session_data.loc[session_data['session_sequence_string'].str.contains('begin_checkout_naver')]
naver_session_data.loc[naver_session_data['session_id'].isin(Sankey.data['session_id']), 'is_target_session'] = True
naver_session_data['is_target_session'] = naver_session_data['is_target_session'].fillna(False)
naver_session_data.to_csv(raw_dir + '/naver_session_data.csv', index=False, encoding = 'utf-8-sig')



sankey_data = Sankey.data
sankey_data_step4_other = sankey_data.loc[sankey_data['Step 4'] == 'Other Events']
sankey_data_step4_other_sessions = period_data.loc[period_data['session_id'].isin(sankey_data_step4_other['session_id'])]
sankey_data_step4_other_sessions = sankey_data_step4_other_sessions.loc[~sankey_data_step4_other_sessions['event_name'].isin(['scroll', 'first_visit', 'session_start'])]
sankey_data_step4_other_sessions = sankey_data_step4_other_sessions.drop_duplicates(subset = 'session_id', keep = 'last')
sankey_data_step4_other_sessions['event_name'].value_counts()

other_session_pageview = bigquery_prep.event_param_parser(sankey_data_step4_other_sessions, 'page_view')
other_session_pageview['page_title'].value_counts()

#######
#### CRM 설계 ####
scroll_funnel_user = sankey_data.loc[sankey_data['Step 3']=='scroll']

view_item = bigquery_prep.event_param_parser(period_data, 'view_item')
view_item_salad_user = view_item.loc[view_item['page_title']=='라티브 샐러드 주스']
view_item_salad_user = view_item_salad_user[['user_pseudo_id', 'session_id']].drop_duplicates()

session_with_salad_view = scroll_funnel_user.merge(view_item_salad_user, on=['session_id'], how = 'inner')
len(session_with_salad_view.loc[session_with_salad_view['session_sequence_string'].str.contains('begin_checkout_naver')])

session_with_non_salad_view = scroll_funnel_user.loc[~scroll_funnel_user['session_id'].isin(view_item_salad_user['session_id'])]
len(session_with_non_salad_view.loc[session_with_non_salad_view['session_sequence_string'].str.contains('begin_checkout_naver')])

### Bound User
scroll_to_bound = sankey_data.loc[sankey_data['Step 4']=='Bound']
scroll_to_bound_last_event = period_data.loc[period_data['session_id'].isin(scroll_to_bound['session_id'])]
scroll_to_bound_last_event = scroll_to_bound_last_event.drop_duplicates('session_id', keep='last')
scroll_to_bound_last_event = bigquery_prep.event_param_parser(scroll_to_bound_last_event, 'scroll')
scroll_to_bound_last_event = scroll_to_bound_last_event.loc[scroll_to_bound_last_event['session_id'].isin(view_item_salad_user['session_id'])]
scroll_to_bound_last_event.to_csv(raw_dir + '/scroll_to_bound_last_event.csv', index=False, encoding = 'utf-8-sig')

scroll_to_bound_user = list(scroll_to_bound['user_id'])
scroll_to_bound_user_basket = period_data.loc[period_data['user_pseudo_id'].isin(scroll_to_bound_user)]
scroll_to_bound_user_basket = scroll_to_bound_user_basket.loc[scroll_to_bound_user_basket['event_name'].isin(['begin_checkout', 'begin_checkout_naver', 'purchase', 'add_to_cart'])]

# begin_checkout
basket_begin_check_out = bigquery_prep.event_param_parser(scroll_to_bound_user_basket, 'begin_checkout')
basket_begin_check_out = basket_begin_check_out.loc[basket_begin_check_out['items'].str.len()>0]
basket_begin_check_out['item'] = basket_begin_check_out['items'].apply(lambda x: x[0]['item_name'])
basket_begin_check_out['item'].value_counts()

# begin_checkout_naver
basket_begin_checkout_naver = bigquery_prep.event_param_parser(scroll_to_bound_user_basket, 'begin_checkout_naver')
basket_begin_checkout_naver['item'] = basket_begin_checkout_naver['items'].apply(lambda x: x[0]['item_name'])
basket_begin_checkout_naver['item'].value_counts()

# purchase
basket_purchase = bigquery_prep.event_param_parser(scroll_to_bound_user_basket, 'purchase')
basket_purchase['item'] = basket_purchase['items'].apply(lambda x: x[0]['item_name'])
basket_purchase['item'].value_counts()

basket_add_to_cart = bigquery_prep.event_param_parser(scroll_to_bound_user_basket, 'add_to_cart')
basket_add_to_cart['item'] = basket_add_to_cart['items'].apply(lambda x: x[0]['item_name'])
basket_add_to_cart['item'].value_counts()


##### 0710 데이터 검증

data_0710 = bigquery_prep.read_data(raw_dir, '0710_log.json')
data_0710 = bigquery_prep.timestamp_to_kst(data_0710)
data_0710 = data_0710.loc[data_0710['event_date']==datetime.date(2023,7,10)]

data_0710['event_name'].value_counts()
