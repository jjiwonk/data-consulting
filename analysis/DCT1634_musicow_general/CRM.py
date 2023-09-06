from analysis.DCT1634_musicow_general.raw_data import data_set
from workers.func import FunnelDataGenerator
import pandas as pd
import numpy as np
from setting import directory as dr

data = data_set.data
install_user_data = data_set.install_user_data

crm_source = ['crm_kakaochannel_start session', 'crm_kakaochannel_accountcomplete', 'CRM',
              'CRM_KakaoChannel_Signup', 'crm_kakaochannel_signup']
crm_data = data.loc[data['Media Source'].isin(crm_source)]
crm_user = set(crm_data['unique_user_id'])
crm_data['Event Name'].value_counts()

first_crm_touch = crm_data.drop_duplicates('unique_user_id', keep = 'first')
first_crm_touch = first_crm_touch[['unique_user_id', 'Install Time', 'Media Source']]
first_crm_touch = first_crm_touch.rename(columns = {'Install Time' : 'CRM Engaged Time', 'Media Source' : 'CRM Source'})

data = data.merge(first_crm_touch, on = 'unique_user_id', how = 'left')
data['Source Category'] = data['Install Source'].apply(lambda x : 'Paid' if x!='Organic' else x)


event_count = data.pivot_table(index = 'unique_user_id',
                                              columns = 'Event Name',
                                              values = 'Cnt',
                                              aggfunc = 'sum')
event_count = event_count.fillna(0)
event_count = event_count[['af_view_muca-point_event', 'af_market_buy',  'af_market_sell',  'af_main_recommend_click', 'af_top5_click',
                           'af_view_muca-shop_product', 'af_md_click', 'af_click_pagecontents', 'af_view_eventpage']]
event_count.sum().sort_values(ascending=False)

# 국가 코드
country_code = data.pivot_table(index = 'unique_user_id', columns = 'Country Code', values = 'Cnt', aggfunc = 'mean')
country_code['Num Of Country'] = country_code.sum(axis=1)
country_code = country_code[['KR', 'Num Of Country']]
country_code['KR'] = country_code['KR'].fillna(0)

# 디바이스
device = data.pivot_table(index = 'unique_user_id', columns = 'Platform', values = 'Cnt', aggfunc = 'mean')
device = device.fillna(0)
device['Device'] = device.apply(lambda x: 'Both' if x['android'] + x['ios'] == 2 else 'AOS' if x['android']==1 else 'iOS', axis=1)
device = device[['Device']]

# 첫 설치 시간, 설치 소스
install_info = data[['unique_user_id','First Install Time', 'Source Category']]
install_info = install_info.drop_duplicates()
install_info = install_info.set_index('unique_user_id')

# 데이터 병합
total_concat = pd.concat([event_count, country_code, device, install_info], sort = False, axis =1)
total_concat = total_concat.reset_index()
total_concat['First Install Time'] = total_concat['First Install Time'].apply(lambda x: x.strftime('%Y%m'))
total_concat.loc[total_concat['unique_user_id'].isin(crm_data['unique_user_id'].unique()), 'treat'] = 1
total_concat['treat'] = total_concat['treat'].fillna(0)
total_concat['treat'].value_counts()
total_concat.columns = [col.lower().replace(" ", "_").replace("-","_") for col in total_concat.columns]
total_concat.to_csv(dr.download_dir + '/musicow.csv', index=False, encoding = 'utf-8-sig')


# 성향 점수 분석 매칭
psm_data = pd.read_csv(dr.download_dir + '/musicow_psm_data.csv')
treat_group = psm_data.loc[psm_data['treat']==1, 'unique_user_id']
len(treat_group)
treat_group_conversion = data.loc[(data['Event Name']=='af_signup_intro')&
                                  (data['unique_user_id'].isin(treat_group))]
len(treat_group_conversion['unique_user_id'].unique())

control_group = psm_data.loc[psm_data['treat']==0, 'unique_user_id']
len(treat_group)
control_group_conversion = data.loc[(data['Event Name']=='af_signup_intro')&
                                  (data['unique_user_id'].isin(control_group))]
len(control_group_conversion['unique_user_id'].unique())

# 구매 전환 lift
psm_data_buy = pd.read_csv(dr.download_dir + '/musicow_psm_data_buy.csv')
treat_group_buy = psm_data_buy.loc[psm_data_buy['treat']==1, 'unique_user_id']
len(treat_group_buy)
treat_group_buy_conversion = data.loc[(data['Event Name'].isin(['af_market_buy', 'af_market_sell']))&
                                  (data['unique_user_id'].isin(treat_group_buy))]
len(treat_group_buy_conversion['unique_user_id'].unique())

control_group_buy = psm_data_buy.loc[psm_data_buy['treat']==0, 'unique_user_id']
len(control_group_buy)
control_group_buy_conversion = data.loc[(data['Event Name'].isin(['af_market_buy', 'af_market_sell']))&
                                  (data['unique_user_id'].isin(control_group_buy))]
len(control_group_buy_conversion['unique_user_id'].unique())



crm_data_pivot = crm_data.pivot_table(index = ['Media Source', 'Campaign', 'Adset', 'Ad'],
                                      columns = 'Event Name',
                                      values=  'Cnt',
                                      aggfunc = 'sum',
                                      margins = True).reset_index()
crm_data_pivot = crm_data_pivot.sort_values(['Media Source','All'], ascending=False)

crm_user = set(crm_data['unique_user_id'])

crm_user_data = data.loc[data['unique_user_id'].isin(crm_user)]
crm_user_data['Install Source'].value_counts()
crm_user_data.loc[(crm_user_data['Media Source']!='Organic')&(crm_user_data['Event Name']=='install'), 'Event Name'] = 'paid_install'

funnel_gen = FunnelDataGenerator(user_array = list(crm_user_data['unique_user_id']),
                                  event_array = list(crm_user_data['Event Name']),
                                  event_time_array= list(crm_user_data['Event Time']),
                                  value_array= list(crm_user_data['Cnt']),
                                  media_array= list(crm_user_data['Media Source']),
                                  kpi_event_name='af_signup_intro',
                                  funnel_period=30*24*3600,
                                  paid_events=['paid_install', 're-engagement', 're-attribution'],
                                  add_end_sequence=True)

funnel_gen.do_work()
funnel_data = funnel_gen.data
funnel_data['session_sequence_string'] = funnel_data['funnel_sequence'].apply(lambda x : ' > '.join(x))

funnel_data_kpi = funnel_data.loc[funnel_data['kpi_achievement']==True]
funnel_data_kpi['first_event'] = funnel_data_kpi['funnel_sequence'].apply(lambda x: x[0])
funnel_data_kpi['first_event'].value_counts()

reengage_user = funnel_data_kpi.loc[funnel_data_kpi['first_event']=='re-engagement', 'user_id'].unique()

crm_user_data_reengage = crm_user_data.loc[crm_user_data['unique_user_id'].isin(reengage_user)]
crm_user_data_reengage = crm_user_data_reengage.loc[crm_user_data_reengage['Event Name']=='re-engagement']
crm_user_data_reengage['ITET'] = crm_user_data_reengage['Event Time'] - crm_user_data_reengage['First Install Time']


## 성향 점수 분석용 RAW 가공
