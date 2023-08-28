from setting import directory as dr
import pandas as pd
import os
import numpy as np
from workers.func import user_identifier
from workers.func import FunnelDataGenerator
from workers.func import SankeyModeling
import datetime

raw_dir = dr.download_dir + '/뮤직카우RD'
raw_files = os.listdir(raw_dir)

df_list = []
for file in raw_files :
    df = pd.read_csv(raw_dir + '/' + file)
    df_list.append(df)

data = pd.concat(df_list)
data = data.sort_values(['Event Time', 'Install Time'])
data['Media Source'] = data['Media Source'].fillna('Organic')
data['Media Source'].value_counts()
data['Customer User ID'] = data['Customer User ID'].fillna('')
data['Customer User ID'] = data['Customer User ID'].astype('str')

user_dict = user_identifier(data, 'AppsFlyer ID', 'Customer User ID')
data['unique_user_id'] = data['AppsFlyer ID'].apply(lambda x : user_dict.get(x))

data = data[['unique_user_id'] + list(data.columns[:-1])]

# 중복 제거를 위한 우선순위 선정 : 리인게이지먼트 > UA > 오가닉 순으로 정렬
data['dedup_order'] = data.apply(lambda x : 3 if x['Media Source'] == 'Organic' else 1 if x['Is Retargeting'] == True else 2, axis= 1)
data = data.sort_values(['unique_user_id', 'Event Time', 'Install Time', 'dedup_order'])

data[['Attributed Touch Time',
      'Install Time',
      'Event Time']] = data[['Attributed Touch Time', 'Install Time', 'Event Time']].apply(lambda x : pd.to_datetime(x))


# Install 유저에 대해서 리타게팅 토글 켜야할지?
install_user = set(data.loc[data['Event Name']=='install', 'unique_user_id'])
install_user_data = data.loc[data['unique_user_id'].isin(install_user)]

first_install_time = install_user_data.loc[install_user_data['Event Name']=='install'].drop_duplicates('unique_user_id', keep = 'first')
first_install_time = first_install_time[['Install Time', 'unique_user_id', 'Media Source']]
first_install_time = first_install_time.rename(columns = {'Install Time' : 'First Install Time', 'Media Source' : 'Install Source'})

install_user_data = install_user_data.merge(first_install_time, on = 'unique_user_id', how = 'left')
install_user_data = install_user_data.loc[install_user_data['First Install Time'] <= install_user_data['Event Time']]
install_user_data = install_user_data.drop(
    install_user_data.loc[(install_user_data['Event Name']=='install') &
                          (install_user_data['Event Time']!=install_user_data['First Install Time'])].index)
install_user_data = install_user_data.drop_duplicates(['Event Time', 'Event Name', 'unique_user_id'], keep = 'first')
install_user_data['Event Name'].value_counts()

install_user_data.index= range(len(install_user_data))

# 리포팅 특성 상 중복 데이터 쌓이는 부분 검증
# duplicate_index = install_user_data.drop_duplicates(['Event Time', 'Event Name', 'unique_user_id'], keep = 'first')
# duplicate_data = install_user_data.loc[~install_user_data.index.isin(duplicate_index.index)]
#
# event_index = set(duplicate_data.index) | set(duplicate_data.index -1)
# duplicate_event_data = install_user_data.loc[install_user_data.index.isin(event_index)]
install_user_data['Cnt'] = 1
install_user_pivot = install_user_data.pivot_table(index = 'Event Name', columns = 'dedup_order', values = 'Cnt', aggfunc = 'sum').reset_index()

install_user_data['ITET'] = install_user_data['Event Time'] - install_user_data['First Install Time']
install_user_data['ITET'] = install_user_data['ITET'].apply(lambda x: x.days)
install_user_data['ITET Group'] = install_user_data['ITET'].apply(lambda x : '0d' if x == 0 else '7d' if x < 7 else '30d' if x <30 else '30d+' )


sign_up_event = install_user_data.loc[install_user_data['Event Name']=='af_signup_success']

print(len(install_user_data['unique_user_id'].unique()))
print(len(sign_up_event['unique_user_id'].unique()))
print(len(install_user_data['unique_user_id'].unique()) - len(sign_up_event['unique_user_id'].unique()))


sign_up_event_pivot = sign_up_event.pivot_table(index = 'Install Source',
                                               columns = 'ITET Group',
                                               values = 'Cnt',
                                               aggfunc = 'sum',
                                                 margins = True)
sign_up_event_pivot = sign_up_event_pivot[['0d', '7d', '30d', '30d+', 'All']]
sign_up_event_pivot = sign_up_event_pivot.reset_index()
sign_up_event_pivot = sign_up_event_pivot.sort_values('All', ascending=False)

## 이탈 퍼널 찾아보기
install_user_data['Event Name'].value_counts()

funnel_gen = FunnelDataGenerator(user_array = list(install_user_data['unique_user_id']),
                                  event_array = list(install_user_data['Event Name']),
                                  event_time_array= list(install_user_data['Event Time']),
                                  value_array= list(install_user_data['Cnt']),
                                  media_array= list(install_user_data['Media Source']),
                                  kpi_event_name='af_market_buy',
                                  funnel_period=30*24*3600,
                                  paid_events=['install'],
                                  add_end_sequence=True)

funnel_gen.do_work()
funnel_data = funnel_gen.data
funnel_data['session_sequence_string'] = funnel_data['funnel_sequence'].apply(lambda x : ' > '.join(x))

sankey = SankeyModeling(funnel_data,
                        funnel_list=['install', 'af_signup_intro', 'af_signup_success', 'af_market_buy'],
                        end_sequence='funnel_end',
                        sequence_column_name='session_sequence_string',
                        destination=dr.download_dir,
                        file_name='musicow_sankey.xlsx')
sankey.do_work()

sankey_data = sankey.data
sankey_data['filter'] = sankey_data['funnel_sequence'].apply(lambda x : True if x[0] == 'install' else False)
sankey_data['Install Source'] = sankey_data['media_sequence'].apply(lambda x : x[0])
sankey.data = sankey_data.loc[sankey_data['filter']==True]

sankey.sankey_to_excel()