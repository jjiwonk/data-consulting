from setting import directory as dr
from workers import read_data
from workers import func
import pandas as pd
import pyarrow as pa
import os
import datetime
import numpy as np

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/삼성증권'

# 공모주 정보

ipo_data_list = []
ipo_start_date_list = ['2023-02-21', '2023-03-03', '2023-03-22', '2023-03-28', '2023-05-16', '2023-07-06', '2023-07-11']
name_list = ['삼성스팩8호', '금양그린파워', '지아이이노베이션', '삼성FN리츠', '기가비스', '필에너지', '센서뷰']

for idx, date in enumerate(ipo_start_date_list) :
    data = pd.DataFrame(index=pd.date_range(end=date, periods=8,freq='D'), data = [True]*8, columns = [name_list[idx]])
    ipo_data_list.append(data)

ipo_data = pd.concat(ipo_data_list, axis = 1)
ipo_data = ipo_data.reset_index()
ipo_data = ipo_data.fillna(False)
ipo_data['Event Date'] = ipo_data['index'].dt.date

def ipo_checker(row) :
    result_list = []
    for name in name_list:
        if row[name] == True :
            result_list.append(name)
    return '/'.join(result_list)

ipo_data['계좌 개설 기간'] = ipo_data.apply(ipo_checker, axis=1)

dtypes = {'Event Category' : pa.string(),
          'Event Action' : pa.string(),
          'Event Label': pa.string(),
          'Event Value': pa.string(),
          'Event Value (Original)': pa.string(),
          'Event Datetime': pa.string(),
          'Event Source': pa.string(),
          'Event Name': pa.string(),
          'Is First Event per Device ID': pa.string(),
          'Is First Event per User ID': pa.string(),
          'Is First Event per User ID By Airbridge': pa.string(),
          'Transaction ID': pa.string(),
          'Channel': pa.string(),
          'Campaign': pa.string(),
          'Ad Group': pa.string(),
          'Tracking Link ID': pa.string(),
          'Airbridge Device ID': pa.string(),
          'User ID': pa.string()}

ao_file_list = os.listdir(raw_dir + '/계좌개설')
trade_file_list = os.listdir(raw_dir + '/거래')

raw_data = read_data.pyarrow_csv(dtypes=dtypes,directory=raw_dir + '/계좌개설', file_list = ao_file_list)
raw_data['Event Datetime'] = pd.to_datetime(raw_data['Event Datetime'])
raw_data['Event Date'] = raw_data['Event Datetime'].dt.date

trade_data = read_data.pyarrow_csv(dtypes=dtypes,directory=raw_dir + '/거래', file_list = trade_file_list)
trade_data['Event Datetime'] = pd.to_datetime(trade_data['Event Datetime'])
trade_data['Event Date'] = trade_data['Event Datetime'].dt.date


user_data = pd.concat([raw_data[['User ID', 'Airbridge Device ID']], trade_data[['User ID', 'Airbridge Device ID']]])
user_data = user_data.sort_values(['Airbridge Device ID', 'User ID'])

user_dict = func.user_identifier(user_data, platform_id='Airbridge Device ID', user_id='User ID')
#####

target_date = ipo_data['Event Date']
ao_data = raw_data.loc[raw_data['Event Category'].str.contains('Ao_complete')]

# 첫 계좌 개설만 필터링
ao_data['uuid'] = ao_data['Airbridge Device ID'].apply(lambda x : user_dict.get(x))
ao_data = ao_data.sort_values(['uuid','Event Datetime'])
ao_data = ao_data.loc[(ao_data['Is First Event per Device ID']=='true')&
                      (ao_data['Is First Event per User ID']=='true')&
                      (ao_data['Is First Event per User ID By Airbridge']=='true')]
ao_data = ao_data.drop_duplicates('uuid')

ao_data_merge = ao_data.merge(ipo_data, on = 'Event Date', how = 'inner')
ao_data_merge = ao_data_merge.loc[(ao_data_merge['기가비스']==True) |
                                  (ao_data_merge['삼성FN리츠']==True) |
                                  (ao_data_merge['필에너지']==True) |
                                  (ao_data_merge['센서뷰']==True)]
ao_data_merge = ao_data_merge.rename(columns = {'Event Date' : '계좌 개설 일자'})

ao_data_merge['Cnt'] = 1

#######

trade_target = trade_data.loc[trade_data['Event Label'].isin(['기가비스', '삼성FN리츠', '필에너지', '센서뷰'])]
trade_target = trade_target.loc[trade_target['Event Action']=='현금매도주문확인']
trade_target['uuid'] = trade_target['Airbridge Device ID'].apply(lambda x : user_dict.get(x))
trade_target['Event Label'].value_counts()
trade_target = trade_target.sort_values(['uuid', 'Event Datetime'])
trade_target = trade_target.merge(ao_data_merge[['계좌 개설 일자', '계좌 개설 기간','uuid']], on = 'uuid', how = 'inner')
trade_target['계좌 개설 후 매도기간'] = trade_target['Event Date'] - trade_target['계좌 개설 일자']
trade_target['계좌 개설 후 매도기간'] = trade_target['계좌 개설 후 매도기간'].apply(lambda x : x.days)
trade_target['Cnt'] = 1

detarget_user = list(trade_target.loc[trade_target['계좌 개설 후 매도기간']<0, 'uuid'])

trade_target = trade_target.loc[~trade_target['uuid'].isin(detarget_user)]
ao_data_merge = ao_data_merge.loc[~ao_data_merge['uuid'].isin(detarget_user)]


# 공모주 청약 데이터
sub_data = raw_data.loc[raw_data['Event Action']=='6_청약신청버튼클릭']

sub_data = sub_data.sort_values('Event Datetime')
sub_data['uuid'] = sub_data['Airbridge Device ID'].apply(lambda x : user_dict.get(x))
sub_data['공모주'] = sub_data['Event Label'].apply(lambda x :x.split('_')[0])
sub_data = sub_data.loc[sub_data['공모주'].isin(['기가비스', '삼성FN리츠', '필에너지', '센서뷰'])]
sub_data['Cnt'] = 1
sub_data = ao_data_merge[['계좌 개설 일자', 'uuid', '계좌 개설 기간']].merge(sub_data, on = 'uuid', how = 'inner')

trade_target_sub = trade_target.loc[trade_target['uuid'].isin(sub_data['uuid'])]
trade_target_sub['Cnt'] = 1

# 청약 수
sub_data_pivot = sub_data.pivot_table(index = '계좌 개설 기간', values = 'Cnt', columns='공모주', aggfunc = 'sum')
sub_data_pivot.columns = [col + ' 청약' for col in sub_data_pivot.columns]
sub_data_pivot = sub_data_pivot.fillna(0)

# 청약 유저 수
sub_data_unique = sub_data.drop_duplicates(['uuid', '공모주'])
sub_data_unique_pivot = sub_data_unique.pivot_table(index = '계좌 개설 기간', values = 'Cnt', columns='공모주', aggfunc = 'sum')
sub_data_unique_pivot.columns = [col + ' 청약 (유니크)' for col in sub_data_unique_pivot.columns]
sub_data_unique_pivot = sub_data_unique_pivot.fillna(0)

# 청약 유저 수 (전체 기준)
sub_data_unique_total = sub_data_unique.drop_duplicates(['uuid', '계좌 개설 기간'])
sub_data_unique_total_pivot = pd.DataFrame(sub_data_unique_total['계좌 개설 기간'].value_counts())
sub_data_unique_total_pivot.columns = ['청약 유저 수 (전체)']

# 매도 수
trade_pivot = trade_target_sub.pivot_table(index = '계좌 개설 기간', values = 'Cnt', columns='Event Label', aggfunc = 'sum')
trade_pivot = trade_pivot.fillna(0)
trade_pivot.columns = [col + ' 매도' for col in trade_pivot.columns]

# 매도 유저 수
trade_unique = trade_target_sub.drop_duplicates(['uuid', 'Event Label'])
trade_unique_pivot = trade_unique.loc[trade_unique['uuid'].isin(sub_data['uuid'])]
trade_unique_pivot = trade_unique_pivot.pivot_table(index = '계좌 개설 기간', values = 'Cnt', columns='Event Label', aggfunc = 'sum')
trade_unique_pivot = trade_unique_pivot.fillna(0)
trade_unique_pivot.columns = [col + ' 매도 (유니크)' for col in trade_unique_pivot.columns]

# 매도 유저 수 (전체 기준)
trade_unique_total = trade_unique.drop_duplicates(['uuid', '계좌 개설 기간'])
trade_unique_total_pivot = pd.DataFrame(trade_unique_total['계좌 개설 기간'].value_counts())
trade_unique_total_pivot.columns = ['매도 유저 수 (전체)']


####
ao_data_merge['계좌 개설 수'] = 1
final_data = ao_data_merge.pivot_table(index = '계좌 개설 기간', values = '계좌 개설 수', aggfunc = 'sum')
final_data = pd.concat([final_data, sub_data_pivot,sub_data_unique_pivot, trade_pivot, trade_unique_pivot, sub_data_unique_total_pivot, trade_unique_total_pivot], axis = 1)
final_data = final_data.reset_index()

final_data['Min Date'] = final_data['index'].apply(lambda x : np.min(ipo_data.loc[ipo_data['계좌 개설 기간']==x , 'Event Date']))
final_data['Max Date'] = final_data['index'].apply(lambda x : np.max(ipo_data.loc[ipo_data['계좌 개설 기간']==x , 'Event Date']))
final_data = final_data.sort_values('Min Date')
final_data.to_csv(raw_dir + '/result_data.csv', index=False, encoding = 'utf-8-sig')

# 상장일

trade_target_sub.loc[trade_target_sub['Event Label'] == '삼성FN리츠', '상장일'] = datetime.date(2023,4,10)
trade_target_sub.loc[trade_target_sub['Event Label'] == '기가비스', '상장일'] = datetime.date(2023,5,24)
trade_target_sub.loc[trade_target_sub['Event Label'] == '필에너지', '상장일'] = datetime.date(2023,7,14)
trade_target_sub.loc[trade_target_sub['Event Label'] == '센서뷰', '상장일'] = datetime.date(2023,7,19)

trade_target_sub['상장 후 매도 기간'] = trade_target_sub['Event Date'] - trade_target_sub['상장일']
trade_target_sub['상장 후 매도 기간'] = trade_target_sub['상장 후 매도 기간'].apply(lambda x : x.days)
trade_target_sub_pivot = trade_target_sub.pivot_table(index = '상장 후 매도 기간', values = 'Cnt', columns = 'Event Label',  aggfunc = 'sum').reset_index()
trade_target_sub_pivot.to_csv(raw_dir + '/sell_data.csv', index=False, encoding = 'utf-8-sig')

# 청약 후 매도
trade_unique_copy = trade_unique[['Event Label', 'uuid', 'Event Date']]
trade_unique_copy = trade_unique_copy.rename(columns = {'Event Label' : '공모주', 'Event Date' : '매도일'})

sub_to_trade = sub_data_unique.merge(trade_unique_copy, on = ['uuid', '공모주'], how = 'left')
sub_to_trade.loc[sub_to_trade['매도일'].notnull(), '매도 수'] = 1
sub_to_trade_pivot = sub_to_trade.pivot_table(index = '공모주', values = ['Cnt', '매도 수'], aggfunc = 'sum').reset_index()