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
          'Event Datetime': pa.string(),
          'Event Name': pa.string(),
          'Is First Event per Device ID': pa.string(),
          'Is First Event per User ID': pa.string(),
          'Is First Event per User ID By Airbridge': pa.string(),
          'Airbridge Device ID': pa.string(),
          'User ID': pa.string()}

ao_file_list = os.listdir(raw_dir + '/계좌개설')
trade_file_list = os.listdir(raw_dir + '/거래')

# 계좌개설, 공모주 청약 데이터 불러오기
raw_data = read_data.pyarrow_csv(dtypes=dtypes,directory=raw_dir + '/계좌개설', file_list = ao_file_list)
raw_data['Event Datetime'] = pd.to_datetime(raw_data['Event Datetime'])
raw_data['Event Date'] = raw_data['Event Datetime'].dt.date

# 거래 관련 데이터 불러오기
trade_data = read_data.pyarrow_csv(dtypes=dtypes,directory=raw_dir + '/거래', file_list = trade_file_list)
trade_data = trade_data.drop_duplicates()
trade_data['Event Datetime'] = pd.to_datetime(trade_data['Event Datetime'])
trade_data['Event Date'] = trade_data['Event Datetime'].dt.date

# 유저 정보 확인
user_data = pd.concat([raw_data[['User ID', 'Airbridge Device ID']].drop_duplicates(), trade_data[['User ID', 'Airbridge Device ID']].drop_duplicates()])
user_data = user_data.sort_values(['Airbridge Device ID', 'User ID'])

user_dict = func.user_identifier(user_data, platform_id='Airbridge Device ID', user_id='User ID')
#####

target_date = ipo_data['Event Date']
ao_data = raw_data.loc[raw_data['Event Category'].str.contains('Ao_complete')]

# 첫 계좌 개설만 필터링
ao_data = ao_data.loc[(ao_data['Is First Event per Device ID']=='true')&
                      (ao_data['Is First Event per User ID']=='true')&
                      (ao_data['Is First Event per User ID By Airbridge']=='true')]
ao_data['uuid'] = ao_data['Airbridge Device ID'].apply(lambda x : user_dict.get(x))
ao_data = ao_data.sort_values(['uuid','Event Datetime'])
ao_data = ao_data.drop_duplicates('uuid', keep = 'first')

ao_data_merge = ao_data.merge(ipo_data, on = 'Event Date', how = 'inner')
ao_data_merge = ao_data_merge.loc[(ao_data_merge['기가비스']==True) |
                                  (ao_data_merge['삼성FN리츠']==True) |
                                  (ao_data_merge['필에너지']==True) |
                                  (ao_data_merge['센서뷰']==True)]
ao_data_merge = ao_data_merge.rename(columns = {'Event Date' : '계좌 개설 일자'})

ao_data_merge['Cnt'] = 1

ao_info = ao_data_merge[['Event Name', '계좌 개설 일자', 'uuid', '계좌 개설 기간']]
ao_info.rename(columns = {'Event Name' : '계좌 유형'}, inplace=True)

#######
trade_data['uuid'] = trade_data['Airbridge Device ID'].apply(lambda x : user_dict.get(x))

trade_target = trade_data.loc[trade_data['uuid'].isin(ao_data_merge['uuid'])]

# 공모주 매도 데이터
trade_target_ipo = trade_target.loc[trade_target['Event Label'].isin(['기가비스', '삼성FN리츠', '필에너지', '센서뷰'])]
trade_target_ipo = trade_target_ipo.loc[trade_target_ipo['Event Action']=='현금매도주문확인']
trade_target_ipo = trade_target_ipo.sort_values(['uuid', 'Event Datetime'])
trade_target_ipo.loc[trade_target_ipo['Event Label'] == '삼성FN리츠', '상장일'] = datetime.date(2023,4,10)
trade_target_ipo.loc[trade_target_ipo['Event Label'] == '기가비스', '상장일'] = datetime.date(2023,5,24)
trade_target_ipo.loc[trade_target_ipo['Event Label'] == '필에너지', '상장일'] = datetime.date(2023,7,14)
trade_target_ipo.loc[trade_target_ipo['Event Label'] == '센서뷰', '상장일'] = datetime.date(2023,7,19)
trade_target_ipo['상장 후 매도 기간'] = trade_target_ipo['Event Date'] - trade_target_ipo['상장일']
trade_target_ipo['상장 후 매도 기간'] = trade_target_ipo['상장 후 매도 기간'].apply(lambda x : x.days)
trade_target_ipo = trade_target_ipo.loc[trade_target_ipo['상장 후 매도 기간']<=6]

trade_target_ipo_merge = trade_target_ipo.merge(ao_info, on ='uuid', how = 'left')
trade_target_ipo_merge = trade_target_ipo_merge.loc[trade_target_ipo_merge['Event Date']>trade_target_ipo_merge['계좌 개설 일자']]
trade_target_ipo_merge['계좌 개설 to 청약'] = trade_target_ipo_merge.apply(lambda x : True if x['Event Label'] in x['계좌 개설 기간'] else False, axis = 1)
trade_target_ipo_merge = trade_target_ipo_merge.loc[trade_target_ipo_merge['계좌 개설 to 청약']==True]

# 첫 계좌 개설 유저의 첫 공모주 매도
trade_target_ipo_merge_unique = trade_target_ipo_merge.drop_duplicates('uuid', keep = 'first')
trade_target_ipo_merge_unique.rename(columns = {'Event Datetime' : '첫 매도일', 'Event Label' : '매도 공모주'}, inplace = True)


# 체리피커 유저 확인
focus_user_trade_data = trade_target.merge(trade_target_ipo_merge_unique[['첫 매도일', 'uuid']], on = 'uuid', how = 'inner')
focus_user_trade_data['매도 후 주문 활동'] = (focus_user_trade_data['Event Datetime'] - focus_user_trade_data['첫 매도일'])
focus_user_trade_data['매도 후 주문 활동'] = focus_user_trade_data['매도 후 주문 활동'].apply(lambda x : x.days)
focus_user_trade_data = focus_user_trade_data.loc[(focus_user_trade_data['매도 후 주문 활동']<30)&(focus_user_trade_data['매도 후 주문 활동']>=1)]
focus_user_trade_data = focus_user_trade_data.loc[~focus_user_trade_data['Event Label'].isin(['삼성FN리츠', '기가비스', '센서뷰', '필에너지'])]
focus_user_trade_data = focus_user_trade_data[['Event Action', 'Event Label', 'Event Datetime', 'Event Name', 'uuid']]
focus_user_trade_data = focus_user_trade_data.drop_duplicates(['uuid'], keep='first')


result_data = trade_target_ipo_merge_unique[['매도 공모주', '첫 매도일', 'Event Date', 'uuid', '상장일', '상장 후 매도 기간', '계좌 유형', '계좌 개설 일자', '계좌 개설 기간']]\
    .merge(focus_user_trade_data, on ='uuid', how='left')
result_data['체리피킹'] = result_data['Event Name'].apply(lambda x : True if pd.isnull(x) else False)
result_data['계좌 개설 유저 수'] = 1

result_pivot = result_data.pivot_table(index = '계좌 개설 기간', columns = '체리피킹', values = '계좌 개설 유저 수', aggfunc = 'sum').reset_index()


# 공모주 청약 데이터
# sub_data = raw_data.loc[raw_data['Event Action']=='6_청약신청버튼클릭']
#
# sub_data = sub_data.sort_values('Event Datetime')
# sub_data['uuid'] = sub_data['Airbridge Device ID'].apply(lambda x : user_dict.get(x))
# sub_data['공모주'] = sub_data['Event Label'].apply(lambda x :x.split('_')[0])
# sub_data = sub_data.loc[sub_data['공모주'].isin(['기가비스', '삼성FN리츠', '필에너지', '센서뷰'])]
# sub_data['Cnt'] = 1
# sub_data = ao_data_merge[['계좌 개설 일자', 'uuid', '계좌 개설 기간']].merge(sub_data, on = 'uuid', how = 'inner')


def result_pivot():
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

