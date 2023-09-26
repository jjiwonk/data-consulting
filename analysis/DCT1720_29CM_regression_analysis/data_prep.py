import datetime
import pandas as pd
from setting import directory as dr
from spreadsheet import spreadsheet

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1720'
raw_data = pd.read_csv(raw_dir +'/RD.csv')

def data_prep(raw_data , depth):

    # 캠페인 라벨링
    doc = spreadsheet.spread_document_read('https://docs.google.com/spreadsheets/d/1gOlsd5HtkdCYkWc9ksCgUByYvLYrPNiSjBCwxYpWH8A/edit#gid=113757096')
    campaign_list = spreadsheet.spread_sheet(doc, '캠페인명 라벨링', 1, 1).reset_index(drop=True).drop_duplicates('campaign')

    raw_data.loc[raw_data['media_source'] == 'restricted','campaign'] = 'restricted'
    raw_data = raw_data.loc[raw_data['attributed_touch_type']!= 'impression']
    prep_data = pd.merge(raw_data, campaign_list, on='campaign',how='left')

    input_data = prep_data.loc[prep_data['event_name'].isin(['install','re-attribution','re-engagement'])]
    output_data = prep_data.loc[prep_data['event_name'] == 'new_purchaser']

    # 인스톨 캠페인은 인스톨만 남기기
    input_data = input_data.dropna(subset = 'label')
    input_data = input_data.loc[(input_data['label']!= '인스톨')|((input_data['label']== '인스톨') & (input_data['event_name'] == 'install'))]

    # 최초 인스톨 or 앱오픈 캠페인으로 기여 부여해주기
    input_data = input_data.sort_values(['appsflyer_id','event_time'])
    input_data = input_data.drop_duplicates('appsflyer_id',keep= 'first')
    input_data = input_data[['install_time','media_source', 'campaign', 'appsflyer_id','media','label']]

    # 첫구매 여부 확인하기
    output_data = output_data.sort_values(['appsflyer_id','event_time','is_retargeting'])
    output_data = output_data.drop_duplicates('appsflyer_id',keep = 'last')
    output_data = output_data[['event_time','appsflyer_id']].rename(columns = {'event_time' : 'first_purchase_time'})
    output_data['new_purchase'] = True

    # 데이터 머징하기
    merge_data = pd.merge(input_data,output_data, on = 'appsflyer_id',how='left')
    merge_data['first_purchase_time'] = merge_data['first_purchase_time'].fillna(merge_data['install_time'])
    merge_data['new_purchase'] = merge_data['new_purchase'].fillna(False)

    # 1d 체크
    merge_data[['first_purchase_time','install_time']] = merge_data[['first_purchase_time','install_time']].apply(pd.to_datetime)
    merge_data['ITET'] = merge_data['first_purchase_time'] - merge_data['install_time']

    merge_data = merge_data.loc[merge_data['ITET'] < datetime.timedelta(days=1)]
    merge_data['install'] = True

    # 날짜 , 시간 별 피벗 테이블 생성
    merge_data['install_date'] = merge_data['install_time'].dt.date
    merge_data['install_date_time'] = merge_data['install_time'].dt.hour

    pivot_ins =  merge_data.pivot_table(index = depth, columns = 'install', values = 'appsflyer_id',aggfunc= 'count' ).reset_index().rename(columns={True:'install'})
    pivot_np = merge_data.pivot_table(index= depth, columns='new_purchase', values='appsflyer_id', aggfunc='count').reset_index()

    pivot_data = pd.merge(pivot_ins, pivot_np, on = depth , how = 'left').rename(columns={True:'new_purchase'})
    pivot_data = pivot_data.drop(columns=False)

    return pivot_data

data = data_prep(raw_data, ['install_date','install_date_time','label', 'media','campaign']).fillna(0)
data.to_csv(raw_dir + '/prep_data.csv',index= False, encoding = 'utf-8-sig')




