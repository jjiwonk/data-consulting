import datetime
import pandas as pd

import analysis.DCT1427_29CM_firstpurchase_analysis.PSM_data_prep as dprep
from workers.func import FunnelDataGenerator

# EDA Depth1. 전환율 비교
def first_purchase_check():

    data = dprep.data_prep()
    data = data.drop_duplicates()
    data.index = range(len(data))

    psm_data = pd.read_csv(dprep.directory.result_dir + '/psm_data.csv')

    psm_group = psm_data['appsflyer_id'].to_list()
    treat_group = psm_data.loc[psm_data['treat'] == 1]['appsflyer_id'].to_list()
    control_group = psm_data.loc[psm_data['treat'] == 0]['appsflyer_id'].to_list()

    data = data.loc[data['appsflyer_id'].isin(psm_group)]
    data['treat'] = '-'

    data.loc[data['appsflyer_id'].isin(treat_group),'treat'] = True
    data.loc[data['appsflyer_id'].isin(control_group),'treat'] = False

    data.loc[data['campaign'] == '', 'campaign'] = 'restricted'

    return data

def campaign_data():

    raw_data = first_purchase_check()

    restricted_install_user = raw_data.loc[raw_data['campaign'].isin(['facebook_install','facebook_etc'])]['appsflyer_id'].tolist()
    raw_data = raw_data.loc[~raw_data['appsflyer_id'].isin(restricted_install_user)]

    data = first_purchase_check()

    data['is_paid'] = False
    data.loc[data['media_source'] =='rtbhouse_int://','media_source'] = 'rtbhouse_int'
    data.loc[data['media_source'].isin(['29cm_mowebtoapp','criteonew_int','Facebook Ads','fbig','googleadwords_int','kakao_int','naver_gfa','naverbsa','naversa','restricted','rtbhouse_int']),'is_paid'] = True

    # 최초 유입 매체 라벨링
    install_time = data.loc[data['event_name'].isin(['install', 're-engagement', 're-attribution'])]
    install_time = install_time.sort_values('install_time')
    install_time = install_time.drop_duplicates('appsflyer_id', keep='first')
    install_time['install_campaign_time'] = install_time['install_time']
    install_time['install_campaign'] = install_time['campaign']
    install_time['install_media'] = install_time['media_source']
    install_time['install_is_paid'] = False
    install_time.loc[install_time['media_source'].isin(['29cm_mowebtoapp','criteonew_int','Facebook Ads','fbig','googleadwords_int','kakao_int','naver_gfa','naverbsa','naversa','restricted','rtbhouse_int']),'install_is_paid'] = True
    install_time = install_time[['appsflyer_id', 'install_campaign_time', 'install_campaign','install_media','install_is_paid','treat']]

    data = pd.merge(data, install_time, on=['appsflyer_id','treat'], how='left').fillna('-')
    data['cnt'] = 1

    # 구매 라벨링
    purchase_data = data.loc[data['event_name'] == 'af_purchase']
    purchase_data = purchase_data.pivot_table(index='appsflyer_id',columns='event_name',values=['event_revenue','cnt'],aggfunc='sum').reset_index()
    purchase_data.columns = ['appsflyer_id','total_purchase','total_revenue']

    # 첫구매 라벨링
    new_purchase_data = data.loc[data['event_name'] == 'new_purchaser']
    new_purchase_data = new_purchase_data.drop_duplicates(subset = 'appsflyer_id',keep= 'first')
    new_purchase_data['newpurchase_time'] = new_purchase_data['event_time']
    new_purchase_data['newpurchase_time'] = pd.to_datetime(new_purchase_data['newpurchase_time'])

    new_purchase_data = new_purchase_data.loc[new_purchase_data['newpurchase_time'] > new_purchase_data['install_campaign_time']]

    new_purchase_data['newpurchase_campaign'] = new_purchase_data['campaign']
    new_purchase_data['newpurchase_media'] = new_purchase_data['media_source']
    new_purchase_data['newpurchase_is_paid'] = False
    new_purchase_data['newpurchase_revenue'] = new_purchase_data['event_revenue']

    new_purchase_data.loc[new_purchase_data['media_source'].isin(['29cm_mowebtoapp', 'criteonew_int', 'Facebook Ads', 'fbig', 'googleadwords_int', 'kakao_int', 'naver_gfa','naverbsa', 'naversa', 'restricted', 'rtbhouse_int']), 'newpurchase_is_paid'] = True
    new_purchase_data = new_purchase_data[['appsflyer_id', 'newpurchase_time', 'newpurchase_campaign', 'newpurchase_media', 'newpurchase_is_paid','newpurchase_revenue']]

    np_id = new_purchase_data['appsflyer_id'].tolist()

    # 첫구매 캠페인 여부 확인
    first_campaign = data.loc[data['campaign'].isin(dprep.columns.first_purchase_campaign)]
    first_campaign = pd.merge(first_campaign,new_purchase_data,on='appsflyer_id',how='left')

    first_campaign['event_time'] = pd.to_datetime(first_campaign['event_time'])
    first_campaign['install_campaign_time'] = pd.to_datetime(first_campaign['install_campaign_time'])
    first_campaign = first_campaign.loc[first_campaign['event_time'] >= first_campaign['install_campaign_time']]

    first_campaign_nnp = first_campaign.loc[~first_campaign['appsflyer_id'].isin(np_id)]
    first_campaign = first_campaign.loc[first_campaign['appsflyer_id'].isin(np_id)]
    first_campaign['newpurchase_time'] = pd.to_datetime(first_campaign['newpurchase_time'])
    first_campaign = first_campaign.loc[first_campaign['event_time'] <= first_campaign['newpurchase_time']]

    first_campaign = pd.concat([first_campaign,first_campaign_nnp])

    first_campaign['np_camapaign_touch'] = True
    first_campaign = first_campaign.drop_duplicates('appsflyer_id')

    first_campaign = first_campaign[['appsflyer_id','np_camapaign_touch']]

    # 데이터 머징
    data = pd.merge(install_time, purchase_data, on='appsflyer_id', how='left').fillna(0)
    data = pd.merge(data, new_purchase_data, on='appsflyer_id', how='left').fillna('-')
    data['new_purchaser'] = 1
    data.loc[data['newpurchase_is_paid'] == '-', 'new_purchaser'] = 0
    data.loc[data['newpurchase_revenue'] == '-', 'newpurchase_revenue'] = 0
    data['cnt'] = 1
    data = data.loc[data['install_media'] != 'restricted']

    merge_data = pd.merge(data, first_campaign, on='appsflyer_id', how='left').fillna(False)
    merge_data['np_campaign_index'] = False
    merge_data.loc[merge_data['newpurchase_campaign'].isin(dprep.columns.first_purchase_campaign),'np_campaign_index'] = True

    merge_data.to_csv(dprep.directory.result_dir +'/np_data.csv',index=False, encoding= 'utf-8-sig')

    return merge_data

def get_funnel():

    raw_data = first_purchase_check()

    raw_data = raw_data.sort_values(['appsflyer_id','event_time','install_time'])
    raw_data = raw_data.loc[raw_data['event_name'].isin(['install','re-engagement','re-attribution','new_purchaser'])]

    raw_data = raw_data.drop_duplicates(subset=['event_time','event_name','appsflyer_id'], keep='first')
    np_data = raw_data.loc[raw_data['event_name'] == 'new_purchaser']
    np_data = np_data.drop_duplicates(subset='appsflyer_id', keep='first')
    np_data['new_purchaser_time'] = np_data['event_time']
    np_data['new_purchaser_campaign'] =  np_data['campaign']
    np_data = np_data[['appsflyer_id','new_purchaser_time','new_purchaser_campaign']]

    merge_data = pd.merge(raw_data,np_data,on='appsflyer_id',how='left')
    merge_data['new_purchaser_campaign'] =  merge_data['new_purchaser_campaign'].fillna('-')
    merge_data['new_purchaser_time'] = merge_data['new_purchaser_time'].fillna(datetime.datetime(year=2023,month=8,day=1))

    merge_data = merge_data.loc[merge_data['event_time'] <= merge_data['new_purchaser_time']]

    # 첫구매 캠페인 터치 여부
    first_campaign = merge_data.loc[merge_data['campaign'].isin(dprep.columns.first_purchase_campaign)]
    first_campaign['np_camapaign_touch'] = True
    first_campaign = first_campaign.drop_duplicates('appsflyer_id')

    first_campaign = first_campaign[['appsflyer_id', 'np_camapaign_touch']]

    merge_data = pd.merge(merge_data,first_campaign,on='appsflyer_id',how='left').fillna(False)
    merge_data['new_purchaser'] = 1
    merge_data.loc[merge_data['new_purchaser_campaign'] == '-','new_purchaser'] = 0

    merge_data['np_campaign_index'] = False
    merge_data.loc[merge_data['new_purchaser_campaign'].isin(dprep.columns.first_purchase_campaign), 'np_campaign_index'] = True

    merge_data.to_csv(dprep.directory.result_dir +'/np_dd.csv',index=False, encoding= 'utf-8-sig')

    return merge_data