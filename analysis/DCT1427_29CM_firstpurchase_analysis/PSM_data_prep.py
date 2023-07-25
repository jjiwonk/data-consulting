import datetime
import pandas as pd
import pyarrow.csv as pacsv
import pyarrow as pa
from setting import directory as dr
from workers import read_data
import os

class directory :
    paid_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/코오롱몰_2022/1. 리포트/#자동화/1차 RAW//appsflyer_prism'
    organic_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/코오롱몰_2022/1. 리포트/#자동화/1차 RAW//appsflyer_organic'
    raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1482'

class columns :

    usecols = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_value': pa.string(),
        'event_revenue' : pa.float64(),
        'campaign': pa.string(),
        'adset': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source': pa.string(),
        'platform': pa.string(),
        'language': pa.string()}

    organic_usecols = {
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Event Value': pa.string(),
        'Event Revenue': pa.float64(),
        'Campaign': pa.string(),
        'Adset': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Media Source': pa.string(),
        'Platform': pa.string(),
        'Language': pa.string()}

    network_camapign = ['cauly_ncpi_install','cauly_re_purchase','criteo_app_install','google_ac_install_IOS','google_ac_install_IOS-video','google_aca_install','google_ace_purchase','google_ace_purchase-dp','google_sdc_purchase','LF_Kolonmall_App_Android','LF_Kolonmall_App_iOS','rtb_feed_purchase']

def data_read():

    paid_file_dir = directory.paid_dir
    paid_file_list = os.listdir(paid_file_dir)
    paid_df = read_data.pyarrow_csv(dtypes=columns.usecols, directory= directory.paid_dir, file_list=paid_file_list)
    paid_df['organic'] = 0

    organic_file_dir = directory.organic_dir
    organic_file_list = os.listdir(organic_file_dir)
    organic_df = read_data.pyarrow_csv(dtypes=columns.usecols, directory=directory.organic_dir, file_list=organic_file_list)
    organic_df['organic'] = 1

    data = pd.concat([paid_df,organic_df])
    data['event_time'] = pd.to_datetime(data['event_time'])
    data = data.sort_values('event_time')

    return data

def data_prep():

    data = data_read()
    treat_data = data.loc[data['campaign'].isin(columns.network_camapign)]
    treat_data = treat_data[['appsflyer_id']].drop_duplicates()
    treat_data['treat'] = 1

    purchase_data = data.loc[data['event_name'].isin(['af_purchase'])]
    purchase_data['event_revenue'] = purchase_data['event_revenue'].fillna(0)

    event_data = data.pivot_table(index='appsflyer_id',columns='event_name',values='organic',aggfunc='count').reset_index().fillna(0)
    event_data.loc[event_data['af_complete_registration'] >= 1, 'af_complete_registration'] = 1
    event_data = event_data[['appsflyer_id','af_complete_registration','af_purchase','install']]

    revenue_data = purchase_data.pivot_table(index='appsflyer_id',columns='event_name',values='event_revenue',aggfunc='sum').reset_index().fillna(0)
    revenue_data = revenue_data.rename(columns = {'af_purchase': 'revenue'})

    os_data = data[['appsflyer_id','platform','language']]
    os_data = os_data.drop_duplicates(subset='appsflyer_id',keep ='first')
    os_data.loc[~os_data['language'].isin(['한국어', 'ko-KR']), 'language'] = 0
    os_data.loc[os_data['language'].isin(['한국어', 'ko-KR']), 'language'] = 1

    #데이터 머징하기
    result_data = pd.merge(os_data, event_data, on= 'appsflyer_id', how = 'left')
    result_data = pd.merge(result_data, revenue_data,on= 'appsflyer_id', how = 'left' ).fillna(0)
    result_data = pd.merge(result_data, treat_data,on= 'appsflyer_id', how = 'left' ).fillna(0)

    result_data.to_csv(dr.download_dir + '/성향점수분석raw.csv', index=False, encoding='utf-8-sig')

    return result_data

# 매칭된 data 를 기준으로 구매 확률 구해보기

def psm_data():

    psm_data = pd.read_csv(directory.raw_dir +'/psm_data.csv')

    treat_group = psm_data.loc[psm_data['treat'] == 1]['appsflyer_id'].to_list()
    control_group = psm_data.loc[psm_data['treat'] == 0]['appsflyer_id'].to_list()

    data = data_read()
    treat_data = data.loc[data['appsflyer_id'].isin(treat_group)]
    treat_data['treat'] = True
    control_data = data.loc[data['appsflyer_id'].isin(control_group)]
    control_data['treat'] = False

    total_data = pd.concat([treat_data,control_data])
    total_data = total_data.sort_values('event_time')
    total_data = total_data[['install_time', 'event_time', 'event_name', 'event_revenue', 'campaign','appsflyer_id', 'media_source', 'platform', 'treat']]
    total_data['event_revenue'] = total_data['event_revenue'].fillna(0)

    total_data.index = range(len(total_data))

    total_data.loc[total_data['campaign'] == '','campaign'] = total_data['campaign'].apply(lambda x: x.replace('', 'organic'))
    total_data.loc[total_data['media_source'] == '','media_source'] = total_data['media_source'].apply(lambda x: x.replace('', 'organic'))

    return total_data

# 첫 install 이 후 첫구매율 확인하기

def first_purchase():

    piv = total_data.pivot_table(index='event_name',columns='treat',values='appsflyer_id',aggfunc='count').reset_index()

