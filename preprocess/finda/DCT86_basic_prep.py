import os
import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import openpyxl

raw_dir = dr.download_dir + '/(매드잇x핀다) RE 데이터 중 UA 볼륨 영향도 측정 관련 데이터 분석_0819'

def get_raw_df(raw_dir):
    dtypes = {
        'Attributed Touch Type': pa.string(),
        'Attributed Touch Time': pa.string(),
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Media Source': pa.string(),
        'Campaign': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Is Retargeting': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    return raw_df

### paid data 전처리 ###
paid_raw_dir = raw_dir + '/paid'
paid_re_raw_df = get_raw_df(paid_raw_dir + '/re')
paid_ua_raw_df = get_raw_df(paid_raw_dir + '/ua')
paid_raw_df = pd.concat([paid_re_raw_df,paid_ua_raw_df], sort=False, ignore_index=True)
paid_raw_df = paid_raw_df.rename(columns={'Attributed Touch Type':'attributed_touch_type', 'Attributed Touch Time':'attributed_touch_time',
                                          'Install Time':'install_time', 'Event Time':'event_time', 'Event Name':'event_name',
                                          'Media Source':'media_source', 'Campaign':'campaign', 'AppsFlyer ID':'appsflyer_id', 'Is Retargeting':'is_retargeting'})
def campaign_type(data):
    if (data == 'TRUE')|(data == 'true'):
        return 're'
    else:
        return 'ua'
paid_raw_df['campaign_type'] = paid_raw_df['is_retargeting'].map(campaign_type)
# paid 데이터 로드

paid_raw_df[['install_time','attributed_touch_time','event_time']] = paid_raw_df[['install_time','attributed_touch_time','event_time']].apply(pd.to_datetime)
# 날짜타입 datetime형으로 변경

paid_raw_df['ctit'] = paid_raw_df['install_time'] - paid_raw_df['attributed_touch_time']
paid_raw_df['itet'] = paid_raw_df['event_time'] - paid_raw_df['install_time']
# ctit, itet 계산

def time_cleansing(df):
    if df['media_source']=='Apple Search Ads':
        return df['install_time']
    else:
        return df['attributed_touch_time']
paid_raw_df['attributed_touch_time'] = paid_raw_df.apply(time_cleansing, axis=1)
# Apple Search Ads 데이터 클리닝
# 30초 소요

paid_raw_df.loc[paid_raw_df['media_source']!='moloco_int'] = paid_raw_df.loc[paid_raw_df['media_source']!='moloco_int'].apply(lambda x: x.replace('MobidaysAgency_','Madit_'))
paid_raw_df.loc[paid_raw_df['media_source']!='moloco_int'] = paid_raw_df.loc[paid_raw_df['media_source']!='moloco_int'].apply(lambda x: x.replace('Mobi_','Madit_'))
def switch(df):
    campaign = {'madit_ka-friend_loan_br_mo_reach_total':'Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL',
                'MobidaysAgency_KA-BIZ_LOAN_NU_AEO-CONT_220307': 'Madit_KA-BIZ_LOAN_NU_iOS_AEO-CONT_220307',
                'alwayson_loan': 'Mobi_Finda_AOS_Viewedlahome',
                'Madit_CAULY_LOAN_NU_AOS_REWARD-CPE': 'Madit_CAULY_LOAN_NU_AOS_NCPI_220622',
                'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AUTO_220712': 'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AM_220712',
                '468706157': 'Madit_AppleSearchAds_0826',
                '500973516': 'Madit_AppleSearchAds_Comp_1203',
                '492604880': 'Madit_AppleSearchAds_Brand_1104',
                '1078291510': 'Madit_ASA_LOAN_NU_iOS_BAU-MAIA_220629',
                '1071110978': 'Madit_ASA_LOAN_NU_iOS_SEARCHMATCH_220617'}.get(df['campaign'], df['campaign'])
    return campaign
paid_raw_df['campaign'] = paid_raw_df.apply(switch, axis=1)
def edit_campaign(df):
    if (df['media_source']=='kakao')&(df['campaign']!='talk'):
        return ''
    elif (df['campaign_type']=='ua')&(df['campaign'] in ['992682','992393']):
        return ''
    elif (df['campaign_type']=='re')&(df['media_source']=='cauly_int'):
        return 'Madit_CAULY_LOAN_RT_AOS_CPC_220714'
    elif (df['campaign_type']=='ua')&(df['media_source']=='tnk_int'):
        return 'Madit_TNK_LOAN_NU_AOS_REWARD-CPA_220715'
    else:
        return df['campaign']
paid_raw_df['campaign'] = paid_raw_df.apply(edit_campaign, axis=1)
# 캠페인명 변경 및 삭제
# 1분30초 소요

wb = openpyxl.load_workbook(raw_dir + '/(핀다) 가공용 캠페인리스트_220819.xlsx')
ws = wb['캠페인 리스트']
data = ws.values
columns = next(data)[0:]
campaign_list = pd.DataFrame(data, columns=columns)
campaign_unique_list = pd.DataFrame({'campaign_list':campaign_list.캠페인.sort_values().unique()})
# paid_raw_campaign_list = paid_raw_df[['campaign','media_source']].drop_duplicates(keep='first')
# campaign_compare = pd.merge(campaign_unique_list, paid_raw_campaign_list, how='outer', left_on='campaign_list', right_on='campaign')
# campaign_compare.to_csv(dr.download_dir + '/campaign_check.csv', encoding='utf-8-sig')

def organic_check(df):
    if (df['ctit'] >= timedelta(days=7.5)) | (df['itet'] >= timedelta(days=30)) | (df['attributed_touch_type'] == 'impression'):
        return 'Organic'
    elif df['campaign'] not in campaign_unique_list['campaign_list'].values:
        return 'Organic'
    else:
        return df['media_source']
paid_raw_df['media_source'] = paid_raw_df.apply(organic_check, axis=1)
# paid 내 organic 데이터 필터링
# 1분 10초 소요

### organic data 전처리 ###
organic_raw_dir = raw_dir + '/오가닉'
organic_raw_df = get_raw_df(organic_raw_dir)
organic_raw_df = organic_raw_df.rename(columns={'Attributed Touch Type':'attributed_touch_type', 'Attributed Touch Time':'attributed_touch_time',
                                                'Install Time':'install_time', 'Event Time':'event_time', 'Event Name':'event_name',
                                                'Media Source':'media_source', 'Campaign':'campaign', 'AppsFlyer ID':'appsflyer_id'})
# organic 데이터 로드

organic_raw_df[['install_time','attributed_touch_time','event_time']] = organic_raw_df[['install_time','attributed_touch_time','event_time']].apply(pd.to_datetime)
# 날짜타입 datetime형으로 변경

organic_raw_df['ctit'] = organic_raw_df['install_time'] - organic_raw_df['attributed_touch_time']
organic_raw_df['itet'] = organic_raw_df['event_time'] - organic_raw_df['install_time']
# ctit, itet 계산

organic_raw_df['media_source'].unique()
organic_raw_df['media_source'] = 'Organic'
# media_source organic 기입

raw_df = pd.concat([paid_raw_df, organic_raw_df], sort=False, ignore_index= True)
raw_df.to_csv(dr.download_dir + '/finda_preprocessed_raw_data.csv', index=False, encoding='utf-8-sig')
# raw 데이터 추출
# 40초 소요
install_raw_df = raw_df.loc[raw_df['event_name']=='install']
install_raw_df.drop_duplicates(subset=['appsflyer_id','install_time'], keep='first').to_csv(dr.download_dir + '/first_touch_extract_install_data.csv', index=False, encoding='utf-8-sig')
# install 데이터 추출
# 20초 소요