import os
import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime
import numpy as np

finda_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT84'
raw_dir = finda_dir + '/(매드잇x핀다) RE 데이터 중 UA 볼륨 영향도 측정 관련 데이터 분석_0819'
result_dir = finda_dir + '/DCT86'

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
        'Is Retargeting': pa.string(),
        'Event Value' : pa.string(),
        'Platform' : pa.string()
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
    raw_df = raw_df.rename(columns=
                                     {'Attributed Touch Type': 'attributed_touch_type',
                                      'Attributed Touch Time': 'attributed_touch_time',
                                      'Install Time': 'install_time',
                                      'Event Time': 'event_time',
                                      'Event Name': 'event_name',
                                      'Media Source': 'media_source',
                                      'Campaign': 'campaign', 'AppsFlyer ID': 'appsflyer_id',
                                      'Is Retargeting': 'is_retargeting',
                                      'Event Value' : 'event_value',
                                      'Platform' : 'platform'})
    return raw_df

def campaign_name_exception(paid_raw_df):
    paid_raw_df.loc[paid_raw_df['media_source'] != 'moloco_int', 'campaign'] = paid_raw_df['campaign'].apply(
        lambda x: x.replace('MobidaysAgency_', 'Madit_'))
    paid_raw_df.loc[paid_raw_df['media_source'] != 'moloco_int', 'campaign'] = paid_raw_df['campaign'].apply(
        lambda x: x.replace('Mobi_', 'Madit_'))

    key_list = ['madit_ka-friend_loan_br_mo_reach_total', 'MobidaysAgency_KA-BIZ_LOAN_NU_AEO-CONT_220307', 'alwayson_loan',
     'Madit_CAULY_LOAN_NU_AOS_REWARD-CPE', 'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AUTO_220712', '468706157', '500973516',
     '492604880', '1078291510', '1071110978']

    value_list = ['Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL', 'Madit_KA-BIZ_LOAN_NU_iOS_AEO-CONT_220307', 'Mobi_Finda_AOS_Viewedlahome',
                     'Madit_CAULY_LOAN_NU_AOS_NCPI_220622', 'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AM_220712', 'Madit_AppleSearchAds_0826',
                     'Madit_AppleSearchAds_Comp_1203', 'Madit_AppleSearchAds_Brand_1104', 'Madit_ASA_LOAN_NU_iOS_BAU-MAIA_220629', 'Madit_ASA_LOAN_NU_iOS_SEARCHMATCH_220617']

    campaign_df = pd.DataFrame({'campaign' : key_list,'campaign_new' : value_list})

    paid_raw_df_merge = paid_raw_df.merge(campaign_df, on = 'campaign', how = 'left')
    paid_raw_df_merge.loc[pd.notnull(paid_raw_df_merge['campaign_new']) ,'campaign'] = paid_raw_df_merge['campaign_new']

    paid_raw_df_merge.loc[(paid_raw_df_merge['media_source'] == 'kakao') & (paid_raw_df_merge['campaign'] != 'talk'), 'campaign'] = ''
    paid_raw_df_merge.loc[(paid_raw_df_merge['is_retargeting'] == 'false') & (paid_raw_df_merge['campaign'].isin(['992682', '992393'])), 'campaign'] = ''
    paid_raw_df_merge.loc[(paid_raw_df_merge['is_retargeting'] == 'true') & (paid_raw_df_merge['media_source'] == 'cauly_int'), 'campaign'] = 'Madit_CAULY_LOAN_RT_AOS_CPC_220714'
    paid_raw_df_merge.loc[(paid_raw_df_merge['is_retargeting'] == 'false') & (paid_raw_df_merge['media_source'] == 'tnk_int'), 'campaign'] = 'Madit_TNK_LOAN_NU_AOS_REWARD-CPA_220715'

    return paid_raw_df_merge

def paid_data_prep():
    ### paid data 전처리 ###
    paid_raw_dir = raw_dir + '/paid'
    paid_re_raw_df = get_raw_df(paid_raw_dir + '/re')
    paid_ua_raw_df = get_raw_df(paid_raw_dir + '/ua')
    paid_raw = pd.concat([paid_re_raw_df, paid_ua_raw_df], sort=False, ignore_index=True)


    # paid 데이터 로드
    paid_raw_df = paid_raw.copy()
    paid_raw_df[['install_time', 'attributed_touch_time', 'event_time']] = paid_raw_df[
        ['install_time', 'attributed_touch_time', 'event_time']].apply(pd.to_datetime)

    # 30초 소요
    paid_raw_df.loc[pd.isnull(paid_raw_df['attributed_touch_time']), 'attributed_touch_time'] = paid_raw_df['install_time']
    paid_raw_df['ctit'] = (paid_raw_df['install_time'] - paid_raw_df['attributed_touch_time'])
    paid_raw_df['itet'] = (paid_raw_df['event_time'] - paid_raw_df['install_time'])

    def time_to_total_sec(x):
        return datetime.timedelta.total_seconds(x)

    paid_raw_df['ctit'] = paid_raw_df['ctit'].map(time_to_total_sec)
    paid_raw_df['itet'] = paid_raw_df['itet'].map(time_to_total_sec)

    # ctit, itet 계산

    paid_raw_df['is_retargeting'] = paid_raw_df['is_retargeting'].apply(str.lower)

    # 예외처리
    paid_raw_df = campaign_name_exception(paid_raw_df)

    # 1분30초 소요

    campaign_list = pd.read_excel(raw_dir + '/(핀다) 가공용 캠페인리스트_220819.xlsx', sheet_name='캠페인 리스트')
    columns = ['매체 (Display)' , '캠페인', '캠페인 구분', '캠페인 라벨', 'OS']
    campaign_list = campaign_list[columns]
    campaign_list = campaign_list.rename(columns = {'캠페인' : 'campaign'})

    paid_raw_df_labeling = paid_raw_df.merge(campaign_list, on = 'campaign', how = 'left')

    def organic_check(row):
        media = row['media_source']
        event_name = row['event_name']
        camp_category = row['캠페인 구분']
        is_ret = row['is_retargeting']
        ctit = row['ctit']
        itet  = row['itet']
        touch_type = row['attributed_touch_type']
        platform = row['platform']
        os = row['OS']

        if (media != 'restricted') & (pd.isnull(camp_category)):   # 캠페인명 조회가 안되는 경우
            return 'Organic'
        elif (is_ret == 'false') & (camp_category == 'Retargeting'):
            return 'Organic'
        elif (is_ret == 'true') & (camp_category == 'User Acquisition'):
            return 'Organic'
        elif touch_type == 'impression' :
            return 'Organic'
        elif (platform == 'ios') & (os == 'AOS'):
            return 'Organic'
        elif (platform == 'android') & (os == 'iOS'):
            return 'Organic'

        if (event_name in ['install', 're-engagement', 're-attribution']) :
            if (ctit >= 864000) :
                return 'Organic'
        else :
            if (itet >= 2592000):
                return 'Organic'

        return 'Paid'

    paid_raw_df_labeling['is_organic'] = paid_raw_df_labeling.apply(organic_check, axis=1)
    return paid_raw_df_labeling

def sample_data_pivot(data):
    data['Date'] = pd.to_datetime(data['event_time']).dt.date
    data['Cnt'] = 1
    data_pivot = data.pivot_table(index = ['Date', 'media_source', 'campaign', '매체 (Display)', '캠페인 구분', '캠페인 라벨',  'is_organic'],
                                  columns = 'event_name', values = 'Cnt', aggfunc = 'sum')
    data_pivot = data_pivot.reset_index()
    data_pivot.to_csv(result_dir + '/finda_data_pivot.csv', index=False, encoding='utf-8-sig')

def organic_data_prep():
    ### organic data 전처리 ###
    organic_raw_dir = raw_dir + '/오가닉'
    organic_raw_df = get_raw_df(organic_raw_dir)
    organic_raw_df = organic_raw_df[['install_time','event_time', 'event_name', 'event_value', 'appsflyer_id']]

    organic_raw_df['is_organic'] = 'Organic'
    organic_raw_df['media_source'] = 'Organic'
    organic_raw_df['매체 (Display)'] = 'Organic'
    # media_source organic 기입
    return organic_raw_df

def cohort_by_media(data):
    paid_raw_df = paid_data_prep()

    paid_df = paid_raw_df[['install_time', 'event_time', 'event_name', '매체 (Display)', 'media_source', 'event_value', 'appsflyer_id', 'is_organic']]
    paid_df.loc[paid_df['is_organic']=='Organic', '매체 (Display)']= 'Organic'
    paid_df.loc[paid_df['is_organic'] == 'Organic', 'media_source'] = 'Organic'
    organic_df = organic_data_prep()

    raw_df = pd.concat([paid_df, organic_df], sort=False, ignore_index=True)
    raw_df['Cnt'] = 1
    raw_df.to_csv(result_dir + '/finda_preprocessed_raw_data.csv', index=False, encoding='utf-8-sig')

    install_data = raw_df.loc[(raw_df['event_name']=='install') & (raw_df['is_organic']=='Paid')]
    install_data = install_data.sort_values('install_time')
    install_data = install_data.drop_duplicates('appsflyer_id', keep = 'first')

    install_data = install_data[['매체 (Display)', 'appsflyer_id']]
    install_data = install_data.rename(columns = {'매체 (Display)' : 'install_source'})

    loan_data = raw_df.loc[raw_df['event_name']=='loan_contract_completed']

    # loan_data_pivot = loan_data.pivot_table(index = ['매체 (Display)'], values = 'Cnt', aggfunc = 'sum').reset_index()
    # loan_data_pivot = loan_data_pivot.sort_values('Cnt', ascending=False)

    loan_data_merge = loan_data.merge(install_data, on = 'appsflyer_id', how = 'inner')
    loan_data_merge_pivot = loan_data_merge.pivot_table(index = ['install_source', '매체 (Display)'], values ='Cnt', aggfunc='sum')
    loan_data_merge_pivot = loan_data_merge_pivot.reset_index()
    loan_data_merge_pivot = loan_data_merge_pivot.sort_values('Cnt', ascending=False)
    return loan_data_merge_pivot