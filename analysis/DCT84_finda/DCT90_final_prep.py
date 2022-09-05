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

# 데이터 전처리 관련
def get_raw_df(raw_dir):
    # usecols = ['Attributed Touch Type', 'Install Time', 'Event Time', 'Event Name', 'Media Source',
    #            'Campaign', 'Appsflyer Id', 'Is Retargeting', 'Event Value', 'Platform']
    # temp = pd.read_csv(raw_dir + '/paid/appsflyer(log)_appsflyer_open_finda_app.csv', usecols= usecols)
    # temp = temp.rename(columns={'Appsflyer Id': 'AppsFlyer ID'})
    # temp['Event Time'] = temp['Event Time'].str.replace('오전 ', '')
    # temp['Event Time'] = temp['Event Time'].str.replace('오후 ', '')
    # temp['Install Time'] = temp['Install Time'].str.replace('오전 ', '')
    # temp['Install Time'] = temp['Install Time'].str.replace('오후 ', '')
    # temp = temp.sort_values('Install Time')
    #
    # ua = temp.loc[temp['Is Retargeting'] == False]
    # ua = ua.drop_duplicates(['AppsFlyer ID'], keep = 'first')
    # ua['Event Name'] = 'install'
    # ua['Event Time'] = ua['Install Time']
    # ua['Media Source'] = ua['Media Source'].fillna('Organic')
    #
    # re = temp.loc[temp['Is Retargeting'] == True]
    # re = re.drop_duplicates(['AppsFlyer ID'], keep = 'first')
    # re['Event Name'] = 're-engagement'
    # re['Event Time'] = re['Install Time']
    #
    # paid_ua = ua.loc[ua['Media Source']!='Organic']
    # organic = ua.loc[ua['Media Source'] == 'Organic']
    #
    # paid_ua.to_csv(raw_dir + '/paid/ua/install_preprocessed_0101-0531.csv', index = False, encoding= 'utf-8-sig')
    # re.to_csv(raw_dir + '/paid/re/reengagement_preprocessed_0101-0531.csv', index=False, encoding='utf-8-sig')
    # organic.to_csv(raw_dir + '/오가닉/install_preprocessed_0101-0531.csv', index=False, encoding='utf-8-sig')

    dtypes = {
        'Attributed Touch Type': pa.string(),
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
    del paid_re_raw_df, paid_ua_raw_df

    # paid 데이터 로드
    paid_raw_df = paid_raw.copy()
    paid_raw_df[['install_time', 'event_time']] = paid_raw_df[
        ['install_time', 'event_time']].apply(pd.to_datetime)

    # 30초 소요
    paid_raw_df['itet'] = (paid_raw_df['event_time'] - paid_raw_df['install_time'])

    def time_to_total_sec(x):
        return datetime.timedelta.total_seconds(x)

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
    campaign_list = campaign_list.drop_duplicates('campaign')

    paid_raw_df_labeling = paid_raw_df.merge(campaign_list, on = 'campaign', how = 'left')
    paid_raw_df_labeling.loc[pd.isnull(paid_raw_df_labeling['매체 (Display)']), '매체 (Display)'] = paid_raw_df_labeling['media_source'].apply(lambda x : x + '(no_index)')

    def organic_check(row):
        media = row['media_source']
        event_name = row['event_name']
        camp_category = row['캠페인 구분']
        is_ret = row['is_retargeting']
        itet  = row['itet']
        touch_type = row['attributed_touch_type']
        platform = row['platform']
        os = row['OS']

        if (event_name not in ['install', 're-engagement', 're-attribution']):
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
            elif (itet >= 2592000) :
                return 'Organic'
        else : pass

        return 'Paid'

    paid_raw_df_labeling['is_organic'] = paid_raw_df_labeling.apply(organic_check, axis=1)
    return paid_raw_df_labeling

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

def paid_organic_concat():
    paid_raw_df = paid_data_prep()

    paid_df = paid_raw_df[['install_time', 'event_time', 'event_name', '매체 (Display)', 'media_source', 'campaign','event_value', 'appsflyer_id', 'is_organic', '캠페인 구분']]
    organic_df = organic_data_prep()

    raw_df = pd.concat([paid_df, organic_df], sort=False, ignore_index=True)
    raw_df['Cnt'] = 1
    raw_df.to_csv(result_dir + '/finda_preprocessed_raw_data.csv', index=False, encoding='utf-8-sig')
    return raw_df

def loan_data_prep(raw_df, is_organic=True, unique=False):
    loan_data = raw_df.loc[raw_df['event_name'] == 'loan_contract_completed']
    if is_organic == False:
        loan_data = loan_data.loc[loan_data['is_organic'] == 'Paid']
    loan_data['event_time'] = pd.to_datetime(loan_data['event_time'])
    loan_data = loan_data.sort_values(['appsflyer_id', 'event_time'])
    if unique == True:
        loan_data = loan_data.drop_duplicates('appsflyer_id')
    return loan_data

def acq_data_prep(raw_df, acq_date):
    install_data = raw_df.loc[raw_df['event_name'] == 'install']
    install_data = install_data.loc[pd.to_datetime(install_data['install_time']).dt.date >= acq_date]

    install_user = set(install_data['appsflyer_id'])

    re_data = raw_df.loc[raw_df['event_name'] == 're-engagement']
    re_data = re_data.loc[pd.to_datetime(re_data['install_time']).dt.date >= acq_date]
    re_data = re_data.loc[~(re_data['appsflyer_id'].isin(install_user))]

    acq_data = pd.concat([install_data, re_data], ignore_index=True)
    acq_data['install_time'] = pd.to_datetime(acq_data['install_time'])
    acq_data = acq_data.sort_values('install_time')
    acq_data = acq_data.drop_duplicates('appsflyer_id', keep='first')
    acq_data['campaign'] = acq_data['campaign'].fillna('None')
    acq_data = acq_data.rename(columns={'매체 (Display)': 'acquisition_source', 'campaign': 'acquisition_campaign'})
    return acq_data


# 계산
def loan_term(raw_df):
    loan_data = loan_data_prep(raw_df)
    loan_data = loan_data.sort_values(['event_time', 'install_time'])
    loan_data = loan_data.drop_duplicates(['appsflyer_id', 'event_time'], keep='last')
    loan_data = loan_data[['appsflyer_id', 'event_time']]

    loan_data = loan_data.sort_values(['appsflyer_id', 'event_time'])

    # 2회 이상 대출 받은 사람 추적
    loan_data_copy = loan_data.iloc[1:]
    loan_data_copy = loan_data_copy.append(loan_data.iloc[0])
    loan_data_copy = loan_data_copy[['appsflyer_id', 'event_time']].rename(columns = {'appsflyer_id' : 'appsflyer_id_comp',
                                                                                      'event_time' : 'event_time_comp'})
    loan_data_copy.index = loan_data.index

    loan_data_concat = pd.concat([loan_data, loan_data_copy], sort=False, axis=1)

    def loan_complete_term(row):
        if row['appsflyer_id'] == row['appsflyer_id_comp'] :
            return (pd.to_datetime(row['event_time_comp']) - pd.to_datetime(row['event_time'])).days
        else :
            return -1

    loan_data_concat['loan_term'] = loan_data_concat.apply(loan_complete_term, axis = 1)
    loan_data_concat = loan_data_concat.loc[loan_data_concat['loan_term']>=0]
    len(loan_data_concat['appsflyer_id'].unique())
    loan_data_concat.to_csv(result_dir + '/loan_data_with_term.csv', index=False, encoding= 'utf-8-sig')


def loan_by_install(raw_df, acq_date):
    acq_data = acq_data_prep(raw_df, acq_date)
    acq_data = acq_data[['appsflyer_id', 'acquisition_source', 'acquisition_campaign', 'install_time']]

    loan_data = loan_data_prep(raw_df)
    loan_data = loan_data.sort_values(['event_time', 'install_time'])
    len(loan_data)
    loan_data = loan_data.drop_duplicates(['appsflyer_id', 'event_time'], keep = 'last')
    loan_data = loan_data.rename(columns={'매체 (Display)': 'event_source', 'campaign': 'event_campaign'})
    loan_data = loan_data.drop_duplicates(['event_source', 'event_campaign', 'is_organic', 'appsflyer_id'])
    loan_data = loan_data[['event_source', 'event_campaign', 'is_organic', 'appsflyer_id', 'event_time']]

    # 구글 ACe의 대출 실행 건수가 5876건 -> 5723건으로 감소했는데, 기존에는 Retargeting 캠페인 내에서 중복제거를 했음
    # Retargeting 필터 없이 중복제거를 했기 때문에 최초 UA 캠페인에서 대출실행 받은 캠페인이 있으면 해당 캠페인으로 카운트됨

    loan_data_merge = loan_data.merge(acq_data, on='appsflyer_id', how='left')
    loan_data_merge = loan_data_merge.loc[loan_data_merge['event_time']>=loan_data_merge['install_time']]

    loan_data_merge['acquisition_campaign'] = loan_data_merge['acquisition_campaign'].fillna('None')
    loan_data_merge['acquisition_source'] = loan_data_merge['acquisition_source'].fillna('Unknown')
    loan_data_merge['event_campaign'] = loan_data_merge['event_campaign'].fillna('None')
    loan_data_merge['Cnt'] = 1

    loan_data_merge_pivot = loan_data_merge.pivot_table(index=['event_source', 'event_campaign','is_organic','acquisition_source', 'acquisition_campaign'],
                                                        values='Cnt', aggfunc='sum')
    loan_data_merge_pivot = loan_data_merge_pivot.reset_index()
    loan_data_merge_pivot = loan_data_merge_pivot.sort_values('Cnt', ascending=False)
    loan_data_merge_pivot.to_csv(result_dir + '/all_loan_by_acquisition_source.csv', index=False, encoding='utf-8-sig')


def all_arpu_by_install_source(raw_df, acq_date):
    acq_data = acq_data_prep(raw_df, acq_date)
    acq_data = acq_data[['appsflyer_id', 'acquisition_source', 'acquisition_campaign', 'install_time']]

    loan_data = loan_data_prep(raw_df)
    loan_data = loan_data.sort_values(['event_time', 'install_time'])
    loan_data = loan_data.drop_duplicates(['appsflyer_id', 'event_time'], keep = 'last')
    loan_data = loan_data[['appsflyer_id', 'event_time']]
    loan_data_merge = loan_data.merge(acq_data, on='appsflyer_id', how='left')
    loan_data_merge = loan_data_merge.loc[loan_data_merge['event_time']>=loan_data_merge['install_time']]
    loan_data_merge['Loan Contract'] = 1

    loan_data_pivot = loan_data_merge.pivot_table(index = ['appsflyer_id'], values = 'Loan Contract', aggfunc='sum').reset_index()
    loan_data_pivot['User'] = 1

    acq_data_merge= acq_data.merge(loan_data_pivot, on ='appsflyer_id', how = 'inner')
    acq_data_merge['Revenue'] = acq_data_merge['Loan Contract'] * 300000
    acq_data_merge_pivot = acq_data_merge.pivot_table(index = ['acquisition_source', 'acquisition_campaign'], values=['Loan Contract', 'User', 'Revenue'],
                                                              aggfunc='sum').reset_index()
    acq_data_merge_pivot['ARPU'] = acq_data_merge_pivot['Revenue'] / acq_data_merge_pivot['User']
    acq_data_merge_pivot.to_csv(result_dir + f'/all_arpu_by_acquisition_source.csv', index=False, encoding ='utf-8-sig')
    return acq_data_merge_pivot

def all_retention_by_source(raw_df, acq_date):
    acq_data = acq_data_prep(raw_df, acq_date)
    acq_data = acq_data[['appsflyer_id', 'acquisition_source', 'acquisition_campaign', 'install_time']]

    loan_data = loan_data_prep(raw_df)
    loan_data = loan_data.sort_values(['event_time', 'install_time'])
    loan_data = loan_data.drop_duplicates(['appsflyer_id', 'event_time'], keep = 'last')
    loan_data = loan_data[['appsflyer_id', 'event_time']]

    loan_data_merge = loan_data.merge(acq_data, on='appsflyer_id', how='left')
    loan_data_merge = loan_data_merge.loc[loan_data_merge['event_time']>=loan_data_merge['install_time']]
    loan_data_merge['Cnt'] = 1

    loan_data_pivot = loan_data_merge.pivot_table(index=['appsflyer_id'], values='Cnt', aggfunc='sum').reset_index()
    loan_data_pivot['Retention'] = 0
    loan_data_pivot.loc[loan_data_pivot['Cnt']>1, 'Retention'] = 1
    loan_data_pivot['User'] = 1
    del loan_data_pivot['Cnt']

    acq_data_merge = acq_data.merge(loan_data_pivot, on ='appsflyer_id', how = 'inner')
    acq_data_merge['acquisition_source'] = acq_data_merge['acquisition_source'].fillna('Unknown')

    acq_data_merge_pivot = acq_data_merge.pivot_table(index = ['acquisition_source', 'acquisition_campaign'], values = ['User', 'Retention'], aggfunc = 'sum')
    acq_data_merge_pivot = acq_data_merge_pivot.reset_index()
    acq_data_merge_pivot.to_csv(result_dir + f'/all_retention_by_install_source.csv', index=False, encoding = 'utf-8-sig')
    return acq_data_merge_pivot


raw_df = paid_organic_concat()
acq_date = datetime.date(2022,6,1)


loan_by_install(raw_df, acq_date)
# Paid로 인정받은 loan만 필터링이 되어있었음

arpu = all_arpu_by_install_source(raw_df, acq_date)
del arpu['User']
retention = all_retention_by_source(raw_df, acq_date)

ltv_merge = arpu.merge(retention, on =['acquisition_source', 'acquisition_campaign'], how = 'outer')
ltv_merge['Retention Rate'] = ltv_merge['Retention'] / ltv_merge['User']
ltv_merge['LTV'] = ltv_merge['ARPU'] / (1 - ltv_merge['Retention Rate'])
ltv_merge.to_csv(result_dir + '/finda_ltv_by_acquisition_source_final.csv', index=False, encoding = 'utf-8-sig')