from analysis.DCT175_finda import info

import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np

def get_paid_df():
    dtypes = {
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'media_source': pa.string(),
        'adset' : pa.string(),
        'ad' : pa.string(),
        'campaign': pa.string(),
        'appsflyer_id': pa.string(),
        'is_retargeting': pa.string(),
        'event_value': pa.string(),
        'platform': pa.string()
    }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df['is_organic'] = False
    raw_df.loc[raw_df['attributed_touch_type']!='click', 'is_organic'] = True
    raw_df.loc[(raw_df['platform']=='ios')&(raw_df['attributed_touch_time']==''), 'is_organic'] = False

    # 모비데이즈 데이터 예외처리
    raw_df_copy = raw_df.copy()
    raw_df_copy = raw_df_copy.loc[raw_df_copy['media_source']!='']
    raw_df_copy_dedup = raw_df_copy[['media_source', 'campaign', 'adset', 'ad']].drop_duplicates(['campaign', 'adset', 'ad'])
    raw_df_copy_dedup = raw_df_copy_dedup.rename(columns = {'media_source' : 'media_source_find'})

    raw_df_merge = raw_df.merge(raw_df_copy_dedup, on = ['campaign', 'adset', 'ad'], how = 'left')

    raw_df_merge.loc[raw_df_merge['media_source']=='', 'media_source'] = raw_df_merge['media_source_find']

    del raw_df_merge['media_source_find']
    return raw_df_merge

def get_organic_df():
    dtypes = {
        'Attributed Touch Time': pa.string(),
        'Attributed Touch Type': pa.string(),
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Media Source': pa.string(),
        'Campaign': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Is Retargeting': pa.string(),
        'Event Value': pa.string(),
        'Platform': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.organic_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df = raw_df.rename(columns=
                           {'Attributed Touch Time': 'attributed_touch_time',
                            'Attributed Touch Type': 'attributed_touch_type',
                            'Install Time': 'install_time',
                            'Event Time': 'event_time',
                            'Event Name': 'event_name',
                            'Media Source': 'media_source',
                            'Campaign': 'campaign', 'AppsFlyer ID': 'appsflyer_id',
                            'Is Retargeting': 'is_retargeting',
                            'Event Value': 'event_value',
                            'Platform': 'platform'})
    raw_df['is_organic'] = True
    return raw_df

def raw_data_concat(media_filter = [], from_date='', to_date=''):
    paid_df = get_paid_df()
    organic_df = get_organic_df()

    raw_df = pd.concat([paid_df, organic_df], sort = False, ignore_index=True)

    raw_df['is_retargeting'] = raw_df['is_retargeting'].str.lower()

    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    raw_df['click_date'] = raw_df['attributed_touch_time'].dt.date
    raw_df['click_hour'] = raw_df['attributed_touch_time'].dt.hour
    raw_df['click_weekday'] = raw_df['attributed_touch_time'].dt.weekday
    raw_df['event_date'] = raw_df['event_time'].dt.date
    raw_df['event_hour'] = raw_df['event_time'].dt.hour
    raw_df['event_weekday'] = raw_df['event_time'].dt.weekday
    raw_df['CTET'] = raw_df['event_time'] - raw_df['attributed_touch_time']

    raw_df = raw_df.sort_values(['event_time','attributed_touch_time'])
    raw_df.index = range(0, len(raw_df))

    raw_df_dedup = raw_df.drop_duplicates(['event_time', 'event_name', 'appsflyer_id'], keep='last')
    raw_df_dedup = raw_df_dedup.loc[~(raw_df_dedup['media_source'].isin(media_filter))]

    if from_date == '':
        from_date = np.min(raw_df_dedup['event_date'])
    if to_date == '' :
        to_date = np.max(raw_df_dedup['event_date'])
    raw_df_dedup = raw_df_dedup.loc[
        (raw_df_dedup['event_date'] >= from_date) & (raw_df_dedup['event_date'] <= to_date)]
    return raw_df_dedup

def campaign_name_exception(raw_df):
    # 캠페인명 수정
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('MobidaysAgency_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('Mobi_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] == 'cauly_int', 'campaign'] = raw_df['campaign'].apply(lambda x: 'Madit_CAULY_LOAN_RT_AOS_CPC_220714' if x.startswith('99') else x)
    raw_df.loc[raw_df['media_source'] == 'tnk_int', 'campaign'] = 'Madit_TNK_LOAN_NU_AOS_REWARD-CPA_220715'

    # 특정 캠페인 구분 organic으로 변경
    raw_df.loc[(raw_df['media_source'] == 'kakao_int')&(raw_df['campaign']=='kakao_biz_loancontract'), 'is_organic'] = True
    raw_df.loc[(raw_df['media_source'] == 'kakao')&(raw_df['campaign'] != 'talk'), 'is_organic'] = True

    # mapping_dic 활용
    campaign_mapping_dic = {
        'kakao_int':{
            'madit_ka-friend_loan_br_mo_reach_total':'Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL',
            'MobidaysAgency_KA-BIZ_LOAN_NU_AEO-CONT_220307':'Madit_KA-BIZ_LOAN_NU_iOS_AEO-CONT_220307',
            'Madit_KA-BIZ_LOAN_NU_AEO-CONT_220307' : 'Madit_KA-BIZ_LOAN_NU_iOS_AEO-CONT_220307'
        },
        'KA-FRIEND':{
            'madit_ka-friend_loan_br_mo_reach_total':'Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL'
        },
        'moloco_int':{
            'alwayson_loan':'Mobi_Finda_AOS_Viewedlahome',
            'Madit_ML_MYDATA_RT_AOS_AEO-MD_220927_미사용':'Madit_ML_MYDATA_RT_AOS_AEO-MD_220927',
            'MobidaysAgency_ML_LOAN_NU_iOS_MAIA_220418' : 'Madit_ML_LOAN_NU_iOS_MAIA_220418'
        },
        'cauly_int':{
            'Madit_CAULY_LOAN_NU_AOS_REWARD-CPE':'Madit_CAULY_LOAN_NU_AOS_NCPI_220622'
        },
        'bytedanceglobal_int':{
            'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AUTO_220712':'Madit_TIKTOK_LOAN_NU_AOS_AEO-VIEWED-AM_220712'
        },
        'Apple Search Ads':{
            '468706157': 'Madit_AppleSearchAds_0826',
            '500973516': 'Madit_AppleSearchAds_Comp_1203',
            '492604880': 'Madit_AppleSearchAds_Brand_1104',
            '1078291510': 'Madit_ASA_LOAN_NU_iOS_BAU-MAIA_220629',
            '1071110978': 'Madit_ASA_LOAN_NU_iOS_SEARCHMATCH_220617'
        }
    }

    for media in campaign_mapping_dic.keys():
        raw_df.loc[raw_df['media_source'] == media, 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic[media][x] if x in campaign_mapping_dic[media].keys() else x)

    return raw_df

def get_campaign_cost_df(from_date = '', to_date = ''):
    cost_file = info.raw_dir + '/핀다_220601_220930 예산.xlsx'
    df_sheet_all = pd.read_excel(cost_file, sheet_name=None, engine='openpyxl')
    df_concat = pd.DataFrame()
    for sheet in df_sheet_all.keys():
        temp = df_sheet_all[sheet]
        temp['월'] = sheet
        df_concat = pd.concat([df_concat, temp], axis=0)

    # 비용 데이터 캠페인 명 > 앱스 캠페인 명 매핑 작업 시
    media_mapping_dic = {
        'Google AC': 'googleadwords_int',
        'Google ACe': 'googleadwords_int',
        'Kakao': 'kakao_int',
        'ASA': 'Apple Search Ads',
        'Moloco': 'moloco_int',
        'Appier': 'appier_int',
        'Liftoff': 'liftoff_int',
        'Kakao BS': '',
        'Naver BS': '',
        'Cauly': 'cauly_int',
        'Nstation': 'nstation_int',
        'Appier_RE': 'appier_int',
        'Cookie Oven': 'adisonofferwall_int',
        'Kakao Page': 'cashfriends_int',
        'V3': 'v3',
        'Bobaedream': 'bobaedream',
        'Encar': 'encar',
        'Remember': 'remember',
        'Nswitch': 'nswitch_int',
        'Naver SD': 'naversd',
        'Ka-Friend': 'ka-friend',
        'Kakao_RE': 'kakao_int',
        'Tiktok': 'bytedanceglobal_int',
        'Tiktok_RE': 'bytedanceglobal_int',
        'Cauly_RE': 'cauly_int',
        'Tnk': 'tnk_int',
        'Toss': 'toss',
        'Naver GFA': 'naver_int',
        'Blind': 'blind',
        'Criteo_RE': 'criteonew_int',
        'Inmobi': 'inmobidsp_int',
        'Tradingworks': 'igaworkstradingworksvideo_int',
        'Naver band': 'naverband',
        'Jobplanet': 'jobplanet_onelink',
        'Toss Reward': 'adisonofferwall_int',
        'Moloco_RE': 'moloco_int',
        'Rtb house_RE': 'rtbhouse_int',
        'no_index': ''
    }
    df_concat = df_concat.loc[~(df_concat['매체'].isin(['Facebook', 'Facebook_RE']))]
    df_concat['매체'] = df_concat['매체'].apply(lambda x: media_mapping_dic[x])
    df_concat = df_concat.reset_index(drop=True)
    df_concat['날짜'] = pd.to_datetime(df_concat['날짜']).dt.date

    if from_date == '':
        from_date = np.min(df_concat['날짜'])
    if to_date == '' :
        to_date = np.max(df_concat['날짜'])
    df_concat = df_concat.loc[
        (df_concat['날짜'] >= from_date) & (df_concat['날짜'] <= to_date)]

    df_concat = df_concat.rename(columns = {'매체' : 'media_source',
                                            '캠페인' : 'campaign',
                                            '광고비(마크업포함)':'cost'})

    df_concat.loc[df_concat['media_source'] == 'moloco_int', 'campaign'] = df_concat['campaign'].apply(lambda x: str(x).replace('_미사용', ''))
    return df_concat

def get_campaign_cost_hour_df(from_date = '', to_date = ''):
    cost_file = info.raw_dir + '/(핀다) 일별-시간대별 캠페인 비용_total_221007.csv'
    hour_df = pd.read_csv(cost_file)

    # 비용 데이터 캠페인 명 > 앱스 캠페인 명 매핑 작업 시
    media_mapping_dic = {
        'Google AC': 'googleadwords_int',
        'Google ACe': 'googleadwords_int',
        'Kakao': 'kakao_int'
    }

    hour_df['media_source'] = hour_df['media_source'].apply(lambda x: media_mapping_dic[x])

    hour_df.loc[hour_df['media_source']=='googleadwords_int', 'cost'] = hour_df['cost'] * 1.013
    hour_df.loc[hour_df['media_source']=='kakao_int', 'cost'] = hour_df['cost'] / 1.1

    hour_df['날짜'] = pd.to_datetime(hour_df['날짜']).dt.date

    if from_date == '':
        from_date = np.min(hour_df['날짜'])
    if to_date == '' :
        to_date = np.max(hour_df['날짜'])
    hour_df = hour_df.loc[
        (hour_df['날짜'] >= from_date) & (hour_df['날짜'] <= to_date)]
    return hour_df