import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT175'
paid_dir = raw_dir + '/appsflyer_prism'
organic_dir = raw_dir + '/appsflyer_organic'
result_dir = dr.download_dir

def get_paid_df(paid_dir):
    dtypes = {
        'attributed_touch_type': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'media_source': pa.string(),
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

    files = os.listdir(paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df['is_organic'] = 'False'
    raw_df[['install_time','event_time']] = raw_df[['install_time','event_time']].apply(pd.to_datetime)

    return raw_df


def get_organic_df(organic_dir):
    dtypes = {
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

    files = os.listdir(organic_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
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
                            'Event Value': 'event_value',
                            'Platform': 'platform'})
    raw_df['is_organic'] = 'True'
    raw_df[['install_time','event_time']] = raw_df[['install_time','event_time']].apply(pd.to_datetime)

    return raw_df


def campaign_name_exception(raw_df):
    # 캠페인 수정
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('MobidaysAgency_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('Mobi_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] == 'cauly_int', 'campaign'] = raw_df['campaign'].apply(lambda x: 'Madit_CAULY_LOAN_RT_AOS_CPC_220714' if x.startswith('99') else x)

    #캠페인 삭제
    raw_df.loc[(raw_df['media_source'] == 'kakao_int')&(raw_df['campaign']=='kakao_biz_loancontract'), 'campaign'] = ''
    raw_df.loc[(raw_df['media_source'] == 'kakao')&(raw_df['campaign'] != 'talk'), 'campaign'] = ''

    # mapping_dic 활용
    campaign_mapping_dic = {
        'kakao_int':{
            'madit_ka-friend_loan_br_mo_reach_total':'Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL',
            'MobidaysAgency_KA-BIZ_LOAN_NU_AEO-CONT_220307':'Madit_KA-BIZ_LOAN_NU_iOS_AEO-CONT_220307'
        },
        'KA-FRIEND':{
            'madit_ka-friend_loan_br_mo_reach_total':'Madit_KA-FRIEND_LOAN_BR_MO_REACH_TOTAL'
        },
        'moloco_int':{
            'alwayson_loan':'Mobi_Finda_AOS_Viewedlahome',
            'Madit_ML_MYDATA_RT_AOS_AEO-MD_220927_미사용':'Madit_ML_MYDATA_RT_AOS_AEO-MD_220927',
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

    raw_df.loc[raw_df['media_source'] == 'kakao_int', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['kakao_int'][x] if x in campaign_mapping_dic['kakao_int'].keys() else x)
    raw_df.loc[raw_df['media_source'] == 'KA-FRIEND', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['KA-FRIEND'][x] if x in campaign_mapping_dic['KA-FRIEND'].keys() else x)
    raw_df.loc[raw_df['media_source'] == 'moloco_int', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['moloco_int'][x] if x in campaign_mapping_dic['moloco_int'].keys() else x)
    raw_df.loc[raw_df['media_source'] == 'cauly_int', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['cauly_int'][x] if x in campaign_mapping_dic['cauly_int'].keys() else x)
    raw_df.loc[raw_df['media_source'] == 'bytedanceglobal_int', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['bytedanceglobal_int'][x] if x in campaign_mapping_dic['bytedanceglobal_int'].keys() else x)
    raw_df.loc[raw_df['media_source'] == 'Apple Search Ads', 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_mapping_dic['Apple Search Ads'][x] if x in campaign_mapping_dic['Apple Search Ads'].keys() else x)

    raw_df['media_source'] = raw_df['media_source'].apply(lambda x: x.lower())
    raw_df['campaign'] = raw_df['campaign'].apply(lambda x: x.lower())
    return raw_df

def get_campaign_cost_df(raw_dir):
    cost_file = raw_dir + '/핀다_220601_220928 예산.xlsx'
    df_sheet_all = pd.read_excel(cost_file, sheet_name=None, engine='openpyxl')
    df_concat = pd.DataFrame()
    for sheet in df_sheet_all.keys():
        temp = df_sheet_all[sheet]
        temp['월'] = sheet
        df_concat = pd.concat([df_concat, temp], axis=0)

    df_concat['매체'] = df_concat['매체'].apply(lambda x: x.lower())
    df_concat['캠페인'] = df_concat['캠페인'].apply(lambda x: x.lower())
    return df_concat


paid_df = get_paid_df(paid_dir)
organic_df = get_organic_df(organic_dir)

raw_df = pd.concat([paid_df, organic_df], axis=0, ignore_index=True)
raw_df = campaign_name_exception(raw_df)

campaign_cost_df = get_campaign_cost_df(raw_dir)
campaign_df = campaign_cost_df.drop_duplicates(['매체','캠페인'])[['매체','캠페인']]


# 앱스 매체명 - 캠페인별 예산 내 매체명 매핑X -> 인덱스 시트 별도로 있는지 확인 필요
raw_df.loc[raw_df['is_organic'] == 'False'].media_source.unique()

# 캠페인 cost 데이터 존재 하는 캠페인 만 남길지?
raw_df.loc[raw_df['is_organic'] == 'False'].loc[(raw_df['campaign'].isin(campaign_cost_df.캠페인.unique()))].campaign.unique()


# raw_df['hour'] = raw_df['event_time'].apply(lambda x: x.date.hour)
# raw_df['weekday'] = raw_df['event_time'].apply(lambda x: x.date.week)