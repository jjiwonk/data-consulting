import datetime
import numpy as np

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
        'attributed_touch_time': pa.string(),
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
    raw_df[['attributed_touch_time','install_time','event_time']] = raw_df[['attributed_touch_time','install_time','event_time']].apply(pd.to_datetime)

    return raw_df


def get_organic_df(organic_dir):
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
                            'Attributed Touch Time': 'attributed_touch_time',
                            'Install Time': 'install_time',
                            'Event Time': 'event_time',
                            'Event Name': 'event_name',
                            'Media Source': 'media_source',
                            'Campaign': 'campaign', 'AppsFlyer ID': 'appsflyer_id',
                            'Is Retargeting': 'is_retargeting',
                            'Event Value': 'event_value',
                            'Platform': 'platform'})
    raw_df['is_organic'] = 'True'
    raw_df[['attributed_touch_time','install_time','event_time']] = raw_df[['attributed_touch_time','install_time','event_time']].apply(pd.to_datetime)

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
raw_df = raw_df.sort_values(['event_time', 'install_time'])

from_date = datetime.date(2022,7,1)
target_campaign = 'madit_ka-biz_loan_rt_aos_cnv-viewed_220708'

campaign_cost_df = get_campaign_cost_df(raw_dir)
campaign_cost_df = campaign_cost_df[['날짜', '매체', '캠페인', '광고비(마크업포함)']]
campaign_cost_df['날짜'] = pd.to_datetime(campaign_cost_df['날짜']).dt.date
campaign_cost_df = campaign_cost_df.loc[campaign_cost_df['날짜']>=from_date]

campaign_cost_pivot = campaign_cost_df.pivot_table(index = ['날짜', '매체', '캠페인'], values = '광고비(마크업포함)', aggfunc = 'sum').reset_index()
campaign_cost_pivot['spend_per_sec'] = (campaign_cost_pivot['광고비(마크업포함)'] / 86400)
campaign_list = list(campaign_cost_df['캠페인'].unique())

cost_sampling = campaign_cost_pivot.loc[campaign_cost_pivot['캠페인']==target_campaign]
cost_input_series = pd.concat([cost_sampling['spend_per_sec'][-1:],cost_sampling['spend_per_sec'][0:-1]], axis = 0)
cost_input_series.index = cost_sampling.index
cost_sampling['spend_per_sec_before'] = cost_input_series
cost_sampling = cost_sampling[['날짜', '광고비(마크업포함)','spend_per_sec', 'spend_per_sec_before']]
np.sum(cost_sampling['광고비(마크업포함)'])

acquisition = raw_df.loc[raw_df['event_name'].isin(['install', 're-engagement', 're-attribution'])]
acquisition = acquisition.loc[acquisition['campaign'].isin(campaign_list)]
acquisition.loc[pd.isnull(acquisition['attributed_touch_time']), 'attributed_touch_time'] = acquisition['install_time']
acquisition['CTIT'] = acquisition['install_time'] - acquisition['attributed_touch_time']
acquisition = acquisition.loc[acquisition['CTIT'] < datetime.timedelta(days = 7)]


acquisition = acquisition.loc[acquisition['attributed_touch_time'].dt.date >= from_date]
acquisition = acquisition.drop_duplicates(['appsflyer_id'], keep = 'first')
acquisition = acquisition[['appsflyer_id', 'attributed_touch_time', 'install_time']]


quantity = raw_df.loc[raw_df['event_name']=='loan_contract_completed']
quantity = quantity.loc[quantity['campaign'].isin(campaign_list)]

quantity.loc[pd.isnull(quantity['attributed_touch_time']), 'attributed_touch_time'] = quantity['install_time']
quantity['CTIT'] = quantity['install_time'] - quantity['attributed_touch_time']
quantity = quantity.loc[quantity['CTIT'] < datetime.timedelta(days = 7)]

quantity['ITET'] = quantity['event_time'] - quantity['install_time']
quantity = quantity.loc[quantity['ITET'] < datetime.timedelta(days = 30)]

quantity = quantity.loc[quantity['attributed_touch_time'].dt.date >= from_date]


quantity_sampling = quantity.loc[quantity['campaign']==target_campaign]
quantity_sampling['날짜'] = quantity_sampling['event_time'].dt.date

input_series = pd.concat([quantity_sampling['event_time'][-1:],quantity_sampling['event_time'][0:-1]], axis = 0)
input_series.index = quantity_sampling.index
quantity_sampling['compare_time'] = input_series

def calc_sec1(time):
    return (time.hour * 3600) + (time.minute * 60) + (time.second)

quantity_sampling['sec1'] = quantity_sampling['event_time'].apply(calc_sec1)
quantity_sampling['sec2'] = (quantity_sampling['event_time'] - quantity_sampling['compare_time']).apply(lambda x : x.total_seconds())

temp = quantity_sampling[['날짜','event_time', 'compare_time', 'sec1', 'sec2']]
temp.index = range(0, len(temp))
temp['index'] = temp.index
temp_merge = temp.merge(cost_sampling[['날짜', 'spend_per_sec', 'spend_per_sec_before']], on ='날짜', how = 'left')
temp_merge['date_over'] = (temp_merge['event_time'].dt.date != temp_merge['compare_time'].dt.date)
temp_merge['is_first_date'] = temp_merge['날짜'] == np.min(temp_merge['날짜'])

sec1_array = np.array(temp_merge['sec1'])
sec2_array = np.array(temp_merge['sec2'])
cost1_array = np.array(temp_merge['spend_per_sec'])
cost2_array = np.array(temp_merge['spend_per_sec_before'])
date_over_array = np.array(temp_merge['date_over'])
is_first_date_array = np.array(temp_merge['date_over'])

cost_list = []
for i in temp_merge['index']:
    sec1 = sec1_array[i]
    sec2 = sec2_array[i]
    cost1 = cost1_array[i]
    cost2 = cost2_array[i]
    is_first_date = is_first_date_array[i]
    date_over = date_over_array[i]
    if i == 0:
        production_cost = sec1 * cost1
    else :
        if is_first_date == True:
            production_cost = sec2 * cost1
        else :
            if date_over == True :
                today_spend = sec1 * cost1
                yesterday_spend = (sec2 - sec1) * cost2
                production_cost = today_spend + yesterday_spend
            else :
                production_cost = sec2 * cost1

    cost_list.append(production_cost)

temp_merge['production_cost'] = cost_list
temp_merge.to_csv(dr.download_dir + '/test_df2.csv', index=False, encoding = 'utf-8-sig')