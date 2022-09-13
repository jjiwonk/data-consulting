import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import setting.directory as dr
import datetime
# import time

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/!자동화리포트/raw_data/appsflyer_prism'
result_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/디덕션'

def deduction_data(raw_dir, required_date):
    dtypes = {
        'attributed_touch_type': pa.string(),
        'attributed_touch_time': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'country_code': pa.string(),
        'language': pa.string(),
        'media_source': pa.string(),
        'carrier': pa.string(),
        'os_version': pa.string(),
        'app_version': pa.string(),
        'sdk_version': pa.string(),
        'device_model': pa.string(),
        'campaign': pa.string(),
        'campaign_id': pa.string(),
        'adset': pa.string(),
        'adset_id': pa.string(),
        'ad': pa.string(),
        'ad_id': pa.string(),
        'advertising_id': pa.string(),
        'appsflyer_id': pa.string(),
        'idfa': pa.string(),
        'idfv': pa.string(),
        'android_id': pa.string(),
        'platform': pa.string(),
        'channel': pa.string(),
        'keywords': pa.string(),
        'is_retargeting': pa.string(),
        'is_primary_attribution': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    given_date = datetime.datetime.strptime(required_date,'%Y-%m-%d')
    date_check = given_date.strftime('%Y%m')
    start_date = given_date.replace(day=1).strftime('%Y%m%d')
    end_date = given_date.strftime('%Y%m%d')

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f and str(f)[-12:-6] == date_check]
    raw_files = [f for f in files if
                 (int(str(f)[-12:-4]) >= int(start_date)) & (int(str(f)[-12:-4]) <= int(end_date))]

    for f in raw_files:
        try:
            temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
        except Exception as e:
            print(f)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()

    return raw_df

def deduction_calculate(required_date, df, file_name):
    df[['attributed_touch_time','install_time','event_time']] = df[['attributed_touch_time','install_time','event_time']].apply(pd.to_datetime)
    df[['con1_event','con2_country','con3_language','con4_carrier','con5_device','con6_osv','con7_appv','con8_sdkv','con9_over3(reattr)','con10_lastm']] = 0

    detarget_event = ['signup_all', 'business_signup_complete', 'job_apply_complete_mobile', 'job_apply_complete_homepage', 'resume_complete']
    df.loc[df['event_name'].isin(detarget_event), 'con1_event'] = 1
    # con1_event

    df.loc[df['country_code'] != 'KR', 'con2_country'] = 1
    # con2_country

    df.loc[(df['platform'] == 'android') & (df['language'] != '한국어'), 'con3_language'] = 1
    # con3_language

    target_carrier = ['SK Telecom', 'SKTelecom', 'SKT', 'KOR SK Telecom', 'KT', 'LGU+', 'LG U+', 'olleh']
    df.loc[(df['platform'] == 'android') & ~(df['carrier'].isin(target_carrier)), 'con4_carrier'] = 1
    # con4_carrier

    detarget_device = [x.lower() for x in ['allwinner', 'GIONEE', 'HONOR', 'HUAWEI', 'Infinix', 'iQOO', 'Itel', 'OnePlus', 'OPPO', 'POCO',
                       'realme', 'Redmi', 'sharp', 'TECNO', 'TGnCo', 'tufen', 'umidigi', 'vivo', 'Xiaomi', 'ZTE',
                       'SMARTISAN', 'Lenovo']]
    df['device_model_name'] = df['device_model'].apply(lambda x: x.split('::')[0]).str.lower()
    df.loc[df['device_model_name'].isin(detarget_device), 'con5_device'] = 1
    # con5_device

    def os_version(row):
        if row['platform'] == 'android':
            if int(row['os_version'].split('.')[0]) < 9:
                return 1
            else:
                return 0
        elif row['platform'] == 'ios':
            if int(row['os_version'].split('.')[0]) < 14:
                return 1
            else:
                return 0
    df.loc[:, 'con6_osv'] = df.apply(os_version, axis = 1)
    # con6_osv

    def app_version(row):
        if row['platform'] == 'android':
            if int(row['app_version'].replace('.','')) < 333:
                return 1
            else:
                return 0
        elif row['platform'] == 'ios':
            if int(row['app_version'].replace('.','')) < 364:
                return 1
            else:
                return 0
    df.loc[:, 'con7_appv'] = df.apply(app_version, axis = 1)
    # con7_appv

    def app_version(row):
        if row['platform'] == 'android':
            if int(row['sdk_version'].replace('.', '').replace('v','')) < 614:
                return 1
            else:
                return 0
        elif row['platform'] == 'ios':
            if int(row['sdk_version'].replace('.', '').replace('v','')) < 626:
                return 1
            else:
                return 0
    df.loc[:, 'con8_sdkv'] = df.apply(app_version, axis=1)
    # con8_sdkv

    over3_check_df = df[['advertising_id','idfa','event_name','event_time']].sort_values(['advertising_id','idfa','event_name','event_time'])
    over3_check_df = over3_check_df.drop(over3_check_df[over3_check_df['idfa'] == ''].index).reset_index(drop=True)
    over3_comp_df = over3_check_df.rename(columns={'advertising_id':'c_advertising_id','idfa':'c_idfa','event_name':'c_event_name','event_time':'c_event_time'}).iloc[2:,:].reset_index(drop=True)
    over3_check = pd.concat([over3_check_df, over3_comp_df], axis=1)
    def over3_check_func(row):
        if (row['advertising_id'] == row['c_advertising_id'])&(row['idfa'] == row['c_idfa'])&(row['event_name'] == row['c_event_name']):
            return (row['c_event_time'] - row['event_time']).total_seconds()
        else:
            return datetime.timedelta(0).total_seconds()
    over3_check['time_diff'] = over3_check.apply(over3_check_func, axis=1)
    deductioin_idfa = over3_check.loc[(over3_check['time_diff'] < 60)&(over3_check['time_diff'] > 0)].idfa.unique()
    df.loc[df['idfa'].isin(deductioin_idfa), 'con9_over3(reattr)'] = 1
    # con9_over3(reattr)

    lastm_date = datetime.datetime.strptime(required_date, '%Y-%m-%d').replace(day=1)
    lastm_date = lastm_date - datetime.timedelta(days=1)
    lastm_df = deduction_data(raw_dir, lastm_date.strftime('%Y-%m-%d'))
    lastm_over3_check_df = lastm_df[['advertising_id','idfa','event_name','event_time']].sort_values(['advertising_id','idfa','event_name','event_time'])
    lastm_over3_check_df = lastm_over3_check_df.drop(lastm_over3_check_df[lastm_over3_check_df['idfa'] == ''].index).reset_index(drop=True)
    lastm_over3_comp_df = lastm_over3_check_df.rename(columns={'advertising_id':'c_advertising_id','idfa':'c_idfa','event_name':'c_event_name','event_time':'c_event_time'}).iloc[2:,:].reset_index(drop=True)
    lastm_over3_check = pd.concat([lastm_over3_check_df, lastm_over3_comp_df], axis=1)
    lastm_over3_check['time_diff'] = over3_check.apply(over3_check_func, axis=1)
    lastm_deductioin_idfa = lastm_over3_check.loc[(lastm_over3_check['time_diff'] < 60)&(lastm_over3_check['time_diff'] > 0)].idfa.unique()
    df.loc[df['idfa'].isin(lastm_deductioin_idfa), 'con10_lastm'] = 1
    # con10_lastm

    df['ITET'] = df['event_time'] - df['install_time']
    df['Date'] = df['event_time'].apply(lambda x: x.date())

    df.index = range(0, len(df))
    fraud_columns = ['con1_event','con2_country','con3_language','con4_carrier','con5_device','con6_osv','con7_appv','con8_sdkv','con9_over3(reattr)','con10_lastm']
    df.loc[df[fraud_columns].values.sum(axis = 1)>=1, 'is_fraud'] = 1
    df['is_fraud'] = df['is_fraud'].fillna(0)
    result_df = df[['ITET','Date','is_fraud','attributed_touch_type','event_name','media_source','campaign','adset','adset_id',
                    'carrier','language','country_code','platform','os_version','app_version','sdk_version','device_model','device_model_name',
                    'con1_event','con2_country','con3_language','con4_carrier','con5_device','con6_osv','con7_appv','con8_sdkv','con9_over3(reattr)',
                    'con10_lastm','idfa','advertising_id']]
    result_df.to_csv(result_dir + '/' + file_name, index=False, encoding = 'utf-8-sig')

required_date = '2022-09-12'
df = deduction_data(raw_dir, required_date)
file_name = f'디덕션 적용_{required_date.replace("-","")}.csv'
deduction_calculate(required_date, df, file_name)
