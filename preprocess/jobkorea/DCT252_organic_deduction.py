import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import setting.directory as dr
import setting.report_date as rdate
import datetime
import math

# 디렉토리
raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/ADID, IDFA'
result_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/잡코리아/디덕션'
# result_dir = dr.download_dir

# 전역변수
detarget_device = ['allwinner', 'GIONEE', 'HONOR', 'HUAWEI', 'Infinix', 'iQOO', 'Itel', 'OnePlus', 'OPPO', 'POCO',
                    'realme', 'Redmi', 'sharp', 'TECNO', 'TGnCo', 'tufen', 'umidigi', 'vivo', 'Xiaomi', 'ZTE',
                    'SMARTISAN', 'Lenovo', 'google']


def deduction_data(raw_dir, rdate_month):
    dtypes = {
        'Attributed Touch Time': pa.string(),
        'Attributed Touch Type': pa.string(),
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Media Source': pa.string(),
        'Country Code': pa.string(),
        'Language': pa.string(),
        'Carrier': pa.string(),
        'OS Version': pa.string(),
        'App Version': pa.string(),
        'SDK Version': pa.string(),
        'Device Model': pa.string(),
        'Advertising ID': pa.string(),
        'IDFA': pa.string(),
        'IDFV': pa.string(),
        'Android ID': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Is Retargeting': pa.string(),
        'Event Value': pa.string(),
        'Platform': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    f_dir = raw_dir + f'/organic_{rdate_month}'
    files = os.listdir(f_dir)
    raw_files = [f for f in files if '.csv' in f and 'in-app-events' in f]

    for f in raw_files:
        try:
            temp = pacsv.read_csv(f_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
        except Exception as e:
            print(f)
            print(e)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df = raw_df.rename(columns=
                           {'Attributed Touch Time': 'attributed_touch_time',
                            'Attributed Touch Type': 'attributed_touch_type',
                            'Install Time': 'install_time',
                            'Event Time': 'event_time',
                            'Event Name': 'event_name',
                            'Media Source': 'media_source',
                            'Campaign': 'campaign',
                            'AppsFlyer ID': 'appsflyer_id',
                            'Is Retargeting': 'is_retargeting',
                            'Event Value': 'event_value',
                            'Platform': 'platform',
                            'Country Code': 'country_code',
                            'Language': 'language',
                            'Carrier': 'carrier',
                            'OS Version': 'os_version',
                            'App Version': 'app_version',
                            'SDK Version': 'sdk_version',
                            'Device Model': 'device_model',
                            'Advertising ID': 'advertising_id',
                            'IDFA': 'idfa',
                            'IDFV': 'idfv',
                            'Android ID': 'android_id'
                            })
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    return raw_df


def deduction_calculate(required_date, df, file_name, detarget_device):
    df['ITET'] = df['event_time'] - df['install_time']
    df['Date'] = df['event_time'].apply(lambda x: x.date())

    df[['con1_event', 'con2_country', 'con3_language', 'con4_carrier', 'con5_device', 'con6_osv', 'con7_appv',
        'con8_sdkv', 'con9_over3(reattr)', 'con10_lastm']] = 0

    target_event = ['signup_all', 'business_signup_complete', 'job_apply_complete_mobile',
                      'job_apply_complete_homepage', 'resume_complete']
    df.loc[~(df['event_name'].isin(target_event)), 'con1_event'] = 1
    # con1_event

    df.loc[df['country_code'] != 'KR', 'con2_country'] = 1
    # con2_country

    df.loc[(df['platform'] == 'android') & (df['language'] != '한국어'), 'con3_language'] = 1
    # con3_language

    target_carrier = [x.lower() for x in ['SK Telecom', 'SKTelecom', 'SKT', 'KOR SK Telecom', 'KT', 'LGU+', 'LG U+', 'olleh']]
    df['carrier'] = df['carrier'].str.lower()
    df.loc[(df['platform'] == 'android') & ~(df['carrier'].isin(target_carrier)), 'con4_carrier'] = 1
    # con4_carrier

    detarget_device = [x.lower() for x in detarget_device]
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

    df.loc[:, 'con6_osv'] = df.apply(os_version, axis=1)
    # con6_osv

    def app_version(row):
        if row['platform'] == 'android':
            if int(row['app_version'].replace('.', '')) < 333:
                return 1
            else:
                return 0
        elif row['platform'] == 'ios':
            if int(row['app_version'].replace('.', '')) < 364:
                return 1
            else:
                return 0

    df.loc[:, 'con7_appv'] = df.apply(app_version, axis=1)
    # con7_appv

    def app_version(row):
        if row['platform'] == 'android':
            if int(row['sdk_version'].replace('.', '').replace('v', '')) < 614:
                return 1
            else:
                return 0
        elif row['platform'] == 'ios':
            if int(row['sdk_version'].replace('.', '').replace('v', '')) < 626:
                return 1
            else:
                return 0

    df.loc[:, 'con8_sdkv'] = df.apply(app_version, axis=1)
    # con8_sdkv

    def over3_check_func(df):
        over3_check_df = df[['advertising_id', 'idfa', 'event_name', 'event_time']].sort_values(
            ['advertising_id', 'idfa', 'event_name', 'event_time'])
        over3_check_df = over3_check_df.drop(over3_check_df[over3_check_df['idfa'] == ''].index).reset_index(drop=True)
        over3_comp_df = over3_check_df.rename(
            columns={'advertising_id': 'c_advertising_id', 'idfa': 'c_idfa', 'event_name': 'c_event_name',
                     'event_time': 'c_event_time'}).iloc[2:, :].reset_index(drop=True)
        over3_check = pd.concat([over3_check_df, over3_comp_df], axis=1)

        def over3_checking(row):
            if (row['advertising_id'] == row['c_advertising_id']) & (row['idfa'] == row['c_idfa']) & (
                    row['event_name'] == row['c_event_name']):
                return (row['c_event_time'] - row['event_time']).total_seconds()
            else:
                return datetime.timedelta(0).total_seconds()

        over3_check['time_diff'] = over3_check.apply(over3_checking, axis=1)
        deductioin_idfa = over3_check.loc[
            (over3_check['time_diff'] < 60) & (over3_check['time_diff'] > 0)].idfa.unique()
        return deductioin_idfa

    deductioin_idfa = over3_check_func(df)
    df.loc[df['idfa'].isin(deductioin_idfa), 'con9_over3(reattr)'] = 1
    # con9_over3(reattr)

    lastm_date = required_date.replace(day=1) - datetime.timedelta(days=1)
    lastm_df = deduction_data(raw_dir, str(lastm_date.month)+'월')
    lastm_deductioin_idfa = over3_check_func(lastm_df)
    df.loc[df['idfa'].isin(lastm_deductioin_idfa), 'con10_lastm'] = 1
    # con10_lastm

    df.index = range(0, len(df))
    fraud_columns = ['con1_event', 'con2_country', 'con3_language', 'con4_carrier', 'con5_device', 'con6_osv',
                     'con7_appv', 'con8_sdkv', 'con9_over3(reattr)', 'con10_lastm']
    df.loc[df[fraud_columns].values.sum(axis=1) >= 1, 'is_fraud'] = 1
    df['is_fraud'] = df['is_fraud'].fillna(0)
    result_df = df[
        ['ITET', 'Date', 'is_fraud', 'event_name', 'media_source', 'carrier', 'language', 'country_code', 'platform',
         'os_version', 'app_version', 'sdk_version', 'device_model', 'device_model_name',
         'con1_event', 'con2_country', 'con3_language', 'con4_carrier', 'con5_device', 'con6_osv', 'con7_appv',
         'con8_sdkv', 'con9_over3(reattr)', 'con10_lastm', 'idfa', 'advertising_id']]

    result_df.to_csv(result_dir + '/' + file_name, index=False, encoding='utf-8-sig')


required_date = rdate.day_1
df = deduction_data(raw_dir, rdate.month_name)
file_name = f'organic_{rdate.month_name} 디덕션 적용.csv'
deduction_calculate(required_date, df, file_name, detarget_device)

df = pd.read_csv(result_dir + '/organic_9월 디덕션 적용.csv', encoding='utf-8')
length = math.ceil(len(df)/1000000)
for i in range(length):
    start = i*1000000
    end = (i+1)*1000000
    if i != length:
        temp = df.loc[start:end, :]
    else:
        temp = df.loc[start:, :]
    temp.to_csv(result_dir + f'/organic_9월 디덕션 적용_{i+1}.csv', index=False, encoding='utf-8-sig')