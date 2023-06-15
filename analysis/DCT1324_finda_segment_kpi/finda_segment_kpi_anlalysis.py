import datetime
import pyarrow as pa
import pandas as pd
import os

from setting import directory as dr
from workers import read_data
from workers.func import segment_analysis


def read_organic():
    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if ('in-app-events' in f)]

        dtypes = {
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Customer User ID': pa.string()}

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
        return data

    ios = read_file('ios')
    aos = read_file('aos')
    opened_app = read_file('Opened Finda App')

    organic_data = pd.concat([ios, aos, opened_app])
    organic_data['Event Time'] = pd.to_datetime(organic_data['Event Time'])
    organic_data = organic_data.drop_duplicates()

    return organic_data


def read_paid():
    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism_2'
    file_list = os.listdir(file_dir)
    file_list = [file for file in file_list if '.csv' in file]

    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'campaign': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source': pa.string(),
        'advertising_id': pa.string(),
        'customer_user_id': pa.string(),
        'idfa': pa.string()}

    data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    data = data.sort_values('event_time')

    data['event_time'] = pd.to_datetime(data['event_time'])
    paid_data = data.drop_duplicates()

    return paid_data


def read_addition(detarget_dir, data_list):
    addition_data = pd.DataFrame()
    for info_dict in data_list:
        file_dir = detarget_dir + info_dict['file_dir']
        file_list = os.listdir(file_dir)
        file_list = [file for file in file_list if '.csv' in file]
        column_dict = info_dict['column_dict']
        dtypes = {}
        for column in column_dict.keys():
            dtypes[column] = pa.string()
        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
        data['is_paid'] = True
        data = data.rename(columns=column_dict).drop_duplicates()
        data = data.sort_values('event_time')
        data['event_time'] = pd.to_datetime(data['event_time'])

        addition_data = pd.concat([addition_data, data]).reset_index(drop=True)
    return addition_data


def kpi_analysis_prep(total_data, kpi_event, conversion_event):
    file_path = detarget_dir + '/detarget_list.txt'
    setting_dict = eval(open(file_path, 'r', encoding='utf-8-sig').read())
    event_dict = setting_dict.pop('event_dict')
    total_data = total_data.drop_duplicates().sort_values(['appsflyer_id', 'event_time']).reset_index(drop=True)

    kpi_list = []
    for kpi in kpi_event:
        kpi_data = total_data.loc[
            total_data['event_name'] == kpi, ['appsflyer_id', 'event_time']].rename(
            columns={'event_time': 'kpi_time'})
        merged_data = total_data.merge(kpi_data, how='left', on=['appsflyer_id'])
        merged_data['time_gap'] = merged_data['kpi_time'] - merged_data['event_time']
        kpi_name = f'{kpi}_achievement'
        kpi_list.append(kpi_name)
        merged_data.loc[merged_data['time_gap'] > datetime.timedelta(0), kpi_name] = True
        merged_data[kpi_name] = merged_data[kpi_name].fillna(False)
        merged_data = merged_data.sort_values(['appsflyer_id', 'event_time', kpi_name], ascending=False)
        col_list = list(merged_data.columns)
        col_list.remove('time_gap')
        col_list.remove('kpi_time')
        merged_data = merged_data[col_list]
        col_list.remove(kpi_name)
        total_data = merged_data.drop_duplicates(col_list, keep='first')

    column_list = ['appsflyer_id', 'conversion_date', 'is_paid', 'conversion_time'] + kpi_list
    daily_segment_analysis = segment_analysis(total_data, event_dict, conversion_event, column_list)
    kpi_analysis_df = daily_segment_analysis.do_work()

    return kpi_analysis_df


detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
organic_data = read_organic()
organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]
organic_data['is_paid'] = False
paid_data = read_paid()
paid_data['is_paid'] = True
total_data = pd.concat([organic_data, paid_data]).reset_index(drop=True)
total_data = total_data.loc[total_data['event_time'] < '2023-06-01']
kpi_event = ['loan_contract_completed', 'Viewed LA Home']
conversion_event = ['Opened Finda App']
del organic_data
del paid_data
total_data = total_data[['appsflyer_id', 'event_name', 'is_paid', 'event_time']]
kpi_analysis_df = kpi_analysis_prep(total_data, kpi_event, conversion_event)
kpi_analysis_df.to_csv(dr.download_dir + '/kpi_analysis_df.csv', index=False, encoding='utf-8-sig')

