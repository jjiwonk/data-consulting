import datetime
from dateutil.relativedelta import relativedelta
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
    organic_data = organic_data.loc[organic_data['Event Time'] >= datetime.datetime(year=2022, month=7, day=1)]

    return organic_data


def read_paid():
    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism_2'
    file_list = os.listdir(file_dir)
    file_list = [file for file in file_list if '.csv' in file]
    # add_file_dir = file_dir + '/(임시)Opened Finda App_0601_0607'
    # add_file_list = os.listdir(add_file_dir)
    # add_file_list = ['(임시)Opened Finda App_0601_0607/' + file for file in add_file_list if '.csv' in file]
    # file_list = file_list + add_file_list

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
        data = data.rename(columns=column_dict).drop_duplicates()
        data = data.sort_values('event_time')
        data['event_time'] = pd.to_datetime(data['event_time'])

        addition_data = pd.concat([addition_data, data]).reset_index(drop=True)
    return addition_data


def detargeting_inspection(total_data, today=None):
    # 디타겟 세그먼트 셋팅
    detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
    file_path = detarget_dir + '/detarget_list.txt'
    setting_dict = eval(open(file_path, 'r', encoding='utf-8-sig').read())
    media_list = setting_dict.pop('media_list')
    event_dict = setting_dict.pop('event_dict')
    detarget_df = pd.read_csv(detarget_dir + setting_dict.pop('file_name')).drop_duplicates()
    detarget_dict = {}
    for type in detarget_df.data_type.unique():
        detarget_dict[type] = detarget_df.loc[detarget_df['data_type'] == type].reset_index(drop=True)
    data_list = setting_dict.pop('data_list')
    addition_df = read_addition(detarget_dir, data_list)
    total_data = pd.concat([total_data, addition_df]).reset_index(drop=True)

    rd_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/result/핀다/retargeting_inspection'
    daily_previous_df = pd.read_csv(rd_dir + '/daily_segment_analysis_df.csv', encoding='utf-8-sig')
    # monthly_previous_df = pd.read_csv(rd_dir + '/monthly_segment_analysis_df.csv', encoding='utf-8-sig')

    if today is None:
        to_date = datetime.datetime.today()
    else:
        to_date = today
    total_data = total_data.loc[total_data['event_time'] < to_date]
    last_day = datetime.datetime.strptime(max(daily_previous_df['conversion_date']), '%Y-%m-%d')
    # last_day = datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')
    from_date = last_day.replace(day=1) - relativedelta(months=2)
    cropped_total_data = total_data.loc[total_data['event_time'] >= from_date]

    # last_month = last_day.strftime('%Y-%m')
    # monthly_segment_analysis = segment_analysis(cropped_total_data, event_dict, detarget_dict, media_list, 'month')
    # monthly_segment_analysis_df = monthly_segment_analysis.do_work()
    # monthly_columns = list(monthly_segment_analysis_df.columns)
    # monthly_columns.remove('advertising_id')
    # monthly_for_update = monthly_segment_analysis_df.loc[:, monthly_columns]
    # monthly_for_update = monthly_for_update.loc[monthly_for_update['conversion_date'] >= last_month]
    # monthly_previous_df = monthly_previous_df.loc[monthly_previous_df['conversion_date'] < last_month]
    # monthly_for_download = pd.concat([monthly_previous_df, monthly_for_update], ignore_index=True)
    # monthly_for_download.to_csv(rd_dir + '/monthly_segment_analysis_df.csv', index=False, encoding='utf-8-sig')

    daily_segment_analysis = segment_analysis(cropped_total_data, event_dict, detarget_dict, media_list)
    daily_segment_analysis_df = daily_segment_analysis.do_work()
    daily_for_update = daily_segment_analysis_df.loc[daily_segment_analysis_df['conversion_date'] >= last_day.strftime('%Y-%m-%d')]
    daily_previous_df = daily_previous_df.loc[daily_previous_df['conversion_date'] < last_day.strftime('%Y-%m-%d')]
    daily_for_download = pd.concat([daily_previous_df, daily_for_update], ignore_index=True)
    daily_for_download.to_csv(rd_dir + '/daily_segment_analysis_df.csv', index=False, encoding='utf-8-sig')

    return daily_for_download


organic_data = read_organic()
organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]
paid_data = read_paid()
total_data = pd.concat([organic_data, paid_data]).reset_index(drop=True)
# 업데이트 기준 날짜 (ex. 2023년 4월 30일까지 업데이트 시 > 2023-05-01 기재)
today = datetime.datetime.strptime('2023-06-11', '%Y-%m-%d')
daily_df = detargeting_inspection(total_data, today)
