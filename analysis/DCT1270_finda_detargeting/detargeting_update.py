import datetime
from dateutil.relativedelta import relativedelta
import pyarrow as pa
import pandas as pd
import os

from setting import directory as dr
from workers import read_data
from workers.func import segment_analysis
from spreadsheet import spreadsheet


def read_organic(from_date):
    yearmonth_list = [(from_date + relativedelta(months=i)).strftime("%Y-%m") for i in range(4)]

    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if any([yearmonth in f for yearmonth in yearmonth_list])]

        dtypes = {
            'Install Time': pa.string(),
            'Attributed Touch Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Customer User ID': pa.string(),
            'Advertising ID': pa.string(),
            'IDFA': pa.string()}

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
        return data

    ios = read_file('ios')
    aos = read_file('aos')
    opened_app = read_file('Opened Finda App')

    organic_data = pd.concat([ios, aos, opened_app]).reset_index(drop=True)

    organic_data.loc[organic_data['Advertising ID'] == '', 'Advertising ID'] = organic_data['IDFA']
    organic_data['Event Time'] = pd.to_datetime(organic_data['Event Time'])
    organic_data['Attributed Touch Time'] = pd.to_datetime(organic_data['Attributed Touch Time'])
    organic_data = organic_data.loc[organic_data['Event Time'] >= datetime.datetime(year=2022, month=7, day=1)]

    return organic_data


def read_paid(from_date):
    yearmonth_list = [(from_date + relativedelta(months=i)).strftime("%Y%m") for i in range(4)]

    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism_2'
    file_list = os.listdir(file_dir)
    file_list = [f for f in file_list if ('.csv' in f) & (any([yearmonth in f for yearmonth in yearmonth_list]))]

    dtypes = {
        'install_time': pa.string(),
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
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

    data.loc[data['advertising_id'] == '', 'advertising_id'] = data['idfa']
    data = data.drop(columns='idfa')
    data['event_time'] = pd.to_datetime(data['event_time'])
    data['attributed_touch_time'] = pd.to_datetime(data['attributed_touch_time'])
    paid_data = data.drop_duplicates()

    return paid_data


def read_addition(detarget_dir, data_list):
    addition_data = pd.DataFrame()
    for info_dict in data_list:
        file_dir = detarget_dir + info_dict['file_dir']
        file_list = os.listdir(file_dir)
        file_list = [file for file in file_list if '.csv' in file]
        column_dict = info_dict['column_dict']
        column_dict['IDFA'] = 'idfa'
        dtypes = {}
        for column in column_dict.keys():
            dtypes[column] = pa.string()

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
        data = data.rename(columns=column_dict).drop_duplicates()
        data = data.sort_values('event_time')
        data['event_time'] = pd.to_datetime(data['event_time'])
        data['attributed_touch_time'] = pd.to_datetime(data['attributed_touch_time'])

        addition_data = pd.concat([addition_data, data]).reset_index(drop=True)
        addition_data.loc[addition_data['advertising_id'] == '', 'advertising_id'] = addition_data['idfa']
        addition_data = addition_data.drop(columns='idfa')

    return addition_data


def detargeting_inspection(total_data, today, from_date):
    # 디타겟 세그먼트 셋팅
    detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
    file_path = detarget_dir + '/detarget_list_update.txt'
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
    daily_previous_df = pd.read_csv(rd_dir + '/daily_segment_analysis_df_all.csv', encoding='utf-8-sig')

    if today is None:
        to_date = datetime.datetime.today()
    else:
        to_date = today
    total_data = total_data.loc[total_data['event_time'] < to_date]
    last_day = datetime.datetime.strptime(max(daily_previous_df['conversion_date']), '%Y-%m-%d')
    # last_day = datetime.datetime.strptime('2023-01-01', '%Y-%m-%d')
    from_date = last_day.replace(day=1) - relativedelta(months=3)
    cropped_total_data = total_data.loc[total_data['event_time'] >= from_date]

    conversion_event = ['Opened Finda App', 're-engagement', 're-attribution']
    column_list = ['campaign', 'media_source', 'advertising_id', 'appsflyer_id', 'customer_user_id', 'conversion_date', 'conversion_time']
    daily_segment_analysis = segment_analysis(total_data, event_dict, conversion_event, column_list, detarget_dict, media_list)
    daily_segment_analysis_df = daily_segment_analysis.do_work()
    daily_for_download = daily_segment_analysis_df
    daily_for_update = daily_segment_analysis_df.loc[daily_segment_analysis_df['conversion_date'] >= last_day.strftime('%Y-%m-%d')]
    daily_previous_df = daily_previous_df.loc[daily_previous_df['conversion_date'] < last_day.strftime('%Y-%m-%d')]
    daily_for_download = pd.concat([daily_previous_df, daily_for_update], ignore_index=True)
    daily_for_download = daily_for_download.loc[daily_for_download['conversion_date'] >= from_date.strftime('%Y-%m-%d')]
    daily_for_download = daily_for_download.replace(True, 1)
    daily_for_download = daily_for_download.replace(False, 0)

    # 운영 여부 확인 컬럼 추가
    spread_sheet_url = 'https://docs.google.com/spreadsheets/d/1OXlBSaK5km6YHxHdvZtgm2nNK033Pjh7dPoA0gBzISA/edit#gid=797669622'
    sheet_name = 'RE 캠페인 리스트'
    doc = spreadsheet.spread_document_read(spread_sheet_url)
    setting_df = spreadsheet.spread_sheet(doc, sheet_name).reset_index(drop=True)
    campaign_list = setting_df.loc[:, '캠페인명']
    daily_for_download.loc[daily_for_download['campaign'].isin(campaign_list), 'is_operating'] = 1
    daily_for_download['is_operating'] = daily_for_download['is_operating'].fillna(0)
    daily_for_download.to_csv(rd_dir + '/daily_segment_analysis_df_all.csv', index=False, encoding='utf-8-sig')

    return daily_for_download


# 업데이트 기준 날짜 (ex. 2023년 4월 30일까지 업데이트 시 > 2023-05-01 기재)
today = datetime.datetime.strptime('2023-08-01', '%Y-%m-%d')
from_date = today.replace(day=1) - relativedelta(months=3)
organic_data = read_organic(from_date)
organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]
paid_data = read_paid(from_date)
paid_data = paid_data.loc[paid_data['attributed_touch_type'] == 'click']
total_data = pd.concat([organic_data, paid_data]).reset_index(drop=True)
daily_df = detargeting_inspection(total_data, today, from_date)
