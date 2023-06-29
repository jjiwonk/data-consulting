import datetime
import pyarrow as pa
import pandas as pd
import os

from setting import directory as dr
from workers import read_data
from workers.func import FunnelDataGenerator


def read_organic():
    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if ('in-app-events' in f)]

        dtypes = {
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Event Revenue': pa.string(),
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
    organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]

    return organic_data


def read_paid():
    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism_2'
    file_list = os.listdir(file_dir)
    file_list = [file for file in file_list if '.csv' in file]

    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_revenue': pa.string(),
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
    paid_data.loc[paid_data['event_name'] == 'install', 'event_name'] = 'paid_install'

    return paid_data


def detarget_labeling(funnel_data, total_data):
    detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
    file_path = detarget_dir + '/detarget_list.txt'
    setting_dict = eval(open(file_path, 'r', encoding='utf-8-sig').read())
    event_dict = setting_dict.pop('event_dict')

    result_data = funnel_data.copy()
    for event in event_dict.keys():
        event_df = total_data.loc[total_data['event_name'] == event].reset_index(drop=True)
        seg_name = event_dict[event]['seg_name']
        target_period = event_dict[event]['period']
        event_df['segment'] = seg_name
        event_df = event_df.drop_duplicates(['advertising_id', 'event_time'])
        segment_df = event_df[['event_time', 'segment', 'advertising_id']]
        col_name = f'{seg_name}_in_{str(target_period)}_days'
        merge_data = funnel_data.merge(segment_df, on='advertising_id', how='left')
        merge_data['time_gap'] = (merge_data['start_time'] - merge_data['event_time']).dt.days

        merge_data.loc[(merge_data['time_gap'] <= target_period) & (merge_data['time_gap'] > 0), col_name] = True

        merge_data[col_name] = merge_data[col_name].fillna(False)
        merge_data = merge_data[['advertising_id', 'start_time', col_name]].sort_values(
            ['advertising_id', 'start_time', col_name], ascending=False)
        merge_data = merge_data.drop_duplicates(['advertising_id', 'start_time'], keep='first')

        result_data = result_data.merge(merge_data, on=['advertising_id', 'start_time'], how='left')

    # ADID 기준
    detarget_df = pd.read_csv(detarget_dir + setting_dict.pop('file_name')).drop_duplicates()
    detarget_dict = {}
    for type in detarget_df.data_type.unique():
        detarget_dict[type] = detarget_df.loc[detarget_df['data_type'] == type].reset_index(drop=True)
    for detarget in detarget_dict.keys():
        detarget_df = detarget_dict[detarget]
        date_df = detarget_df[['start_date', 'end_date']].drop_duplicates().reset_index(drop=True)
        merge_data = result_data.copy()
        for i in range(len(date_df)):
            start_date = date_df.loc[i, 'start_date']
            end_date = date_df.loc[i, 'end_date']
            adid_list = detarget_df.loc[
                (detarget_df['data_type'] == detarget) & (detarget_df['start_date'] == start_date) & (
                        detarget_df['end_date'] == end_date), 'advertising_id'].drop_duplicates().to_list()
            merge_data.loc[(merge_data['advertising_id'].isin(adid_list)
                            & (merge_data['start_time'] >= pd.to_datetime(start_date))
                            & (merge_data['start_time'] <= pd.to_datetime(end_date))), detarget] = True
        merge_data_dedup = merge_data[['advertising_id', 'start_time', detarget]]

        result_data = result_data.merge(merge_data_dedup, on=['advertising_id', 'start_time'], how='left')
        result_data[detarget] = result_data[detarget].fillna(False)

    return result_data


def data_prep(total_data):
    total_data['advertising_id'] = total_data['advertising_id'].fillna('')
    total_data = total_data.loc[total_data['advertising_id'] != '']
    total_data = total_data.loc[~((total_data['event_name'].isin(['re-engagement', 're-attribution'])) &
                                  ~(total_data['media_source'].isin(
                                      ['appier_int', 'cauly_int', 'remerge_int', 'rtbhouse_int', 'kakao_int',
                                       'googleadwords_int'])))]
    total_data.loc[total_data['event_name'] == 'loan_contract_completed_fee'].event_time.sort_values()  # 매출 데이터 4월부터 기록됨
    cropped_total_data = total_data.loc[(total_data['event_time'] < '2023-06-01') & (total_data['event_time'] >= '2023-04-01')]
    cropped_total_data = cropped_total_data.sort_values(['advertising_id', 'event_time', 'event_name', 'install_time'])
    cropped_total_data = cropped_total_data.drop_duplicates(['advertising_id', 'event_time'], keep='last').reset_index(drop=True)
    cropped_total_data['event_revenue'] = cropped_total_data['event_revenue'].fillna(0)
    paid_adid_list = cropped_total_data.loc[(cropped_total_data['event_name'].isin(['re-engagement', 're-attribution'])), 'advertising_id'].unique()
    cropped_total_data = cropped_total_data.loc[cropped_total_data['advertising_id'].isin(paid_adid_list)].reset_index(drop=True)
    return cropped_total_data


organic_data = read_organic()
paid_data = read_paid()
total_data = pd.concat([organic_data, paid_data]).reset_index(drop=True)
cropped_total_data = data_prep(total_data)

user_arr = cropped_total_data['advertising_id']
event_arr = cropped_total_data['event_name']
event_time_arr = cropped_total_data['event_time']
value_arr = cropped_total_data['event_revenue']
kpi_event = 'loan_contract_completed'
kpi_period = 14*24*60*60
paid_events = ['re-engagement', 're-attribution']

# 1. 퍼널(세션) 데이터 생성
# 컬럼: advertising_id, funnel_id, 'funnel_sequence', start_time, end_time, is_paid, kpi_achievement, fee_value
funnel_generator = FunnelDataGenerator(user_arr, event_arr, event_time_arr, value_arr, kpi_event, kpi_period, paid_events)
funnel_generator.do_work()
funnel_data = funnel_generator.data
funnel_data = funnel_data.rename(columns={'user_id':'advertising_id'})

# 2. 디타겟 대상 여부 라벨링
result_data = detarget_labeling(funnel_data, total_data)

# 3. 추출 후 태블로에서 계산
result_data = result_data.loc[result_data['advertising_id'] != '00000000-0000-0000-0000-000000000000']
result_data = result_data.sort_values(['advertising_id', 'start_time'])
result_data.to_csv(dr.download_dir + '/kpi_funnel_analysis_df_14days_all.csv', index=False, encoding='utf-8-sig')





# 백업 로그
# def kpi_analysis_prep(total_data, kpi_event, conversion_event):
#     file_path = detarget_dir + '/detarget_list.txt'
#     setting_dict = eval(open(file_path, 'r', encoding='utf-8-sig').read())
#     event_dict = setting_dict.pop('event_dict')
#     total_data = total_data.drop_duplicates().sort_values(['appsflyer_id', 'event_time']).reset_index(drop=True)
#
#     kpi_list = []
#     for kpi in kpi_event:
#         kpi_data = total_data.loc[
#             total_data['event_name'] == kpi, ['appsflyer_id', 'event_time']].rename(
#             columns={'event_time': 'kpi_time'})
#         merged_data = total_data.merge(kpi_data, how='left', on=['appsflyer_id'])
#         merged_data['time_gap'] = merged_data['kpi_time'] - merged_data['event_time']
#         kpi_name = f'{kpi}_achievement'
#         kpi_list.append(kpi_name)
#         merged_data.loc[merged_data['time_gap'] > datetime.timedelta(0), kpi_name] = True
#         merged_data[kpi_name] = merged_data[kpi_name].fillna(False)
#         merged_data = merged_data.sort_values(['appsflyer_id', 'event_time', kpi_name], ascending=False)
#         col_list = list(merged_data.columns)
#         col_list.remove('time_gap')
#         col_list.remove('kpi_time')
#         merged_data = merged_data[col_list]
#         col_list.remove(kpi_name)
#         total_data = merged_data.drop_duplicates(col_list, keep='first')
#
#     column_list = ['appsflyer_id', 'conversion_date', 'is_paid', 'conversion_time'] + kpi_list
#     daily_segment_analysis = segment_analysis(total_data, event_dict, conversion_event, column_list)
#     kpi_analysis_df = daily_segment_analysis.do_work()
#
#     return kpi_analysis_df
# kpi_event = ['loan_contract_completed', 'Viewed LA Home']
# conversion_event = ['Opened Finda App']
# del organic_data
# del paid_data
# total_data = total_data[['appsflyer_id', 'event_name', 'is_paid', 'event_time']]
# kpi_analysis_df = kpi_analysis_prep(total_data, kpi_event, conversion_event)
# kpi_analysis_df.to_csv(dr.download_dir + '/kpi_analysis_df.csv', index=False, encoding='utf-8-sig')

