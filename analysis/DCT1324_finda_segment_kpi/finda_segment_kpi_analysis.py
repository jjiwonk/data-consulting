import datetime
import pyarrow as pa
import pandas as pd
import os

from setting import directory as dr
from workers import read_data


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


detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
organic_data = read_organic()
organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]
paid_data = read_paid()
paid_data.loc[paid_data['event_name'] == 'install', 'event_name'] = 'paid_install'
total_data = pd.concat([organic_data, paid_data]).reset_index(drop=True)
total_data = total_data.loc[total_data['event_time'] < '2023-06-01']
# total_data = total_data.loc[~(total_data['event_name'].isin(['loan_contract_completed', 'loan_contract_completed_fee', 'Click LA Apply Btn']))]
total_data = total_data.sort_values(['appsflyer_id', 'event_time', 'event_name', 'install_time'])
total_data = total_data.drop_duplicates(['appsflyer_id', 'event_time'], keep='last').reset_index(drop=True)
total_data['advertising_id'] = total_data['advertising_id'].fillna('')

kpi_event = 'loan_contract_completed'
achieve_period = 24*60*60
user_arr = total_data['appsflyer_id']
adid_arr = total_data['advertising_id']
event_arr = total_data['event_name']
event_time_arr = total_data['event_time']


# 1. 퍼널(세션) 데이터 생성 > 컬럼: appsflyer_id, advertising_id, funnel, start_time, end_time, is_paid, kpi_achievement
class FunnelDataGenerator():
    def __init__(self, user_array, adid_array, event_array, event_time_array, kpi_event, achieve_period):
        self.num = 0
        self.funnel_id = 'funnel ' + str(self.num)
        self.kpi_event = kpi_event
        self.achieve_period = achieve_period

        self.array_list = {
            'user': user_array,
            'adid': adid_array,
            'event': event_array,
            'event_time': event_time_array
        }

        self.start_time = self.array_list['event_time'][0]
        self.end_time = None
        self.current_event_time = None

        self.current_user = None
        self.before_user = self.array_list['user'][0]

        self.before_adid = self.array_list['adid'][0]
        if self.before_adid != '':
            self.adid_list = [].append(self.before_adid)
        else:
            self.adid_list = []

        self.current_event = None
        self.before_event = self.array_list['event'][0]
        self.funnel_sequence = [self.before_event]
        self.kpi_achievement = False
        self.is_paid = False

        self.data = []
        self.column_names = ['user_id', 'funnel_id', 'funnel_sequence', 'start_time', 'end_time', 'advertising_id', 'kpi_achievement', 'is_paid']

    def start_new_funnel(self):
        self.num += 1
        self.funnel_sequence = []
        self.funnel_id = 'session ' + str(self.num)
        self.funnel_sequence.append(self.current_event)
        self.start_time = self.current_event_time
        self.kpi_achievement = False
        self.is_paid = False

    def append_row(self):
        self.funnel_sequence.append('funnel_end')
        self.adid_list = list(set(self.adid_list))
        row = [self.before_user, self.funnel_id, self.funnel_sequence,
               self.start_time, self.end_time, self.adid_list, self.kpi_achievement, self.is_paid]
        self.data.append(row)

    def discriminator(self):
        # 현재 유저가 이전 유저와 같은 경우
        if self.current_user == self.before_user:
            # 현재 이벤트가 kpi 이벤트라면 퍼널이 종료되고 새로운 퍼널 시작
            if self.before_event == self.kpi_event:
                if (self.end_time - self.start_time) <= datetime.timedelta(seconds=self.achieve_period):
                    self.kpi_achievement = True
                self.append_row()
                self.start_new_funnel()

            else:  # 현재 유저와 이전 유저가 같으면서 현재 이벤트가 kpi 이벤트가 아니기 때문에 시퀀스 정보만 추가
                self.funnel_sequence.append(self.current_event)

        else:
            # 유저 정보가 같지 않다면 새로운 퍼널이 시작된 것
            self.append_row()
            self.start_new_funnel()

    def do_work(self):
        for i, user in enumerate(self.array_list['user']):
            if i == 0:
                pass
            else:
                self.current_event_time = self.array_list['event_time'][i]
                self.end_time = self.array_list['event_time'][i - 1]

                self.current_user = self.array_list['user'][i]
                self.before_user = self.array_list['user'][i - 1]

                self.before_adid = self.array_list['adid'][i - 1]
                if self.before_adid != '':
                    self.adid_list.append(self.before_adid)

                self.current_event = self.array_list['event'][i]
                self.before_event = self.array_list['event'][i - 1]
                if self.before_event in ['paid_install', 're-engagement', 're-attribution']:
                    self.is_paid = True

                self.discriminator()

        self.data = pd.DataFrame(data=self.data, columns=self.column_names)


funnel_generator = FunnelDataGenerator(user_arr, adid_arr, event_arr, event_time_arr, kpi_event, achieve_period)
funnel_generator.do_work()
funnel_data = funnel_generator.data


# 2. 디타겟 대상 여부 라벨링
# 이벤트 기여기간 기준
file_path = detarget_dir + '/detarget_list.txt'
setting_dict = eval(open(file_path, 'r', encoding='utf-8-sig').read())
event_dict = setting_dict.pop('event_dict')

result_data = funnel_data.copy()
for event in event_dict.keys():
    event_df = total_data.loc[total_data['event_name'] == event].reset_index(drop=True)
    seg_name = event_dict[event]['seg_name']
    target_period = event_dict[event]['period']
    event_df['segment'] = seg_name
    event_df = event_df.drop_duplicates(['appsflyer_id', 'event_time'])
    segment_df = event_df[['event_time', 'segment', 'appsflyer_id']]
    col_name = f'{seg_name}_in_{str(target_period)}_days'
    merge_data = funnel_data.merge(segment_df, left_on='user_id', right_on='appsflyer_id', how='left')
    merge_data['time_gap'] = (merge_data['start_time'] - merge_data['event_time']).dt.days

    merge_data.loc[(merge_data['time_gap'] <= target_period) & (merge_data['time_gap'] > 0), col_name] = True

    merge_data[col_name] = merge_data[col_name].fillna(False)
    merge_data = merge_data[['user_id', 'start_time', col_name]].sort_values(
        ['user_id', 'start_time', col_name], ascending=False)
    merge_data = merge_data.drop_duplicates(['user_id', 'start_time'], keep='first')

    result_data = result_data.merge(merge_data, on=['user_id', 'start_time'], how='left')

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
        merge_data.loc[(merge_data['advertising_id'].isin(adid_list) & (merge_data['conversion_date'] >= start_date) & (merge_data['conversion_date'] <= end_date)), detarget] = True
    merge_data_dedup = merge_data[['appsflyer_id', 'conversion_time', detarget]]

    result_data = result_data.merge(merge_data_dedup, on=['appsflyer_id', 'conversion_time'], how='left')
    result_data[detarget] = result_data[detarget].fillna(False)


# 3. 추출 후 태블로에서 계산
result_data.to_csv(dr.download_dir + '/kpi_funnel_analysis_df.csv', index=False, encoding='utf-8-sig')



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

