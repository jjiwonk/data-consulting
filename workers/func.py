import pandas as pd
import numpy as np
import re
import json
import datetime


class SessionDataGenerator():
    def __init__(self, user_array, event_array, event_time_array, time_gap_array, value_array):
        self.num = 0
        self.session_id = 'session ' + str(self.num)

        self.array_list = {
            'user': user_array,
            'event': event_array,
            'event_time': event_time_array,
            'time_gap': time_gap_array,
            'value' : value_array
        }

        self.first_session_time = self.array_list['event_time'][0]
        self.last_session_time = None

        self.current_event_time = None
        self.before_event_time = self.array_list['event_time'][0]

        self.current_event_value = None
        self.before_event_value = self.array_list['value'][0]

        self.total_value = self.before_event_value

        self.time_gap = self.array_list['time_gap'][0]

        self.current_user = None
        self.before_user = self.array_list['user'][0]

        self.current_event = None
        self.before_event = self.array_list['event'][0]
        self.session_sequence = [self.before_event]
        self.data = []
        self.column_names = ['user_id', 'session_id', 'session_sequence', 'first_session_time', 'last_session_time', 'value']

    def start_new_session(self):
        self.num += 1
        self.session_sequence = []
        self.session_id = 'session ' + str(self.num)
        self.session_sequence.append(self.current_event)
        self.total_value = self.current_event_value
        self.first_session_time = self.current_event_time

    def append_row(self):
        self.session_sequence.append('session_end')
        self.last_session_time = self.before_event_time
        row = [self.before_user, self.session_id, self.session_sequence,
               self.first_session_time, self.last_session_time, self.total_value]
        self.data.append(row)

    def discriminator(self):
        # 현재 유저가 이전 유저와 같은 경우
        if self.current_user == self.before_user:
            # 이전 이벤트와의 time_gap이 30분 이상이라면 세션이 종료되고 새로운 세션이 시작된 것
            if self.time_gap > 1800:
                self.append_row()
                self.start_new_session()

            else:  # 현재 유저와 이전 유저가 같으면서 time_gap이 30분을 넘지 않았기 때문에 시퀀스 정보만 추가
                self.session_sequence.append(self.current_event)
                self.total_value = self.total_value + self.current_event_value

        else:
            # 유저 정보가 같지 않다면 새로운 세션이 시작된 것
            self.append_row()
            self.start_new_session()

    def do_work(self):
        for i, user in enumerate(self.array_list['user']):
            if i == 0:
                pass
            else:
                self.current_event_time = self.array_list['event_time'][i]
                self.before_event_time = self.array_list['event_time'][i - 1]

                self.time_gap = self.array_list['time_gap'][i - 1]

                self.current_user = self.array_list['user'][i]
                self.before_user = self.array_list['user'][i - 1]

                self.current_event = self.array_list['event'][i]
                self.current_event_value = int(self.array_list['value'][i])

                self.discriminator()

        self.data = pd.DataFrame(data=self.data, columns=self.column_names)


class SankeyModeling():
    def __init__(self, raw_data, funnel_list, end_sequence, sequence_column_name, destination, file_name = 'sankey_data.xlsx', sep = ' > '):
        self.data = raw_data
        self.model = None

        self.funnel_list = funnel_list
        self.end_sequence = end_sequence
        self.sequence_column_name = sequence_column_name

        self.destination = destination
        self.file_name = file_name
        self.sep = sep
        self.step_column_list = ['Step ' + str(i + 1) for i in range(len(self.funnel_list))]

    def define_pat_list(self):
        pat_list = []
        for i in range(len(self.funnel_list)) :
            pat = re.compile('^' + '.*'.join(self.funnel_list[:i+1]))
            pat_list.append(pat)
        pat_list.reverse()
        return pat_list

    def sankey_data(self):
        pat_list = self.define_pat_list()
        sequence_column_name = self.sequence_column_name
        raw_data = self.data.loc[self.data[sequence_column_name].str.contains(self.funnel_list[0])]
        step_column_list = self.step_column_list

        raw_data[step_column_list] = np.nan

        for idx, pat in enumerate(pat_list):
            max_depth = len(self.funnel_list)-idx

            if len((pd.isnull(raw_data[step_column_list[0]]) == True)) == 0:
                break

            match_index = (pd.isnull(raw_data[step_column_list[0]]))&(raw_data[sequence_column_name].str.contains(pat=pat, regex=True))

            for i in range(max_depth):
                raw_data.loc[match_index, step_column_list[i]] = self.funnel_list[i]

            if idx!=0:
                exit_pat = self.funnel_list[max_depth-1] + self.sep + self.end_sequence
                raw_data.loc[match_index &
                             (raw_data[sequence_column_name].str.contains(pat=exit_pat)), step_column_list[max_depth]] = 'Bound'
                raw_data.loc[match_index &
                             (pd.isnull(raw_data[step_column_list[max_depth]])), step_column_list[max_depth]] = 'Other Events'
        raw_data['Link'] = 'link'
        raw_data = raw_data.loc[raw_data['Step 1'] == self.funnel_list[0]]

        return raw_data

    def sankey_model(self):
        path_series = pd.Series(range(98))
        data = pd.DataFrame(path_series, columns = ['Path'])
        data['Link'] = 'link'
        data['Min or Max'] = data['Path'].apply(lambda x : 'Min' if x <= 48 else 'Max')
        data['t'] = data['Path'].apply(lambda x : -6 + (0.25) * x if x <= 48 else 6 - (0.25) * (x-49))

        return data

    def do_work(self):
        self.data = self.sankey_data()
        self.model = self.sankey_model()

    def sankey_to_excel(self):
        writer = pd.ExcelWriter(self.destination + '/' + self.file_name, engine='xlsxwriter')
        self.data.to_excel(writer, sheet_name='Data',index=False)
        self.model.to_excel(writer, sheet_name='Model',index=False)
        writer.close()


class EventValueParser():
    def __init__(self, data, value_column):
        self.keys = []
        self.data = data
        self.value_column = value_column
        self.row_list = []

    def data_parse(self):
        data = self.data.copy()
        data.index = range(len(data))
        data = data.reset_index()

        value_array = np.array(data[self.value_column])

        for value in value_array :
            try :
                row = json.loads(value)
                self.row_list.append(row)
            except :
                pass
        value_data = pd.DataFrame(self.row_list)
        return value_data


class segment_analysis():
    def __init__(self, raw_data: pd.DataFrame, event_dict: dict, conversion_event: list, column_list: list, detarget_dict: dict = None, media_list: list = None):
        self.raw_data = raw_data
        self.event_dict = event_dict
        if detarget_dict is None:
            self.detarget_dict = {}
        else:
            self.detarget_dict = detarget_dict
        self.media_list = media_list
        self.conversion_event = conversion_event
        self.column_list = column_list
        self.conversion_data = None
        self.result_data = None

    def update_conversion_data(self):
        df = self.raw_data.copy()

        base_data = df.loc[df['event_name'].isin(self.conversion_event)]
        base_data = base_data.rename(columns={'attributed_touch_time': 'conversion_time'})
        base_data['conversion_date'] = pd.to_datetime(base_data['conversion_time']).dt.strftime('%Y-%m-%d')
        base_data = base_data.drop_duplicates(['conversion_time', 'advertising_id', 'event_name'])
        base_data['Cnt'] = 1
        if self.media_list is not None:
            base_data = base_data.loc[base_data['media_source'].isin(self.media_list)]
        base_data_pivot = base_data.pivot_table(index=self.column_list,
                                                values='Cnt',
                                                aggfunc='sum').reset_index()

        self.conversion_data = base_data_pivot
        self.result_data = base_data_pivot

    def make_segment_dataset(self, target_event, seg_name):
        df = self.raw_data.copy()

        seg_df = df.loc[df['event_name'] == target_event]
        seg_df['segment'] = seg_name
        seg_df = seg_df.drop_duplicates(['advertising_id', 'event_time'])
        seg_df = seg_df[['event_time', 'segment', 'advertising_id']]

        return seg_df

    def segment_match(self):
        conversion_data = self.conversion_data.copy()

        for target_event in self.event_dict.keys() :
            seg_name = self.event_dict[target_event]['seg_name']
            target_period = self.event_dict[target_event]['period']

            segment_df = self.make_segment_dataset(target_event, seg_name)
            col_name = f'{seg_name}_in_{str(target_period)}_days'

            merge_data = conversion_data.merge(segment_df, on='advertising_id', how='left')
            merge_data['time_gap'] = (merge_data['conversion_time'] - merge_data['event_time']).dt.days

            merge_data.loc[(merge_data['time_gap'] < target_period) & (merge_data['time_gap'] > 0), col_name] = True

            merge_data[col_name] = merge_data[col_name].fillna(False)
            merge_data = merge_data[['advertising_id', 'conversion_time', col_name]].sort_values(['advertising_id', 'conversion_time', col_name], ascending=False)
            merge_data = merge_data.drop_duplicates(['advertising_id', 'conversion_time'], keep='first')

            result_data = self.result_data.merge(merge_data, on=['advertising_id', 'conversion_time'], how='left')
            result_data[col_name] = result_data[col_name].fillna(False)

            self.result_data = result_data

        if len(self.detarget_dict) != 0:
            for detarget in self.detarget_dict.keys():
                detarget_df = self.detarget_dict[detarget]
                date_df = detarget_df[['start_date', 'end_date']].drop_duplicates().reset_index(drop=True)
                merge_data = conversion_data.copy()
                for i in range(len(date_df)):
                    start_date = date_df.loc[i, 'start_date']
                    end_date = date_df.loc[i, 'end_date']
                    adid_list = detarget_df.loc[(detarget_df['data_type'] == detarget) & (detarget_df['start_date'] == start_date) & (detarget_df['end_date'] == end_date), 'advertising_id'].drop_duplicates().to_list()
                    merge_data.loc[(merge_data['advertising_id'].isin(adid_list) & (merge_data['conversion_date'] >= start_date)&(merge_data['conversion_date'] <= end_date)), detarget] = True
                merge_data_dedup = merge_data[['advertising_id', 'conversion_time', detarget]]

                result_data = self.result_data.merge(merge_data_dedup, on=['advertising_id', 'conversion_time'], how='left')
                result_data[detarget] = result_data[detarget].fillna(False)

                self.result_data = result_data

    def do_work(self):
        self.update_conversion_data()
        self.segment_match()
        num = -1 * (len(self.event_dict.keys()) + len(self.detarget_dict.keys()))
        detarget_columns = self.result_data.columns[num:]
        self.result_data.loc[self.result_data[detarget_columns].sum(axis=1) > 0, 'result'] = True
        self.result_data['result'] = self.result_data['result'].fillna(False)
        result_columns = list(self.result_data.columns)
        result_columns.remove('Cnt')
        result_columns.remove('conversion_time')
        self.result_data = self.result_data.pivot_table(index=result_columns, values='Cnt', aggfunc='sum').reset_index()

        return self.result_data


class FunnelDataGenerator():
    def __init__(self, user_array, event_array, event_time_array, value_array, kpi_event_name, funnel_period, paid_events):
        self.num = 0
        self.funnel_id = 'funnel ' + str(self.num)
        self.kpi_event = kpi_event_name
        self.funnel_period = datetime.timedelta(seconds=funnel_period)

        self.array_list = {
            'user': user_array,
            'event': event_array,
            'event_time': event_time_array,
            'value': value_array
        }

        self.start_time = self.array_list['event_time'][0]
        self.end_time = None
        self.current_event_time = None

        self.current_user = None
        self.before_user = self.array_list['user'][0]

        self.current_event = None
        self.before_event = self.array_list['event'][0]
        self.funnel_sequence = [self.before_event]
        self.kpi_achievement = False
        self.is_paid = False

        self.current_value = None
        self.before_value = self.array_list['value'][0]

        self.paid_events = paid_events
        self.data = []
        self.column_names = ['user_id', 'funnel_id', 'funnel_sequence', 'start_time', 'end_time', 'is_paid', 'kpi_achievement', 'value']

    def start_new_funnel(self):
        self.num += 1
        self.funnel_sequence = []
        self.funnel_id = 'funnel ' + str(self.num)
        self.funnel_sequence.append(self.current_event)
        self.start_time = self.current_event_time
        self.kpi_achievement = False
        self.is_paid = False

    def append_row(self):
        self.funnel_sequence.append('funnel_end')
        row = [self.before_user, self.funnel_id, self.funnel_sequence,
               self.start_time, self.end_time, self.is_paid, self.kpi_achievement, self.before_value]
        self.data.append(row)

    def discriminator(self):
        # 현재 유저가 이전 유저와 같은 경우
        if self.current_user == self.before_user:
            # 현재 이벤트가 kpi 이벤트 or 첫 이벤트와 시간차가 period 이상인 경우 퍼널이 종료되고 새로운 퍼널 시작
            time_gap = (self.current_event_time - self.start_time)
            if self.before_event == self.kpi_event:
                self.kpi_achievement = True
                self.append_row()
                self.start_new_funnel()
            elif time_gap >= self.funnel_period:
                self.append_row()
                self.start_new_funnel()
            else:  # 현재 유저와 이전 유저가 같으면서 현재 이벤트가 kpi 이벤트가 아니고 period 이상이 아니기 때문에 시퀀스 정보만 추가
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

                self.current_event = self.array_list['event'][i]
                self.before_event = self.array_list['event'][i - 1]
                if self.before_event in self.paid_events:
                    self.is_paid = True

                self.before_value = self.array_list['value'][i - 1]

                self.discriminator()

        self.data = pd.DataFrame(data=self.data, columns=self.column_names)


def user_identifier(df, platform_id, user_id):
    df = df.loc[df[user_id].str.len()>0]
    df = df.drop_duplicates([platform_id, user_id])

    pid_array = np.array(df[platform_id])
    pid_dict = {}

    uid_array = np.array(df[user_id])
    uid_dict = {}

    num = 1
    for idx, pid in enumerate(pid_array) :
        uid = uid_array[idx]

        if pid not in pid_dict.keys() :
            if uid in uid_dict.keys() :
                user_name = uid_dict.get(uid)
                pid_dict[pid] = user_name
            else :
                user_name = 'user ' + str(num)
                pid_dict[pid] = user_name
                uid_dict[uid] = user_name
                num += 1

    return pid_dict


def get_event_from_values(event_values, col_name):
    result_array = np.zeros(len(event_values)).astype(str)
    for i, data in enumerate(event_values):
        if len(data) >= 2000:
            if '"%s":' % col_name in data:
                split_data = data.split('"%s":' % col_name)[1]
                if split_data == '':
                    data = '{}'
                else:
                    if '}' in split_data:
                        data = '{"%s":' % col_name + split_data.split(',"')[0]
                        if data[-1:] == '"':
                            data = data + '}'
                        elif data[-1:] == '}':
                            pass
                    else:
                        data = '{"%s":' % col_name + split_data.split(',"')[0]
                        if data[-1:] == '"':
                            data = data + '}'
                        else:
                            data = data + '..."}'
            else:
                data = '{}'
        elif len(data) == 0:
            data = '{}'
        if col_name in json.loads(data.replace(">", '/').replace("'", ''), strict=False).keys():
            result_array[i] = json.loads(data)[col_name]
        else:
            result_array[i] = ''

    return result_array


def date_diff(df, user_id, date, day):
    df.sort_values(by=[user_id, date], inplace=True)
    df[date] = pd.to_datetime(df[date])
    df['diff_days'] = df.groupby(user_id)[date].diff().dt.days

    user_id_array = np.array(df[user_id])
    date_diff_array = np.array(df['diff_days'])

    before_user_id = user_id_array[0]
    total_date_diff = 0
    group_id = 0
    group_array = np.array(range(len(user_id_array)))
    result_array = np.array(range(len(user_id_array)))

    for idx, (user_id, date_diff) in enumerate(zip(user_id_array, date_diff_array)):
        if pd.isnull(date_diff):
            date_diff = 0

        total_date_diff += date_diff

        if before_user_id == user_id:
            if total_date_diff >= day:
                group_id += 1
                total_date_diff = 0
        else:
            total_date_diff = 0
            group_id = 0

        group_array[idx] = group_id
        result_array[idx] = total_date_diff
        before_user_id = user_id

    df['group'] = group_array
    df['result'] = result_array

    return df
