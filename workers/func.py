import pandas as pd
import numpy as np
import re
import json


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
                uid_dict[pid] = user_name
                num += 1

    return pid_dict