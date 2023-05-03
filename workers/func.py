import pandas as pd
import numpy as np


class SessionDataGenerator():
    def __init__(self, user_array, event_array, event_time_array, time_gap_array):
        self.num = 0
        self.session_id = 'session ' + str(self.num)

        self.array_list = {
            'user': user_array,
            'event': event_array,
            'event_time': event_time_array,
            'time_gap': time_gap_array
        }

        self.first_session_time = self.array_list['event_time'][0]
        self.last_session_time = None

        self.current_event_time = None
        self.before_event_time = self.array_list['event_time'][0]

        self.time_gap = self.array_list['time_gap'][0]

        self.current_user = None
        self.before_user = self.array_list['user'][0]

        self.current_event = None
        self.before_event = self.array_list['event'][0]
        self.session_sequence = [self.before_event]
        self.data = []
        self.column_names = ['user_id', 'session_id', 'session_sequence', 'first_session_time', 'last_session_time']

    def start_new_session(self):
        self.num += 1
        self.session_sequence = []
        self.session_id = 'session ' + str(self.num)
        self.session_sequence.append(self.current_event)
        self.first_session_time = self.current_event_time

    def append_row(self):
        self.session_sequence.append('session_end')
        self.last_session_time = self.before_event_time
        row = [self.before_user, self.session_id, self.session_sequence,
               self.first_session_time, self.last_session_time]
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
                self.before_event = self.array_list['event'][i - 1]
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
                uid_dict[pid] = user_name
                num += 1

    return pid_dict