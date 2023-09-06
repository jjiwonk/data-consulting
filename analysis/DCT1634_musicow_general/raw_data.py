from setting import directory as dr
import pandas as pd
import os
from workers.func import user_identifier

class data_set():
    def __init__(self):
        raw_dir = dr.download_dir + '/뮤직카우RD'
        raw_files = os.listdir(raw_dir)

        df_list = []
        for file in raw_files:
            df = pd.read_csv(raw_dir + '/' + file)
            df_list.append(df)

        self.data = pd.concat(df_list, ignore_index=True)

    def basic_prep(self):
        data = self.data
        data = data.sort_values(['Event Time', 'Install Time'])
        data.loc[data['Attributed Touch Type'].isnull(), 'Media Source'] = 'Organic'
        data.loc[data['Media Source'].isnull(), 'Media Source'] = data['Partner']
        data['Media Source'] = data['Media Source'].fillna('none')

        data['Customer User ID'] = data['Customer User ID'].fillna('')
        data['Customer User ID'] = data['Customer User ID'].astype('str')

        user_dict = user_identifier(data, 'AppsFlyer ID', 'Customer User ID')
        data['unique_user_id'] = data['AppsFlyer ID'].apply(lambda x: user_dict.get(x))

        data = data[['unique_user_id'] + list(data.columns[:-1])]

        # 중복 제거를 위한 우선순위 선정 : 리인게이지먼트 > UA > 오가닉 순으로 정렬
        data['dedup_order'] = data.apply(
            lambda x: 3 if x['Media Source'] == 'Organic' else 1 if x['Is Retargeting'] == True else 2, axis=1)
        data = data.sort_values(['unique_user_id', 'Event Time', 'Install Time', 'dedup_order'])
        data = data.drop_duplicates(['Event Time', 'Event Name', 'unique_user_id'], keep='first')

        data[['Attributed Touch Time','Install Time','Event Time']] = data[['Attributed Touch Time',
                                                                            'Install Time',
                                                                            'Event Time']].apply(lambda x: pd.to_datetime(x))

        # 오가닉 인스톨 추출
        pre_install = data.drop_duplicates(['unique_user_id', 'Install Time'],keep='first')
        pre_install = pre_install[['Install Time', 'unique_user_id', 'Media Source']]

        # 첫 번째 인스톨 시간
        first_install_time = data.loc[data['Event Name'] == 'install'].drop_duplicates('unique_user_id', keep='first')
        first_install_time = first_install_time[['Install Time', 'unique_user_id', 'Media Source']]
        first_install_time = pd.concat([first_install_time, pre_install], ignore_index=True)
        first_install_time = first_install_time.sort_values('Install Time').drop_duplicates('unique_user_id')
        first_install_time = first_install_time.rename(
            columns={'Install Time': 'First Install Time', 'Media Source': 'Install Source'})

        data = data.merge(first_install_time, on='unique_user_id', how='left')
        data['Cnt'] = 1

        self.data = data

    def install_user_log(self):
        data = self.data

        install_user = set(data.loc[data['Event Name'] == 'install', 'unique_user_id'])

        install_user_data = data.loc[data['unique_user_id'].isin(install_user)]
        install_user_data = install_user_data.loc[install_user_data['First Install Time'] <= install_user_data['Event Time']]

        multiple_install_user = install_user_data.loc[(install_user_data['Event Name'] == 'install') &
                                                      (install_user_data['Event Time'] != install_user_data['First Install Time'])]

        install_user_data = install_user_data.drop(multiple_install_user.index)
        install_user_data.index = range(len(install_user_data))

        install_user_data['ITET'] = install_user_data['Event Time'] - install_user_data['First Install Time']
        install_user_data['ITET'] = install_user_data['ITET'].apply(lambda x: x.days)
        install_user_data['ITET Group'] = install_user_data['ITET'].apply(
            lambda x: '0d' if x == 0 else '7d' if x < 7 else '30d' if x < 30 else '30d+')

        self.install_user_data = install_user_data

data_set = data_set()
data_set.basic_prep()
data_set.install_user_log()