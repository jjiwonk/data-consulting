import datetime
import pandas as pd
from setting import directory as dr
from workers import read_data
import os
import pyarrow as pa
from workers.func import user_identifier

rename = {'매체 (Display)': 'media',
        '캠페인 라벨': 'campaign_label',
        'campaign_name': 'campaign',
        'impression': 'I',
        'click': 'C',
        'cost(calculated)': 'S'}

def read_organic():
    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if 'in-app-events' in f]

        dtypes = {
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Customer User ID': pa.string()}

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
        data = data.loc[data['Event Name'].isin(['Viewed LA Home','Viewed LA Home No Result'])]
        return data

    ios = read_file('ios')
    aos = read_file('aos')

    organic_data = pd.concat([ios,aos])

    organic_data['Event Time'] = pd.to_datetime(organic_data['Event Time'])
    organic_data = organic_data.loc[organic_data['Event Time'] >= datetime.datetime(year=2022, month=7, day=1)]

    return organic_data

def read_paid():

    file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT1268/RD'
    file_list = os.listdir(file_dir)

    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'appsflyer_id': pa.string(),
        'customer_user_id': pa.string()}

    data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    data = data.loc[data['event_name'].isin(['Viewed LA Home', 'Viewed LA Home No Result'])]
    data = data.sort_values('event_time')

    data['event_time'] = pd.to_datetime(data['event_time'])
    paid_data = data.loc[(data['event_time'] >= datetime.datetime(year=2022, month=7, day=1)) & (data['event_time'] <= datetime.datetime(year=2023, month=5, day=1))]

    return paid_data

def read_media_da():

    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/integrated_report_3/최종본'
    file_list = os.listdir(file_dir)

    dtypes = {
        'date': pa.string(),
        '매체 (Display)': pa.string(),
        '캠페인 라벨': pa.string(),
        'campaign_name': pa.string(),
        'impression': pa.float64(),
        'click': pa.float64(),
        'cost(calculated)': pa.float64()}

    media_data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    media_data = media_data.loc[media_data['매체 (Display)'] != 'no_index']
    media_data = media_data.rename(columns = rename)

    media_data = media_data.pivot_table(index='date', columns=['media'], values=['I', 'S'],
                                  aggfunc='sum').reset_index().fillna(0)

    media_data.columns = [media + '_' + value for value, media in media_data.columns]
    media_data = media_data.rename(columns={'_date': 'date'})

    return media_data

def data_prep():

    organic_data = read_organic()

    rename = {
        'Install Time': 'install_time',
        'Event Time': 'event_time',
        'Event Name': 'event_name',
        'AppsFlyer ID': 'appsflyer_id',
        'Customer User ID': 'customer_user_id'}

    organic_data = organic_data.rename(columns = rename)

    paid_data = read_paid()
    data = pd.concat([organic_data,paid_data])
    data.index = range(len(data))

    user_dic = user_identifier(data ,'appsflyer_id','customer_user_id')
    data['user_id'] = data['appsflyer_id'].apply(lambda x: x.replace(x, user_dic[x]) if x in user_dic.keys() else x)
    data['date'] = data['event_time'].dt.date
    data['cnt'] = 1

    # 1일 유저 당 1회 한도조회 수
    data = data.drop_duplicates(['date','user_id'],keep='first')
    piv =  pd.pivot_table(data ,index= 'date', columns= 'event_name', values= 'cnt',aggfunc='sum').reset_index()
    piv['VLH'] = piv['Viewed LA Home'] + piv['Viewed LA Home No Result']

    kpi_data = piv[['date','VLH']]

    return kpi_data

