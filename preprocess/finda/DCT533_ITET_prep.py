import pandas as pd
import setting.directory as dr
import pyarrow as pa
from workers import read_data
import os

file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT533/raw'
file_list = os.listdir(file_dir)

dtypes = {
        'install_time': pa.string(),
        'attributed_touch_time': pa.string(),
        'event_time': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'appsflyer_id':pa.string(),
        'event_name':pa.string(),
        'is_retargeting' :pa.string() }

def install_time_prep():

        df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

        df['install_time'] = pd.to_datetime(df['install_time'])
        df['attributed_touch_time'] = pd.to_datetime(df['attributed_touch_time'])
        df['event_time'] = pd.to_datetime(df['event_time'])

        event_raw = df.loc[df['event_name'].isin(['Viewed LA Home', 'Viewed LA Home No Result', 'loan_contract_completed'])]
        conversion_raw = df.loc[df['event_name'].isin(['re-engagement', 're-attribution', 'install'])]

        conversion_raw = conversion_raw.sort_values(['appsflyer_id','install_time'])

        conversion_raw['str_install_time'] = conversion_raw['install_time'].astype(str)
        conversion_raw['key'] =conversion_raw['appsflyer_id']  +  conversion_raw['str_install_time'] + conversion_raw['media_source'] + conversion_raw['campaign']
        conversion_raw = conversion_raw.drop_duplicates('key', keep='last')
        conversion_raw['install_time2'] = conversion_raw['install_time'].shift(1)
        conversion_raw['appsflyer_id2'] = conversion_raw['appsflyer_id'].shift(1)
        conversion_raw = conversion_raw.loc[conversion_raw['appsflyer_id'] == conversion_raw['appsflyer_id2']]

        conversion_raw = conversion_raw[['key','install_time2']]

        event_raw['str_install_time'] = event_raw['install_time'].astype(str)
        event_raw['key'] =event_raw['appsflyer_id'] + event_raw['str_install_time'] + event_raw['media_source'] + event_raw['campaign']

        df = pd.merge(event_raw,conversion_raw, on = 'key',how = 'left').fillna('-')

        return df

def pivoting():

        df = install_time_prep()

        no_install = df.loc[df['install_time2'] == '-']
        df = df.loc[df['install_time2'] != '-']

        df['install_time2'] = pd.to_datetime(df['install_time2'])

        df['time_gap'] = df['event_time'] - df['install_time2']
        df['time_gap'] = df['time_gap'].dt.days
        df['구분'] = 'Y'

        no_install['구분'] = 'N'

        no_install.loc[no_install['is_retargeting'] == 'False' ,'install_time2'] = no_install['install_time']
        no_install['time_gap'] ='-'
        no_install2 = no_install.loc[no_install['install_time2'] != '-']
        no_install = no_install.loc[no_install['install_time2'] == '-']

        no_install2['install_time2'] = pd.to_datetime(no_install2['install_time2'])
        no_install2['time_gap'] = no_install2['event_time'] - no_install2['install_time2']
        no_install2['time_gap'] = no_install2['time_gap'].dt.days

        final_df = pd.concat([no_install2,no_install,df])

        final_df['월'] = final_df['event_time'].apply(lambda x: x.strftime('%m'))
        final_df['날짜'] = final_df['event_time'].apply(lambda x: x.strftime('%Y-%m-%d'))

        final_df = final_df[['월','날짜','event_name','time_gap','구분','is_retargeting']]

        return final_df

df = pivoting()

df['Cnt'] = 1

piv = df.pivot_table(index=['월','날짜','event_name','time_gap','구분','is_retargeting'], values='Cnt', aggfunc='sum').reset_index()

piv.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT533/total_raw.csv',index = False, encoding = 'utf-8-sig')




