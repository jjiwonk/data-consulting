import pandas as pd
import os
import setting.directory as dr
import datetime

def file_read(file):
    raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/GS SHOP/airbridge_rawdata_output/' +file
    file_path = raw_dir
    file_list = os.listdir(file_path)
    raw_data = pd.DataFrame()

    for file in file_list:
        file_df = pd.read_csv(file_path + '/' + file, header=0, encoding='utf-8-sig')
        file_df = file_df.loc[file_df['Channel'] != 'unattributed']
        raw_data = pd.concat([raw_data, file_df])
    return raw_data

open_web = file_read('web')
open_app = file_read('app')

def app_open_prep(df):
    df['Event Datetime'] = pd.to_datetime(df['Event Datetime'])
    df['Touchpoint Datetime'] = pd.to_datetime(df['Touchpoint Datetime'])
    df = df.loc[(df['Event Datetime'].dt.date >= datetime.date(2022, 11, 1))]
    df = df.loc[(df['Event Datetime'] - df['Touchpoint Datetime']) < datetime.timedelta(minutes=30)]
    df = df.sort_values(by=['Event Datetime'] ,ascending= True)
    df1 = df.loc[df['Event Category'] == 'Open (App)']
    df2 = df.loc[df['Event Category'] == 'Deeplink Open (App)']
    df1.drop_duplicates(subset = 'Airbridge Device ID', keep='first', inplace = True)
    df2.drop_duplicates(subset = 'Airbridge Device ID', keep='first', inplace = True)
    df = pd.concat([df1,df2])
    df = df.loc[df['Is View-through'] == False]
    return df

open_app2 = app_open_prep(open_app)

def web_open_prep(df):
    df['Event Datetime'] = pd.to_datetime(df['Event Datetime'])
    df = df.loc[(df['Event Datetime'].dt.date >= datetime.date(2022, 11, 1))]
    df = df.sort_values(by=['Event Datetime'] ,ascending= True)
    df.drop_duplicates(subset='Cookie ID', keep='first', inplace=True)
    return df

open_web2 = web_open_prep(open_web)

def data_pivot(df):
    df['캠페인'] = df['Campaign'].str.slice(0,6)
    df = df.loc[df['캠페인'].isin(['Shoppy','shoppy'])]
    df['Cnt'] = 1
    pivot_df = df.pivot_table(index = 'Channel',  columns = 'Event Category', values= 'Cnt', aggfunc = 'sum').reset_index()
    return pivot_df

app_pivot = data_pivot(open_app2)
app_pivot[['Deeplink Open (App)', 'Open (App)']] = app_pivot[['Deeplink Open (App)', 'Open (App)']].fillna(0).astype(int)
web_pivot = data_pivot(open_web2)
open_pivot = pd.merge(app_pivot,web_pivot, how = 'outer', on = 'Channel').fillna(0)
open_pivot.to_csv(dr.download_dir +'/GSSHOP_open_data.csv', index = False)


