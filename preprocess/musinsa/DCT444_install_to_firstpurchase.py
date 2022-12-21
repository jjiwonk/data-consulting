import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime

paid_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/DCT444/paid'

def paid_data_prep():
    dtypes = {
        'event_time': pa.string(),
        'attributed_touch_time': pa.string(),
        'install_time': pa.string(),
        'media_source': pa.string(),
        'event_name': pa.string(),
        'appsflyer_id': pa.string(),
        'platform': pa.string()}
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)

    raw_df = table.to_pandas()
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
    raw_df = raw_df.loc[(raw_df['install_time'] - raw_df['attributed_touch_time']) < datetime.timedelta(days=1)]
    raw_df = raw_df.loc[raw_df['event_name'].isin(['install','af_purchase','first_purchase'])]
    raw_df['is_organic'] = 'False'
    raw_df = raw_df.loc[raw_df['platform'] == 'android']
    raw_df = raw_df.drop(['attributed_touch_time'], axis =1)

    return raw_df

paid_df = paid_data_prep()

organic_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/DCT444/organic'

def organic_data_prep():
    dtypes = {
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Media Source': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Platform': pa.string()}
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(organic_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)

    raw_df = table.to_pandas()
    raw_df = raw_df.rename(columns=
                           {'Install Time': 'install_time',
                            'Event Time': 'event_time',
                            'Event Name': 'event_name',
                            'Media Source': 'media_source',
                            'AppsFlyer ID': 'appsflyer_id',
                            'Platform': 'platform'})

    raw_df[['install_time', 'event_time']] = raw_df[
        ['install_time', 'event_time']].apply(pd.to_datetime)
    raw_df = raw_df.loc[raw_df['event_name'].isin(['install','af_purchase','first_purchase'])]
    raw_df['is_organic'] = 'True'
    raw_df = raw_df.loc[raw_df['platform'] == 'android']
    raw_df['media_source'] = 'organic'

    return raw_df

organic_df = organic_data_prep()

df = pd.concat([paid_df,organic_df])

install_df = df.loc[df['event_name'] == 'install']
install_df = install_df.loc[install_df['media_source'].isin(['googleadwords_int' , 'Facebook Ads', 'kakao_banner', 'facebook_network', 'adisonofferwall_int', 'adisonofferwall_int'])]

first_purchase = df.loc[df['event_name'].isin(['af_purchase','first_purchase'])]
first_purchase = first_purchase.sort_values( by= 'event_time' )
first_purchase = first_purchase.drop_duplicates( 'appsflyer_id', keep= 'first')
first_purchase = first_purchase[['event_time','appsflyer_id']]
first_purchase = first_purchase.rename(columns = {'event_time': 'first_time'})

df = pd.merge(install_df, first_purchase, on ='appsflyer_id', how = 'left')
df = df.dropna(subset = ['first_time'])
df['ITET'] = df['first_time'] - df['install_time']

#raw 추출

df.to_csv(dr.download_dir+'/무신사_install_to_firstpurchase.csv', index = False)
#피벗

df['day'] = df['ITET'].dt.days
pivot2 = df.pivot_table(index = 'media_source', columns= 'platform' , values= 'day', aggfunc = ['min','mean','median','max']).reset_index()
pivot2.to_csv(dr.download_dir+'/무신사_install_to_firstpurchase_피벗2.csv', index = False)

#비중 구하기
df['Cnt'] = 1

def day (x):
    if x < 1:
        return '~Day 1'
    elif 1<= x < 7:
        return 'Day 1~7'
    elif 7<= x < 14:
        return 'Day 7~14'
    elif 14<= x < 30:
        return 'Day 14~30'
    else :
        return 'over Day 30'

df['day'] = df.apply(lambda x : day(x['day']),axis =1)

pivot1 = df.pivot_table(index = 'media_source', columns= 'day' , values= 'Cnt', aggfunc = 'sum').reset_index()
pivot1.to_csv(dr.download_dir+'/무신사_install_to_firstpurchase_피벗.csv', index = False)























