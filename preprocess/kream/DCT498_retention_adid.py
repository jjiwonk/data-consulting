from setting import directory as dr
import pandas as pd
import json
import datetime
import pyarrow.csv as pacsv
import pyarrow as pa

use_col = ['event_name','install_time','event_time','media_source','appsflyer_id','platform']

def apps_read():
    dir = dr.dropbox_dir + '/광고사업부/4. 광고주/크림/자동화 리포트/appsflyer_prism'

    from_date = datetime.date(2022, 12, 1)
    to_date =datetime.date(2023, 1, 9)

    convert_ops = pacsv.ConvertOptions( include_columns= list(use_col))
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding = 'utf-8-sig')
    table_list = []

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'appsflyer_daily_report_{date_name}.csv'

        table = pacsv.read_csv(dir + '/' + file_name, convert_options=convert_ops, read_options=ro)
        table_list.append(table)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()
    raw_data = raw_data.loc[raw_data['event_name'].isin(['purchase','sell'])]
    raw_data = raw_data.loc[raw_data['platform'] == 'android']

    return raw_data

df = apps_read()
df = df.sort_values(by='event_time')


df['재구매'] = df.duplicated(subset='appsflyer_id', keep=False)
rep_df = df.loc[df['재구매'] == True]
rep_df = rep_df.sort_values(by=['appsflyer_id','event_time'])

fir_df = rep_df.drop_duplicates(['appsflyer_id'] , keep = 'first')
fir_df['첫구매일'] = fir_df['event_time']
fir_df = fir_df[['appsflyer_id','첫구매일']]

pur_df = pd.merge(rep_df, fir_df, on='appsflyer_id', how='left')

pur_df['event_time'] = pd.to_datetime(pur_df['event_time'])
pur_df['첫구매일'] = pd.to_datetime(pur_df['첫구매일'])

pur_df['첫구매to재구매'] = pur_df['event_time'] - pur_df['첫구매일']

appid = pur_df.drop_duplicates(['appsflyer_id'] , keep = 'first')
appid = appid[['appsflyer_id']]

appid.to_csv(dr.download_dir+'/재구매유저id.csv', index=False, encoding='utf-8-sig')
pur_df.to_csv(dr.download_dir+'/재구매raw.csv', index=False, encoding='utf-8-sig')

pur_df['재구매텀'] = pur_df['첫구매to재구매'].dt.days

#분석용

df = pur_df.loc[pur_df['재구매텀'] >= 1]
df = df.drop_duplicates(['appsflyer_id'] , keep = 'first')

piv = df.pivot_table(index = 'media_source', columns= 'platform' , values= '재구매텀', aggfunc = ['min','mean','median','max']).reset_index()
piv.to_csv(dr.download_dir+'/재구매분석.csv', index = False)

#분석용 2

df['Cnt'] = 1

def day (x):
    if x == 1:
        return 'Day 1'
    elif 1 < x < 8:
        return 'Day 2~7'
    elif 8<= x < 15:
        return 'Day 8~14'
    elif 15<= x < 31:
        return 'Day 15~30'
    else :
        return 'over Day 30'

df['day'] = df.apply(lambda x : day(x['재구매텀']),axis =1)

piv2 = df.pivot_table(index = 'media_source', columns= 'day' , values= 'Cnt', aggfunc = 'sum').reset_index().fillna(0)
piv2.to_csv(dr.download_dir+'/재구매분석2.csv', index = False)