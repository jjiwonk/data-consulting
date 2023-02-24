from analysis.DCT512_wconcept import info
from analysis.DCT512_wconcept import prep
import pandas as pd
import os
from workers import read_data
import datetime
import setting.directory as dr
import json

def order_item():

    file_dir = info.paid_dir
    file_list = os.listdir(file_dir)
    paid_df = read_data.pyarrow_csv(dtypes=info.dtypes, directory=info.paid_dir, file_list=file_list)

    file_dir = info.organic_dir
    file_list = os.listdir(file_dir)
    organic_df = read_data.pyarrow_csv(dtypes=info.organic_dtypes, directory=info.organic_dir, file_list=file_list)
    df = pd.concat([paid_df,organic_df]).fillna('-').sort_values('event_time')
    df = df.drop_duplicates(['event_time', 'appsflyer_id', 'event_revenue'], keep='first')

    df = df[['event_time','event_revenue','event_value','appsflyer_id']]
    df = df.loc[df['event_revenue'] !='-']
    df = df.drop_duplicates()

    promo_df = prep.data_prep()
    promo_df = promo_df[['media_source','campaign','event_revenue','appsflyer_id','프로모션', '첫구매', '첫구매일자','event_value']].sort_values('첫구매일자').rename(columns= {'event_value':'첫구매value','event_revenue':'첫구매revenue'})
    promo_df = promo_df.loc[promo_df['media_source'].isin(['KAKAOBIZBOARD', 'NAVERGFA', 'facebook', 'NAVERSPECIALDA', 'BRANDINGDA', 'Twitter', 'appier_int','adisonofferwall_int'])]

    promo_df.loc[promo_df['프로모션'] == '나이키드로우2','프로모션'] = '나이키드로우'
    promo_df.loc[promo_df['프로모션'] == '나이키드로우1','프로모션'] = '나이키드로우'

    df = pd.merge(df,promo_df,on ='appsflyer_id', how = 'left' ).fillna('-')
    df = df.loc[df['프로모션'] != '-']
    df['첫구매일자'] = pd.to_datetime(df['첫구매일자'])
    df = df.loc[df['첫구매일자'] < df['event_time']]

    # 첫구매 가공
    df.loc[df['첫구매일자'] != df['event_time'], '첫구매'] = False

    df['재구매일자'] = df['event_time']
    df['재구매revenue'] = df['event_revenue']

    df['cnt'] = 1

    df['첫구매금액대'] = '-'
    df.loc[df['첫구매revenue'] < 50000, '첫구매금액대'] = '~5만원'
    df.loc[(df['첫구매revenue'] >= 50000) & (df['첫구매revenue'] < 100000), '첫구매금액대'] = '5~10만원'
    df.loc[(df['첫구매revenue'] >= 100000) & (df['첫구매revenue'] < 200000), '첫구매금액대'] = '10~20만원'
    df.loc[(df['첫구매revenue'] >= 200000) & (df['첫구매revenue'] < 300000), '첫구매금액대'] = '20~30만원'
    df.loc[df['첫구매revenue'] >= 300000, '첫구매금액대'] = '30만원~'

    df['재구매금액대'] = '-'
    df.loc[df['재구매revenue'] < 50000, '재구매금액대'] = '~5만원'
    df.loc[(df['재구매revenue'] >= 50000) & (df['재구매revenue'] < 100000), '재구매금액대'] = '5~10만원'
    df.loc[(df['재구매revenue'] >= 100000) & (df['재구매revenue'] < 200000), '재구매금액대'] = '10~20만원'
    df.loc[(df['재구매revenue'] >= 200000) & (df['재구매revenue'] < 300000), '재구매금액대'] = '20~30만원'
    df.loc[df['재구매revenue'] >= 300000, '재구매금액대'] = '30만원~'

    df['재구매일자'] = pd.to_datetime(df['재구매일자'])
    df['재구매월'] = df['재구매일자'].apply(lambda x: x.strftime('%Y-%m'))

    fir = df.pivot_table(index=['프로모션', '첫구매금액대', '재구매금액대','재구매월'], values='cnt', aggfunc='sum').reset_index()

    rev = df.pivot_table(index=['프로모션', '첫구매금액대', '재구매금액대','재구매월'], values='재구매revenue', aggfunc='sum').reset_index()

    data = pd.merge(fir, rev, on=['프로모션', '첫구매금액대', '재구매금액대','재구매월'], how='left')

    data.to_csv(info.raw_dir + '/카테고리_이동량_월반영.csv', index=False, encoding='utf-8-sig')

    return data