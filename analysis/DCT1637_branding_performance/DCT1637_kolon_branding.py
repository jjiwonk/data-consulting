from setting import directory as dr
from workers import read_data
from workers import func
import pandas as pd
import pyarrow as pa
import os
import datetime
import numpy as np

# 결과 파일 경로
raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/코오롱/브랜딩-퍼포 연계분석/노출-매출'

def data_prep():

    organic = pd.read_csv(raw_dir + '/organic_f.csv')
    non_organic = pd.read_csv(raw_dir + '/non_organic.csv')

    apps_data = pd.concat([organic , non_organic])
    apps_data = apps_data.groupby(['event_date'])['event_revenue'].sum().reset_index()
    apps_data = apps_data[0:116]

    report_data = pd.read_excel(raw_dir + '/리포트 취합.xlsx',sheet_name=0)

    rename_dict = {'인스톨': '앱 설치','앱설치': '앱 설치','앱설치 ':'앱 설치'}
    report_data['목표'] = report_data['목표'].apply(lambda x: x.replace(x, rename_dict[x])if x in rename_dict.keys() else x )

    piv = report_data.pivot_table(index='date',columns='목표',values='합계 : 노출',aggfunc='sum').reset_index()
    piv = piv[['date','매출','앱 설치','브랜딩']].fillna(0)
    piv = piv.rename(columns = {'date':'event_date'})
    piv['event_date'] = piv['event_date'].dt.date

    apps_data['event_date'] = pd.to_datetime(apps_data['event_date'])
    apps_data['event_date'] = apps_data['event_date'].dt.date

    merge = pd.merge(piv,apps_data, on='event_date',how= 'left')

    merge.to_csv(raw_dir +'/RD.csv', index= False, encoding='utf-8-sig')

    return merge

# 인덱스 시트 정리
def index_prep():

    index_col = ['매체','캠페인','&pid=','&c=']

    def col_rename(df):
        df = df[index_col]
        df = df.rename(columns={'&pid=':'media_source','&c=' : 'campaign'})
        df = df.drop_duplicates(subset = ['media_source','campaign'])
        df = df.dropna()
        return df

    bs_index = pd.read_csv(raw_dir + '/인덱스/[내부용] 코오롱_인덱스시트_2302의 사본 - INDEX_BS.csv', header= 2)
    sa_index = pd.read_csv(raw_dir + '/인덱스/[내부용] 코오롱_인덱스시트_2302의 사본 - INDEX_SA.csv', header=2)
    da_index = pd.read_csv(raw_dir + '/인덱스/[내부용] 코오롱_인덱스시트_2302의 사본 - INDEX_DA.csv', header=2).rename(columns={'Unnamed: 24': '캠페인'})
    pf_index = pd.read_csv(raw_dir + '/인덱스/[내부용] 코오롱_인덱스시트_2302의 사본 - INDEX_PF.csv', header=2).rename(columns={'Unnamed: 18': '캠페인'})

    bs_index = col_rename(bs_index)
    sa_index = col_rename(sa_index)
    da_index = col_rename(da_index)
    pf_index = col_rename(pf_index)

    index = pd.concat([bs_index,sa_index,da_index,pf_index])

    return index

# 코오롱 데이터 정리

def raw_data_read():

    athena_inapp = pd.read_csv(raw_dir + '/RD/athena/paid_purchase.csv')
    athena_conversion = pd.read_csv(raw_dir + '/RD/athena/paid_conversion.csv')
    athena_organic = pd.read_csv(raw_dir + '/RD/athena/organic.csv')
    athena_data = pd.concat([athena_inapp,athena_conversion,athena_organic])

    def organic_read():
        organic_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/코오롱몰_2022/1. 리포트/#자동화/1차 RAW/content_view제외'
        organic_files = os.listdir(organic_dir)

        organic_col = {
            'Install Time': pa.string(),
            'Attributed Touch Time': pa.string(),
            'Attributed Touch Type': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Event Revenue': pa.string(),
            'Campaign': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Media Source': pa.string(),
            'Is Retargeting': pa.string()}

        organic_data = read_data.pyarrow_csv(dtypes=organic_col, directory=organic_dir, file_list=organic_files)
        organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]

        organic_data['event_time'] = pd.to_datetime(organic_data['event_time'])
        organic_data['event_date'] = organic_data['event_time'].dt.date

        return organic_data

    organic_data = organic_read()
    raw_data = pd.concat([athena_data,organic_data])

    return raw_data

raw_data = raw_data_read()

# 매출 분기
def revenue_prep():

    raw_data = raw_data_read()

    purchase_data = raw_data.loc[raw_data['event_name'].isin(['af_purchase','first_purchase'])]
    conversion_data = raw_data.loc[raw_data['event_name'].isin(['install','re-engegement','re-attribution'])]

    # 첫구매 중복 바르기
    purchase_data = purchase_data.loc[purchase_data['is_retargeting'] != True]
    purchase_data = purchase_data.sort_values(['appsflyer_id','event_time','event_name'])
    purchase_data = purchase_data.drop_duplicates(subset=['event_time','appsflyer_id'],keep='last')

    purchase_data.loc[purchase_data['event_revenue'].isna() == True,'event_revenue' ] = 0
    purchase_data.loc[purchase_data['event_revenue'] == '', 'event_revenue'] = 0
    purchase_data['event_revenue'] = purchase_data['event_revenue'].astype(int)
    purchase_data['event_date'] = pd.to_datetime(purchase_data['event_date']).dt.date

    #revenue 확인
    revenue_data = purchase_data.pivot_table(index= 'event_date',columns = 'event_name',values = 'event_revenue',aggfunc = 'sum').reset_index()
    revenue_data['total_revenue'] =  revenue_data['af_purchase'] + revenue_data['first_purchase']

    revenue_data = revenue_data.sort_values('event_date')[:116]

    #리포트 붙이기
    report_data = pd.read_excel(raw_dir + '/리포트 취합.xlsx',sheet_name=0)

    rename_dict = {'인스톨': '앱 설치','앱설치': '앱 설치','앱설치 ':'앱 설치'}
    report_data['목표'] = report_data['목표'].apply(lambda x: x.replace(x, rename_dict[x])if x in rename_dict.keys() else x )

    report_data = report_data.pivot_table(index='date', columns='목표', values='합계 : 노출', aggfunc='sum').reset_index()
    #report_data = report_data.pivot_table(index='date', columns='목표', values='합계 : 광고비', aggfunc='sum').reset_index()
    report_data = report_data[['date', '매출', '앱 설치', '브랜딩']].fillna(0)
    report_data = report_data.rename(columns={'date': 'event_date'})
    report_data['event_date'] = report_data['event_date'].dt.date
    report_data['총노출'] = report_data['매출'] + report_data['앱 설치'] + report_data['브랜딩']

    # 머징
    merge_data = pd.merge(revenue_data,report_data,on= 'event_date',how='left')
    merge_data['브랜딩 집행여부'] = True
    merge_data.loc[merge_data['브랜딩'] == 0 ,'브랜딩 집행여부'] = False

    merge_data.to_csv(raw_dir +'/RD.csv', index= False, encoding='utf-8-sig')

    corr = merge_data.corr(method='pearson').reset_index()
    corr.to_csv(raw_dir+'/상관관계.csv', index= False, encoding= 'utf-8-sig')

    return merge_data
