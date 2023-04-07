import pandas as pd
import datetime
from workers import read_data
import os
import pyarrow as pa
import setting.directory as dr

def raw_read():
    file_dir = dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/raw/event_value'
    file_list = os.listdir(file_dir)

    dtypes = {'event_time': pa.string(),
              'event_name' : pa.string(),
                  'event_value': pa.string(),
                  'appsflyer_id': pa.string(),
                  'media_source': pa.string()}

    df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

    return df

def purchase_raw_read():
    file_dir = dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/raw/apps'
    file_list = os.listdir(file_dir)

    dtypes = {'appsflyer_id' : pa.string(),
                  '날짜': pa.date32(),
                  '구매브랜드': pa.string()}

    df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

    return df

def apps_prep():

    df = raw_read()
    df = df.loc[df['event_name'].isin(['af_add_to_cart', 'af_brand_view', 'af_search','af_add_to_wishlist','af_search'])]

    df['브랜드구분'] = df['event_value'].apply(lambda x: eval(x))
    df['관심브랜드'] = '-'

    df.loc[df['event_name'] == 'af_brand_view' , '관심브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_content_list'))
    df.loc[df['event_name'] == 'af_add_to_cart', '관심브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_brand'))
    df.loc[df['event_name'] == 'af_add_to_wishlist', '관심브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_brand'))
    df.loc[df['event_name'] == 'af_search', '관심브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_search_string'))

    apps = purchase_raw_read()
    apps = apps.drop_duplicates(keep = 'first' , subset= ['appsflyer_id','구매브랜드'] )

    apps['key'] = apps['appsflyer_id'] + apps['구매브랜드']
    df['key'] = df['appsflyer_id'] + df['관심브랜드']

    merge_df = pd.merge(df , apps , how='left', on = 'key').fillna(0)
    true_df = merge_df.loc[merge_df['구매브랜드'] != 0]

    true_df['event_time'] = true_df['event_time'].apply(pd.to_datetime)
    true_df['event_time'] = true_df['event_time'].dt.date

    true_df = true_df.loc[true_df['event_time'] <= true_df['날짜']]
    true_df['구분'] = True
    true_df['event_time'] = true_df['event_time'].apply(lambda x: str(x))
    true_df['event_key'] =  true_df['event_time'] + true_df['event_name'] + true_df['key']
    true_df = true_df[['event_key','구분']]

    merge_df['event_time'] = merge_df['event_time'].apply(pd.to_datetime)
    merge_df['event_time'] = merge_df['event_time'].dt.date
    merge_df = merge_df.loc[merge_df['event_time'] <=  datetime.date(year=2023,month=3,day=27)]

    merge_df['event_time'] = merge_df['event_time'].apply(lambda x: str(x))
    merge_df['event_key'] = merge_df['event_time'] + merge_df['event_name']
    merge_df['key'] = merge_df['key'].apply(lambda x: str(x))
    merge_df['event_key'] = merge_df['event_key']+ merge_df['key']

    true_df = true_df.drop_duplicates(keep = 'first')

    piv_df = pd.merge(merge_df, true_df, how='left', on='event_key').fillna(False)
    piv_df['cnt'] = 1

    piv = piv_df.pivot_table(values='cnt',columns='구분',index='event_name',aggfunc='sum',margins=True).reset_index()
    piv['전환율'] = piv[True] / piv['All']
    piv.to_csv(dr.dropbox_dir +'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/brand_conversionrate.csv',index = False, encoding= 'utf-8-sig')

    return brand_df
