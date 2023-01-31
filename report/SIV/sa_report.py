import pandas as pd
from report.SIV import ref
from report.SIV import directory as dr
import re
import report.SIV.ga_prep as gprep
import report.SIV.media_prep as mprep
import datetime

def media_read() :
    kakaosa_df = mprep.get_카카오SA_SA()
    kakaobsa_df = ref.kakaobsa_df
    googlesa_df = mprep.get_구글SA_SA()
    naversa_df = mprep.get_네이버SA_SA()
    naverbsa_df = mprep.get_NOSP_SA()

    df = pd.concat([kakaosa_df,kakaobsa_df,googlesa_df,naversa_df,naverbsa_df]).fillna(0)
    df = ref.adcode_mediapps(df)

    metric = ['노출','도달','클릭','비용','조회','SPEND_AGENCY','구매(대시보드)','매출(대시보드)','설치(대시보드)']
    df[metric] = df[metric].astype(float)
    df['sum'] = df[metric].sum(axis=1)
    df = df.loc[df['sum'] >= 1].drop(columns = ['sum','소재'])

    #날짜 구하기
    df['날짜'] = df['날짜'].apply(pd.to_datetime)
    df['연도'] = df['날짜'].apply(lambda x: x.strftime('%Y'))
    df['월'] = df['날짜'].apply(lambda x: x.strftime('%m'))
    df['날짜'] = df['날짜'].dt.date
    week_day = 7
    df['주차'] = pd.to_datetime(df['날짜']).apply(lambda x: (x + datetime.timedelta(week_day)).isocalendar()[1]) -1

    df2 = df.loc[df['날짜'] < datetime.date(2023,1,11)].drop(columns = ['연도','월','주차'])
    df2.to_csv(dr.download_dir + 'sa_media_raw_인덱스x.csv', index=False, encoding='utf-8-sig')

    dimension = ['연도', '월', '주차','머징코드','캠페인', '세트', '키워드']
    metric = ['노출', '도달', '클릭', '조회','비용', 'SPEND_AGENCY']
    df = df.groupby(dimension)[metric].sum().reset_index()

    return df

df = media_read()

index = ref.index_df[['지면/상품','매체','캠페인 구분','KPI','캠페인 라벨','OS','파트(주체)','파트 구분','머징코드']].drop_duplicates()
index['중복'] = index.duplicated(['머징코드'])
index = index.loc[index['중복'] == False]
df = pd.merge(df, index, on='머징코드', how='left').fillna('no_index')

df = df[['파트 구분', '연도', '월', '주차', '매체', '지면/상품', '캠페인 구분', 'KPI', '캠페인', '세트', '키워드', '캠페인 라벨', 'OS', '노출', '도달', '클릭', '조회','비용', 'SPEND_AGENCY']]

df.to_csv(dr.download_dir + 'sa_media_raw.csv', index=False, encoding='utf-8-sig')

#ga 가공

def ga_read():
    df = gprep.ga_prep()
    df =df.loc[(df['medium'] == 'cpc')|(df['medium'] == 'bsa')]
    df = df.loc[df['campaign']!='[2023.01]Pmax_Pmax_SVGG0001']

    df['날짜'] = df['날짜'].apply(pd.to_datetime)
    df['연도'] = df['날짜'].apply(lambda x: x.strftime('%Y'))
    df['월'] = df['날짜'].apply(lambda x: x.strftime('%m'))
    df['날짜'] = df['날짜'].dt.date
    week_day = 7
    df['주차'] = pd.to_datetime(df['날짜']).apply(lambda x: (x + datetime.timedelta(week_day)).isocalendar()[1]) -1

    dimension = ['머징코드','﻿dataSource', 'browser', 'campaign', 'source', 'medium','keyword', 'adContent', '연도', '월', '주차']
    metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)','브랜드구매(GA)', '브랜드매출(GA)', '가입(GA)']

    df = df.groupby(dimension)[metric].sum().reset_index().rename(columns ={'keyword' : '키워드'})
    df.to_csv(dr.download_dir + 'sa_ga_raw.csv', index=False, encoding='utf-8-sig')

    return df

def sa_merging():
    media_df = media_read()
    ga_df = ga_read()

    ga_dimension = ['머징코드','키워드','연도', '월', '주차']
    ga_metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)','브랜드구매(GA)', '브랜드매출(GA)', '가입(GA)']

    ga_df.loc[ga_df['키워드'].isin(['{keyword}', '{query}']), '키워드'] = '(not set)'
    ga_df = ga_df.groupby(ga_dimension)[ga_metric].sum().reset_index().rename(columns ={'keyword' : '키워드'})

    # 인덱스 (다음달에 raw 추가하기)
    merge_index = media_df[['머징코드', '캠페인', '세트','키워드']].drop_duplicates(keep='last')
    merge_index['key'] = merge_index['머징코드'] + merge_index['키워드']
    merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
    merge_index = merge_index.drop_duplicates(subset='key',keep='last')

    #last 기준으로 없애기
    merge_cdict = dict(zip(merge_index['key'], merge_index['캠페인']))
    merge_gdict = dict(zip(merge_index['key'], merge_index['세트']))
    merge_ndict = dict(zip(merge_index['머징코드'], merge_index['캠페인']))

    media_df = media_df.loc[media_df['머징코드'] != 'None']
    ga_df = ga_df.loc[ga_df['머징코드'] != 'None']

    media_df['key'] = media_df['머징코드'] + media_df['키워드']
    ga_df['key'] = ga_df['머징코드'] + ga_df['키워드']

    metric = ga_dimension + ['key']

    #ga 예외처리
    ga_notset = ga_df.loc[ga_df['키워드'] =='(not set)']
    notset_merge_df = pd.merge(media_df, ga_notset, how='outer', on= metric).fillna(0)
    notset_merge_df = notset_merge_df.loc[notset_merge_df['키워드'] =='(not set)']
    notset_merge_df.loc[notset_merge_df['캠페인'] == 0, '캠페인'] = notset_merge_df['머징코드'].apply(lambda x: x.replace(x, merge_ndict[x]) if x in merge_ndict.keys() else '-')
    notset_merge_df = notset_merge_df.loc[notset_merge_df['캠페인'] != '-']
    notset_merge_df['세트'] = '-'

    notset_merge_df = notset_merge_df.loc[notset_merge_df['주차'] >= 3]

    # 본래 ga 코드
    merge_df = pd.merge(media_df, ga_df, how='outer', on= metric).fillna(0)

    merge_df = merge_df.loc[merge_df['주차'] >= 3 ]

    merge_df.loc[merge_df['캠페인'] == 0,'캠페인'] = merge_df['key'].apply(lambda x: x.replace(x, merge_cdict[x]) if x in merge_cdict.keys() else '-')
    merge_df.loc[merge_df['세트'] == 0, '세트'] = merge_df['key'].apply(lambda x: x.replace(x, merge_gdict[x]) if x in merge_gdict.keys() else '-')

    merge_df = merge_df.loc[merge_df['캠페인'] != '-']

    merge_df = pd.concat([merge_df,notset_merge_df])

    index = ref.index_df[['지면/상품','매체','캠페인 구분','KPI','캠페인 라벨','OS','파트(주체)','파트 구분','머징코드']].drop_duplicates()
    index['중복'] = index.duplicated(['머징코드'])
    index = index.loc[index['중복'] == False]
    df = pd.merge(merge_df, index, on='머징코드', how='left').fillna('no_index')

    df = df[['파트 구분', '연도', '월', '주차', '매체', '지면/상품', '캠페인 구분', 'KPI', '캠페인', '세트', '키워드', '캠페인 라벨', 'OS', '노출', '도달', '클릭', '조회','비용', 'SPEND_AGENCY','세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)',
       '브랜드구매(GA)', '브랜드매출(GA)', '가입(GA)']]

    return df

df = sa_merging()

df.to_csv(dr.download_dir + 'sa_merge_raw.csv', index=False, encoding='utf-8-sig')
