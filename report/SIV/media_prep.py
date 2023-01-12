from report.SIV import directory as dr
from report.SIV import ref
import re

import pandas as pd

def media_raw_read(media):
    info_dict = ref.info_dict[media]
    use_col = list(info_dict['dimension'].values()) + list(info_dict['metric'].values()) + list(info_dict['temp'].values())
    df = pd.read_csv( dr.report_dir + info_dict['read']['경로'] +'/'+ info_dict['read']['파일명'] + info_dict['read']['suffix'] ,usecols= use_col)

    df = df.rename(columns = { v: k for k, v in info_dict['dimension'].items()})
    df = df.rename(columns= { v: k for k, v in info_dict['metric'].items()})
    df = df.rename(columns= { v: k for k, v in info_dict['temp'].items()})

    for col in info_dict['dimension'].keys():
        df[col] = df[col].fillna('-')
        df[col] = df[col].astype(str)

    for col in info_dict['metric'].keys():
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype(float)

    df['구분'] = media

    if media in ref.columns.prep_media:
        index = ref.index_df[['캠페인', '지면/상품']]
        index = index.drop_duplicates(keep='first')
        df = pd.merge(df, index, on='캠페인', how='left').fillna('no_index')
        df = df.loc[df['지면/상품'] == media]
        df = df.drop(columns=['지면/상품'])

    return df

def cost_calc(media, df):
    cal_dict = ref.info_dict[media]['prep']
    if media in ref.columns.google_media:
        df['비용'] = df['비용']/1000000

    df['SPEND_AGENCY'] = df['비용'] * float(cal_dict['곱하기'])
    df['SPEND_AGENCY'] = df['SPEND_AGENCY'] / float(cal_dict['나누기'])
    return df

def media_prep(media):
    df = media_raw_read(media)
    df2 = cost_calc(media,df)
    return df2

#머징 함수를 위해서 함수명은 무조건 구분명과 일치하게 지정

#구분 별로 ! 예외 처리는 요기다가

def get_FBIG():
    df = media_prep('FBIG')
    return df

def get_비즈보드():
    df = media_prep('비즈보드')
    return df

def get_디스플레이():
    df = media_prep('디스플레이')
    return df

def get_채널메시지():
    df = media_prep('채널메시지')
    return df

def get_카카오SA():
    df = media_prep('카카오SA')
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_Pmax():
    df = media_prep('Pmax')
    df.loc[df['캠페인'] == '[2023.01]Pmax_Pmax','캠페인'] = '[2023.01]Pmax_Pmax_SVGG0001'
    df.loc[df['캠페인'] == '정기_pmax_sales_2301_Pmax', '캠페인'] = '정기_pmax_sales_2301_Pmax_JJGG0015'
    df['세트'] = '-'
    df['소재'] = '-'
    return df


def get_GDN_P():
    df = media_prep('GDN_P')
    return df

def get_GDN_M():
    df = media_prep('GDN_M')
    return df

def get_YT디스커버리():
    df = media_prep('YT디스커버리')
    return df

def get_YT인스트림():
    df = media_prep('YT인스트림')
    return df

def get_AC():
    df = media_prep('AC')
    return df

df = get_AC()


def get_구글SA_P():
    df = media_prep('구글SA_P')
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_구글SA_M():
    df = media_prep('구글SA_M')
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_네이버SA():
    df = media_prep('네이버SA')
    df['세트'] = '-'
    df['소재'] = '-'
    return df


def get_AppleSA():
    df = media_prep('AppleSA')
    df['소재'] = '-'
    return df

def get_GFA():
    df = media_prep('GFA')
    return df

def get_크리테오():
    df = media_prep('크리테오')
    df.loc[df['구분']== '크리테오','소재'] = df['소재'].apply(lambda x: x.replace(x, ref.exc_adict[x]) if x in ref.exc_adict.keys() else x)
    df.loc[df['소재']== '-','소재'] = df['세트']
    df.loc[df['구분'] == '크리테오', '소재'] = df['소재'].apply(lambda x: x.replace(x, ref.exc_adict[x]) if x in ref.exc_adict.keys() else x)
    return df

def get_NOSP():
    df = media_prep('NOSP')
    df['세트'] = '-'

    pat = re.compile('[A-Z]{4,4}\d{4,4}')
    df['소재'] = df['캠페인']
    df['캠페인'] = df['캠페인'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.loc[df['구분'] == 'NOSP','캠페인'] = df['캠페인'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)

    return df

def get_버티컬():
    df = ref.vertical_df
    #index = ref.index_df[['캠페인','매체','지면/상품','캠페인 구분','KPI','캠페인 라벨','OS','파트 구분']]
    #index = index.drop_duplicates(keep ='first')
    #df = pd.merge(df,index ,on = '캠페인',how = 'left').fillna('no_index')

    df[ref.columns.media_mertic] = df[ref.columns.media_mertic].astype(float)
    df[ref.columns.media_dimension] = df[ref.columns.media_dimension].astype(str)
    return  df




