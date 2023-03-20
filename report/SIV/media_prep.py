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

# 머징 함수를 위해서 함수명은 무조건 구분명과 일치하게 지정

def get_FBIG():
    df = media_prep('FBIG')
    df['구매(대시보드)'] = df['페북전환1'] + df['페북전환2']
    df['매출(대시보드)'] = df['페북매출1'] + df['페북매출2']
    df = df.drop(columns=['페북전환1','페북전환2','페북매출1','페북매출2'])
    return df

def get_카카오모먼트():
    df = media_prep('카카오모먼트')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['비즈보드','디스플레이','스폰서드보드'])].drop(columns=['지면/상품'])
    return df

def get_채널메시지():
    df = media_prep('채널메시지')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['채널메시지'])].drop(columns=['지면/상품'])
    return df

def get_카카오SA():
    df = media_prep('카카오SA')
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_Pmax():
    df = media_prep('Pmax')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['Pmax'])].drop(columns=['지면/상품'])
    df.loc[df['캠페인'] == '[2023.01]Pmax_Pmax','캠페인'] = '[2023.01]Pmax_Pmax_SVGG0001'
    df.loc[df['캠페인'] == '정기_pmax_sales_2301_Pmax', '캠페인'] = '정기_pmax_sales_2301_Pmax_JJGG0015'
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_구글DA():
    df = media_prep('구글DA')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['GDN_P','GDN_M','YT디스커버리','YT인스트림','AC'])].drop(columns=['지면/상품'])
    return df

def get_구글SA():
    df = media_prep('구글SA')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['구글SA_P', '구글SA_M', '구글SA_PM'])].drop(columns=['지면/상품'])
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_네이버SA():
    df = media_raw_read('네이버SA')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['검색_P', '검색_M', '쇼검브랜드형_P', '쇼검브랜드형_M','네이버BSA_P','네이버BSA_M'])]
    df['세트'] = '-'
    df['소재'] = '-'
    #df = df.drop(columns=['지면/상품'])
    #df = cost_calc('네이버SA', df)
    df = df.groupby(['날짜', '캠페인', '세트', '소재','지면/상품'])[['노출', '클릭', '비용', '구매(대시보드)', '매출(대시보드)']].sum().reset_index()
    sib_df = ref.sib_bsa.drop_duplicates()
    merge = pd.merge(df,sib_df,on =['날짜','캠페인'], how = 'left').fillna(0)
    merge.loc[merge['지면/상품'].isin(['네이버BSA_P','네이버BSA_M']),'비용'] = merge['시뷰티 비용']
    merge['비용'] = merge['비용'].astype(float)
    merge = cost_calc('네이버SA', merge)
    merge = merge.drop(columns = ['지면/상품','시뷰티 비용'])
    merge['구분'] = '네이버SA'
    return merge

def get_네이버SS():
    df = media_prep('네이버SS')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['쇼핑검색_P', '쇼핑검색_M'])]
    df = df.drop(columns=['지면/상품'])
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
    df[ref.columns.media_dimension] = df[ref.columns.media_dimension].astype(str)
    df[ref.columns.media_mertic] = df[ref.columns.media_mertic].astype(float)
    df['날짜'] = pd.to_datetime(df['날짜'])
    df['날짜'] = df['날짜'].dt.date
    df = df.loc[(ref.r_date.start_date <= df['날짜']) & (df['날짜']<= ref.r_date.target_date )]
    return  df

#SA용 매체 코드

def get_카카오SA_SA():
    df = media_prep('카카오SA')
    df['소재'] = '-'
    return df

def get_네이버SA_SA():
    df = media_prep('네이버SA')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['검색_P', '검색_M', '쇼검브랜드형_P', '쇼검브랜드형_M','네이버BSA_P','네이버BSA_M'])].drop(columns=['지면/상품'])
    df['소재'] = '-'
    return df

def get_NOSP_SA():
    df = media_prep('NOSP')
    df['세트'] = '-'

    pat = re.compile('[A-Z]{4,4}\d{4,4}')
    df['소재'] = df['캠페인']
    df['캠페인'] = df['캠페인'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.loc[df['구분'] == 'NOSP','캠페인'] = df['캠페인'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)

    return df

def get_구글SA_SA():
    df = media_prep('구글SA')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['구글SA_P', '구글SA_M','구글SA_PM'])].drop(columns=['지면/상품'])
    df['소재'] = '-'
    return df

def get_카카오BSA_SA():
    df = ref.kakaobsa_df
    df['날짜'] = pd.to_datetime(df['날짜'])
    df['날짜'] = df['날짜'].dt.date
    df = df.loc[(ref.r_date.start_date <= df['날짜']) & (df['날짜'] <= ref.r_date.target_date)]
    return  df

def get_네이버SS_SA():
    df = media_prep('네이버SS')
    df = pd.merge(df, ref.media_index, on='캠페인', how='left').fillna('no_index')
    df = df.loc[df['지면/상품'].isin(['쇼핑검색_P', '쇼핑검색_M'])]
    df = df.drop(columns=['지면/상품'])
    df['소재'] = '-'
    df['키워드'] = '-'
    return df
