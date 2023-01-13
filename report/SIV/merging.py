import pandas as pd
from report.SIV import ref
import report.SIV.media_prep as mprep
#import report.SIV.ga_prep as gprep
#import report.SIV.apps_prep as aprep
from report.SIV import directory as dr

import re

def total_media_raw():

    info_df = ref.info_df
    media_list = info_df.loc[info_df['사용 여부'] == 'TRUE']['구분'].to_list()
    media_list = media_list + ['버티컬']
    func_list = []

    for media in media_list:
        func = f'get_{media}'
        func_list.append(func)

    df = pd.DataFrame()

    for func in func_list:
        media_df = getattr(mprep,func)()
        df = pd.concat([df, media_df])
        df = df.groupby(ref.columns.media_dimension)[ref.columns.media_mertic].sum().reset_index()
        df = df.sort_values(by = ['날짜', '구분'])

    df['sum'] = df[ref.columns.media_mertic].sum(axis=1)
    df = df.loc[df['sum'] >= 1]

    df = ref.adcode_mediapps(df)

    df = df[['구분','날짜', '캠페인', '세트', '소재', '노출', '도달', '클릭', '조회','비용','구매(대시보드)','매출(대시보드)','설치(대시보드)','SPEND_AGENCY','머징코드']]

    index = ref.index_df[['지면/상품','매체','캠페인 구분','KPI','캠페인 라벨','OS','파트(주체)','파트 구분','머징코드']]
    index = index.drop_duplicates(keep='first')
    merge = pd.merge(df, index, on='머징코드', how='left').fillna('no_index')

    merge.to_csv(dr.download_dir + 'media_raw.csv', index=False, encoding='utf-8-sig')

    #df = df.drop(columns='구분')

    return df

df = total_media_raw()

