import pandas as pd
from report.SIV import ref
import report.SIV.ga_prep as gprep
import report.SIV.apps_prep as aprep
import report.SIV.merging as merge
import report.SIV.directory as dr
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def ga_prep():

    df = gprep.ga_read('type2', ga4='(ga4contents)',header= 6)
    df.columns = ['거래 ID','상품 브랜드','항목 이름','세션 캠페인','세션 수동 광고 콘텐츠','상품 수량','상품 수익','총 합계','날짜']

    df = df.rename(columns = ref.columns.ga4_rename)
    df = gprep.brand_exception(df)
    df = ref.adcode_ga(df)
    df = df.loc[df['머징코드'] != 'None']

    df['구매건수(GA)'] = df['상품 수량']
    df['매출(GA)'] = df['상품 수익']

    df['날짜'] = pd.to_datetime(df['날짜'])
    df['날짜'] = df['날짜'].dt.date

    df = df.rename(columns = {'상품 브랜드':'구매브랜드','항목 이름' : '구매상품'})
    df = df[['날짜', '머징코드','구매상품', '구매브랜드', '구매건수(GA)','매출(GA)']]
    df = df.groupby(['날짜', '머징코드','구매상품', '구매브랜드'])[['구매건수(GA)','매출(GA)']].sum().reset_index()

    return df

def apps_prep():

    df = aprep.apps_prep()

    df = df.loc[df['event_name'].isin(['completed_purchase', 'first_purchase'])]

    df['브랜드구분'] = df['event_value'].apply(lambda x: eval(x))
    df['구매브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_brand'))
    df['구매금액'] = df['브랜드구분'].apply(lambda x: x.get('af_price'))
    df['구매건수'] = df['브랜드구분'].apply(lambda x: x.get('af_quantity'))
    df['구매상품'] = df['브랜드구분'].apply(lambda x: x.get('af_content_name'))

    # 머징코드 적용
    df = ref.adcode(df, 'campaign', 'adset', 'ad')
    df = df.loc[df['머징코드'] != 'None']
    df = df.dropna(subset = ['구매브랜드'])
    df.index = range(len(df))

    brand_df = pd.DataFrame({'날짜': '-', '머징코드': '-', '구매상품': '-', '구매브랜드': '-', '구매건수(AF)':'-','매출(AF)': '-'}, index=[0])

    for i in range(len(df)):
        brand_cnt  = len(df['구매브랜드'][i])

        for n in range(brand_cnt):
            date = df['date'][i]
            merge_code = df['머징코드'][i]
            order_brand = df['구매브랜드'][i][n]
            order_cnt = df['구매건수'][i][n]
            order_item = df['구매상품'][i][n]
            order_reve = df['구매금액'][i][n][0]
            append_df = pd.DataFrame({'날짜': date ,'머징코드': merge_code,'구매상품': order_item , '구매브랜드': order_brand , '구매건수(AF)' : order_cnt,'매출(AF)': order_reve}, index=[0])
            brand_df = brand_df.append(append_df, ignore_index= True)

    brand_df = brand_df.loc[brand_df['날짜'] != '-']
    brand_df['날짜'] = pd.to_datetime(brand_df['날짜'])
    brand_df['날짜'] = brand_df['날짜'].dt.date

    brand_df[['구매건수(AF)','매출(AF)']] = brand_df[['구매건수(AF)','매출(AF)']].astype(float)
    brand_df = brand_df.groupby(['날짜', '머징코드','구매상품', '구매브랜드'])[['구매건수(AF)','매출(AF)']].sum().reset_index()

    return brand_df

def df_merge():

    ga = ga_prep()
    apps = apps_prep()
    df = pd.merge(ga,apps,on = ['날짜', '머징코드','구매상품', '구매브랜드'],how ='outer').fillna(0)

    # 캠페인 인덱스
    media_raw = merge.total_media_raw()

    merge_index = media_raw[['머징코드', '캠페인', '세트', '소재']]
    merge_index2 = pd.read_csv(dr.download_dir + f'media_raw/media_raw_{ref.r_date.index_date}.csv')
    merge_index2 = merge_index2[['머징코드', '캠페인', '세트', '소재']]
    merge_index = pd.concat([merge_index, merge_index2])

    merge_index = merge_index.drop_duplicates(keep='last')
    merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
    merge_index = merge_index.drop_duplicates(subset='머징코드', keep='last')

    df = pd.merge(df,merge_index, on = '머징코드',how = 'left').fillna(0)
    df = df.loc[df['캠페인'] != 0]

    # 캠페인 라벨 인덱싱
    campaign_index = ref.index_df[['파트 구분','매체','지면/상품','캠페인 구분','KPI','머징코드','캠페인 라벨','OS']]
    campaign_index = campaign_index.drop_duplicates(subset='머징코드', keep='last')

    # 소재 라벨 인덱싱
    ad_index = ref.index_df[['머징코드','캠페인(인덱스)', '세트(인덱스)', '프로모션', '브랜드', '카테고리', '소재형태', '소재이미지', '소재카피']]
    ad_index = ref.index_dup_drop(ad_index,'머징코드')
    ad_index = ad_index.loc[ad_index['캠페인(인덱스)'] != ''].fillna('-')

    df = pd.merge(df,campaign_index, on = '머징코드',how = 'left').fillna('-')
    df = pd.merge(df, ad_index, on='머징코드', how='left').fillna('-')

    df = ref.week_day(df)
    df = df[['파트 구분','연도','월','주차','날짜','매체','지면/상품','캠페인 구분','KPI','캠페인','세트','소재','머징코드','캠페인 라벨','OS','구매브랜드','구매상품','구매건수(GA)','매출(GA)','구매건수(AF)','매출(AF)','캠페인(인덱스)','세트(인덱스)','프로모션','브랜드','카테고리','소재형태','소재이미지','소재카피']].replace('','-')

    df.to_csv(dr.download_dir + f'/brand_order_report/brand_order_report_{ref.r_date.yearmonth}.csv',index = False, encoding = 'utf-8-sig')
    print('브랜드 구매/매출 리스트 추출 완료')

    return df

