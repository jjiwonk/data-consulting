import pandas as pd
from report.SIV import ref
import report.SIV.apps_prep as aprep
import report.SIV.directory as dr
from workers import read_data
import os
import pyarrow as pa

def apps_prep():

    df = aprep.apps_prep()

    df = df.loc[df['event_name'].isin(['completed_purchase', 'first_purchase'])]

    df['브랜드구분'] = df['event_value'].apply(lambda x: eval(x))
    df['구매브랜드'] = df['브랜드구분'].apply(lambda x: x.get('af_brand'))
    df['구매금액'] = df['브랜드구분'].apply(lambda x: x.get('af_price'))
    df['구매건수'] = df['브랜드구분'].apply(lambda x: x.get('af_quantity'))
    df['구매상품'] = df['브랜드구분'].apply(lambda x: x.get('af_content_name'))
    df['주문번호'] =  df['브랜드구분'].apply(lambda x: x.get('af_order_id'))
    df['제품번호'] =  df['브랜드구분'].apply(lambda x: x.get('af_content_id'))

    # 머징코드 적용
    df = ref.adcode(df, 'campaign', 'adset', 'ad')
    df = df.loc[df['머징코드'] != 'None']
    df = df.dropna(subset = ['구매브랜드'])
    df.index = range(len(df))

    brand_df = pd.DataFrame({'날짜': '-', '주문번호' : '-','머징코드': '-', '구매상품': '-', '구매브랜드': '-', '구매':'-','매출': '-','appsflyer_id': '-'}, index=[0])

    for i in range(len(df)):
        brand_cnt  = len(df['구매브랜드'][i])

        for n in range(brand_cnt):
            date = df['date'][i]
            order_id = df['주문번호'][i]
            merge_code = df['머징코드'][i]
            order_brand = df['구매브랜드'][i][n]
            order_cnt = df['구매건수'][i][n]
            order_item = df['구매상품'][i][n]
            order_reve = df['구매금액'][i][n][0]
            apps_id = df['appsflyer_id'][i]
            append_df = pd.DataFrame({'날짜': date ,'주문번호' : order_id,'머징코드': merge_code,'구매상품': order_item , '구매브랜드': order_brand , '구매' : order_cnt,'매출': order_reve,'appsflyer_id': apps_id}, index=[0])
            brand_df = brand_df.append(append_df, ignore_index= True)

    brand_df = brand_df.loc[brand_df['날짜'] != '-']
    brand_df['날짜'] = pd.to_datetime(brand_df['날짜'])
    brand_df['날짜'] = brand_df['날짜'].dt.date

    brand_df[['구매','매출']] = brand_df[['구매','매출']].astype(float)

    brand_df = brand_df[['날짜','appsflyer_id', '머징코드','주문번호','구매상품', '구매브랜드','구매','매출']]

    return brand_df

def df_merge():

    apps = apps_prep()

    # 캠페인 인덱스
    file_dir = dr.report_dir + 'report/media_raw'
    file_list = os.listdir(file_dir)

    dtypes = {
        '머징코드': pa.string(),
        '캠페인': pa.string(),
        '세트': pa.string(),
        '소재': pa.string()}

    media_df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    media_df = media_df.drop_duplicates(keep='last')
    media_df = media_df.loc[media_df['머징코드'] != 'None']
    media_df = media_df.drop_duplicates(subset='머징코드', keep='last')

    merge_df = pd.merge(apps,media_df, on = '머징코드',how = 'left').fillna(0)
    merge_df = merge_df.loc[merge_df['캠페인'] != 0]

    # 캠페인 라벨 인덱싱
    campaign_index = ref.index_df[['파트 구분','매체','지면/상품','캠페인 구분','KPI','머징코드','캠페인 라벨','OS']]
    campaign_index = campaign_index.drop_duplicates(subset='머징코드', keep='last')

    # 소재 라벨 인덱싱
    ad_index = ref.index_df[['머징코드','캠페인(인덱스)', '세트(인덱스)', '프로모션', '브랜드', '카테고리', '소재형태', '소재이미지', '소재카피']]
    ad_index = ref.index_dup_drop(ad_index,'머징코드')
    ad_index = ad_index.loc[ad_index['캠페인(인덱스)'] != ''].fillna('-')

    merge_df = pd.merge(merge_df,campaign_index, on = '머징코드',how = 'left').fillna('-')
    merge_df = pd.merge(merge_df, ad_index, on='머징코드', how='left').fillna('-')

    merge_df = ref.week_day(merge_df)
    merge_df = merge_df[['주문번호','appsflyer_id','파트 구분','연도','월','주차','날짜','매체','지면/상품','캠페인 구분','KPI','캠페인','세트','소재','머징코드','캠페인 라벨','OS','구매브랜드','구매상품','구매','매출','캠페인(인덱스)','세트(인덱스)','프로모션','브랜드','카테고리','소재형태','소재이미지','소재카피']].replace('','-')

    return merge_df

merge_df = df_merge()

# 위 코드 구동 시 당월 데이터만 뽑히는 구조로 ,시트 변경 필요하여 아래 경로에 미리 돌려놓은 파일들 적재해둠
# dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/raw/apps'