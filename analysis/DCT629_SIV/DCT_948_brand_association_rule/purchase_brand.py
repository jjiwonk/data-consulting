import setting.directory as dr
import pandas as pd

df = pd.read_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/raw.csv')

df = df[['주문번호', 'appsflyer_id', '파트 구분','매체', '지면/상품', '캠페인', '세트', '소재', '구매브랜드',
       '구매상품', 'num', '구매브랜드리스트', 'order_brand']]

def brand_purchase(df, part ,brand):
       brand_df = df.loc[(df['파트 구분'] == part) & (df['구매브랜드'] == brand)]

       brand_user = brand_df[['appsflyer_id', '주문번호', '지면/상품']]
       brand_user['브랜드구매여부'] = True
       brand_user = brand_user.drop_duplicates(['appsflyer_id', '주문번호', '지면/상품'])
       brand_user = brand_user.drop_duplicates('appsflyer_id', keep='first')
       brand_user = brand_user.rename(columns={'주문번호': '주문번호2', '지면/상품': '지면/상품2'})

       merge_df = pd.merge(df, brand_user, on='appsflyer_id', how='left')
       merge_df = merge_df.loc[merge_df['브랜드구매여부'] == True]

       check = merge_df.loc[merge_df['주문번호'] > merge_df['주문번호2']]
       check.to_csv(dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/{part}_{brand}.csv', encoding='utf-8-sig',
                    index=False)

       return check

tomboy = brand_purchase(df, 'SIVILLAGE','STUDIO TOMBOY')
JLIN = brand_purchase(df, '국내패션','J.LINDEBERG')
jaju = brand_purchase(df, 'JAJU','JAJU')
GROBAL = brand_purchase(df, '글로벌패션','EMPORIO ARMANI UNDERWEAR')
JLIN = brand_purchase(df, '국내패션','J.LINDEBERG')
GROBAL = brand_purchase(df, 'SIBEAUTY','BYREDO')
GROBAL = brand_purchase(df, '글로벌패션','EMPORIO ARMANI UNDERWEAR')
