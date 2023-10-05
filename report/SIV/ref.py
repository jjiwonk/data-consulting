import pandas as pd
from spreadsheet import spreadsheet
import pyarrow as pa
import re
import datetime
from report.SIV import directory as dr

# 시트  raw
doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/1dGT5KHlB6r0spjwU8xFPbL30WjgKARi3d6uwB9eTw0s/edit#gid=871682176')

info_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
setting_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 0).reset_index(drop=True)
index_df = spreadsheet.spread_sheet(doc, '3.캠그소인덱싱', 0, 1).reset_index(drop=True)
exc_df = spreadsheet.spread_sheet(doc, '예외처리', 0, 0).reset_index(drop=True)
vertical_df = spreadsheet.spread_sheet(doc, '버티컬', 0, 0).reset_index(drop=True)
kakaobsa_df = spreadsheet.spread_sheet(doc, '카카오BSA_raw', 0, 0).reset_index(drop=True)
media_index = index_df[['캠페인', '지면/상품']].drop_duplicates()
ds_df = spreadsheet.spread_sheet(doc, 'DS데이터', 0, 0 ).reset_index(drop=True)
ds_raw = ds_df[['날짜', '머징코드', '방문수(DS)', '방문자수(DS)', '구매방문수(DS)', '구매금액(DS)', '회원가입방문수(DS)']]
sib_bsa = spreadsheet.spread_sheet(doc, '뷰티_BSA_RD', 0, 0 ).reset_index(drop=True)
brand_index = spreadsheet.spread_sheet(doc, '브랜드매출_인덱싱', 0, 3).reset_index(drop=True)
shoppingsa_index = spreadsheet.spread_sheet(doc, '쇼핑검색 그룹ID', 0, 0).reset_index(drop=True)

# 브랜드 구매용 별도 인덱스
ind = index_df[['머징코드','브랜드']].drop_duplicates(keep = 'last').rename(columns = {'브랜드':'브랜드_인덱스'})
ind = ind.drop_duplicates('머징코드')
br_ind = brand_index[['브랜드(index)', '실제 브랜드명 (productBrand, af_brand)']].drop_duplicates(keep = 'last')
br_ind = br_ind.rename(columns = {'브랜드(index)':'브랜드_인덱스','실제 브랜드명 (productBrand, af_brand)':'브랜드'})
br_ind = pd.merge(ind, br_ind, on = '브랜드_인덱스', how = 'left').fillna('-').drop(columns='브랜드_인덱스')

class r_date :
    target_date = datetime.datetime.strptime(setting_df['리포트 기준 날짜'][0], '%Y-%m-%d').date()
    start_date = datetime.date(target_date.year, target_date.month, 1)
    yearmonth = target_date.strftime('%Y%m')
    agg_date = datetime.datetime.strptime(setting_df['집약형 종료 날짜'][0], '%Y-%m-%d').date()
    index_date = int(yearmonth) - 1

class columns:
    metric_cols = []
    dimension_cols = ['매체'] #매체는 타입으로 되어있지만, dimension도 되어야하므로 미리 지정
    temp_cols =[] #전처리에선 쓰지만,최종에선 안쓰는 컬럼들

    for i , type in enumerate(info_df.iloc[0]):
        i = info_df.columns[i]
        if type == 'metric':
            metric_cols.append(i)
        elif type == 'dimension':
            dimension_cols.append(i)
        elif type == 'temp':
            temp_cols.append(i)


    media_dimension = ['날짜', '캠페인', '세트', '소재','구분','머징코드']
    media_mertic = ['노출','도달','클릭', '비용', '조회','SPEND_AGENCY','구매(대시보드)','매출(대시보드)','설치(대시보드)','대시보드(친구추가)','대시보드(참여)']

    code_campaign = ['네이버SA','네이버BSA','카카오SA','카카오BSA','구글SA','naver','kakao','google','googleadwords_int']
    code_set = ['구글','AppleSA','google','googleadwords_int','Apple Search Ads']

    apps_media = ['Apple Search Ads','Facebook Ads','adisonofferwall_int','criteonew_int','googleadwords_int','naver','buzzvil','kakao','kakao_plus_channel','meta','kakao_int','doohub_int','buzzad_int','naver_int','adisonofferwall_int','blind','rtbhouse_int', 'tiktok', 'snow', 'smartscore']
    google_media = ['구글SA', '구글DA','Pmax']

    # 앱스 데이터 컬럼
    apps_dtype = {
        'attributed_touch_type' : pa.string(),
        'attributed_touch_time' : pa.string(),
        'install_time' : pa.string(),
        'event_time' : pa.string(),
        'event_name' : pa.string(),
        'event_value' : pa.string(),
        'event_revenue' : pa.float64(),
        'event_revenue_currency': pa.string(),
        'media_source' : pa.string(),
        'appsflyer_id' : pa.string(),
        'channel' : pa.string(),
        'is_primary_attribution' : pa.string(),
        'campaign' : pa.string(),
        'campaign_id' : pa.string(),
        'adset' : pa.string(),
        'adset_id': pa.string(),
        'ad' : pa.string(),
        'keywords' : pa.string(),
        'partner' : pa.string(),
        'platform' : pa.string(),
        'app_id' : pa.string(),
        'original_url': pa.string(),
    }

    apps_index =['date', 'media_source', 'campaign', 'adset', 'ad','keyword']

    apps_agg_dtype = {
        'date': pa.string(),
        'media_source_pid': pa.string(),
        'agencypmd_af_prt' : pa.string(),
        'campaign_name': pa.string(),
        'conversion_type' : pa.string(),
        'conversions' : pa.float64(),
        'adset_name': pa.string(),
        'adgroup_name': pa.string(),
        'installs': pa.float64(),
        'af_complete_registration_event_counter': pa.float64(),
        'completed_purchase_event_counter': pa.float64(),
        'completed_purchase_sales_in_krw': pa.float64(),
        'first_purchase_event_counter': pa.float64(),
        'first_purchase_sales_in_krw': pa.float64(),
        'cancel_purchase_event_counter': pa.float64(),
        'cancel_purchase_sales_in_krw': pa.float64()}

    apps_rename = {'date' : '날짜',
                   'media_source' : '매체',
                   'campaign' : '캠페인',
                   'adset' : '세트',
                   'ad' : '소재' ,
                   'keyword' : '키워드',
                   'completed_purchase' : '주문' ,
                   'revenue': '주문매출',
                   'first_purchase' : '첫구매(AF)',
                   'first_revenue': '첫구매매출(AF)',
                   'cancel_purchase': '주문취소(AF)',
                   'cancel_revenue': '주문취소매출(AF)',
                   'af_complete_registration': '가입(AF)',
                   'install' : '설치(AF)',
                   're-attribution' : '재설치(AF)',
                   're-engagement' : '리인게이지먼트'}

    apps_agg_rename = {'date': '날짜',
        'media_source_pid': '매체',
        'campaign_name': '캠페인',
        'adset_name': '세트',
        'adgroup_name': '소재',
        'keyword' :'키워드',
        'installs': '설치(AF)',
        'af_complete_registration_event_counter': '가입(AF)',
        'completed_purchase_event_counter': '주문',
        'completed_purchase_sales_in_krw': '주문매출',
        'first_purchase_event_counter': '첫구매(AF)',
        'first_purchase_sales_in_krw': '첫구매매출(AF)',
        'cancel_purchase_event_counter': '주문취소(AF)',
        'cancel_purchase_sales_in_krw':'주문취소매출(AF)',
        're-attribution': '재설치(AF)',
        're-engagement': '리인게이지먼트'}


    apps_agg_metric = ['re-attribution','re-engagement','installs','af_complete_registration_event_counter','completed_purchase_event_counter','completed_purchase_sales_in_krw','first_purchase_event_counter','first_purchase_sales_in_krw','cancel_purchase_event_counter','cancel_purchase_sales_in_krw']
    apps_agg_index = ['date','media_source_pid','campaign_name', 'adset_name', 'adgroup_name','keyword']
    apps_metric =[ '유입(AF)', 'UV(AF)', 'appopen(AF)','구매(AF)', '매출(AF)', '주문취소(AF)', '주문취소매출(AF)', '총주문건(AF)', '총매출(AF)','브랜드구매(AF)', '브랜드매출(AF)', '첫구매(AF)', '첫구매매출(AF)', '설치(AF)', '재설치(AF)','가입(AF)']

    #ga 데이터 컬럼
    ga1_media = ['naver','google','kakao','criteo','meta','kakao_plus_channel','buzzvil','kakao_int','naver_int','buzzad_int','doohub_int','adisonofferwall_int','blind','navershoppingsa','rtbhouse_int', 'tiktok', 'snow', 'smartscore','criteo_cca','zum']

    ga1_dtype = {
        '﻿dataSource': pa.string(),
        'browser': pa.string(),
        'campaign': pa.string(),
        'source': pa.string(),
        'keyword': pa.string(),
        'adContent': pa.string(),
        'medium' : pa.string(),
        'sessions': pa.float64(),
        'users': pa.float64(),
        'goal1Completions': pa.float64(),
        'transactions': pa.float64(),
        'transactionRevenue': pa.float64()}

    ga1_rename = { 'sessions' : '세션(GA)',
                   'users' : 'UA(GA)',
                   'transactions' : '구매(GA)',
                   'transactionRevenue' : '매출(GA)',
                   'goal1Completions': '가입(GA)'
                   }

    ga1_metric = ga1_rename.keys()
    ga_metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)', '가입(GA)','브랜드구매(GA)', '브랜드매출(GA)']

    ga3_dtype = {
        '﻿dataSource': pa.string(),
        'browser': pa.string(),
        'campaign': pa.string(),
        'sessions': pa.float64(),
        'users': pa.float64(),
        'goal1Completions': pa.float64(),
        'transactions': pa.float64(),
        'transactionRevenue': pa.float64()}

    ga2_dtype = {
        '﻿dataSource': pa.string(),
        'browser': pa.string(),
        'campaign': pa.string(),
        'sourceMedium': pa.string(),
        'transactionId': pa.string(),
        'keyword': pa.string(),
        'adContent': pa.string(),
        'productName' : pa.string(),
        'productBrand': pa.string(),
        'uniquePurchases': pa.float64(),
        'itemQuantity': pa.float64(),
        'itemRevenue': pa.float64()}

    ga4_rename = {'세션 캠페인' : 'campaign',
                   '세션 수동 광고 콘텐츠' : 'adContent',
                   '세션 소스/매체' : 'sourceMedium',
                   '세션 수동 검색어' : 'keyword',
                   '거래' : 'transactions',
                   '구매 수익' : 'transactionRevenue',
                   '이벤트 이름': 'eventname',
                   '이벤트 수' : 'eventcount'
                   }

    ga4_rename_final = { 'session_start' : '세션(GA)',
                   'users' : 'UA(GA)',
                   'transactions' : '구매(GA)',
                   'transactionRevenue' : '매출(GA)',
                   'sign_up': '가입(GA)'
                   }


    def merge_index(df):
        merge_index = df[['머징코드', '캠페인', '세트', '소재']].drop_duplicates(keep='first')
        merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
        merge_index['중복'] = merge_index.duplicated(['머징코드'], False)
        merge_index = merge_index.loc[merge_index['중복'] == False].drop(columns=['중복'])
        return merge_index

    #DS 데이터 컬럼
    ds_raw_metric = ['방문수(DS)', '방문자수(DS)', '구매방문수(DS)', '구매금액(DS)', '회원가입방문수(DS)']
    ds_raw[ds_raw_metric] = ds_raw[ds_raw_metric].astype(float)

    report_col = ['파트 구분','연도','월','주차','날짜','매체','지면/상품','캠페인 구분','KPI','캠페인','세트','소재','머징코드','캠페인 라벨','OS','노출','도달','클릭','조회','구매(대시보드)','매출(대시보드)','설치(대시보드)', '대시보드(친구추가)', '대시보드(참여)','비용','SPEND_AGENCY','세션(GA)','UA(GA)','구매(GA)','매출(GA)','브랜드구매(GA)','브랜드매출(GA)','가입(GA)','유입(AF)','UV(AF)','appopen(AF)','구매(AF)','매출(AF)','주문취소(AF)','주문취소매출(AF)','총주문건(AF)','총매출(AF)','브랜드구매(AF)','브랜드매출(AF)','첫구매(AF)','첫구매매출(AF)','설치(AF)','재설치(AF)','가입(AF)','방문수(DS)','방문자수(DS)','구매방문수(DS)','구매금액(DS)','회원가입방문수(DS)','캠페인(인덱스)','세트(인덱스)','프로모션','브랜드','카테고리','소재형태','소재이미지','소재카피']
    media_report_col = ['파트 구분', '날짜', '매체', '지면/상품', '캠페인 구분', 'KPI', '캠페인', '세트', '소재', '머징코드', '캠페인 라벨', 'OS', '노출', '도달', '클릭','조회', '구매(대시보드)', '매출(대시보드)', '설치(대시보드)', '대시보드(친구추가)', '대시보드(참여)', '비용', 'SPEND_AGENCY']

item_list = ['read', 'prep', 'temp', 'dimension', 'metric']

info_dict ={}

for media in info_df['구분'][1:].to_list() :
    info_dict[media] = {}
    for item in item_list :
        item_df = info_df.loc[info_df['구분'].isin(['type',media])]
        item_df = item_df.set_index('구분').transpose()
        item_df = item_df.loc[item_df['type'] == item]
        item_df = item_df.loc[item_df[media].str.len()>0]

        item_dict = dict(zip(item_df.index,item_df[media]))

        info_dict[media][item] = item_dict

exc_cdict = dict(zip(exc_df['캠페인'],exc_df['캠페인(변경)']))
exc_gdict = dict(zip(exc_df['그룹'],exc_df['그룹(변경)']))
exc_adict = dict(zip(exc_df['소재'],exc_df['소재(변경)']))
exc_ga_adict = dict(zip(exc_df['GA소재'],exc_df['GA소재(변경)']))
exc_code_dict = dict(zip(exc_df['코드'],exc_df['코드(변경)']))

def adcode(df,depth1,depth2,depth3):
    pat = re.compile('[A-Z]{4,4}\d{4,4}')

    df['머징코드'] = df[depth1].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    df.index = range(len(df))
    df.loc[df['머징코드'] == 'None', '머징코드'] = df[depth2].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.loc[df['머징코드'] == 'None', '머징코드'] = df[depth3].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    return df

def adcode_ga(df):
    pat = re.compile('[A-Z]{4,4}\d{4,4}')

    df['머징코드'] = df['campaign'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.index = range(len(df))
    df.loc[df['머징코드'] == 'None', '머징코드'] = df['adContent'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    return df

def adcode_sa(df):
    pat = re.compile('[A-Z]{4,4}\d{4,4}')

    df['머징코드'] = df['캠페인'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    df.index = range(len(df))
    df.loc[df['머징코드'] == 'None', '머징코드'] = df['세트'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.loc[df['머징코드'] == 'None', '머징코드'] = df['키워드'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    return df

def date_dt(df):
    df['날짜'] = pd.to_datetime(df['날짜'])
    df['날짜'] = df['날짜'].dt.date
    return df

def week_day(df):
    df['날짜'] = df['날짜'].apply(pd.to_datetime)
    df['연도'] = df['날짜'].apply(lambda x: x.strftime('%Y'))
    df['월'] = df['날짜'].apply(lambda x: x.strftime('%m'))
    df['날짜'] = df['날짜'].dt.date
    week_day = 7
    df['주차'] = pd.to_datetime(df['날짜']).apply(lambda x: (x + datetime.timedelta(week_day)).isocalendar()[1]) -1
    return df

def index_dup_drop(index,depth):
    index = index.drop_duplicates(keep='last')
    index['중복'] = index.duplicated([depth], False)
    index_dup = index.loc[index['중복'] != False].drop(columns=['중복'])
    index_dup.to_csv(dr.download_dir + f'/index_dup_{depth}.csv', index=False, encoding='utf-8-sig')
    index = index.loc[index[depth] != '']
    index = index.loc[index['중복'] == False].drop(columns = '중복')
    return index

