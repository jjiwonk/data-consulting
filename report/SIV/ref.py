import pandas as pd
from setting import directory
from spreadsheet import spreadsheet
import pyarrow as pa
import re
import datetime

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

class r_date :
    target_date = datetime.datetime.strptime(setting_df['리포트 기준 날짜'][0], '%Y-%m-%d').date()
    start_date = datetime.date(target_date.year, target_date.month, 1)
    yearmonth = target_date.strftime('%Y%m')

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

    apps_media = ['Apple Search Ads','Facebook Ads','adisonofferwall_int','criteonew_int','googleadwords_int','naver','buzzvil','kakao','kakao_plus_channel','meta','kakao_int','doohub_int','buzzad_int','naver_int','adisonofferwall_int']
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
        'app_id' : pa.string()
    }

    apps_index =['date', 'media_source', 'campaign', 'adset', 'ad']

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
    apps_agg_index = ['date','media_source_pid','campaign_name', 'adset_name', 'adgroup_name']
    apps_metric =[ '유입(AF)', 'UV(AF)', 'appopen(AF)','구매(AF)', '매출(AF)', '주문취소(AF)', '주문취소매출(AF)', '총주문건(AF)', '총매출(AF)','브랜드구매(AF)', '브랜드매출(AF)', '첫구매(AF)', '첫구매매출(AF)', '설치(AF)', '재설치(AF)','가입(AF)']

    #ga 데이터 컬럼
    ga1_media = ['naver','google','kakao','criteo','meta','kakao_plus_channel','buzzvil','kakao_int','naver_int','buzzad_int','doohub_int','adisonofferwall_int']

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

    def merge_index(df):
        merge_index = df[['머징코드', '캠페인', '세트', '소재']].drop_duplicates(keep='first')
        merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
        merge_index['중복'] = merge_index.duplicated(['머징코드'], False)
        merge_index = merge_index.loc[merge_index['중복'] == False].drop(columns=['중복'])
        return merge_index

    #DS 데이터 컬럼
    ds_raw_metric = ['방문수(DS)', '방문자수(DS)', '구매방문수(DS)', '구매금액(DS)', '회원가입방문수(DS)']
    ds_raw[ds_raw_metric] = ds_raw[ds_raw_metric].astype(float)


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


def adcode_mediapps(df):
    pat = re.compile('[A-Z]{4,4}\d{4,4}')

    df['머징코드'] = df['캠페인'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

    df.index = range(len(df))
    df.loc[df['머징코드'] == 'None', '머징코드'] = df['세트'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')
    df.loc[df['머징코드'] == 'None', '머징코드'] = df['소재'].apply(lambda x: pat.findall(str(x))[-1] if pat.search(str(x)) else 'None')

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
