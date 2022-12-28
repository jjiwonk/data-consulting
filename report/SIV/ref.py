import pandas as pd
from setting import directory
from spreadsheet import spreadsheet
import pyarrow as pa

import datetime

doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/14u22kp7HAfm9Q6lghJ4Q5VzsWZuYxQnRAr8C9ESPJN0/edit#gid=589872474')

info_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
setting_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 0).reset_index(drop=True)
media_df = spreadsheet.spread_sheet(doc, '자동화 리포트룰', 0, 3).reset_index(drop=True)

index_df = spreadsheet.spread_sheet(doc, '광고 인덱스', 0, 0).reset_index(drop=True)

class r_date :
    target_date = datetime.datetime.strptime(setting_df['리포트 기준 날짜'][0], '%Y-%m-%d').date()
    #start_date = datetime.date(target_date.year, target_date.month, 1)
    start_date = datetime.date(target_date.year, target_date.month, 15)
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

    result_cols = dimension_cols +  metric_cols
    read_cols = result_cols + temp_cols

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
        'adset' : pa.string(),
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



    ga1_dtype = {
        '﻿dataSource': pa.string(),
        'browser': pa.string(),
        'campaign': pa.string(),
        'source': pa.string(),
        'keyword': pa.string(),
        'adContent': pa.string(),
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


item_list = ['read', 'prep', 'temp', 'dimension', 'metric']

info_dict ={}

for media in info_df['매체'][1:].to_list() :
    info_dict[media] = {}
    for item in item_list :
        item_df = info_df.loc[info_df['매체'].isin(['type',media])]
        item_df = item_df.set_index('매체').transpose()
        item_df = item_df.loc[item_df['type'] == item]
        item_df = item_df.loc[item_df[media].str.len()>0]

        item_dict = dict(zip(item_df.index,item_df[media]))

        info_dict[media][item] = item_dict

rule_dict_p = dict(zip(media_df['캠페인명'], media_df['매체']))
rule_dict_f = dict(zip(media_df['캠페인 명 마지막 문자열 기준(규칙)'], media_df['매체(규칙)']))




