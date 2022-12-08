from spreadsheet import spreadsheet
import datetime
import os
import pyarrow as pa

doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')

info_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
setting_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 0).reset_index(drop=True)
index_df = spreadsheet.spread_sheet(doc, '광고 인덱스').reset_index(drop=True)
index_df = index_df.loc[index_df['매체'] != '', ['매체', '캠페인', '광고그룹', '소재', 'campaign_id', 'group_id']]


class report_date :
    target_date = datetime.datetime.strptime(setting_df['리포트 기준 날짜'][0], '%Y-%m-%d').date()
    start_date = datetime.date(target_date.year, target_date.month, 1)
    yearmonth = target_date.strftime('%Y%m')


class columns :
    temp_cols = []
    dimension_cols = ['매체']
    metric_cols = []

    for k, v in enumerate(info_df.iloc[0]):
        k = info_df.columns[k]
        if v == 'temp':
            temp_cols.append(k)
        elif v == 'dimension':
            dimension_cols.append(k)
        elif v == 'metric':
            metric_cols.append(k)

    result_columns = dimension_cols + metric_cols
    read_columns = temp_cols + result_columns

    apps_dtype = {
        'attributed_touch_time' : pa.string(),
        'install_time' : pa.string(),
        'event_time' : pa.string(),
        'event_name' : pa.string(),
        'event_revenue' : pa.string(),
        'media_source' : pa.string(),
        'channel' : pa.string(),
        'keywords' : pa.string(),
        'is_primary_attribution' : pa.string(),
        'campaign' : pa.string(),
        'adset' : pa.string(),
        'ad' : pa.string(),
        'sub_param_1' : pa.string(),
        'sub_param_2' : pa.string(),
        'sub_param_3' : pa.string(),
        'sub_param_4' : pa.string(),
        'partner' : pa.string(),
        'platform' : pa.string(),
        'original_url' : pa.string()
    }

    apps_pivot_columns = ['date','partner', 'media_source', 'campaign', 'adset', 'ad',
                          'sub_param_1', 'sub_param_2', 'sub_param_3', 'sub_param_4', 'platform', 'original_url', 'keywords']
    apps_result_columns = ['date', 'partner', 'media_source', 'campaign', 'adset', 'ad', 'sub_param_1', 'sub_param_2',
                           'sub_param_3', 'sub_param_4', 'platform', 'original_url', 'keywords', 'Installs', 're-install',
                           're-open', 'Register', 'Add to Cart_app', 'Purchases_app', 'Revenue_app', 'Open']

    apps_aggregated_dtypes = {
        'view_type': pa.string(),
        'date': pa.string(),
        'agencypmd_af_prt': pa.string(),
        'media_source_pid': pa.string(),
        'campaign_name': pa.string(),
        'adset_name': pa.string(),
        'adgroup_name': pa.string(),
        'os': pa.string(),
        'conversion_type': pa.string(),
        'installs': pa.string(),
        'conversions': pa.string(),
        'af_login_event_counter': pa.string(),
        'af_login_unique_users': pa.string(),
        'af_login_sales_in_krw': pa.string(),
        'af_complete_registration_event_counter': pa.string(),
        'af_complete_registration_unique_users': pa.string(),
        'af_complete_registration_sales_in_krw': pa.string(),
        'af_add_to_cart_event_counter': pa.string(),
        'af_add_to_cart_unique_users': pa.string(),
        'af_add_to_cart_sales_in_krw': pa.string(),
        'af_purchase_event_counter': pa.string(),
        'af_purchase_unique_users': pa.string(),
        'af_purchase_sales_in_krw': pa.string()
    }

    index_columns = list(index_df[4:].columns)

    ### GA
    ga_dimension_cols = ['date','dimension50','sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource']
    ga_metric_cols = ['sessions', 'bounces', 'sessionDuration', 'goal3Completions', 'goal1Completions', 'goal2Completions', 'pageviews', 'transactions', 'transactionRevenue']

item_list = ['read', 'prep', 'temp', 'dimension', 'metric']

info_dict = {}

for media in info_df['매체'][1:].to_list() :
    info_dict[media] = {}
    for item in item_list :
        item_df = info_df.loc[info_df['매체'].isin(['type',media])]
        item_df = item_df.set_index('매체').transpose()
        item_df = item_df.loc[item_df['type']==item]
        item_df = item_df.loc[item_df[media].str.len()>0]

        item_dict = dict(zip(item_df.index, item_df[media]))

        info_dict[media][item] = item_dict

class apps_info:
    target_event_dict = {
        'install' : 'Installs',
        're-engagement' : 're-open',
        'af_add_to_cart' : 'Add to Cart_app',
        'af_complete_registration' : 'Register',
        're-attribution' : 're-install',
        'af_purchase' : 'Purchases_app'
    }

    ctit = 7
    itet = 7

class ga_info:
    ga_rename_dict =  {
        'date': '날짜',
        'sourceMedium': '소스/매체',
        'campaign': '캠페인',
        'dimension50' : 'utm_trg',
        'adContent' : '광고콘텐츠',
        'keyword' : '키워드',
        'deviceCategory' : '기기 카테고리',
        'operatingSystem' : '운영체제',
        'dataSource' : '데이터 소스',
        'sessions' : '세션',
        'goal1Completions': '회원가입목표완료수',
        'goal2Completions': '장바구니목표완료수',
        'transactions': '거래수',
        'transactionRevenue': '수익',
        'bounces' : '이탈수',
        'goal3Completions': '로그인목표완료수',
        'pageviews': '페이지뷰',
        'sessionDuration' : '세션시간'}

    ga_device_dict = {
        'mobile' : 'Mobile',
        'desktop' : 'PC',
        'tablet' : 'Mobile'
    }