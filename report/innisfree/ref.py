from spreadsheet import spreadsheet
import datetime
import os
import pyarrow as pa

doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')

info_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
setting_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 0).reset_index(drop=True)
index_df = spreadsheet.spread_sheet(doc, '광고 인덱스').reset_index(drop=True)
index_df = index_df.loc[index_df['매체(표기)'] != '']
merging_df = spreadsheet.spread_sheet(doc, '자동화 머징 정보', 0, 1).reset_index(drop=True)
handi_df = spreadsheet.spread_sheet(doc, '수기 데이터').reset_index(drop=True)


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
        'attributed_touch_type' : pa.string(),
        'attributed_touch_time' : pa.string(),
        'install_time' : pa.string(),
        'event_time' : pa.string(),
        'event_name' : pa.string(),
        'event_revenue' : pa.string(),
        'media_source' : pa.string(),
        'appsflyer_id' : pa.string(),
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
    apps_index_columns = ['date', 'campaign', 'adset', 'ad']
    apps_metric_columns = ['Installs', 're-install', 're-open', 'Register', 'Add to Cart_app', 'Purchases_app', 'Revenue_app', 'Open']
    apps_result_columns = ['date', 'partner', 'media_source', 'campaign', 'adset', 'ad', 'sub_param_1', 'sub_param_2',
                           'sub_param_3', 'sub_param_4', 'platform', 'original_url', 'keywords', 'Installs', 're-install',
                           're-open', 'Register', 'Add to Cart_app', 'Purchases_app', 'Revenue_app', 'Open']

    index_columns = ['랜딩2', '캠페인 목표', 'OS', '디바이스', 'Part', 'Promotion', 'Promotion Name', '매체 구분자', '소재설명1', '소재설명2', '소재설명3',
                     '품목', '구분', '랜딩', '전략', 'source', 'medium', '이미지 업로드URL']

    ### GA
    ga_dimension_cols = ['date','dimension50','sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource']
    ga_metric_cols = ['sessions', 'bounces', 'sessionDuration', 'goal3Completions', 'goal1Completions', 'goal2Completions', 'pageviews', 'transactions', 'transactionRevenue']
    ga_metric_cols_kor = ['세션','회원가입목표완료수','장바구니목표완료수','거래수','수익','이탈수','로그인목표완료수','페이지뷰','세션시간']

    ### 최종 리포트
    final_dict = {'품목': '품목',
                  'Part': 'Part',
                  '일자': 'date',
                  '캠페인 목표': '캠페인',
                  'campaign_id': 'campaign ID',
                  'source': 'source',
                  'medium': 'medium',
                  '캠페인': 'campaign_name',
                  'Promotion': 'promotion',
                  'Promotion Name': 'promotion_name',
                  '구분': '구분',
                  '매체': '매체',
                  '광고그룹': 'group',
                  'ad': 'ad',
                  '디바이스': 'Device',
                  'OS': 'os',
                  '랜딩': '랜딩',
                  '랜딩2': '랜딩2',
                  '전략': '전략',
                  '이미지 업로드URL': '이미지 링크',
                  'cost(대시보드)': 'cost(dashboard)',
                  'cost(정산기준)': 'Cost',
                  'imp': 'impression',
                  'click': 'click',
                  '네이버 purchase_web': '네이버 purchase_web',
                  '네이버 revenue_web': '네이버 revenue_web',
                  '스마트스토어sessions': '스마트스토어sessions',
                  '스마트스토어결제수(마지막클릭)': '스마트스토어결제수(마지막클릭)',
                  '스마트스토어결제금액(마지막클릭)': '스마트스토어결제금액(마지막클릭)',
                  '세션': 'Sessions_ga',
                  '회원가입목표완료수': 'Register_ga',
                  '장바구니목표완료수': 'Add to Cart_ga',
                  '거래수': 'Transactions_ga',
                  '수익': 'Revenue_ga',
                  '이탈수': '이탈수',
                  'Installs': 'Installs_app',
                  're-install': 're-install_app',
                  're-open': 're-open_app',
                  'Open': 'Open(Installs+re-install+re-open)_app',
                  'Register': 'Register_app',
                  'Add to Cart_app': 'Add to Cart_app',
                  'Purchases_app': 'Purchases_app',
                  'Revenue_app': 'Revenue_app',
                  'view': 'view',
                  'reach': 'reach',
                  'engage': 'engage',
                  'engage_like': 'engage_like',
                  'engage_comment': 'engage_comment',
                  'engage_save': 'engage_save',
                  'engage_share': 'engage_share'
                  }
    final_cols = ['품목',
                  'Part',
                  'Year',
                  'month',
                  'week',
                  'date',
                  'day',
                  '캠페인',
                  'campaign ID',
                  'source',
                  'medium',
                  'campaign_name',
                  'promotion',
                  'promotion_name',
                  '구분',
                  '매체',
                  'group',
                  'ad',
                  'Device',
                  'os',
                  '랜딩',
                  '랜딩2',
                  '전략',
                  '이미지 링크',
                  'cost(dashboard)',
                  'Cost',
                  'impression',
                  'click',
                  '네이버 purchase_web',
                  '네이버 revenue_web',
                  '스마트스토어sessions',
                  '스마트스토어결제수(마지막클릭)',
                  '스마트스토어결제금액(마지막클릭)',
                  'Sessions_ga',
                  'Register_ga',
                  'Add to Cart_ga',
                  'Transactions_ga',
                  'Revenue_ga',
                  '이탈수',
                  'Installs_app',
                  're-install_app',
                  're-open_app',
                  'Open(Installs+re-install+re-open)_app',
                  'Register_app',
                  'Add to Cart_app',
                  'Purchases_app',
                  'Revenue_app',
                  'cost(0.88)',
                  'cost(0.90)',
                  'view',
                  'reach',
                  'engage',
                  'engage_like',
                  'engage_comment',
                  'engage_save',
                  'engage_share'
                  ]


item_list = ['read', 'prep', 'temp', 'dimension', 'metric']

info_dict = {}

for media in info_df['소스'][1:].to_list() :
    info_dict[media] = {}
    for item in item_list :
        item_df = info_df.loc[info_df['소스'].isin(['type',media])]
        item_df = item_df.set_index('소스').transpose()
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

    apps_aggregated_rename_dict = {
        'media_source_pid': 'media_source',
        'campaign_name': 'campaign',
        'adset_name': 'adset',
        'adgroup_name': 'ad',
        'installs': 'Installs',
        're-attribution': 're-install',
        're-engagement' : 're-open',
        'af_complete_registration_event_counter': 'Register',
        'af_add_to_cart_event_counter': 'Add to Cart_app',
        'af_purchase_event_counter': 'Purchases_app',
        'af_purchase_sales_in_krw': 'Revenue_app'
    }

    ctit = 7
    itet = 7

    agg_data_media_filter = ['Facebook Ads']
    agg_data_column_order = ['view_type', 'date', 'agencypmd_af_prt', 'media_source_pid', 'campaign_name', 'adset_name', 'adgroup_name',
         'os', 'conversion_type', 'installs', 'conversions', 'af_complete_registration_event_counter',
         'af_add_to_cart_event_counter',
         'af_purchase_event_counter', 'af_purchase_sales_in_krw']

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


class media_info:
    bs_ad_dict = {'메인이미지.링크': 'main', '메인이미지.이미지': 'main', '메인텍스트.타이틀': 'main',
                  '버튼[1].텍스트': 'button_1', '버튼[2].텍스트': 'button_2',
                  '브랜드소식.링크': 'brandnews', '브랜드추천.더보기링크': 'more',
                  '브랜드추천.브랜드추천[1].링크': 'product_1', '브랜드추천.브랜드추천[2].링크': 'product_2',
                  '브랜드추천.브랜드추천[3].링크': 'product_3', '브랜드추천.브랜드추천[4].링크': 'product_4',
                  '서브링크[1].텍스트': 'button_1', '서브링크[2].텍스트': 'button_2',
                  '썸네일[1].링크': 'thumb_1', '썸네일[2].링크': 'thumb_2', '썸네일[3].링크': 'thumb_3',
                  '홈링크.링크': 'homelink', 'x': '-', '쇼핑라이브.라이브링크': 'brandnews', '쇼핑라이브.예고페이지': 'brandnews',
                  '서브링크[3].텍스트': 'button_3', '서브링크[4].텍스트': 'button_4', '동영상.스틸이미지': 'main_video',
                  '우측메뉴[1].텍스트': 'button_1', '우측메뉴[2].텍스트': 'button_2', '우측메뉴[3].텍스트': 'button_3',
                  '우측메뉴[4].텍스트': 'button_4', '우측메뉴[5].텍스트': 'button_5', '브랜딩배너.이미지': 'main_banner',
                  '메인텍스트.링크': 'main', '이미지형.메인이미지.링크': 'main', '이미지형.메인텍스트.링크': 'main',
                  '이미지형.서브링크[1].텍스트': 'button_1', '이미지형.서브링크[2].텍스트': 'button_2'}
