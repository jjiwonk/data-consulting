import datetime
from dateutil.relativedelta import relativedelta
import pyarrow as pa
import pandas as pd
import numpy as np
from workers.func import get_event_from_values
import os

from setting import directory as dr
from workers import read_data
from workers.func import FunnelDataGenerator


def get_total_data():
    athena_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/코오롱몰_2022/1. 리포트/#자동화/1차 RAW/아데나 데이터'
    athena_files = os.listdir(athena_dir)
    organic_files = [f for f in athena_files if 'organic' in f]
    paid_files = [f for f in athena_files if 'paid' in f]
    direct_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/코오롱몰_2022/1. 리포트/#자동화/1차 RAW/오가닉 데이터'
    direct_files = os.listdir(direct_dir)
    paid_dtypes = {
        'install_time': pa.string(),
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_value': pa.string(),
        'campaign': pa.string(),
        'adset': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source': pa.string(),
        'advertising_id': pa.string(),
        'customer_user_id': pa.string(),
        'idfa': pa.string()}
    organic_dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_revenue': pa.string(),
        'appsflyer_id': pa.string(),
        'advertising_id': pa.string(),
        'customer_user_id': pa.string(),
        'idfa': pa.string()}
    dtypes2 = {
        'Install Time': pa.string(),
        'Attributed Touch Time': pa.string(),
        'Attributed Touch Type': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Event Revenue': pa.string(),
        'Campaign': pa.string(),
        'AppsFlyer ID': pa.string(),
        'Media Source': pa.string(),
        'Advertising ID': pa.string(),
        'Customer User ID': pa.string(),
        'IDFA': pa.string()}

    paid_data = read_data.pyarrow_csv(dtypes=paid_dtypes, directory=athena_dir, file_list=paid_files)
    paid_data['event_revenue'] = get_event_from_values(np.array(paid_data['event_value']), 'af_revenue')
    paid_data.loc[paid_data['event_name'] == 'install', 'event_name'] = 'paid_install'
    organic_data = read_data.pyarrow_csv(dtypes=organic_dtypes, directory=athena_dir, file_list=organic_files)
    athena_data = pd.concat([paid_data, organic_data])

    direct_data = read_data.pyarrow_csv(dtypes=dtypes2, directory=direct_dir, file_list=direct_files)
    direct_data.columns = [col.lower().replace(' ', '_') for col in direct_data.columns]
    data = pd.concat([athena_data, direct_data])
    data['event_time'] = pd.to_datetime(data['event_time'])
    data['attributed_touch_time'] = pd.to_datetime(data['attributed_touch_time'])
    total_data = data.drop_duplicates().sort_values('event_time').reset_index(drop=True)

    return total_data


def data_prep(total_data):
    # ad_type 구분
    total_data.loc[
        (total_data['media_source'].isin(['kakao_int'])) & (total_data['campaign'] == 'kakao_biz_purchase') & (
                'rt' in str(total_data['adset'])), 'ad_type'] = 'RE'
    total_data.loc[(total_data['media_source'].isin(['facebook', 'facebook_ads'])) & (
            total_data['campaign'] == 'meta_feed_purchase') & ('rt' in str(total_data['adset'])), 'ad_type'] = 'RE'
    total_data.loc[(total_data['media_source'].isin(['googleadwords_int'])) & (
            total_data['campaign'] == 'google_pmax_purchase'), 'ad_type'] = 'RE'
    total_data.loc[(total_data['media_source'].isin(['rtbhouse_int'])), 'ad_type'] = 'RE'

    total_data.loc[(total_data['media_source'].isin(['kakao_int']))  & (total_data['campaign'] == 'kakao_biz_purchase')
                        & (total_data['adset'].isin(['AOS_al_mf_2559_rt_pur', 'IOS_al_mf_2559_rt_pur'])), 'ad_type'] = 'UA'
    total_data.loc[(total_data['media_source'].isin(['kakao_int'])) & (total_data['campaign'] == 'kakao_display_traffic'), 'ad_type'] = 'UA'
    total_data.loc[(total_data['media_source'].isin(['facebook', 'facebook_ads'])) & (
        total_data['campaign'].isin(['meta_aaa_install', 'meta_aaa_install_IOS'])), 'ad_type'] = 'UA'
    total_data.loc[total_data['media_source'].isin(
        ['kakao_plus', 'google_dsc', 'cauly_int', 'criteonew_int', 'buzzvill', 'naver_pc', 'naver_m', 'daum_pc',
         'daum_m', 'naverbs_pc', 'naverbs_m', 'Apple', 'Search', 'Ads', 'naver_gfa', 'clearpier',
         'tiktok_display']), 'ad_type'] = 'UA'

    total_data.loc[(total_data['media_source'].isin(['facebook', 'facebook_ads'])) & (
        total_data['campaign'].isin(['ig_reels_branding', 'meta_feed_purchase_attomax'])), 'ad_type'] = '제외'
    total_data.loc[(total_data['media_source'].isin(['naver_band'])) | (
    (total_data['media_source'].isin(['naver_brandingda']) & ('branding' in str(total_data['adset'])))), 'ad_type'] = '제외'
    total_data.loc[(total_data['media_source'].isin(['googleadwords_int'])) & (total_data['campaign'] == 'branding') & (
            'branding' in str(total_data['adset'])), 'ad_type'] = '제외'
    total_data = total_data.fillna('')

    # media 구분
    total_data.loc[total_data['media_source'].isin(['kakao_int', 'kakao_plus']), 'media'] = '카카오'
    total_data.loc[total_data['media_source'].isin(['facebook', 'facebook_ads']), 'media'] = '메타'
    total_data.loc[total_data['media_source'].isin(['googleadwords_int', 'google_dsc']), 'media'] = '구글'
    total_data.loc[total_data['media_source'].isin(['cauly_int', 'criteonew_int']), 'media'] = 'NCPI, DSP'
    total_data.loc[total_data['media_source'].isin(['buzzvill']), 'media'] = '버즈빌'
    total_data.loc[total_data['media_source'].isin(['naver_pc', 'naver_m']), 'media'] = '네이버 SA'
    total_data.loc[total_data['media_source'].isin(['daum_pc', 'daum_m']), 'media'] = '다음 SA'
    total_data.loc[total_data['media_source'].isin(['naverbs_pc', 'naverbs_m']), 'media'] = '네이버 브검'
    total_data.loc[total_data['media_source'].isin(['Apple Search Ads']), 'media'] = '애플서치애드'
    total_data.loc[total_data['media_source'].isin(['naver_gfa', 'naver_band', 'naver_brandingda']), 'media'] = 'GFA'
    total_data.loc[total_data['media_source'].isin(['clearpier']), 'media'] = 'NCPI'
    total_data.loc[total_data['media_source'].isin(['tiktok_display']), 'media'] = '틱톡'
    total_data.loc[total_data['media_source'].isin(['rtbhouse_int']), 'media'] = 'DSP'
    total_data['media'] = total_data['media'].fillna('')

    total_data['appsflyer_id'] = total_data['appsflyer_id'].fillna('')
    total_data = total_data.loc[total_data['appsflyer_id'] != ''].reset_index(drop=True)
    total_data.loc[total_data['event_name'] == 'first_purchase', 'event_name'] = 'af_purchase'
    sorted_total_data = total_data.sort_values(['appsflyer_id', 'event_time', 'event_name', 'install_time'])
    dedup_total_data = sorted_total_data.drop_duplicates(['appsflyer_id', 'event_time'], keep='last').reset_index(drop=True)
    dedup_total_data['event_revenue'] = dedup_total_data['event_revenue'].fillna(0)

    return dedup_total_data


today = datetime.datetime.strptime('2023-07-01', '%Y-%m-%d')
from_date = today - relativedelta(months=3)
data = get_total_data()
paid_event = ['re-engagement', 're-attribution']
kpi_event = 'af_purchase'
kpi_period = 7*24*60*60

# 전체 유저 데이터 기반
# prep_data = data_prep(data, paid_event)
# user_arr = prep_data['appsflyer_id']
# event_arr = prep_data['event_name']
# event_time_arr = prep_data['event_time']
# value_arr = prep_data['event_revenue']
# media_arr = prep_data['ad_type']

# 광고 유입 유저 데이터 기반
adid_list = set(data.loc[data['event_name'].isin(['paid_install', 're-engagement', 're-attribution']), 'appsflyer_id'])
paid_user_data = data.loc[data['appsflyer_id'].isin(adid_list)].reset_index(drop=True)
paid_prep_data = data_prep(paid_user_data)
user_arr = paid_prep_data['appsflyer_id']
event_arr = paid_prep_data['event_name']
event_time_arr = paid_prep_data['event_time']
value_arr = paid_prep_data['event_revenue']
media_arr = paid_prep_data['ad_type'] + '/' + paid_prep_data['media']


# 1. 퍼널(세션) 데이터 생성
# 컬럼: appsflyer_id, funnel_id, funnel_sequence, start_time, end_time, is_paid, kpi_achievement, fee_value
funnel_generator = FunnelDataGenerator(user_arr, event_arr, event_time_arr, value_arr, media_arr, kpi_event, kpi_period, paid_event)
funnel_generator.do_work()
funnel_data = funnel_generator.data
funnel_data['ad_type'] = funnel_data['media'].apply(lambda x: x.split('/')[0])
funnel_data['media_div'] = funnel_data['media'].apply(lambda x: x.split('/')[1] if '/' in x else '')
funnel_data.loc[funnel_data['media_div'] == 'NCPI, DCP', 'media_div'] = 'NCPI, DSP'

# 2. 추출 후 태블로에서 계산
result_data = funnel_data.sort_values(['user_id', 'start_time'])
result_data.to_csv(dr.download_dir + '/kpi_funnel_analysis_df_7days_media.csv', index=False, encoding='utf-8-sig')

# del paid_prep_data, user_arr, event_arr, event_time_arr, value_arr, result_data, funnel_data
