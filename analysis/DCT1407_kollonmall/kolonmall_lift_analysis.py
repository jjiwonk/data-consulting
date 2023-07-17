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
        'idfa': pa.string(),
        'is_retargeting': pa.string()}
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
    total_data = total_data.fillna('')
    total_data['media_source'] = total_data['media_source'].str.lower()
    # media 구분
    total_data.loc[total_data['media_source'].isin(['kakao_int', 'kakao_plus']), 'media'] = '카카오'
    total_data.loc[total_data['media_source'].isin(['facebook', 'facebook ads']), 'media'] = '메타'
    total_data.loc[(total_data['media_source'].isin(['googleadwords_int']))
                   & (total_data['campaign'].isin(['google_pmax_purchase'])), 'media'] = '구글'
    total_data.loc[(total_data['media_source'].isin(['googleadwords_int']))
                   & (total_data['campaign'].isin(['google_sdc_purchase', 'google_aca_install', 'google_ace_purchase',
                                                   'google_ace_purchase-dp', 'google_ac_install_IOS', 'google_ac_install'])), 'media'] = '네트워크'
    total_data.loc[total_data['media_source'].isin(['cauly_int', 'criteonew_int', 'rtbhouse_int']), 'media'] = '네트워크'
    total_data.loc[total_data['media_source'].isin(['naver_m', 'daum_m', 'naverbs_m', 'apple search ads']), 'media'] = 'SA'
    total_data.loc[total_data['media_source'].isin(['naver_gfa']), 'media'] = 'GFA'
    total_data.loc[total_data['media_source'].isin(['clearpier_int']), 'media'] = '클리어피어'
    total_data.loc[total_data['media_source'].isin(['bytedanceglobal_int']), 'media'] = '틱톡'
    total_data['media'] = total_data['media'].fillna('')


    # ad_type 구분
    total_data.loc[(total_data['media'].isin(['카카오']))
                   & (total_data['campaign'].isin(['kakao_biz_purchase']))
                   & (total_data['campaign'].str.contains('rt')), 'ad_type'] = 'RE_통합'
    total_data.loc[(total_data['media'].isin(['카카오']))
                   & (total_data['campaign'].isin(['kakao_biz_purchase']))
                   & (total_data['adset'].isin(['AOS_al_mf_2559_rt_pur'])), 'ad_type'] = 'UA_AOS'
    total_data.loc[(total_data['media'].isin(['카카오']))
                   & (total_data['campaign'].isin(['kakao_biz_purchase']))
                   & (total_data['adset'].isin(['IOS_al_mf_2559_rt_pur'])), 'ad_type'] = 'UA_IOS'
    total_data.loc[(total_data['media'].isin(['카카오']))
                   & (total_data['campaign'].isin(['kakao_display_traffic']))
                   & (total_data['campaign'].str.contains('ALL')), 'ad_type'] = 'RE_통합'
    total_data.loc[(total_data['media'].isin(['카카오']))
                   & (total_data['campaign'].isin(['sns_app_message'])), 'ad_type'] = 'UA_통합'

    total_data.loc[((total_data['media'].isin(['메타']))
                   & (total_data['campaign'].isin(['meta_feed_purchase'])))
                   | (total_data['campaign'].str.contains('rt')), 'adset'] = 'RE_통합'
    total_data.loc[(total_data['media'].isin(['메타']))
                   & (total_data['campaign'].isin(['meta_aaa_install'])), 'ad_type'] = 'UA_AOS'

    total_data.loc[(total_data['media'].isin(['구글']))
                   & (total_data['campaign'].isin(['google_pmax_purchase'])), 'ad_type'] = 'RE_통합'

    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['google_aca_install', 'google_ac_install'])), 'ad_type'] = 'UA_AOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['google_ace_purchase', 'google_ace_purchase-dp'])), 'ad_type'] = 'RE_AOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['google_sdc_purchase'])), 'ad_type'] = 'UA_통합'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['google_ac_install_IOS'])), 'ad_type'] = 'UA_IOS'

    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['cauly_ncpi_install']))
                   & (total_data['adset'].isin(['AOS_al_mf_ALL_nt'])), 'ad_type'] = 'UA_AOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['cauly_ncpi_install']))
                   & (total_data['adset'].isin(['IOS_al_mf_ALL_nt'])), 'ad_type'] = 'UA_IOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['cauly_re_purchase']))
                   & (total_data['adset'].isin(['AOS_al_mf_ALL_rt_pur'])), 'ad_type'] = 'RE_AOS'

    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['criteo_app_install'])), 'ad_type'] = 'UA_AOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['LF_Kolonmall_App_Android'])), 'ad_type'] = 'RE_AOS'
    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['LF_Kolonmall_App_iOS'])), 'ad_type'] = 'RE_IOS'
    total_data.loc[(total_data['campaign'].isin(['tiktok_display_install_AOS'])), 'ad_type'] = 'UA_AOS'

    total_data.loc[(total_data['media'].isin(['네트워크']))
                   & (total_data['campaign'].isin(['rtbhouse-retargeting', 'rtb_feed_purchase'])), 'ad_type'] = 'RE_통합'


    total_data.loc[(total_data['media'].isin(['SA']))
                   & (total_data['media_source'].isin(['naver_m', 'daum_m', 'naverbs_m'])), 'ad_type'] = 'UA_통합'
    total_data.loc[(total_data['media'].isin(['SA']))
                   & (total_data['media_source'].isin(['Apple Search Ads'])), 'ad_type'] = 'UA_IOS'

    total_data.loc[(total_data['media'].isin(['GFA']))
                   & (total_data['campaign'].str.contains('naver_gfa')), 'ad_type'] = 'RE_통합'

    total_data.loc[(total_data['media'].isin(['클리어피어']))
                   & (total_data['campaign'].str.contains('clearpier_ncpi_install')), 'ad_type'] = 'UA_IOS'

    total_data.loc[(total_data['media'].isin(['틱톡']))
                   & (total_data['campaign'].str.contains('tiktok_display_install_AOS')), 'ad_type'] = 'UA_AOS'
    total_data.loc[(total_data['media'].isin(['틱톡']))
                   & (total_data['campaign'].str.contains('tiktok_display_install_iOS')), 'ad_type'] = 'UA_IOS'

    total_data = total_data.fillna('')

    total_data = total_data.loc[total_data['appsflyer_id'] != ''].reset_index(drop=True)
    total_data.loc[total_data['event_name'] == 'first_purchase', 'event_name'] = 'af_purchase'
    sorted_total_data = total_data.sort_values(['appsflyer_id', 'event_time', 'event_name', 'install_time'])
    dedup_total_data = sorted_total_data.drop_duplicates(['appsflyer_id', 'event_time'], keep='last').reset_index(drop=True)
    dedup_total_data['event_revenue'] = dedup_total_data['event_revenue'].fillna(0)

    return dedup_total_data


def get_funnel_data(prep_data, contribute):
    user_arr = prep_data['appsflyer_id']
    event_arr = prep_data['event_name']
    event_time_arr = prep_data['event_time']
    value_arr = prep_data['event_revenue']
    media_arr = prep_data['ad_type'] + '/' + prep_data['media'] + '/' + prep_data['campaign'] + '/' + prep_data['media_source']
    funnel_generator = FunnelDataGenerator(user_arr, event_arr, event_time_arr, value_arr, media_arr, kpi_event,
                                           kpi_period, paid_event, contribute)
    funnel_generator.do_work()
    funnel_data = funnel_generator.data
    funnel_data['ad_type'] = funnel_data['media'].apply(lambda x: x.split('/')[0])
    funnel_data['media_div'] = funnel_data['media'].apply(lambda x: x.split('/')[1] if '/' in x else '')
    funnel_data['campaign'] = funnel_data['media'].apply(lambda x: x.split('/')[2] if '/' in x else '')
    funnel_data['media_source'] = funnel_data['media'].apply(lambda x: x.split('/')[3] if '/' in x else '')
    result_data = funnel_data.sort_values(['user_id', 'start_time'])

    return result_data


total_data = get_total_data()
paid_event = ['paid_install', 're-engagement', 're-attribution']
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
total_prep_data = data_prep(total_data)
# 퍼널(세션) 데이터 컬럼: 'user_id', 'funnel_id', 'funnel_sequence', 'start_time', 'end_time', 'is_paid', 'kpi_achievement', 'value', 'media'
# 라스트 태핑 매체 기준 퍼널
last_funnel_data = get_funnel_data(total_prep_data, 'last')
last_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_last_all.csv', index=False, encoding='utf-8-sig')

# 퍼스트 태핑 매체 기준 퍼널
first_funnel_data = get_funnel_data(total_prep_data, 'first')
first_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_first_all.csv', index=False, encoding='utf-8-sig')


# paid 유입 유저 필터링
adid_list = set(total_data.loc[total_data['event_name'].isin(['paid_install', 're-engagement', 're-attribution']), 'appsflyer_id'])
paid_user_data = total_data.loc[total_data['appsflyer_id'].isin(adid_list)].reset_index(drop=True)
paid_prep_data = data_prep(paid_user_data)

# 라스트 태핑 매체 기준 퍼널
paid_last_funnel_data = get_funnel_data(paid_prep_data, 'last')
paid_last_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_last.csv', index=False, encoding='utf-8-sig')

# 퍼스트 태핑 매체 기준 퍼널
paid_first_funnel_data = get_funnel_data(paid_prep_data, 'first')
paid_first_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_first.csv', index=False, encoding='utf-8-sig')

# 라벨링 안된 데이터 제외하고 퍼널 생성
only_labeled_paid_data = paid_prep_data.loc[~((paid_prep_data['event_name'].isin(paid_event)) & (paid_prep_data['ad_type'] == ''))].reset_index(drop=True)
only_labeled_paid_last_funnel_data = get_funnel_data(only_labeled_paid_data, 'last')
only_labeled_paid_last_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_last_labeled.csv', index=False, encoding='utf-8-sig')
only_labeled_paid_first_funnel_data = get_funnel_data(only_labeled_paid_data, 'first')
only_labeled_paid_first_funnel_data.to_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/RD/코오롱몰/kpi_funnel_analysis_df_7days_first_labeled.csv', index=False, encoding='utf-8-sig')

