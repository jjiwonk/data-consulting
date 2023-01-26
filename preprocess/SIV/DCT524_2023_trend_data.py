from setting import directory as dr
from workers import madup_campaign_info
import pandas as pd
import numpy as np

# raw_file = pd.read_csv(dr.download_dir + '/adgroup_ad_conv_daily_report.csv')
#
# class google_info :
#     google_dict = {
#         'advertising_channel_sub_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'SEARCH_MOBILE_APP',
#              3: 'DISPLAY_MOBILE_APP',
#              4: 'SEARCH_EXPRESS',
#              5: 'DISPLAY_EXPRESS',
#              6: 'SHOPPING_SMART_ADS',
#              7: 'DISPLAY_GMAIL_AD',
#              8: 'DISPLAY_SMART_CAMPAIGN',
#              9: 'VIDEO_OUTSTREAM',
#              10: 'VIDEO_ACTION',
#              11: 'VIDEO_NON_SKIPPABLE',
#              12: 'APP_CAMPAIGN',
#              13: 'APP_CAMPAIGN_FOR_ENGAGEMENT',
#              14: 'LOCAL_CAMPAIGN',
#              15: 'SHOPPING_COMPARISON_LISTING_ADS',
#              16: 'SMART_CAMPAIGN',
#              17: 'VIDEO_SEQUENCE',
#              18: 'APP_CAMPAIGN_FOR_PRE_REGISTRATION'},
#         'advertising_channel_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'SEARCH',
#              3: 'DISPLAY',
#              4: 'SHOPPING',
#              5: 'HOTEL',
#              6: 'VIDEO',
#              7: 'MULTI_CHANNEL',
#              8: 'LOCAL',
#              9: 'SMART',
#              10: 'PERFORMANCE_MAX',
#              11: 'LOCAL_SERVICES',
#              12: 'DISCOVERY'},
#         'bidding_strategy_goal_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'OPTIMIZE_INSTALLS_TARGET_INSTALL_COST',
#              3: 'OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST',
#              4: 'OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST',
#              5: 'OPTIMIZE_RETURN_ON_ADVERTISING_SPEND',
#              6: 'OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME',
#              7: 'OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST'},
#         'bidding_strategy_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'COMMISSION',
#              3: 'ENHANCED_CPC',
#              4: 'INVALID',
#              5: 'MANUAL_CPA',
#              6: 'MANUAL_CPC',
#              7: 'MANUAL_CPM',
#              8: 'MANUAL_CPV',
#              9: 'MAXIMIZE_CONVERSIONS',
#              10: 'MAXIMIZE_CONVERSION_VALUE',
#              11: 'PAGE_ONE_PROMOTED',
#              12: 'PERCENT_CPC',
#              13: 'TARGET_CPA',
#              14: 'TARGET_CPM',
#              15: 'TARGET_IMPRESSION_SHARE',
#              16: 'TARGET_OUTRANK_SHARE',
#              17: 'TARGET_ROAS',
#              18: 'TARGET_SPEND'},
#         'field_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'HEADLINE',
#              3: 'DESCRIPTION',
#              4: 'MANDATORY_AD_TEXT',
#              5: 'MARKETING_IMAGE',
#              6: 'MEDIA_BUNDLE',
#              7: 'YOUTUBE_VIDEO',
#              8: 'BOOK_ON_GOOGLE',
#              9: 'LEAD_FORM',
#              10: 'PROMOTION',
#              11: 'CALLOUT',
#              12: 'STRUCTURED_SNIPPET',
#              13: 'SITELINK',
#              14: 'MOBILE_APP',
#              15: 'HOTEL_CALLOUT',
#              16: 'CALL',
#              17: 'PRICE',
#              18: 'LONG_HEADLINE',
#              19: 'BUSINESS_NAME',
#              20: 'SQUARE_MARKETING_IMAGE',
#              21: 'PORTRAIT_MARKETING_IMAGE',
#              22: 'LOGO',
#              23: 'LANDSCAPE_LOGO',
#              24: 'VIDEO',
#              25: 'CALL_TO_ACTION_SELECTION'},
#         'ad_network_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'SEARCH',
#              3: 'SEARCH_PARTNERS',
#              4: 'CONTENT',
#              5: 'YOUTUBE_SEARCH',
#              6: 'YOUTUBE_WATCH',
#              7: 'MIXED'},
#         'asset_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'YOUTUBE_VIDEO',
#              3: 'MEDIA_BUNDLE',
#              4: 'IMAGE',
#              5: 'TEXT',
#              6: 'LEAD_FORM',
#              7: 'BOOK_ON_GOOGLE',
#              8: 'PROMOTION',
#              9: 'CALLOUT',
#              10: 'STRUCTURED_SNIPPET',
#              11: 'SITELINK',
#              12: 'PAGE_FEED',
#              13: 'DYNAMIC_EDUCATION',
#              14: 'MOBILE_APP',
#              15: 'HOTEL_CALLOUT',
#              16: 'CALL',
#              17: 'PRICE',
#              18: 'CALL_TO_ACTION',
#              19: 'DYNAMIC_REAL_ESTATE',
#              20: 'DYNAMIC_CUSTOM',
#              21: 'DYNAMIC_HOTELS_AND_RENTALS',
#              22: 'DYNAMIC_FLIGHTS',
#              23: 'DISCOVERY_CAROUSEL_CARD',
#              24: 'DYNAMIC_TRAVEL',
#              25: 'DYNAMIC_LOCAL',
#              26: 'DYNAMIC_JOBS'},
#         'performance_label':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'PENDING',
#              3: 'LEARNING',
#              4: 'LOW',
#              5: 'GOOD',
#              6: 'BEST'},
#         'ad_serving_optimization_status':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'CTR_OPTIMIZE',
#              3: 'CONVERSION_OPTIMIZE',
#              4: 'ROTATE',
#              5: 'ROTATE_INDEFINITELY',
#              6: 'UNAVAILABLE'},
#         'video_brand_safety_suitability':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'EXPANDED_INVENTORY',
#              3: 'STANDARD_INVENTORY',
#              4: 'LIMITED_INVENTORY'},
#         'ad_rotation_mode':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'OPTIMIZE',
#              3: 'ROTATE_FOREVER'},
#         'ad_group_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'SEARCH_STANDARD',
#              3: 'DISPLAY_STANDARD',
#              4: 'SHOPPING_PRODUCT_ADS',
#              5: 'HOTEL_ADS',
#              6: 'SHOPPING_SMART_ADS',
#              7: 'VIDEO_BUMPER',
#              8: 'VIDEO_TRUE_VIEW_IN_STREAM',
#              9: 'VIDEO_TRUE_VIEW_IN_DISPLAY',
#              10: 'VIDEO_NON_SKIPPABLE_IN_STREAM',
#              11: 'VIDEO_OUTSTREAM',
#              12: 'SEARCH_DYNAMIC_ADS',
#              13: 'SHPPING_COMPARISON_LISTING_ADS',
#              14: 'PROMOTED_HOTEL_ADS',
#              15: 'VIDEO_RESPONSIVE',
#              16: 'VIDEO_EFFICIENT_REACH',
#              17: 'SMART_CAMPAIGN_ADS'},
#         'ad_type':
#             {0: 'UNSPECIFIED',
#              1: 'UNKNOWN',
#              2: 'TEXT_AD',
#              3: 'EXPANDED_TEXT_AD',
#              4: 'EXPANDED_DYNAMIC_SEARCH_AD',
#              5: 'HOTEL_AD',
#              6: 'SHOPPING_SMART_AD',
#              7: 'SHOPPING_PRODUCT_AD',
#              8: 'VIDEO_AD',
#              9: 'IMAGE_AD',
#              10: 'RESPONSIVE_SEARCH_AD',
#              11: 'LEGACY_RESPONSIVE_DISPLAY_AD',
#              12: 'APP_AD',
#              13: 'LEGACY_APP_INSTALL_AD',
#              14: 'RESPONSIVE_DISPLAY_AD',
#              15: 'LOCAL_AD',
#              16: 'HTML_5_UPLOAD_AD',
#              17: 'DYNAMIC_HTML_5_AD',
#              18: 'APP_ENGAGEMENT_AD',
#              19: 'SHOPPING_COMPARISON_LISTING_AD',
#              20: 'VIDEO_BUMPER_AD',
#              21: 'VIDEO_UNSKIPPABLE_IN_STREAM_AD',
#              22: 'VIDEO_OUTSTREAM_AD',
#              23: 'VIDEO_TRUEVIEW_IN_STREAM_AD',
#              24: 'VIDEO_RESPONSIVE_AD',
#              25: 'SMART_CAMPAIGN_AD',
#              26: 'CALL_AD',
#              27: 'APP_PRE_REGISTRATION_AD',
#              28: 'IN_FEED_VIDEO_AD',
#              29: 'DISCOVERY_MULTI_ASSET_AD',
#              30: 'DISCOVERY_CAROUSEL_AD'}
#     }
#     enum_dict_ad_group = {
#         'campaign.advertising_channel_sub_type': 'advertising_channel_sub_type',
#         'campaign.advertising_channel_type': 'advertising_channel_type',
#         'ad_group.type': 'ad_group_type',
#
#     }
#
# def enum_to_string(array, ref_dict, key):
#     return [ref_dict[key].get(enum) for enum in array]
#
#
# for key in google_info.enum_dict_ad_group.keys() :
#     value = google_info.enum_dict_ad_group.get(key)
#     raw_file[key] = enum_to_string(raw_file[key], google_info.google_dict, value)
#
#
# raw_file.to_csv(dr.download_dir + '/adgroup_ad_conv_daily_report(주요 명칭 변환 버전).csv', encoding = 'utf-8-sig', index=False)


result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/2022 Trend'
raw_dir = result_dir + '/raw'
prep_dir = result_dir + '/prep'

def prep_data_pivot(df):
    pivot_index = ['owner_id', 'collected_at', 'adv_type', 'device']
    df[pivot_index] = df[pivot_index].fillna('-')

    metric_columns = ['cost', 'click', 'impression', '매출(1D)', '매출(7D)']
    df[metric_columns] = df[metric_columns].apply(lambda x: pd.to_numeric(x))
    df[metric_columns] = df[metric_columns].fillna(0)

    df_pivot = df.pivot_table(index= pivot_index, values = metric_columns, aggfunc = 'sum').reset_index()

    owner_pivot = df.pivot_table(index = 'owner_id', values = ['매출(1D)', '매출(7D)'], aggfunc = 'sum').reset_index()
    owner_pivot['is_sales'] = False
    owner_pivot.loc[owner_pivot[['매출(1D)', '매출(7D)']].sum(axis = 1)> 0, 'is_sales'] = True
    owner_pivot = owner_pivot[['owner_id', 'is_sales']]

    df_pivot = df_pivot.merge(owner_pivot, on = 'owner_id', how = 'left')
    return df_pivot


def naver_sa_prep():
    naver_sa_campaign_type = pd.read_csv(raw_dir + '/naver_sa_campaign_type.csv')
    naver_sa_campaign_type = naver_sa_campaign_type.drop_duplicates(['customer_id', 'campaign_id', 'campaign_type'])[['customer_id', 'campaign_id', 'campaign_type']]

    naver_sa_conv = pd.read_csv(raw_dir + '/naver_sa_conv_2022.csv')
    naver_sa_conv = naver_sa_conv.loc[naver_sa_conv['conversion_type'] == 1]

    naver_sa_conv_pivot = naver_sa_conv.pivot_table(index = ['owner_id', 'collected_at', 'campaign_id'], columns = 'conversion_method', values = 'sales_by_conversion', aggfunc = 'sum').reset_index()
    naver_sa_conv_pivot = naver_sa_conv_pivot.fillna(0)
    naver_sa_conv_pivot = naver_sa_conv_pivot.sort_values(['collected_at', 'owner_id'])
    naver_sa_conv_pivot = naver_sa_conv_pivot.rename(columns = {1 : '매출(1D)', 2 : '매출(7D)'})


    naver_sa_detail1 = pd.read_csv(raw_dir + '/naver_sa_detail_202204-202206.csv')
    naver_sa_detail2 = pd.read_csv(raw_dir + '/naver_sa_detail_202207-202209.csv')
    naver_sa_detail3 = pd.read_csv(raw_dir + '/naver_sa_detail_202210-202212.csv')
    naver_sa_total = pd.concat([naver_sa_detail1, naver_sa_detail2, naver_sa_detail3], sort=False, ignore_index=True)

    device_info = naver_sa_total.drop_duplicates(['campaign_id', 'pc_mobile_type'])
    device_info['cnt'] = 1
    device_info = device_info.pivot_table(index = 'campaign_id', columns = 'pc_mobile_type', values = 'cnt', aggfunc = 'count').reset_index()
    device_info.loc[device_info['M']==1, 'device'] = 'MO'
    device_info.loc[device_info['P'] == 1, 'device'] = 'PC'
    device_info.loc[(device_info['M']==1) & (device_info['P'] == 1), 'device'] = 'ALL'

    naver_sa_total = naver_sa_total.merge(device_info[['campaign_id', 'device']], on = 'campaign_id', how='left')
    naver_sa_total = naver_sa_total.pivot_table(index = ['owner_id', 'collected_at', 'campaign_id','device'], values = ['cost', 'click', 'impression'], aggfunc='sum').reset_index()

    naver_sa_total_merge = naver_sa_total.merge(naver_sa_conv_pivot, on = ['owner_id', 'collected_at', 'campaign_id'], how = 'left')
    naver_sa_total_merge = naver_sa_total_merge.fillna(0)
    naver_sa_total_merge = naver_sa_total_merge.merge(naver_sa_campaign_type, on = 'campaign_id', how = 'left')

    naver_sa_powerlink = naver_sa_total_merge.loc[naver_sa_total_merge['campaign_type']==1]
    naver_sa_powerlink['adv_type'] = 'SA'
    naver_sa_powerlink = naver_sa_powerlink.rename(columns = {'pc_mobile_type' : 'device'})
    naver_sa_powerlink_pivot = prep_data_pivot(naver_sa_powerlink)

    naver_sa_powerlink_pivot['media'] = 'Naver'
    naver_sa_powerlink_pivot['매출(7D)'] = naver_sa_powerlink_pivot['매출(7D)'] + naver_sa_powerlink_pivot['매출(1D)']
    naver_sa_powerlink_pivot.to_csv(prep_dir + '/naver_sa.csv', index=False, encoding = 'utf-8-sig')
    return naver_sa_powerlink_pivot

def kakao_sa_prep():
    kakao_sa = pd.read_csv(raw_dir + '/kakao_sa_2022.csv')
    kakao_sa['device'] = kakao_sa['devicetypes'].apply(lambda x : 'PC' if x=='["PC"]' else 'MO' if x == '["MOBILE"]' else 'ALL')
    kakao_sa['adv_type'] = 'SA'
    kakao_sa = kakao_sa.rename(columns = {'imp' : 'impression',
                                          'spending' : 'cost',
                                          'convpurchasep1d' : '매출(1D)',
                                          'convpurchasep7d' : '매출(7D)'})
    kakao_sa['매출(7D)'] = kakao_sa['매출(7D)'] + kakao_sa['매출(1D)']

    kakao_sa_pivot = prep_data_pivot(kakao_sa)
    kakao_sa_pivot['media'] = 'Kakao'
    kakao_sa_pivot.to_csv(prep_dir + '/kakao_sa.csv', index=False, encoding = 'utf-8-sig')

    return kakao_sa_pivot

def google_prep():
    google_conv1 = pd.read_csv(raw_dir + '/google_conv_202204-202206.csv')
    google_conv2 = pd.read_csv(raw_dir + '/google_conv_202207-202212.csv')
    google_detail1 = pd.read_csv(raw_dir + '/google_detail_202204-202206.csv')
    google_detail2 = pd.read_csv(raw_dir + '/google_detail_202207-202212.csv')

    google_data = pd.concat([google_detail1, google_detail2, google_conv1, google_conv2], sort = False, ignore_index=True)
    google_data['adv_type'] = google_data['campaign.advertising_channel_type'].apply(lambda x : 'SA' if x==2 else 'DA' if x == 3 else 'AC')
    google_data['device'] = google_data['segments.device'].apply(lambda x : 'MO' if x in [2, 3] else 'PC' if x in [4] else 'OTHER')
    google_data['매출(1D)'] = google_data['sales']
    google_data['매출(7D)'] = google_data['sales']

    google_data_pivot = prep_data_pivot(google_data)
    google_data_pivot['media'] = 'Google'
    google_data_pivot.to_csv(prep_dir + '/google_total.csv', index=False, encoding='utf-8-sig')
    return google_data_pivot



def integrated_data(is_prep = False, is_blind = True):
    if is_prep == True :
        naver = naver_sa_prep()
        kakao = kakao_sa_prep()
        google = google_prep()
    else :
        naver = pd.read_csv(prep_dir + '/naver_sa.csv')
        kakao = pd.read_csv(prep_dir + '/kakao_sa.csv')
        google = pd.read_csv(prep_dir + '/google_total.csv')

    data = pd.concat([naver, kakao, google], sort= False, ignore_index= True)

    #target_owner = ['musinsa', 'wconcept', 'aurora_b115', 'chicor', 'clio', 'innisfree', 'kream']
    target_owner = ['musinsa', 'wconcept', 'aurora_b115']
    data['is_target'] = False
    data.loc[data['owner_id'].isin(target_owner), 'is_target'] = True

    owner_category = madup_campaign_info.get_owner_info()
    owner_category = owner_category[['owner_id', '광고주','업종 (대분류)', '업종 (소분류)']]

    data_merge = data.merge(owner_category, on = 'owner_id', how = 'left')
    data_merge[['광고주', '업종 (대분류)', '업종 (소분류)']] = data_merge[['광고주', '업종 (대분류)', '업종 (소분류)']].fillna('미분류')

    # 미분류 광고주 정보
    temp = data_merge.loc[data_merge['광고주'] == '미분류']
    temp = temp.drop_duplicates(['owner_id', 'media', 'adv_type'])[['owner_id', 'media', 'adv_type']]
    temp.to_csv(dr.download_dir + '/temp_owner.csv', index=False, encoding = 'utf-8-sig')

    data_merge['is_smb'] = False
    data_merge.loc[data_merge['owner_id'].str.startswith('b15'), 'is_smb'] = True

    if is_blind == True :
        account_key = list(data_merge['owner_id'].unique())
        account_dict = dict(zip(account_key, range(len(account_key))))
        data_merge['account_number'] = data_merge['owner_id'].apply(lambda x : str(account_dict.get(x)))
        data_merge = data_merge.drop(['광고주', 'owner_id'], axis = 1)

    data_merge = data_merge.loc[data_merge[['cost', 'click', 'impression', '매출(1D)', '매출(7D)']].sum(axis=1) > 0]
    data_merge.to_csv(result_dir + '/final_2022_data.csv', index=False, encoding = 'utf-8-sig')

integrated_data(is_prep= True, is_blind= True)