import setting.directory as dr
import setting.report_date as rdate

import pandas as pd
import json
import re
import os
import datetime

asset_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/asset'

def modify_check():
    try :
        # 수정 날짜 확인
        mtime = os.path.getmtime(asset_dir + f'/total_asset_data_{rdate.yearmonth}.csv')

        def unixtime(x):
            return datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d')

        mdate = unixtime(mtime)

        if rdate.today.strftime('%Y-%m-%d') == mdate:
            print('오늘은 이미 다른 분이 업데이트를 하셨네요')
            return 1
        else:
            print('코드 실행 감사드립니다 업데이트 시작합니다!')
            return 0
    except :
        print('코드 실행 감사드립니다 업데이트 시작합니다!')
        return 0

def facebook_read():
    ## 페이스북
    facebook_img = pd.read_csv(asset_dir + f'/facebook_ad_image_asset_daily_report_{rdate.yearmonth}.csv',
                               encoding='utf-8-sig')
    facebook_img = facebook_img.loc[pd.notnull(facebook_img['image_asset'])]
    facebook_img['소재 URL'] = facebook_img['image_asset'].apply(lambda x: json.loads(x)['url'] if x != '{}' else '')
    facebook_img['소재 유형'] = 'image'
    facebook_img = facebook_img[['owner_id', 'campaign_name', 'adset_name', 'ad_name', '소재 유형', '소재 URL', 'collected_at']]

    facebook_vid = pd.read_csv(asset_dir + f'/facebook_ad_video_asset_daily_report_{rdate.yearmonth}.csv', encoding='utf-8-sig')
    facebook_vid = facebook_vid.loc[pd.notnull(facebook_vid['video_asset'])]
    facebook_vid['소재 URL'] = facebook_vid['video_asset'].apply(lambda x: json.loads(x)['thumbnail_url'] if x != '{}' else '')
    facebook_vid['소재 유형'] = 'video'
    facebook_vid = facebook_vid[['owner_id', 'campaign_name', 'adset_name', 'ad_name', '소재 유형', '소재 URL', 'collected_at']]

    facebook = pd.concat([facebook_img, facebook_vid], axis=0, ignore_index=True)
    facebook = facebook.rename(columns=
                               {'campaign_name': '캠페인',
                                'adset_name': '광고그룹',
                                'ad_name': '소재'})

    facebook = facebook.sort_values(['owner_id', '캠페인', '광고그룹', '소재', '소재 유형', 'collected_at'],
                                    ascending=[True, True, True, True, False, True])
    facebook = facebook.drop_duplicates(['owner_id', '캠페인', '광고그룹', '소재'], keep='last')
    facebook = facebook[['owner_id', '캠페인', '광고그룹', '소재', '소재 유형', '소재 URL']]
    facebook['매체'] = '페이스북'
    return facebook

def kakao_read():
    ## 카카오
    kakao = pd.read_csv(asset_dir + f'/kakao_all_creative_list_{rdate.yearmonth}.csv', encoding= 'utf-8-sig')
    kakao = kakao.loc[pd.notnull(kakao['image'])]
    kakao['소재 URL'] = kakao['image'].apply(lambda x : 'https:' + json.loads(x)['url'] if x != '{}' else '')
    kakao.columns

    kakao = kakao.rename(columns = {'campaign_name' : '캠페인', 'adgroup_name' : '광고그룹','creative_name' : '소재', 'format' : '소재 유형'})
    kakao = kakao[['owner_id', '캠페인', '광고그룹', '소재', '소재 유형','소재 URL']]
    kakao = kakao.drop_duplicates(['owner_id', '캠페인', '광고그룹', '소재'])
    kakao['매체'] = '카카오'
    return kakao

def google_read():
    ## 구글
    google = pd.read_csv(asset_dir + f'/google_ads_asset_report_{rdate.yearmonth}.csv', encoding= 'utf-8-sig')
    asset_type_dict = {
    0 : 'BOOK_ON_GOOGLE',
    1 : 'CALL',
    2 : 'CALLOUT',
    3 : 'CALL_TO_ACTION',
    4 : 'DISCOVERY_CAROUSEL_CARD',
    5 : 'DYNAMIC_CUSTOM',
    6 : 'DYNAMIC_EDUCATION',
    7 : 'DYNAMIC_FLIGHTS',
    8 : 'DYNAMIC_HOTELS_AND_RENTALS',
    9 : 'DYNAMIC_JOBS',
    10 : 'DYNAMIC_LOCAL',
    11 : 'DYNAMIC_REAL_ESTATE',
    12 : 'DYNAMIC_TRAVEL',
    13 : 'HOTEL_CALLOUT',
    14 : 'IMAGE',
    15 : 'LEAD_FORM',
    16 : 'MEDIA_BUNDLE',
    17 : 'MOBILE_APP',
    18 : 'PAGE_FEED',
    19 : 'PRICE',
    20 : 'PROMOTION',
    21 : 'SITELINK',
    22 : 'STRUCTURED_SNIPPET',
    23 : 'TEXT',
    24 : 'UNKNOWN',
    25 : 'UNSPECIFIED',
    26 : 'YOUTUBE_VIDEO'
    }
    google = google.loc[pd.notnull(google['asset_image_asset_full_size_url'])]
    google['asset_type'] = google['asset_type'].apply(lambda x : asset_type_dict.get(x))
    google = google.rename(columns = {
        'campaign_name' : '캠페인',
        'ad_group_name' : '광고그룹',
        'asset_image_asset_full_size_url' : '소재 URL',
        'asset_type' : '소재 유형'})

    google = google.drop_duplicates(['캠페인', '광고그룹'])
    google['소재'] = google['광고그룹']
    google = google[['owner_id', '캠페인', '광고그룹', '소재', '소재 유형','소재 URL']]
    google['매체'] = '구글'
    return google

if modify_check() == 1:
    pass
else :
    facebook = facebook_read()
    kakao = kakao_read()
    google = google_read()

    asset_data = pd.concat([facebook, kakao, google], sort=False, ignore_index=True)
    asset_data.to_csv(asset_dir + f'/total_asset_data_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')
    print('업데이트가 완료되었습니다.')