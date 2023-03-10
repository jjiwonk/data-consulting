import pandas as pd
import os
from datetime import date , timedelta
import datetime
import utils.dropbox_util as dropbox
from setting import directory as dr


# 기존 경로 + 어제 날짜 해서 파일 불러오기
yesterday = date.today() - timedelta(1)
yesterday = yesterday.strftime('%Y%m%d')

file_dir = f'C:/Users/MADUP/Dropbox (주식회사매드업)/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism/appsflyer_daily_report_{yesterday}.csv'
use_col = ['attributed_touch_time', 'install_time', 'event_time', 'event_name','keywords', 'media_source','appsflyer_id','platform','sub_param_1','sub_param_2','sub_param_3']

df = pd.read_csv(file_dir,usecols = use_col)

def common_df(df):
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['install_time'] = pd.to_datetime(df['install_time'])
    df['attributed_touch_time'] = pd.to_datetime(df['attributed_touch_time'])
    df = df.sort_values(by=['event_time'], ascending=True)

    install_df = df.loc[df['event_name'].str.contains('install')]
    event_df = df.loc[(df['event_name'].str.contains('loan_contract_completed'))|(df['event_name'].str.contains('Viewed LA Home'))|(df['event_name'].str.contains('Clicked Signup'))]

    install_df = install_df.loc[(install_df['install_time'] - install_df['attributed_touch_time']) < datetime.timedelta(days=1)]
    event_df = event_df.loc[(event_df['event_time'] - event_df['attributed_touch_time']) < datetime.timedelta(days=1)]

    df = pd.concat([install_df,event_df])

    df = df.loc[df['media_source'] == 'googlesamo']
    df['Conversion Name'] = '-'

    df.loc[(df['event_name'].str.contains('loan_contract_completed'))&(df['platform'] == 'android'),'Conversion Name'] = '대출실행_aos_gclid_madup_230220'
    df.loc[(df['event_name'].str.contains('loan_contract_completed'))&(df['platform'] == 'ios'), 'Conversion Name'] = '대출실행_ios_gclid_madup_230220'

    df.loc[(df['event_name'].str.contains('Viewed LA Home'))&(df['platform'] == 'android'),'Conversion Name'] = '한도조회_aos_gclid_madup_230220'
    df.loc[(df['event_name'].str.contains('Viewed LA Home'))&(df['platform'] == 'ios'), 'Conversion Name'] = '한도조회_ios_gclid_madup_230220'

    df.loc[(df['event_name'].str.contains('Clicked Signup'))&(df['platform'] == 'android'),'Conversion Name'] = '회원가입_aos_gclid_madup_230220'
    df.loc[(df['event_name'].str.contains('Clicked Signup'))&(df['platform'] == 'ios'), 'Conversion Name'] = '회원가입_ios_gclid_madup_230220'

    df.loc[(df['event_name'].str.contains('install'))&(df['platform'] == 'android'),'Conversion Name'] = '인스톨_aos_gclid_madup_230220'
    df.loc[(df['event_name'].str.contains('install'))&(df['platform'] == 'ios'), 'Conversion Name'] = '인스톨_ios_gclid_madup_230220'

    limit_df = df.loc[df['event_name'].str.contains('Viewed LA Home')]
    limit_df = limit_df.drop_duplicates(['appsflyer_id'], keep = 'first')

    df = df.loc[~df['event_name'].str.contains('Viewed LA Home')]

    google_df = pd.concat([df,limit_df])

    google_df = google_df[['sub_param_1','sub_param_2','sub_param_3', 'Conversion Name', 'attributed_touch_time']].fillna('-')
    google_df = google_df.drop_duplicates()

    google_df = google_df.rename(columns = {'sub_param_1' : 'Google Click ID','attributed_touch_time' :'Conversion Time','sub_param_2' :'gbraid','sub_param_3' :'wbraid'})

    return google_df

df = common_df(df)

# 캠페인 가져오기

df.to_csv(dr.download_dir +'/샘플.csv',encoding = 'utf-8-sig')

import os
import argparse
import sys
import datetime

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException



get_yaml = {"client_id": "300152565038-2aqabln45206jk14iic019kk2amh7r0q.apps.googleusercontent.com",
            "client_secret": "dnHNMa0MrrFvQoKZG12FqYFb",
            "developer_token": "2DKmBuEfKim2PzBfRv5qBw",
            "login_customer_id": "4983132762",
            "refresh_token": "1/4HcbodjHTDka6xj6q3Mk_0Ftf6w-n756I4mrJi7I8ND6n_ARSJd2RIsMmoE_o8uh",
            "madup_account": "madup_madit@madup.com",
            "use_proto_plus": "True"}

for key in get_yaml.keys():
    env_key = "GOOGLE_ADS_" + key.upper()
    os.environ[env_key] = get_yaml[key]

client = GoogleAdsClient.load_from_env()

customer_id = '4983132762'
conversion_action_id = '10890838645'
gclid = 'Cj0KCQiApKagBhC1ARIsAFc7Mc7QJGl18RKW9kMVxqHO1mWT0h-v_u8yCL--7_tldIwXkaiyVSYVP3kaAkOUEALw_wcB'
conversion_date_time = '2023-03-09 19:18:07-05:00'
conversion_value = 1
conversion_custom_variable_id = 2600119784864
conversion_custom_variable_value = '한도조회_aos_gclid_madup_230220'

'frame: pages [pagers.py:66]  id:2600119784864'


# [START upload_offline_conversion]
def main(
    client,
    customer_id,
    conversion_action_id,
    gclid,
    conversion_date_time,
    conversion_value,
    conversion_custom_variable_id,
    conversion_custom_variable_value,
    gbraid,
    wbraid,
):
    """Creates a click conversion with a default currency of USD.
    Args:
        client: An initialized GoogleAdsClient instance.
        customer_id: The client customer ID string.
        conversion_action_id: The ID of the conversion action to upload to.
        gclid: The Google Click Identifier ID. If set, the wbraid and gbraid
            parameters must be None.
        conversion_date_time: The the date and time of the conversion (should be
            after the click time). The format is 'yyyy-mm-dd hh:mm:ss+|-hh:mm',
            e.g. '2021-01-01 12:32:45-08:00'.
        conversion_value: The conversion value in the desired currency.
        conversion_custom_variable_id: The ID of the conversion custom
            variable to associate with the upload.
        conversion_custom_variable_value: The str value of the conversion custom
            variable to associate with the upload.
        gbraid: The GBRAID for the iOS app conversion. If set, the gclid and
            wbraid parameters must be None.
        wbraid: The WBRAID for the iOS app conversion. If set, the gclid and
            gbraid parameters must be None.
    """
    click_conversion = client.get_type("ClickConversion")
    conversion_upload_service = client.get_service("ConversionUploadService")
    conversion_action_service = client.get_service("ConversionActionService")
    click_conversion.conversion_action = conversion_action_service.conversion_action_path(
        customer_id, conversion_action_id
    )

    # Sets the single specified ID field.
    if gclid:
        click_conversion.gclid = gclid
    elif gbraid:
        click_conversion.gbraid = gbraid
    else:
        click_conversion.wbraid = wbraid

    click_conversion.conversion_value = float(conversion_value)
    click_conversion.conversion_date_time = conversion_date_time
    click_conversion.currency_code = "USD"

    if conversion_custom_variable_id and conversion_custom_variable_value:
        conversion_custom_variable = client.get_type("CustomVariable")
        conversion_custom_variable.conversion_custom_variable = conversion_upload_service.conversion_custom_variable_path(
            customer_id, conversion_custom_variable_id
        )
        conversion_custom_variable.value = conversion_custom_variable_value
        click_conversion.custom_variables.append(conversion_custom_variable)

    request = client.get_type("UploadClickConversionsRequest")
    request.customer_id = customer_id
    request.conversions.append(click_conversion)
    request.partial_failure = True
    conversion_upload_response = conversion_upload_service.upload_click_conversions(
        request=request,
    )
    uploaded_click_conversion = conversion_upload_response.results[0]
    print(
        f"Uploaded conversion that occurred at "
        f'"{uploaded_click_conversion.conversion_date_time}" from '
        f'Google Click ID "{uploaded_click_conversion.gclid}" '
        f'to "{uploaded_click_conversion.conversion_action}"'
    )
    # [END upload_offline_conversion]

ga_service = client.get_service("GoogleAdsService")

query = """
        SELECT conversion_custom_variable.id FROM conversion_custom_variable"""

    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    page_size = 5
    search_request.page_size = page_size


    results = ga_service.search(request=search_request)