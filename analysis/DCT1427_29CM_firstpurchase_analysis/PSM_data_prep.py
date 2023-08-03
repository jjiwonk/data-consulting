import datetime
import pandas as pd
import pyarrow.csv as pacsv
import pyarrow as pa
from setting import directory as dr
from workers import read_data
import os
import numpy as np
import workers.func as func

class directory :
    result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1482'

class columns :

    usecols = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'attributed_touch_time': pa.string(),
        'event_name': pa.string(),
        'event_revenue': pa.float64(),
        'event_value': pa.string(),
        'campaign': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source': pa.string(),
        'platform': pa.string(),
        'country_code': pa.string(),
        'channel': pa.string(),
        'customer_user_id': pa.string()
    }

    agg_usecols = {
        'date': pa.string(),
        'media_source_pid': pa.string(),
        'campaign_name': pa.string(),
        'os': pa.string(),
        'installs' : pa.float64(),
        'af_complete_registration_unique_users': pa.float64(),
        'af_purchase_event_counter': pa.float64(),
        'af_purchase_unique_users': pa.float64(),
        'af_purchase_sales_in_krw': pa.float64(),
        'new_purchaser_event_counter': pa.float64(),
        'new_purchaser_unique_users': pa.float64(),
        'new_purchaser_sales_in_krw': pa.float64()}

    report_rename = {'[AOS] ACa_action_UA':'[AOS] ACa_action_UA',
                    '[AOS] ACa_action_UA_new_purchaser':'[AOS] ACa_action_UA_new_purchaser',
                    '[AOS] ACa_advanced_UA_2':'[AOS] ACa_advanced_UA_2',
                    '[AOS] AC_UA':'[AOS] AC_UA',
                    'fb_app_install_AAA_aos_2205':'facebook_install',
                    'fb_app_install_AAA_ios_2205':'facebook_install',
                    'fb_app_install_catalog_aos_2201':'facebook_install',
                    'fb_app_install_catalog_ios_2301':'facebook_install',
                    'fb_app_install_purchase_catalog_aos_2302':'facebook_install',
                    'fb_app_install_purchase_catalog_ios_2302':'facebook_install',
                    'UI_fb_app_install_catalog_ios_2301':'facebook_install',
                    'UI_fb_app_install_purchase_catalog_aos_2302':'facebook_install',
                    '230609_카카오비즈보드(전환_설치_앱)':'kakao_bizboard_install_IOS'}

    install_campaign = ['[AOS] ACa_action_UA','[AOS] ACa_action_UA_new_purchaser','[AOS] ACa_advanced_UA_2','[AOS] AC_UA','[AOS] ACa_advanced_UA','fb_app_install_AAA_aos_2205','fb_app_install_AAA_ios_2205','fb_app_install_catalog_aos_2201','fb_app_install_catalog_ios_2301','fb_app_install_purchase_catalog_aos_2302','fb_app_install_purchase_catalog_ios_2302','UI_fb_app_install_catalog_ios_2301','UI_fb_app_install_purchase_catalog_aos_2302','kakao_bizboard_install_IOS','facebook_install']
    report_install_campaign = ['[AOS] ACa_action_UA','[AOS] ACa_action_UA_new_purchaser','[AOS] ACa_advanced_UA_2','[AOS] AC_UA','fb_app_install_AAA_aos_2205','fb_app_install_AAA_ios_2205','fb_app_install_catalog_aos_2201','fb_app_install_catalog_ios_2301','fb_app_install_purchase_catalog_aos_2302','fb_app_install_purchase_catalog_ios_2302','UI_fb_app_install_catalog_ios_2301','UI_fb_app_install_purchase_catalog_aos_2302','230609_카카오비즈보드(전환_설치_앱)']
    agg_install_campaign = ['fb_app_install_catalog_aos_2201','fb_app_install_catalog_ios_2301','fb_app_install_purchase_catalog_aos_2302','UI_fb_app_install_catalog_ios_2301','UI_fb_app_install_purchase_catalog_aos_2302']

def data_read():

    raw_file_dir = directory.result_dir + '/new'
    raw_file_list = os.listdir(raw_file_dir)
    raw_df = read_data.pyarrow_csv(dtypes=columns.usecols, directory= raw_file_dir, file_list=raw_file_list)

    raw_df['event_time'] = pd.to_datetime(raw_df['event_time'])
    raw_df = raw_df.sort_values('event_time')
    raw_df = raw_df.drop_duplicates()

    return raw_df

def agg_data_read():

    raw_file_dir = directory.result_dir + '/집약형RD'
    raw_file_list = os.listdir(raw_file_dir)
    raw_df = read_data.pyarrow_csv(dtypes=columns.agg_usecols, directory=raw_file_dir, file_list=raw_file_list)

    raw_df = raw_df.loc[(raw_df['media_source_pid'] == 'Facebook Ads') & (raw_df['campaign_name'] != 'None')]
    raw_df['index'] = 'no-install'
    raw_df.loc[raw_df['campaign_name'].isin(columns.agg_install_campaign),'index'] ='install'

    daily_install_user = raw_df.pivot_table(index= 'date', columns = ['index','os'], values = 'installs',aggfunc='sum').reset_index()
    daily_install_user.columns = ['date','install_and','install_ios','no-install_and','no-install_ios']

    return daily_install_user

def data_prep():

    data = data_read()
    agg_data = agg_data_read()

    #페이스북 정리하기
    facebook_user = data.loc[(data['media_source'] == 'restricted') & (data['event_name'] == 'install')]
    facebook_user['install_time'] = pd.to_datetime(facebook_user['install_time'])
    facebook_user['date'] = facebook_user['install_time'].dt.date
    facebook_user = facebook_user[['date', 'platform', 'appsflyer_id']].drop_duplicates()

    user_list = facebook_user['appsflyer_id'].drop_duplicates().tolist()

    facebook_purchase = data.loc[(data['media_source'] == 'restricted') & (data['event_name'] == 'af_purchase') & (data['appsflyer_id'].isin(user_list))]
    facebook_purchase['install_time'] = pd.to_datetime(facebook_purchase['install_time'])
    facebook_purchase['date'] = facebook_purchase['install_time'].dt.date
    facebook_purchase = facebook_purchase.groupby(['date','appsflyer_id','platform'])['event_revenue'].sum().reset_index()

    facebook_user = pd.merge(facebook_user,facebook_purchase, on= ['date','appsflyer_id','platform'], how= 'left').fillna(0)

    def facebook_labeling(agg_data, facebook_user):

        agg_data['date'] = pd.to_datetime(agg_data['date'])
        facebook_user['date'] = pd.to_datetime(facebook_user['date'])
        facebook_user['campaign'] = '-'
        facebook_user = facebook_user.sort_values(['date','platform'])

        facebook_purchase_user = facebook_user.loc[facebook_user['event_revenue'] > 0]
        facebook_nonpurchase_user = facebook_user.loc[facebook_user['event_revenue'] == 0]

        facebook_user_data = pd.DataFrame()

        for i in range(len(agg_data)):

            date = agg_data['date'][i]
            aos_install = agg_data['install_and'][i]
            aos_noninstall = agg_data['no-install_and'][i]
            ios_install = agg_data['install_ios'][i]
            ios_noninstall = agg_data['no-install_and'][i]

            aos_install_ratio = aos_install /(aos_noninstall + aos_install)
            ios_install_ratio = ios_install / (ios_noninstall + ios_install)

            def labeling(data,date):
                user_list = data.loc[data['date'] == date]
                user_list.index = range(len(user_list))

                ios_cnt = user_list['platform'].tolist().count('ios')
                aos_cnt = user_list['platform'].tolist().count('android')

                user_list['campaign'][0: int(aos_cnt * aos_install_ratio)] = 'facebook_install'
                user_list['campaign'][int(aos_cnt * aos_install_ratio): int(aos_cnt * aos_install_ratio) + (aos_cnt - int(aos_cnt * aos_install_ratio))] = 'facebook_etc'

                user_list['campaign'][int(aos_cnt * aos_install_ratio) + (aos_cnt - int(aos_cnt * aos_install_ratio)): int(aos_cnt * aos_install_ratio) + (aos_cnt - int(aos_cnt * aos_install_ratio)) + int(ios_cnt * ios_install_ratio)] = 'facebook_install'
                user_list['campaign'][int(aos_cnt * aos_install_ratio) + (aos_cnt - int(aos_cnt * aos_install_ratio)) + int(ios_cnt * ios_install_ratio): int(aos_cnt * aos_install_ratio) + (aos_cnt - int(aos_cnt * aos_install_ratio)) + int(ios_cnt * ios_install_ratio) + (ios_cnt - int(ios_cnt * ios_install_ratio))] = 'facebook_etc'

                return user_list

            purchase_user = labeling(facebook_purchase_user,date)
            nonpurchase_user = labeling(facebook_nonpurchase_user,date)

            user_list = pd.concat([purchase_user,nonpurchase_user])
            facebook_user_data = pd.concat([facebook_user_data,user_list])

        return facebook_user_data

    facebook_user_data = facebook_labeling(agg_data,facebook_user)
    facebook_user_data = facebook_user_data.drop_duplicates(subset='appsflyer_id',keep='first')
    facebook_user_dict = dict(zip(facebook_user_data['appsflyer_id'], facebook_user_data['campaign']))

    data.loc[data['media_source'] =='restricted','campaign'] = data['appsflyer_id'].apply(lambda x: x.replace(x, facebook_user_dict[x]) if x in facebook_user_dict.keys() else 'restricted')

    return data

def PSM_data_prep():

    data = data_prep()

    paid_user = data.loc[data['event_name'].isin(['install','re-engagement','re-attribution'])]['appsflyer_id'].drop_duplicates()
    paid_user = paid_user.tolist()

    data = data.loc[data['appsflyer_id'].isin(paid_user)]

    treat_data = data.loc[(data['campaign'].isin(columns.install_campaign))& (data['event_name'] =='install')]
    treat_data['campaign'].unique()

    treat_data = treat_data[['appsflyer_id']].drop_duplicates()
    treat_data['treat'] = 1

    #purchase_data = data.loc[data['event_name'].isin(['af_purchase'])]
    #purchase_data['event_revenue'] = purchase_data['event_revenue'].fillna(0)

    event_data = data.pivot_table(index='appsflyer_id',columns='event_name',values='platform',aggfunc='count').reset_index().fillna(0)
    event_data = event_data[['appsflyer_id','af_complete_registration','re-engagement','re-attribution']]

    #purchase_data['event_revenue'] = purchase_data['event_revenue'].astype(float)
    #revenue_data = purchase_data.pivot_table(index='appsflyer_id',columns='event_name',values='event_revenue',aggfunc='sum').reset_index().fillna(0)
    #revenue_data = revenue_data.rename(columns = {'af_purchase': 'revenue'})

    os_data = data[['appsflyer_id','platform','country_code']]
    os_data = os_data.drop_duplicates(subset='appsflyer_id',keep ='first')

    data['install_time'] = pd.to_datetime(data['install_time'])
    install_year_data = data.sort_values(['appsflyer_id','install_time'])
    install_year_data = install_year_data.drop_duplicates(subset='appsflyer_id',keep='first')
    install_year_data['install_year'] = install_year_data['install_time'].dt.strftime('%Y')
    install_year_data = install_year_data[['appsflyer_id','install_year']]

    #데이터 머징하기
    result_data = pd.merge(os_data, event_data, on= 'appsflyer_id', how = 'left').fillna(0)
    result_data = pd.merge(result_data, install_year_data, on='appsflyer_id', how='left').fillna(0)
    result_data = pd.merge(result_data, treat_data,on= 'appsflyer_id', how = 'left' ).fillna(0)
    result_data.loc[result_data['country_code'] != 'KR','country_code'] = 'ETC'

    result_data.loc[result_data['af_complete_registration'] >= 1, 'af_complete_registration'] = 1

    result_data.to_csv(dr.download_dir + '/성향점수분석raw.csv', index=False, encoding='utf-8-sig')

    return result_data
