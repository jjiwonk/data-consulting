import setting.directory as dr
import pandas as pd

raw_dir = dr.download_dir
raw1 = pd.read_csv(raw_dir + '/appsflyer report_0801-0831.csv')
raw2 = pd.read_csv(raw_dir + '/appsflyer report_0901-0925.csv')

raw_data = pd.concat([raw1, raw2], sort=False, ignore_index=True)
raw_data = raw_data[['attributed_touch_type', 'attributed_touch_time', 'install_time',
       'event_time', 'event_name', 'event_value', 'event_revenue',
       'event_revenue_currency', 'partner', 'media_source', 'channel',
       'keywords', 'campaign', 'campaign_id', 'adset', 'adset_id', 'ad']]
raw_data = raw_data.loc[raw_data['event_name']=='loan_contract_completed']

target_media_source = ['googleadwords_int','appier_int','kakao_int', 'cauly_int','moloco_int','네이버SA','Naver_SA_Mo_Main','Naver_SA_Mo_direct']
raw_data = raw_data.loc[raw_data['media_source'].isin(target_media_source)]
raw_data.to_csv(dr.download_dir + '/finda_appsflyer_data_sampling.csv', index=False, encoding = 'utf-8-sig')