import os
import pyarrow as pa
import pandas as pd
import setting.directory as dr
from workers.read_data import pyarrow_csv


def get_raw_data(event_folder):
    raw_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/더한섬/raw_data/{event_folder}'
    file_list = os.listdir(raw_dir)
    csv_list = []
    for file in file_list:
        if 'csv' in file:
            csv_list.append(file)
    dtypes = {
            'Attributed Touch Type': pa.string(),
            'Attributed Touch Time': pa.string(),
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Event Value': pa.string(),
            'Event Revenue': pa.string(),
            'Event Revenue KRW': pa.string(),
            'Event Source': pa.string(),
            # 'Partner': pa.string(),
            # 'Media Source': pa.string(),
            # 'Channel': pa.string(),
            # 'Keywords': pa.string(),
            # 'Campaign': pa.string(),
            # 'Campaign ID': pa.string(),
            # 'Adset': pa.string(),
            # 'Adset ID': pa.string(),
            # 'Ad': pa.string(),
            # 'Ad ID': pa.string(),
            # 'Ad Type': pa.string(),
            # 'Region': pa.string(),
            # 'Country Code': pa.string(),
            # 'State': pa.string(),
            # 'City': pa.string(),
            # 'Language': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Advertising ID': pa.string(),
            'IDFA': pa.string(),
            'Android ID': pa.string(),
            'Customer User ID': pa.string(),
            # 'IMEI': pa.string(),
            # 'IDFV': pa.string(),
            # 'Platform': pa.string(),
            # 'Device Type': pa.string(),
            # 'Is Retargeting': pa.string(),
            # 'Retargeting Conversion Type': pa.string(),
            # 'User Agent': pa.string(),
            # 'Keyword ID': pa.string(),
            # 'Device Download Time': pa.string(),
            # 'Device Model': pa.string(),
            # 'Conversion Type': pa.string(),
            # 'Campaign Type': pa.string()
        }

    df = pyarrow_csv(dtypes=dtypes, directory=raw_dir, file_list=csv_list)
    cols = list(df.columns)
    rename_cols = {}
    for col in cols:
        rename_cols[col] = col.lower().replace(' ', '_')
    df = df.rename(columns=rename_cols)

    return df


def prep_purchase_raw_data(df):
    purchase_raw = df.loc[in_app_raw['event_name'].isin(['af_purchase', 'af_first_purchase'])]
    purchase_raw = purchase_raw.sort_values(['appsflyer_id', 'event_time']).reset_index(drop=True)
    temp_id = pd.concat([pd.Series(['']), purchase_raw['appsflyer_id']])[:-1].reset_index(drop=True)
    purchase_raw['compare_id'] = temp_id
    purchase_raw['is_first_purchase'] = purchase_raw.apply(lambda x: True if x['appsflyer_id'] != x['compare_id'] else False, axis=1)
    purchase_raw = purchase_raw.drop('compare_id', axis=1)
    purchase_raw['event_revenue_krw'] = purchase_raw['event_revenue_krw'].apply(pd.to_numeric)
    purchase_raw[['attributed_touch_time', 'install_time', 'event_time']] = purchase_raw[['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime, axis=1)
    return purchase_raw


re_engagement_raw = get_raw_data('리인게이지먼트')
install_raw = get_raw_data('설치')
uninstall_raw = get_raw_data('앱삭제')
in_app_raw = get_raw_data('인앱이벤트')
purchase_df = prep_purchase_raw_data(in_app_raw)
af_first_purchase_user_list = purchase_df.loc[purchase_df['event_name'] == 'af_first_purchase', 'appsflyer_id'].unique().tolist()
first_purchase_user_list = purchase_df.loc[purchase_df['is_first_purchase'] == True, 'appsflyer_id'].unique().tolist()
rfm_segment_df = purchase_df.pivot_table(index=['appsflyer_id'], values=['event_time', 'event_revenue_krw', 'event_name'], aggfunc=['last', 'sum', 'count'])
rfm_segment_df = rfm_segment_df.reset_index().iloc[:, [0, 3, 4, 5]]
rfm_segment_df.columns = ['appsflyer_id', 'recent_purchase_time', 'total_purchase_revenue', 'total_purchase_count']
