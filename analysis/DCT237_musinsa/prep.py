from analysis.DCT237_musinsa import info
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np
def get_paid_df():
    dtypes = {
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_revenue' : pa.float32(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'adset' : pa.string(),
        'ad' : pa.string(),
        'appsflyer_id': pa.string(),
        'platform': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)

    raw_df = table.to_pandas()
    raw_df['is_organic'] = 'False'
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
    return raw_df

def get_campaign_list():
    paid_df = get_paid_df()
    paid_df['Cnt'] = 1

    pivot_index = ['media_source', 'campaign', 'adset','ad']
    paid_df[pivot_index] = paid_df[pivot_index].fillna('')

    paid_pivot = paid_df.pivot_table(index = pivot_index, columns = 'event_name', values = 'Cnt', aggfunc='sum').reset_index()
    paid_pivot.to_csv(info.raw_dir + '/musinsa_campaign_list.csv', index= False, encoding = 'utf-8-sig')
    return paid_pivot

def get_organic_df():
    dtypes = {
        'Attributed Touch Time': pa.string(),
        'Attributed Touch Type': pa.string(),
        'Install Time': pa.string(),
        'Event Time': pa.string(),
        'Event Name': pa.string(),
        'Event Revenue' : pa.float32(),
        'Media Source': pa.string(),
        'Campaign': pa.string(),
        'Adset': pa.string(),
        'Ad' : pa.string(),
        'AppsFlyer ID': pa.string(),
        'Platform': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(info.organic_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(info.organic_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df = raw_df.rename(columns=
                           {'Attributed Touch Time': 'attributed_touch_time',
                            'Attributed Touch Type': 'attributed_touch_type',
                            'Install Time': 'install_time',
                            'Event Time': 'event_time',
                            'Event Name': 'event_name',
                            'Event Revenue' : 'event_revenue',
                            'Media Source': 'media_source',
                            'Campaign': 'campaign',
                            'Adset': 'adset',
                            'Ad' : 'ad',
                            'AppsFlyer ID': 'appsflyer_id',
                            'Event Value': 'event_value',
                            'Platform': 'platform'})
    raw_df['is_organic'] = True
    return raw_df

def raw_data_concat():
    paid_df = get_paid_df()
    organic_df = get_organic_df()

    raw_df = pd.concat([paid_df, organic_df], sort = False, ignore_index=True)

    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    raw_df = raw_df.sort_values(['event_time','attributed_touch_time'])
    raw_df.index = range(0, len(raw_df))

    raw_df_dedup = raw_df.drop_duplicates(['event_time', 'event_name', 'appsflyer_id'], keep='last')
    return raw_df_dedup

def first_purchase_total():
    def first_purchase_from_paid():
        paid_df = get_paid_df()
        purchase_data = paid_df.loc[
            paid_df['event_name'].isin(['af_purchase', 'first_purchase']), ['appsflyer_id', 'event_time']]
        purchase_data = purchase_data.sort_values('event_time')
        purchase_data = purchase_data.drop_duplicates(['appsflyer_id'])
        purchase_data.to_csv(info.raw_dir + '/purchase/paid_202207-202209.csv', index=False, encoding='utf-8-sig')
    def first_purchase_from_organic():
        organic_df = get_organic_df()
        purchase_data = organic_df.loc[
            organic_df['event_name'].isin(['af_purchase', 'first_purchase']), ['appsflyer_id', 'event_time']]
        purchase_data = purchase_data.sort_values('event_time')
        purchase_data = purchase_data.drop_duplicates(['appsflyer_id'])
        purchase_data.to_csv(info.raw_dir + '/purchase/organic_202207-202209.csv', index=False, encoding='utf-8-sig')

    original_df = pd.read_csv(info.raw_dir + '/purchase/' + '/fp_2020-01_2022_06.csv')
    paid_df = pd.read_csv(info.raw_dir + '/purchase/' + '/paid_202207-202209.csv')
    organic_df = pd.read_csv(info.raw_dir + '/purchase/' + '/organic_202207-202209.csv')

    total_df = pd.concat([original_df, paid_df, organic_df])
    total_df = total_df.sort_values('event_time')
    total_df = total_df.drop_duplicates('appsflyer_id')
    total_df.to_csv(info.raw_dir + '/purchase/' + '/fp_2020-01_2022_09.csv', index=False, encoding = 'utf-8-sig')

def conversion_data():
    raw_data = raw_data_concat()
    raw_data = raw_data.loc[raw_data['event_name'].isin(['install', 're-engagement', 're-attribution'])]
    return raw_data

def signup_data():
    raw_data = raw_data_concat()
    raw_data = raw_data.loc[raw_data['event_name'].isin(['af_complete_registration'])]
    return raw_data