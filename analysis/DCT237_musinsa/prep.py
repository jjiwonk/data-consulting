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

def first_purchase_from_paid():
    paid_df = get_paid_df()
    purchase_data = paid_df.loc[paid_df['event_name'].isin(['af_purchase', 'first_purchase']), ['appsflyer_id', 'event_time']]
    purchase_data = purchase_data.sort_values('event_time')
    purchase_data = purchase_data.drop_duplicates(['appsflyer_id'])
    purchase_data.to_csv(info.raw_dir + '/purchase/paid_202207-202209.csv', index=False, encoding = 'utf-8-sig')