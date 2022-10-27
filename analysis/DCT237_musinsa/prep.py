from analysis.DCT237_musinsa import info
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np
import re

def extract_ad_code(df):
    df['광고코드'] = df['ad'].apply(lambda x : info.code_pat.findall(str(x))[0] if info.code_pat.search(str(x)) else '')

    df.loc[df['광고코드']=='', '광고코드'] = df['adset'].apply(lambda x : info.code_pat.findall(str(x))[0] if info.code_pat.search(str(x)) else '')
    df.loc[df['광고코드']=='', '광고코드'] = df['campaign'].apply(lambda x : info.code_pat.findall(str(x))[0] if info.code_pat.search(str(x)) else '')
    return df

def event_data_parse(raw_data, file_name):
    purchase_data = raw_data.loc[raw_data['event_name'].isin(['af_purchase', 'first_purchase'])]
    purchase_data.to_csv(info.raw_dir + f'/purchase/{file_name}_purchase_data.csv', index=False, encoding = 'utf-8-sig')

    conversion_data = raw_data.loc[raw_data['event_name'].isin(['install', 're-engagement', 're-attribution'])]
    conversion_data.to_csv(info.raw_dir + f'/conversion/{file_name}_conversion_data.csv', index=False, encoding = 'utf-8-sig')

    signup_data = raw_data.loc[raw_data['event_name'].isin(['af_complete_registration'])]
    signup_data.to_csv(info.raw_dir + f'/signup/{file_name}_signup_data.csv', index=False, encoding = 'utf-8-sig')

def paid_data_prep():
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
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
    raw_df.loc[raw_df['attributed_touch_time'].isnull(), 'attributed_touch_time'] = raw_df['install_time']
    raw_df = raw_df.loc[(raw_df['attributed_touch_time'].dt.date>=info.from_date) &
                        (raw_df['attributed_touch_time'].dt.date<=info.to_date)]
    raw_df['is_organic'] = 'False'

    # 광고코드 붙이기
    raw_df = extract_ad_code(raw_df)

    index_df = pd.read_csv(info.raw_dir + '/campaign_index_f.csv')
    index_df.loc[index_df['광고코드'].isnull(), '광고코드'] = index_df['campaign'].apply(lambda x : info.code_pat.findall(str(x))[0] if info.code_pat.search(str(x)) else '')
    adcode_dict = dict(zip(index_df['campaign'], index_df['광고코드']))

    raw_df.loc[raw_df['광고코드']=='', '광고코드'] = raw_df['campaign'].apply(lambda x: adcode_dict.get(x))

    part_index = index_df[['campaign', '계정']]
    part_index = part_index.drop_duplicates(['campaign'])
    raw_df = raw_df.merge(part_index, on = ['campaign'], how = 'left')

    raw_df.loc[raw_df['adset']=='210708_무신사_퍼포먼스_파이어베이스_990원상품_CPP_AO', '광고코드'] = 'GOUBANE001'
    event_data_parse(raw_df, 'paid')
    return raw_df

def get_campaign_list():
    paid_df = paid_data_prep()
    paid_df['Cnt'] = 1

    pivot_index = ['media_source', 'campaign', 'adset','ad']
    paid_df[pivot_index] = paid_df[pivot_index].fillna('')

    paid_pivot = paid_df.pivot_table(index = pivot_index, columns = 'event_name', values = 'Cnt', aggfunc='sum').reset_index()
    paid_pivot.to_csv(info.raw_dir + '/musinsa_campaign_list.csv', index= False, encoding = 'utf-8-sig')
    return paid_pivot

def organic_data_prep():
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
    event_data_parse(raw_df, 'organic')
    return raw_df

def raw_data_concat():
    paid_df = paid_data_prep()
    organic_df = organic_data_prep()

    raw_df = pd.concat([paid_df, organic_df], sort = False, ignore_index=True)

    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    raw_df = raw_df.sort_values(['event_time','attributed_touch_time'])
    raw_df.index = range(0, len(raw_df))

    raw_df_dedup = raw_df.drop_duplicates(['event_time', 'event_name', 'appsflyer_id'], keep='last')
    return raw_df_dedup

def first_purchase_total():
    def first_purchase_from_paid():
        paid_df = paid_data_prep()
        purchase_data = paid_df.loc[
            paid_df['event_name'].isin(['af_purchase', 'first_purchase']), ['appsflyer_id', 'event_time']]
        purchase_data = purchase_data.sort_values('event_time')
        purchase_data = purchase_data.drop_duplicates(['appsflyer_id'])
        purchase_data.to_csv(info.raw_dir + '/purchase/paid_202207-202209.csv', index=False, encoding='utf-8-sig')
    def first_purchase_from_organic():
        organic_df = organic_data_prep()
        purchase_data = organic_df.loc[
            organic_df['event_name'].isin(['af_purchase', 'first_purchase']), ['appsflyer_id', 'event_time']]
        purchase_data = purchase_data.sort_values('event_time')
        purchase_data = purchase_data.drop_duplicates(['appsflyer_id'])
        purchase_data.to_csv(info.raw_dir + '/purchase/organic_202207-202209.csv', index=False, encoding='utf-8-sig')

    first_purchase_from_paid()
    first_purchase_from_organic()

    original_df = pd.read_csv(info.raw_dir + '/purchase/fp_2020-01_2022_09.csv')
    paid_df = pd.read_csv(info.raw_dir + '/purchase/paid_202207-202209.csv')
    organic_df = pd.read_csv(info.raw_dir + '/purchase/organic_202207-202209.csv')

    total_df = pd.concat([original_df, paid_df, organic_df])
    total_df = total_df.sort_values('event_time')
    total_df = total_df.drop_duplicates('appsflyer_id')
    total_df.to_csv(info.raw_dir + '/purchase/' + '/fp_2020-01_2022_09.csv', index=False, encoding = 'utf-8-sig')

def cost_data_prep():
    files = os.listdir(info.report_dir)
    cost_df = pd.read_excel(info.report_dir + '/' + files[0], sheet_name='raw_무신사')
    cost_df = cost_df.rename(columns = {'매체' : 'media_source',
                                        '캠페인 이름' : 'campaign',
                                        '광고 세트 이름' : 'adset',
                                        '광고명' : 'ad',
                                        '파트' : '계정',
                                        '광고비_Fee포함' : 'cost'})
    cost_df['date'] = pd.to_datetime(cost_df['일']).dt.date
    cost_df = cost_df.loc[(cost_df['date']>=info.from_date)&(cost_df['date']<=info.to_date)]
    cost_df.to_csv(info.raw_dir + '/cost/cost_data.csv', index=False, encoding = 'utf-8-sig')
def get_cost_pivot(pivot_index):
    cost_df = pd.read_csv(info.raw_dir + '/cost/cost_data.csv')
    cost_df[pivot_index] = cost_df[pivot_index].fillna('')
    cost_pivot = cost_df.pivot_table(index=pivot_index, values=['cost','AF_사용자'], aggfunc='sum').reset_index()
    return cost_pivot