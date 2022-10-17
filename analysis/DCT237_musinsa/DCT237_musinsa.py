import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
start = time.time()

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/DCT237/RD'
paid_dir = raw_dir + '/paid'
organic_dir = raw_dir + '/organic'
result_dir = dr.download_dir

def get_paid_df(paid_dir):
    dtypes = {
        'attributed_touch_time': pa.string(),
        'attributed_touch_type': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_revenue' : pa.float32(),
        'event_revenue_currency' : pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'adset' : pa.string(),
        'ad' : pa.string(),
        'appsflyer_id': pa.string(),
        'is_retargeting': pa.string(),
        'platform': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(paid_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(paid_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df['is_organic'] = 'False'
    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    raw_df['attributed_touch_date'] = raw_df.loc[:, 'attributed_touch_time'].apply(lambda x: x.date())
    raw_df['attributed_touch_hour'] = raw_df.loc[:, 'attributed_touch_time'].apply(lambda x: x.hour)
    raw_df['attributed_touch_week'] = raw_df.loc[:, 'attributed_touch_time'].apply(lambda x: x.weekday())
    raw_df['event_date'] = raw_df.loc[:, 'event_time'].apply(lambda x: x.date())
    raw_df['event_hour'] = raw_df.loc[:, 'event_time'].apply(lambda x: x.hour)
    raw_df['event_week'] = raw_df.loc[:, 'event_time'].apply(lambda x: x.weekday())
    raw_df['etat'] = raw_df['event_time'] - raw_df['attributed_touch_time']

    return raw_df

paid_df = get_paid_df(paid_dir)


