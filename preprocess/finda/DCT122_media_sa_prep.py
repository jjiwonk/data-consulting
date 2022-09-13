import os

import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

finda_raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트'
n_raw_dir = finda_raw_dir + '/naver_prism'
g_raw_dir = finda_raw_dir + '/google_sa_prism'
result_dir = dr.download_dir
# 전일자 yyyy-mm-dd 형식으로 입력
required_date = '2022-09-07'

def get_raw_df(raw_dir, required_date):
    if 'naver' in raw_dir:
        dtypes = {
            'date': pa.string(),
            'campaign_name': pa.string(),
            'adgroup_name': pa.string(),
            'ad_keyword': pa.string(),
            'impression': pa.string(),
            'click': pa.string(),
            'cost': pa.string()
        }
    elif 'google' in raw_dir:
        dtypes = {
            'campaign_name': pa.string(),
            'ad_group_name': pa.string(),
            'ad_group_ad_ad_name': pa.string(),
            'segments_keyword_info_text': pa.string(),
            'segments_date': pa.string(),
            'impressions': pa.string(),
            'clicks': pa.string(),
            'cost_micros': pa.string()
        }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f]

    date = datetime.datetime.strptime(required_date,'%Y-%m-%d').strftime('%Y%m')
    raw_files = [f for f in files if date in str(f)]

    for f in raw_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    return raw_df

n_raw_df = get_raw_df(n_raw_dir, required_date)
n_raw_df['date'] = n_raw_df['date'].apply(pd.to_datetime)
n_raw_df = n_raw_df.loc[~(n_raw_df['campaign_name']=='핀다_BS')]
# 네이버 가공

g_raw_df = get_raw_df(g_raw_dir, required_date)
g_raw_df['segments_date'] = g_raw_df['segments_date'].apply(pd.to_datetime)
g_raw_df['sum_metrix'] = pd.to_numeric(g_raw_df['impressions']) + pd.to_numeric(g_raw_df['clicks']) + pd.to_numeric(g_raw_df['cost_micros'])
g_raw_df = g_raw_df.loc[g_raw_df['sum_metrix'] > 0]
# 구글 가공

date = datetime.datetime.strptime(required_date,'%Y-%m-%d').strftime('%Y%m%d')
n_raw_df.to_excel(result_dir + f'/naver_automation_{date}.xlsx', encoding='utf-8-sig', index=False)
g_raw_df.to_excel(result_dir + f'/google_automation_{date}.xlsx', encoding='utf-8-sig', index=False)
