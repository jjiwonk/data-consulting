# !pip install xlsxwriter
# !pip install pyarrow

import setting.directory as dr
import setting.report_date as rdate
from spreadsheet import spreadsheet

import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


finda_raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트'
n_raw_dir = finda_raw_dir + '/naver_prism'
g_raw_dir = finda_raw_dir + '/google_sa_prism'
result_dir = dr.download_dir
required_date = rdate.day_1


def get_raw_df(raw_dir, required_date):
    if 'naver' in raw_dir:
        dtypes = {
            'date': pa.string(),
            'campaign_name': pa.string(),
            'adgroup_name': pa.string(),
            'ad_keyword': pa.string(),
            'ad_id': pa.string(),
            'impression': pa.string(),
            'click': pa.string(),
            'cost': pa.string()
        }
    elif 'google' in raw_dir:
        dtypes = {
            'segments_date': pa.string(),
            'campaign_name': pa.string(),
            'ad_group_name': pa.string(),
            'segments_keyword_info_text': pa.string(),
            'ad_group_ad_ad_final_urls': pa.string(),
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

    date = required_date.strftime('%Y%m')
    raw_files = [f for f in files if date in str(f)]

    for f in raw_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    return raw_df

def prep_df(n_raw_df, g_raw_df, doc):
    n_raw_df = n_raw_df.loc[~(n_raw_df['campaign_name'] == '핀다_BS')]
    n_raw_df['date'] = n_raw_df['date'].apply(lambda x: pd.to_datetime(x).date())
    n_raw_df[['impression','click','cost']] = n_raw_df[['impression','click','cost']].apply(lambda x: pd.to_numeric(x))
    n_raw_df.loc[:, 'cost'] = n_raw_df.loc[:, 'cost'].apply(lambda x: x * 1.1)
    # 네이버 가공

    g_raw_df['segments_date'] = g_raw_df['segments_date'].apply(lambda x: pd.to_datetime(x).date())
    g_raw_df[['impressions', 'clicks', 'cost_micros']] = g_raw_df[['impressions', 'clicks', 'cost_micros']].apply(lambda x: pd.to_numeric(x))
    g_raw_df['cost_micros'] = g_raw_df['cost_micros'].apply(lambda x: x / 1000000)
    g_raw_df['sum_metrix'] = g_raw_df['impressions'] + g_raw_df['clicks'] + g_raw_df['cost_micros']
    g_raw_df = g_raw_df.loc[g_raw_df['sum_metrix'] > 0]
    def utm_content(list):
        for x in list:
            if 'utm_content' in x:
                return x
    g_raw_df['ad_group_ad_ad_final_urls'] = g_raw_df['ad_group_ad_ad_final_urls'].apply(lambda x: utm_content(x.split('&'))[12:])
    sheet_data = spreadsheet.spread_sheet(doc, '구글라벨(자동화:터치NO)') # 소재명-브릿지페이지 매핑
    label_dict = dict(zip(sheet_data['광고 소재명'], sheet_data['브릿지 페이지명']))
    g_raw_df['ad_group_ad_ad_final_urls'] = g_raw_df['ad_group_ad_ad_final_urls'].apply(lambda x: x+label_dict[x])
    # 구글 가공

    return n_raw_df, g_raw_df

def download_df(n_result_df, g_result_df):
    # 결과파일 다운로드
    date = required_date.strftime('%Y%m%d')
    writer = pd.ExcelWriter(result_dir + f'/매체종합_{date}.xlsx', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}})
    n_result_df.to_excel(writer, sheet_name='naver', encoding='utf-8-sig', index=False)
    g_result_df.to_excel(writer, sheet_name='google', encoding='utf-8-sig', index=False)
    writer.close()
    print('download success')


n_raw_df = get_raw_df(n_raw_dir, required_date)
g_raw_df = get_raw_df(g_raw_dir, required_date)
doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/1DEDtSe3LGC2bqS3-UHL62h50vtLIvzkbk3Nx512zZow/edit#gid=2004266056')
n_result_df, g_result_df = prep_df(n_raw_df, g_raw_df, doc)
download_df(n_result_df, g_result_df)
