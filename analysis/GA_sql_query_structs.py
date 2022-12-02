import setting.report_date as rdate
import setting.directory as dr

import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime
import os

# DCT387 이니스프리 GA 쿼리 구조 분석
raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/이니스프리/자동화리포트/GA'

def get_raw_df(raw_dir, columns, required_date):
    index_columns = columns
    dtypes = dict()
    for col in index_columns:
        dtypes[col] = pa.string()

    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    date_check = required_date.strftime('%Y%m')
    start_date = required_date.replace(day=20).strftime('%Y%m%d')
    end_date = required_date.strftime('%Y%m%d')

    files = os.listdir(raw_dir)
    if 'dimension50' in index_columns:
        files = [f for f in files if '.csv' in f and 'non_trg' not in f]
    else:
        files = [f for f in files if '.csv' in f and 'non_trg' in f]
    files = [f for f in files if '.csv' in f and str(f)[-12:-6] == date_check]
    raw_files = [f for f in files if
                 (int(str(f)[-12:-4]) >= int(start_date)) & (int(str(f)[-12:-4]) <= int(end_date))]

    for f in raw_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        date = datetime.datetime.strptime(str(f)[-12:-4], '%Y%m%d').strftime('%Y-%m-%d')
        col_date = [date for i in range(len(temp))]
        temp = temp.add_column(0, 'date', [col_date])
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df = raw_df.rename(columns={'﻿campaign':'campaign'})
    raw_df = raw_df.loc[raw_df['dataSource'] == 'web']
    raw_df['check_sourcemedium'] = raw_df['sourceMedium'].apply(lambda x: ('organic' not in x) & ('referral' not in x))
    raw_df = raw_df.loc[raw_df['check_sourcemedium'] == True]
    raw_df = raw_df.drop(columns='check_sourcemedium')
    raw_df.loc[:, ['goal1Completions', 'goal2Completions', 'goal3Completions', 'sessions', 'bounces', 'sessionDuration', 'transactions', 'transactionRevenue', 'pageviews']] \
        = raw_df.loc[:, ['goal1Completions','goal2Completions','goal3Completions','sessions','bounces', 'sessionDuration','transactions','transactionRevenue','pageviews']].apply(pd.to_numeric)
    return raw_df

non_trg_index_columns = ['﻿campaign','sourceMedium','keyword','adContent','operatingSystem',
                 'deviceCategory','dataSource','goal1Completions','goal2Completions','goal3Completions',
                 'sessions','bounces','sessionDuration','transactions','transactionRevenue','pageviews','view_id']
trg_index_columns = ['﻿campaign','sourceMedium','keyword','adContent','dimension50','operatingSystem',
                 'deviceCategory','dataSource','goal1Completions','goal2Completions','goal3Completions',
                 'sessions','bounces','sessionDuration','transactions','transactionRevenue','pageviews','view_id']

required_date = rdate.day_1
non_trg_df = get_raw_df(raw_dir, non_trg_index_columns, required_date)
trg_df = get_raw_df(raw_dir, trg_index_columns, required_date)

df_for_dimension50 = trg_df.drop_duplicates(['date', 'sourceMedium', 'campaign', 'dimension50', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource'])
non_trg_merge_df = pd.merge(non_trg_df, df_for_dimension50, how='left', on=['date', 'sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource'])
non_trg_merge_df = non_trg_merge_df.loc[:, ['date', 'sourceMedium', 'campaign', 'dimension50', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource',
                                           'sessions_x', 'bounces_x', 'sessionDuration_x', 'goal3Completions_x', 'goal1Completions_x', 'goal2Completions_x',
                                           'pageviews_x', 'transactions_x', 'transactionRevenue_x']]
df_for_trg = non_trg_df.drop_duplicates(['date', 'sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource'])
trg_merge_df = pd.merge(trg_df, df_for_trg, how='left', on=['date', 'sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource'])
trg_merge_df = trg_merge_df.loc[trg_merge_df['goal1Completions_y'].isnull(), ['date', 'sourceMedium', 'campaign', 'dimension50', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource',
                                           'sessions_x', 'bounces_x', 'sessionDuration_x', 'goal3Completions_x', 'goal1Completions_x', 'goal2Completions_x',
                                           'pageviews_x', 'transactions_x', 'transactionRevenue_x']]

total_df = pd.concat([non_trg_merge_df, trg_merge_df])
total_df = total_df.rename(columns={'date':'날짜', 'sourceMedium':'소스/매체', 'campaign':'캠페인', 'dimension50':'utm_trg', 'adContent':'광고콘텐츠', 'keyword':'키워드', 'dataSource':'데이터 소스', 'deviceCategory':'기기 카테고리', 'operatingSystem':'운영체제',
                                    'sessions_x':'세션', 'bounces_x':'이탈수', 'sessionDuration_x':'세션시간',
                                    'goal3Completions_x':'로그인목표완료수', 'goal1Completions_x':'회원가입목표완료수', 'goal2Completions_x':'장바구니목표완료수',
                                           'pageviews_x':'페이지뷰', 'transactions_x':'거래수', 'transactionRevenue_x':'수익'})
final_result = total_df.groupby(['날짜', '소스/매체', '캠페인', 'utm_trg', '광고콘텐츠', '키워드', '데이터 소스', '기기 카테고리', '운영체제']).sum().reset_index()
final_result = final_result.loc[(final_result['세션']+final_result['로그인목표완료수']+final_result['회원가입목표완료수']
                                 +final_result['장바구니목표완료수']+final_result['거래수']+final_result['수익']
                                 +final_result['페이지뷰']+final_result['이탈수']+final_result['세션시간']) > 0, :]
final_result = final_result.sort_values('소스/매체')
