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
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')
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

    raw_df = raw_df.loc[raw_df['dataSource'] == 'web']
    raw_df['check_sourcemedium'] = raw_df['sourceMedium'].apply(lambda x: ('organic' not in x) & ('referral' not in x))
    raw_df = raw_df.loc[raw_df['check_sourcemedium'] == True]
    raw_df = raw_df.drop(columns='check_sourcemedium')
    raw_df.loc[:, index_columns[-9:]] = raw_df.loc[:, index_columns[-9:]].apply(pd.to_numeric)

    return raw_df

non_trg_index_columns = ['sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource',
                         'sessions', 'bounces', 'sessionDuration', 'goal3Completions', 'goal1Completions', 'goal2Completions',
                         'pageviews', 'transactions', 'transactionRevenue']
trg_index_columns = ['dimension50'] + non_trg_index_columns

required_date = rdate.day_1
non_trg_df = get_raw_df(raw_dir, non_trg_index_columns, required_date)
trg_df = get_raw_df(raw_dir, trg_index_columns, required_date)

trg_index = ['date'] + trg_index_columns[:-9]
non_trg_index = ['date'] + non_trg_index_columns[:-9]

# 매핑되는 데이터는 trg 값 / goal값은 non_trg..!
df_for_dimension50 = trg_df.drop_duplicates(trg_index)
non_trg_merge_df = pd.merge(non_trg_df, df_for_dimension50, how='left', on=non_trg_index)
non_mapping_non_trg = non_trg_merge_df.loc[non_trg_merge_df['goal1Completions_y'].isnull(),
                                           trg_index + ['sessions_x', 'bounces_x', 'sessionDuration_x',
                                                        'goal3Completions_x', 'goal1Completions_x', 'goal2Completions_x',
                                                        'pageviews_x', 'transactions_x', 'transactionRevenue_x']]
mapping_non_trg = non_trg_merge_df.loc[non_trg_merge_df['goal1Completions_y'].notnull(),
                                       trg_index + ['sessions_y', 'bounces_y', 'sessionDuration_y',
                                                    'pageviews_y', 'transactions_y', 'transactionRevenue_y']]
for_non_mapping = non_trg_df.drop_duplicates(non_trg_index)
trg_merge_df = pd.merge(trg_df, for_non_mapping, how='left', on=non_trg_index)
trg_merge_df = trg_merge_df.loc[trg_merge_df['goal1Completions_y'].isnull(),
                                trg_index + ['sessions_x', 'bounces_x', 'sessionDuration_x',
                                             'goal3Completions_x', 'goal1Completions_x', 'goal2Completions_x',
                                             'pageviews_x', 'transactions_x', 'transactionRevenue_x']]

non_mapping_rename_dict = {'date':'날짜', 'sourceMedium':'소스/매체', 'campaign':'캠페인', 'dimension50':'utm_trg', 'adContent':'광고콘텐츠',
                           'keyword':'키워드', 'dataSource':'데이터 소스', 'deviceCategory':'기기 카테고리', 'operatingSystem':'운영체제',
                           'sessions_x':'세션', 'bounces_x':'이탈수', 'sessionDuration_x':'세션시간',
                           'goal3Completions_x':'로그인목표완료수', 'goal1Completions_x':'회원가입목표완료수', 'goal2Completions_x':'장바구니목표완료수',
                           'pageviews_x':'페이지뷰', 'transactions_x':'거래수', 'transactionRevenue_x':'수익'}
mapping_rename_dict = {'date':'날짜', 'sourceMedium':'소스/매체', 'campaign':'캠페인', 'dimension50':'utm_trg', 'adContent':'광고콘텐츠',
                       'keyword':'키워드', 'dataSource':'데이터 소스', 'deviceCategory':'기기 카테고리', 'operatingSystem':'운영체제',
                       'sessions_y':'세션', 'bounces_y':'이탈수', 'sessionDuration_y':'세션시간',
                       'goal3Completions_x':'로그인목표완료수', 'goal1Completions_x':'회원가입목표완료수', 'goal2Completions_x':'장바구니목표완료수',
                       'pageviews_y':'페이지뷰', 'transactions_y':'거래수', 'transactionRevenue_y':'수익'}

non_mapping_non_trg = non_mapping_non_trg.rename(columns=non_mapping_rename_dict)
mapping_non_trg = mapping_non_trg.rename(columns=mapping_rename_dict)
trg_merge_df = trg_merge_df.rename(columns=non_mapping_rename_dict)

total_df = pd.concat([non_mapping_non_trg, mapping_non_trg, trg_merge_df])
final_result = total_df.groupby(['날짜', '소스/매체', '캠페인', 'utm_trg', '광고콘텐츠', '키워드', '데이터 소스', '기기 카테고리', '운영체제']).sum().reset_index()
final_result = final_result.loc[final_result[['세션', '로그인목표완료수', '회원가입목표완료수', '장바구니목표완료수',
                                              '거래수', '수익', '페이지뷰', '이탈수', '세션시간']].values.sum(axis=1) > 0, :]
final_result = final_result.sort_values('소스/매체')
