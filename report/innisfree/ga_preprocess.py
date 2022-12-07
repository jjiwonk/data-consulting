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
    start_date = required_date.replace(day=1).strftime('%Y%m%d')
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


def ga_prep():
    required_date = rdate.day_1

    index = ['sourceMedium', 'campaign', 'adContent', 'keyword', 'deviceCategory', 'operatingSystem', 'dataSource']
    metric = ['sessions', 'bounces', 'sessionDuration', 'goal3Completions', 'goal1Completions', 'goal2Completions', 'pageviews', 'transactions', 'transactionRevenue']
    non_trg_index_columns = index + metric
    trg_index_columns = ['dimension50'] + index + metric

    non_trg_df = get_raw_df(raw_dir, non_trg_index_columns, required_date)
    trg_df = get_raw_df(raw_dir, trg_index_columns, required_date)

    non_trg_cols = list(non_trg_df.columns)
    trg_cols = list(trg_df.columns)
    non_trg_index = non_trg_cols[:-9]
    trg_index = trg_cols[:-9]

    merge_df = pd.merge(non_trg_df, trg_df, how='left', on=non_trg_index)
    # dimension50 매핑 불가 데이터
    non_mapping = merge_df.loc[merge_df['dimension50'].isnull(), trg_index + [x+'_x' for x in metric]]
    non_mapping = non_mapping.rename(columns=dict(zip([col+'_x' for col in metric], metric)))
    non_mapping = non_mapping.fillna('')
    # dimension50 매핑 데이터
    mapping = merge_df.loc[merge_df['dimension50'].notnull(), non_trg_index].drop_duplicates(non_trg_index)
    mapping_non_trg = pd.merge(mapping, non_trg_df, how='left', on=non_trg_index)
    grouping_trg = trg_df.groupby(non_trg_index).sum()
    grouping_trg['dimension50'] = ''
    merge_for_dif = pd.merge(mapping_non_trg, grouping_trg, how='left', on=non_trg_index)
    for col in metric:
        merge_for_dif[col] = merge_for_dif[col+'_x'] - merge_for_dif[col+'_y']
    merge_for_dif = merge_for_dif[trg_cols]

    total_df = pd.concat([non_mapping, merge_for_dif, trg_df]).groupby(trg_index).sum().reset_index()

    new_cols = ['날짜', 'utm_trg', '소스/매체', '캠페인', '광고콘텐츠', '키워드', '기기 카테고리', '운영체제', '데이터 소스',
                '세션', '이탈수', '세션시간', '로그인목표완료수', '회원가입목표완료수', '장바구니목표완료수', '페이지뷰', '거래수', '수익']
    rename_dict = dict(zip(trg_cols, new_cols))
    total_df = total_df.rename(columns=rename_dict)
    def check_device(data):
        if data in ['mobile','tablet']:
            return 'Mobile'
        elif data == 'desktop':
            return 'PC'
        else:
            return data
    total_df.loc[:, '기기 카테고리'] = total_df.loc[:, '기기 카테고리'].apply(check_device)

    total_df = total_df.loc[total_df[['세션', '로그인목표완료수', '회원가입목표완료수', '장바구니목표완료수',
                                      '거래수', '수익', '페이지뷰', '이탈수', '세션시간']].values.sum(axis=1) > 0, :]
    total_df = total_df.sort_values('소스/매체')
    total_df.to_csv(dr.download_dir + '/ga_result.csv', encoding='utf-8-sig', index=False)

    return total_df