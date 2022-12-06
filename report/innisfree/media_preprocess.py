import setting.directory as dr
import setting.report_date as rdate
from spreadsheet import spreadsheet

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import os
import datetime
import warnings
warnings.filterwarnings("ignore")


def get_data(raw_dir, required_date, columns, name_check: dict = None):
    index_columns = columns
    dtypes = dict()
    for col in index_columns:
        dtypes[col] = pa.string()

    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')
    table_list = []

    date_check = required_date.strftime('%Y%m')
    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f and str(f)[-10:-4] == date_check]

    if name_check is not None:
        if 'in' in name_check.keys():
            files = [f for f in files if name_check['in'] in f]
        else:
            files = [f for f in files if name_check['not_in'] not in f]

    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()

    return raw_df


def integrate_media_data():
    doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')
    column_info = spreadsheet.spread_sheet(doc, '매체 전처리').reset_index(drop=True)
    total_cols = list(column_info.columns[3:])
    total_df = pd.DataFrame(columns=['매체구분']+total_cols)

    for index in range(len(column_info)):
        info = column_info.loc[index, ['경로', '마크업', '매체']]
        print(f'merging {info["매체"]}')
        media = info['매체']
        markup = float(info['마크업'])
        raw_dir = dr.dropbox_dir + info['경로']

        cols = column_info.iloc[index, 3:].loc[lambda x: x != '']
        col_keys = list(cols.index)
        col_values = list(cols.values)

        # 구글 데이터 분리
        if media == 'google':
            name_check = {'in': 'adgroup'}
        elif media == 'pmax':
            name_check = {'in': 'campaign'}
        else:
            name_check = None

        try:
            df = get_data(raw_dir, rdate.day_1, col_values, name_check)
        except Exception as e:
            print(f'check the {info["매체"]} with error that {e}.')
            df = pd.DataFrame(columns=col_keys)

        cols_for_rename = dict(zip(col_values, col_keys))
        df = df.rename(columns=cols_for_rename)
        for col in total_cols:
            if col in col_keys:
                continue
            else:
                df[col] = 0

        # 마크업 적용
        df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) * markup
        # 매체 컬럼 추가
        df['매체구분'] = media

        # 네이버 데이터 전처리
        if media == 'naver_sa':
            df = df.loc[df['캠페인타입'].isin(['1.0', '2.0'])]
            df['네이버 purchase_web'] = df['1_1_conversion_count'].apply(pd.to_numeric) + df['2_1_conversion_count'].apply(pd.to_numeric)
            df['네이버 revenue_web'] = df['1_1_sales_by_conversion'].apply(pd.to_numeric) + df['2_1_sales_by_conversion'].apply(pd.to_numeric)
            # 데이터 통합
            total_df = pd.concat([total_df, df[['매체구분', 'cost(정산기준)', '네이버 purchase_web', '네이버 revenue_web'] + col_keys]]).reset_index(drop=True)
        elif media == 'naver_bs':
            df = df.loc[df['캠페인타입'].isin(['0.0', '4.0'])]
            df['네이버 purchase_web'] = df['1_1_conversion_count'].apply(pd.to_numeric) + df['2_1_conversion_count'].apply(pd.to_numeric)
            df['네이버 revenue_web'] = df['1_1_sales_by_conversion'].apply(pd.to_numeric) + df['2_1_sales_by_conversion'].apply(pd.to_numeric)
            # 데이터 통합
            total_df = pd.concat([total_df, df[['매체구분', 'cost(정산기준)', '네이버 purchase_web', '네이버 revenue_web'] + col_keys]]).reset_index(drop=True)
        else:
            # 데이터 통합
            total_df = pd.concat([total_df, df[['매체구분', 'cost(정산기준)'] + col_keys]]).reset_index(drop=True)

    # NaN값 0 처리
    total_df.iloc[:, 7:] = total_df.iloc[:, 7:].fillna(0)
    # 데이터 타입 변환
    total_df.iloc[:, 7:] = total_df.iloc[:, 7:].apply(pd.to_numeric)

    return total_df


integrated_media_df = integrate_media_data()
date = datetime.datetime.today().strftime('%y%m%d')
integrated_media_df.to_csv(dr.download_dir + f'/integrated_media_{date}.csv', index=False, encoding='utf-8-sig')
print('download successfully')