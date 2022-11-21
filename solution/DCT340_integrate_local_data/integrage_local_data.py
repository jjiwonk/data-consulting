import setting.directory as dr
from spreadsheet import spreadsheet

import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd

ADVERTISER = '이니스프리'


def get_data(raw_dir, file_name, cols):
    index_columns = cols
    dtypes = dict()
    for col in index_columns:
        dtypes[col] = pa.string()

    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = [file_name]
    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()

    return raw_df


def integrate_data():
    doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1TXE1ZXyQJRTgiH-5R7tubn1dONSQbAZCFI6eUvaga_o/edit#gid=0')
    column_info = spreadsheet.spread_sheet(doc, ADVERTISER).reset_index(drop=True)

    total_df = pd.DataFrame()
    for index in range(len(column_info)):
        info = column_info.loc[index, ['매체', '경로', '파일명', '인코딩']]
        media = info['매체']
        raw_dir = dr.dropbox_dir + info['경로']
        file_name = info['파일명']
        cols = column_info.iloc[index, 4:]
        col_keys = list(cols.loc[lambda x: x != ''].index)
        col_values = list(cols.loc[lambda x: x != ''].values)
        df = get_data(raw_dir, file_name, col_values)

        cols_for_rename = dict()
        for col in col_keys:
            cols_for_rename[cols[col]] = col
        df = df.rename(columns=cols_for_rename)

        total_cols = list(cols.index)
        for col in total_cols:
            if col in col_keys or col == '...':
                continue
            else:
                df[col] = 0
        df['매체구분'] = media
        total_df = pd.concat([total_df, df]).reset_index(drop=True)

    return total_df


integrated_df = integrate_data()