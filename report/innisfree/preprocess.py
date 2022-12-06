import setting.report_date as rdate
import report.innisfree.media_preprocess as info

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import os
import datetime


def get_data(raw_dir, columns, required_date, data_type, name_check: dict = None) -> pd.DataFrame:
    index_columns = columns
    dtypes = dict()
    for col in index_columns:
        dtypes[col] = pa.string()

    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')
    table_list = []

    date_check = required_date.strftime('%Y%m')
    files = os.listdir(raw_dir)
    if data_type == 'media':
        files = [f for f in files if '.csv' in f and str(f)[-10:-4] == date_check]
    else:
        files = [f for f in files if '.csv' in f and str(f)[-12:-6] == date_check]

    if name_check is not None:
        files = [f for f in files if name_check['in'] in f and name_check['not_in'] not in f]

    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        if data_type == 'ga':
            date = datetime.datetime.strptime(str(f)[-12:-4], '%Y%m%d').strftime('%Y-%m-%d')
            col_date = [date for i in range(len(temp))]
            temp = temp.add_column(0, 'date', [col_date])
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()

    return raw_df


# 매체 데이터 전처리
def asa_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.asa_dir, info.cols.asa_cols, rdate.day_1, 'media')
    return raw_df


def criteo_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.criteo_dir, info.cols.criteo_cols, rdate.day_1, 'media')
    return raw_df


def fb_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.fb_dir, info.cols.fb_cols, rdate.day_1, 'media')
    return raw_df


def gg_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.gg_dir, info.cols.gg_cols, rdate.day_1, 'media')
    return raw_df


def kkm_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.kkm_dir, info.cols.kkm_cols, rdate.day_1, 'media')
    return raw_df


def nasa_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.nasa_dir, info.cols.nasa_cols, rdate.day_1, 'media')
    return raw_df


def nosp_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.nosp_dir, info.cols.nosp_cols, rdate.day_1, 'media')
    return raw_df


def remerge_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.remerge_dir, info.cols.remerge_cols, rdate.day_1, 'media')
    return raw_df


def rtb_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.rtb_dir, info.cols.rtb_cols, rdate.day_1, 'media')
    return raw_df


def tw_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.tw_dir, info.cols.tw_cols, rdate.day_1, 'media')
    return raw_df


# 트래커 데이터 전처리
def apps_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.criteo_dir, info.cols.criteo_cols, rdate.day_1, 'apps')
    return raw_df


def ga_prep() -> pd.DataFrame:
    raw_df = get_data(info.dir.criteo_dir, info.cols.criteo_cols, rdate.day_1, 'ga')
    return raw_df