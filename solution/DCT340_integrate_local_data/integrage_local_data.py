import setting.directory as dr
from spreadsheet import spreadsheet

import pandas as pd
import datetime
import chardet
import warnings
warnings.filterwarnings("ignore")

ADVERTISER = '이니스프리'

def get_data(raw_dir, file_name, cols, header):
    path = raw_dir + '/' + file_name
    rawdata = open(path, 'rb').read()
    encodings = chardet.detect(rawdata)['encoding']
    if file_name[-4:] == 'xlsx':
        raw_df = pd.read_excel(path, header=header)
        raw_df = raw_df[cols]
    else:
        if encodings == 'UTF-16':
            raw_df = pd.read_csv(path, header=header, encoding=encodings, skip_blank_lines=False, delimiter='\t')
        else:
            raw_df = pd.read_csv(path, header=header, encoding=encodings, skip_blank_lines=False)
        raw_df = raw_df[cols]

    return raw_df


def integrate_data():
    doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1TXE1ZXyQJRTgiH-5R7tubn1dONSQbAZCFI6eUvaga_o')
    column_info = spreadsheet.spread_sheet(doc, ADVERTISER).reset_index(drop=True)
    total_cols = list(column_info.columns[4:])
    total_df = pd.DataFrame(columns=['매체구분']+total_cols)

    for index in range(len(column_info)):
        info = column_info.loc[index, ['매체', '경로', '파일명', '헤더번호']]
        print(f'merging {info["파일명"]}')
        media = info['매체']
        raw_dir = dr.dropbox_dir + info['경로']
        file_name = info['파일명']
        if info['헤더번호'] == '':
            header = 0
        else:
            header = int(info['헤더번호']) - 1
        cols = column_info.iloc[index, 4:].loc[lambda x: x != '']
        col_keys = list(cols.index)
        col_values = list(cols.values)
        try:
            df = get_data(raw_dir, file_name, col_values, header)
        except Exception as e:
            print(f'check the {info["파일명"]} with error that {e}.')
            df = pd.DataFrame(columns=col_keys)

        cols_for_rename = dict(zip(col_values, col_keys))
        df = df.rename(columns=cols_for_rename)

        for col in total_cols:
            if col in col_keys:
                continue
            else:
                df[col] = 0
        df['매체구분'] = media
        total_df = pd.concat([total_df, df[['매체구분'] + col_keys]]).reset_index(drop=True)

    return total_df


integrated_df = integrate_data()
date = datetime.datetime.today().strftime('%y%m%d')
integrated_df.to_csv(dr.download_dir + f'/integrated_report_from_local_{date}.csv', index=False, encoding='utf-8-sig')
print('download successfully')