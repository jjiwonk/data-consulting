import setting.directory as dr
from spreadsheet import spreadsheet

import pandas as pd
import datetime
import warnings
warnings.filterwarnings("ignore")


# doc = spreadsheet.spread_document_read(
#     'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')
# # def get_info(dir_name):
#     column_info = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
#     info = column_info.loc[column_info['경로'].str.contains(dir_name)]
#     return info


def asa_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def criteo_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def fb_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def gg_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def pmax_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def kkm_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def nasa_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # naver_sa 데이터 선별
    df = df.loc[df['캠페인타입'].isin(['1.0', '2.0'])]
    # 데이터 추가 가공
    df['네이버 purchase_web'] = df['1_1_conversion_count'].apply(pd.to_numeric) + df['2_1_conversion_count'].apply(
        pd.to_numeric)
    df['네이버 revenue_web'] = df['1_1_sales_by_conversion'].apply(pd.to_numeric) + df['2_1_sales_by_conversion'].apply(
        pd.to_numeric)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def nabs_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # naver_bs 데이터 선별
    df = df.loc[df['캠페인타입'].isin(['0.0', '4.0'])]
    # 데이터 추가 가공
    df['네이버 purchase_web'] = df['1_1_conversion_count'].apply(pd.to_numeric) + df['2_1_conversion_count'].apply(
        pd.to_numeric)
    df['네이버 revenue_web'] = df['1_1_sales_by_conversion'].apply(pd.to_numeric) + df['2_1_sales_by_conversion'].apply(
        pd.to_numeric)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def nosp_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def remerge_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def rtb_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def tw_prep(info, total_cols) -> pd.DataFrame:
    raw_dir = info['경로']
    file_name = info['파일명'] + info['suffix']
    markup = {'div': info['나누기'], 'mul': info['곱하기']}
    media = info['매체']
    # 데이터 불러오기
    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig')
    except Exception as e:
        print(f"{info['매체']} is error with {e}.")
        df = pd.DataFrame(columns=total_cols)
    # 컬럼명 변환
    cols = info.iloc[7:].loc[lambda x: x != '']
    col_keys = list(cols.index)
    col_values = list(cols.values)
    cols_for_rename = dict(zip(col_values, col_keys))
    df = df.rename(columns=cols_for_rename)
    for col in total_cols:
        if col in col_keys:
            continue
        else:
            df[col] = ''

    # 마크업 적용
    # df['cost(정산기준)'] = df['cost(대시보드)'].apply(pd.to_numeric) / markup['div'] * markup['mul']
    # 매체 컬럼 추가
    df['매체'] = media

    return df[total_cols]


def integrate_media_data():
    doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')
    column_info = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
    total_cols = list(column_info.columns[6:])
    total_df = pd.DataFrame(columns=total_cols)

    for info in column_info:
        if info['사용 여부'] == 'TRUE':
            print(f"{info['매체']} merging")
            if info['매체'] == 'facebook':
                df = fb_prep(info, total_cols)
            elif info['매체'] == 'google':
                df = gg_prep(info, total_cols)
            elif info['매체'] == 'pmax':
                df = pmax_prep(info, total_cols)
            elif info['매체'] == 'kakaomoment':
                df = kkm_prep(info, total_cols)
            elif info['매체'] == 'naver_sa':
                df = nasa_prep(info, total_cols)
            elif info['매체'] == 'naver_bs':
                df = nabs_prep(info, total_cols)
            elif info['매체'] == 'ASA':
                df = asa_prep(info, total_cols)
            elif info['매체'] == 'criteo':
                df = criteo_prep(info, total_cols)
            elif info['매체'] == 'twitter':
                df = tw_prep(info, total_cols)
            elif info['매체'] == 'nosp':
                df = nosp_prep(info, total_cols)
            elif info['매체'] == 'remerge':
                df = remerge_prep(info, total_cols)
            elif info['매체'] == 'rtbhouse':
                df = rtb_prep(info, total_cols)
            else:
                df = pd.DataFrame(columns=total_cols)

            total_df = pd.concat([total_df, df], axis=0).reset_index(drop=True)

    # 데이터 타입 변환
    total_df.iloc[:, 7:] = total_df.iloc[:, 7:].apply(pd.to_numeric)
    # NaN값 0 처리
    total_df.iloc[:, 7:] = total_df.iloc[:, 7:].fillna(0)

    return total_df


integrated_media_df = integrate_media_data()
date = datetime.datetime.today().strftime('%y%m%d')
# integrated_media_df.to_csv(dr.download_dir + f'/integrated_media_{date}.csv', index=False, encoding='utf-8-sig')
# print('download successfully')