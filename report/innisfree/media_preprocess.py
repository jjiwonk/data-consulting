from report.innisfree import directory as dr
from report.innisfree import ref

import pandas as pd
import numpy as np


def get_media_raw_data(media_name):
    info = ref.info_dict[media_name]

    result_cols = list(info['dimension'].keys()) + list(info['metric'].keys())
    read_cols = list(info['temp'].values()) + list(info['dimension'].values()) + list(info['metric'].values())

    # 매체전처리 체크 박스 반영 코드
    # table = ref.info_df[['사용 여부', '소스']].iloc[1:]
    # media_list = table.loc[table['사용 여부']=='TRUE', '소스'].to_list()
    # if media_name not in media_list:
    #     df = pd.DataFrame(columns=result_cols)
    #     return df

    raw_dir = info['read']['경로']
    file_name = info['read']['파일명'] + info['read']['suffix']

    try:
        df = pd.read_csv(dr.dropbox_dir + raw_dir + '/' + file_name, encoding='utf-8-sig', usecols = read_cols)
    except Exception as e:
        print(f"{media_name} is error with {e}.")
        df = pd.DataFrame(columns=result_cols)
        return df

    df_rename = df.rename(columns={v: k for k, v in info['temp'].items()})
    df_rename = df_rename.rename(columns = {v: k for k, v in info['dimension'].items()})
    df_rename = df_rename.rename(columns = {v: k for k, v in info['metric'].items()})

    for col in ref.columns.dimension_cols:
        if col in df_rename.columns:
            df_rename[col] = df_rename[col].fillna('')
        else:
            df_rename[col] = ''

    for col in ref.columns.metric_cols:
        if col in df_rename.columns:
            df_rename[col] = pd.to_numeric(df_rename[col])
            df_rename[col] = df_rename[col].fillna(0)
        else:
            df_rename[col] = 0

    df_rename['매체'] = media_name
    return df_rename


def calc_cost(df, media_name):
    info = ref.info_dict[media_name]
    div_list = info['prep']['나누기'].split('/')
    div_list = [float(div) for div in div_list]

    df['cost(정산기준)'] = df['cost(대시보드)'].copy()

    for div in div_list :
        df['cost(정산기준)'] = df['cost(정산기준)'].astype(float) / div

    mul_list = info['prep']['곱하기'].split('/')
    mul_list = [float(mul) for mul in mul_list]

    for mul in mul_list :
        df['cost(정산기준)'] = df['cost(정산기준)'].astype(float) * mul

    # 만약에 대시보드에 구글은 100만 나눠서, 애플은 1200 곱해서 보여주고 싶다고 하면 아래 코드 사용

    if media_name in ['Google_SA', 'AC_Install', 'Google_PMAX'] :
        df['cost(대시보드)'] = df['cost(대시보드)'] / 1000000
    elif media_name == 'Apple_SA' :
        df['cost(대시보드)'] = df['cost(대시보드)'] * 1200

    return df


def get_basic_data(media_name):
    df = get_media_raw_data(media_name)
    df = calc_cost(df, media_name)
    return df


def asa_prep() -> pd.DataFrame:
    df = get_basic_data('Apple_SA')
    return df


def criteo_prep() -> pd.DataFrame:
    df = get_basic_data('Criteo')
    return df


def fb_prep() -> pd.DataFrame:
    df = get_basic_data('FBIG')
    return df


def gg_sa_prep() -> pd.DataFrame:
    df = get_basic_data('Google_SA')
    return df


def gg_ac_prep() -> pd.DataFrame:
    df = get_basic_data('Google_SA')
    gg_ac_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'AC_Install', '캠페인'].unique().tolist()
    df = df.loc[df['캠페인'].isin(gg_ac_camp_list)]
    df['매체'] = 'AC_Install'
    return df


def pmax_prep() -> pd.DataFrame:
    df = get_basic_data('Google_PMAX')
    pmax_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Google_PMAX', '캠페인'].unique().tolist()
    df = df.loc[df['캠페인'].isin(pmax_camp_list)]
    return df


def kkm_prep() -> pd.DataFrame:
    df = get_basic_data('Kakao_Moment')
    kkm_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Kakao_Moment', '캠페인'].unique().tolist()
    df = df.loc[df['캠페인'].isin(kkm_camp_list)]
    return df


def kkbz_prep() -> pd.DataFrame:
    df = get_basic_data('Kakao_Moment')
    df = df.loc[df['캠페인'].str.contains('madit')]
    kkbz_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Kakao_Bizboard', '캠페인'].unique().tolist()
    df = df.loc[df['캠페인'].isin(kkbz_camp_list)]
    df['매체'] = 'Kakao_Bizboard'
    return df


def nasa_prep() -> pd.DataFrame:
    df = get_basic_data('Naver_SA')

    # naver_sa 데이터 선별
    df = df.loc[df['캠페인타입'].isin([1.0, 2.0])]
    # 데이터 추가 가공
    df['네이버 purchase_web'] = df['1_1_conversion_count'].apply(pd.to_numeric) + df['2_1_conversion_count'].apply(
        pd.to_numeric)
    df['네이버 revenue_web'] = df['1_1_sales_by_conversion'].apply(pd.to_numeric) + df['2_1_sales_by_conversion'].apply(
        pd.to_numeric)
    return df


def nabs_prep() -> pd.DataFrame:
    df1 = get_basic_data('Naver_SA')
    df1 = df1.loc[df1['캠페인타입'].isin([0.0, 4.0])]
    # 데이터 추가 가공
    df1['네이버 purchase_web'] = df1['1_1_conversion_count'].apply(pd.to_numeric) + df1['2_1_conversion_count'].apply(
        pd.to_numeric)
    df1['네이버 revenue_web'] = df1['1_1_sales_by_conversion'].apply(pd.to_numeric) + df1['2_1_sales_by_conversion'].apply(
        pd.to_numeric)

    df2 = get_basic_data('Naver_NOSP')
    nabs_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Naver_BSA', '캠페인'].unique().tolist()
    df2 = df2.loc[df2['캠페인'].isin(nabs_camp_list)]
    ad_dict = ref.media_info.bs_ad_dict
    df2['ad'] = df2['ad_detail'].apply(lambda x: ad_dict[x])

    df = pd.concat([df1, df2], sort=False, ignore_index=True)
    df['매체'] = 'Naver_BSA'
    return df


def nosp_prep() -> pd.DataFrame:
    df = get_basic_data('Naver_NOSP')
    except_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Naver_BSA', '캠페인'].unique().tolist()
    df = df.loc[~(df['캠페인'].isin(except_camp_list))]
    return df


def na_gfa_prep() -> pd.DataFrame:
    df = get_basic_data('Naver_GFA')
    df = df.loc[~(df['광고그룹'].str.contains('smartchannel'))]
    return df


def na_smch_prep() -> pd.DataFrame:
    df = get_basic_data('Naver_GFA')
    df = df.loc[df['광고그룹'].str.contains('smartchannel')]
    df['매체'] = 'Naver_Smartchannel'
    return df


def remerge_prep() -> pd.DataFrame:
    df = get_basic_data('Remerge')
    return df


def rtb_prep() -> pd.DataFrame:
    df = get_basic_data('RTBhouse')
    return df


def tw_prep() -> pd.DataFrame:
    df = get_basic_data('twitter')
    return df