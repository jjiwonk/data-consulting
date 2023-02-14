import pandas as pd
from report.SIV import ref
import report.SIV.media_prep as mprep
import report.SIV.ga_prep as gprep
import report.SIV.apps_prep as aprep
from report.SIV import directory as dr

def total_media_raw():

    info_df = ref.info_df
    media_list = info_df.loc[info_df['사용 여부'] == 'TRUE']['구분'].to_list()
    media_list = media_list + ['버티컬']
    func_list = []

    for media in media_list:
        func = f'get_{media}'
        func_list.append(func)

    df = pd.DataFrame()

    for func in func_list:
        media_df = getattr(mprep,func)()
        df = pd.concat([df, media_df])
        df = df.sort_values(by = ['날짜', '구분'])

    df = ref.adcode(df,'캠페인','세트','소재')
    df.loc[df['구분'] == 'NOSP', '소재'] = '-'
    df = ref.date_dt(df)
    df = df.groupby(ref.columns.media_dimension)[ref.columns.media_mertic].sum().reset_index()
    df['sum'] = df[ref.columns.media_mertic].sum(axis=1)
    df = df.loc[df['sum'] >= 1].drop(columns = ['sum','구분'])

    index = ref.index_df[['지면/상품', '매체', '캠페인 구분', 'KPI', '캠페인 라벨', 'OS', '파트(주체)', '파트 구분', '머징코드']]
    index = index.drop_duplicates(keep='first')
    merge = pd.merge(df, index, on='머징코드', how='left').fillna('no_index')

    merge = merge[ref.columns.media_report_col]
    merge.to_csv(dr.download_dir + f'media_raw/media_raw_{ref.r_date.yearmonth}.csv', index=False, encoding='utf-8-sig')

    return df


def media_tracker():

    media_raw = total_media_raw()
    ga_raw = gprep.ga_report()
    apps_raw = aprep.apps_concat()
    ds_raw = ref.ds_raw

    #DS 예외처리
    ds_raw['구분'] ='-'
    ds_raw.loc[ds_raw['구분'] == '-', '머징코드'] = ds_raw['머징코드'].apply(lambda x: x.replace(x, ref.exc_code_dict[x]) if x in ref.exc_code_dict.keys() else x)
    ds_raw = ref.date_dt(ds_raw)
    ds_raw = ds_raw.drop(columns =['구분'])

    ds_raw['날짜'] = pd.to_datetime(ds_raw['날짜'])
    ds_raw['날짜'] = ds_raw['날짜'].dt.date
    ds_raw = ds_raw.loc[(ref.r_date.start_date <= ds_raw['날짜']) & (ds_raw['날짜']<= ref.r_date.target_date )]

    # 인덱스 (전월 + 당월)
    merge_index = media_raw[['머징코드', '캠페인', '세트', '소재']]
    merge_index2 = pd.read_csv(dr.download_dir + f'media_raw/media_raw_{ref.r_date.index_date}.csv')
    merge_index2 = merge_index2[['머징코드', '캠페인', '세트', '소재']]
    merge_index = pd.concat([merge_index, merge_index2])

    merge_index = merge_index.drop_duplicates(keep='last')
    merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
    merge_index = merge_index.drop_duplicates(subset='머징코드',keep='last')

    merge_cdict = dict(zip(merge_index['머징코드'], merge_index['캠페인']))
    merge_gdict = dict(zip(merge_index['머징코드'], merge_index['세트']))
    merge_adict = dict(zip(merge_index['머징코드'], merge_index['소재']))

    def merge_code(df,merge_raw,metric):

        df = df.loc[df['머징코드'] != 'None']
        merge_raw = merge_raw.loc[merge_raw['머징코드'] != 'None']

        merge_raw = merge_raw.groupby(['날짜', '머징코드'])[metric].sum().reset_index()
        merge_df = pd.merge(df, merge_raw, how='outer', on=['날짜', '머징코드']).fillna(0)

        merge_df.loc[merge_df['캠페인'] == 0,'캠페인'] = merge_df['머징코드'].apply(lambda x: x.replace(x, merge_cdict[x]) if x in merge_cdict.keys() else '-')
        merge_df.loc[merge_df['세트'] == 0, '세트'] = merge_df['머징코드'].apply(lambda x: x.replace(x, merge_gdict[x]) if x in merge_gdict.keys() else '-')
        merge_df.loc[merge_df['소재'] == 0, '소재'] = merge_df['머징코드'].apply(lambda x: x.replace(x, merge_adict[x]) if x in merge_adict.keys() else '-')

        merge_df = merge_df.loc[merge_df['캠페인'] != '-']

        return merge_df

    df = merge_code(media_raw,apps_raw,ref.columns.apps_metric)
    df = merge_code(df, ga_raw, ref.columns.ga_metric)
    df = merge_code(df, ds_raw,ref.columns.ds_raw_metric)

    return df

def merge_indexing() :

    df = media_tracker()

    index = ref.index_df[['지면/상품', '매체', '캠페인 구분', 'KPI', '캠페인 라벨', 'OS', '파트(주체)', '파트 구분', '머징코드']]
    index = ref.index_dup_drop(index,'머징코드')
    merge = pd.merge(df, index, on='머징코드', how='left').fillna('no_index')

    # 예외처리
    merge.loc[merge['지면/상품'].isin(['쇼핑검색_P','쇼핑검색_M']),'구매(GA)'] = merge['구매(대시보드)']
    merge.loc[merge['지면/상품'].isin(['쇼핑검색_P','쇼핑검색_M']),'매출(GA)'] = merge['매출(대시보드)']

    ad_index = ref.index_df[['소재','캠페인(인덱스)','세트(인덱스)','프로모션','브랜드','카테고리','소재형태','소재이미지','소재카피']]
    ad_index = ref.index_dup_drop(ad_index,'소재')
    ad_index = ad_index.loc[ad_index['캠페인(인덱스)'] != ''].fillna('-')

    merge = pd.merge(merge, ad_index, on='소재', how='left').fillna('-')

    merge = ref.week_day(merge)
    merge = merge[ref.columns.report_col]
    # 삭제 필요
    merge[['브랜드구매(GA)', '브랜드매출(GA)','브랜드구매(AF)','브랜드매출(AF)']] = 0

    print('데일리 리포트 추출 완료')

    return merge

merge = merge_indexing()

merge.to_csv(dr.download_dir +f'daily_report/daily_report_{ref.r_date.yearmonth}.csv', index= False , encoding= 'utf-8-sig')
