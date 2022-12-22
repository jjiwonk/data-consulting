import pandas as pd
from report.innisfree import ref
import report.innisfree.media_preprocess as load
import report.innisfree.tracker_preprocess as tracker_preprocess


def integrate_media_data():
    table = ref.info_df[['사용 여부', '매체']].iloc[1:]
    media_list = table.loc[table['사용 여부']=='TRUE', '매체'].to_list()

    df_list = []

    for media in media_list:
        print(f"{media} merging")

        if media == 'FBIG':
            df = load.fb_prep()
        elif media == 'Google_SA':
            df = load.gg_sa_prep()
        elif media == 'AC_Install':
            df = load.gg_ac_prep()
        elif media == 'Google_PMAX':
            df = load.pmax_prep()
        elif media == 'Kakao_Moment':
            df = load.kkm_prep()
        elif media == 'Kakao_Bizboard':
            df = load.kkbz_prep()
        elif media == 'Naver_SA':
            df = load.nasa_prep()
        elif media == 'Naver_BSA':
            df = load.nabs_prep()
        elif media == 'Apple_SA':
            df = load.asa_prep()
        elif media == 'Criteo':
            df = load.criteo_prep()
        elif media == 'twitter':
            df = load.tw_prep()
        elif media == 'Naver_NOSP':
            df = load.nosp_prep()
        elif media == 'Remerge':
            df = load.remerge_prep()
        elif media == 'RTBhouse':
            df = load.rtb_prep()
        else:
            df = pd.DataFrame()
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df_pivot = total_df.pivot_table(index = ref.columns.dimension_cols, values = ref.columns.metric_cols, aggfunc = 'sum').reset_index()
    total_df_pivot = total_df_pivot[ref.columns.dimension_cols + ref.columns.metric_cols]

    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.metric_cols].values.sum(axis = 1) > 0]

    return total_df_pivot


def index_mapping(df, data_type, source, medium, right_on, index_source) -> pd.DataFrame:
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']
    index_df = ref.index_df.loc[ref.index_df['매체'] == index_source][key_columns + ref.columns.index_columns]

    if data_type == 'media' :
        media_col = '매체'
        metric_cols = ref.columns.metric_cols
        df = df.loc[df[media_col].isin(source)]

    elif data_type == 'apps' :
        media_col = 'media_source'
        source_col = 'sub_param_2'
        metric_cols = ref.columns.apps_metric_columns
        df['ad_detail'] = ''
        df = df.rename(columns={'date': '일자',
                                'campaign': '캠페인',
                                'adset': '광고그룹'})
        if (len(medium) == 1) & (medium[0] == ''):
            df = df.loc[df[media_col].isin(source)]
        else:
            df = df.loc[(df[media_col].isin(source)) & (df[source_col].isin(medium))]

    elif data_type == 'ga' :
        df['source'] = df['소스/매체'].apply(lambda x : x.split(' / ')[0])
        df['medium'] = df['소스/매체'].apply(lambda x : x.split(' / ')[-1])
        media_col = 'source'
        source_col = 'medium'
        metric_cols = ref.columns.ga_metric_cols_kor
        df['ad_detail'] = ''
        df = df.rename(columns = {'날짜' : '일자',
                                  'utm_trg' : '광고그룹',
                                  '광고콘텐츠' : 'ad'})
        if (len(medium) == 1) & (medium[0] == ''):
            df = df.loc[df[media_col].isin(source)]
        else:
            df = df.loc[(df[media_col].isin(source)) & (df[source_col].isin(medium))]

    df['매체'] = index_source

    df_pivot = df.pivot_table(index=ref.columns.dimension_cols, values=metric_cols, aggfunc='sum').reset_index()

    if 'campaign_id' in right_on :
        df_pivot['campaign_id'] = df_pivot['캠페인']
        df_pivot = df_pivot.drop('캠페인', axis = 1)
    if 'group_id' in right_on :
        df_pivot['group_id'] = df_pivot['광고그룹']
        df_pivot = df_pivot.drop('광고그룹', axis=1)


    add_cols = [col for col in key_columns if col not in df_pivot.columns]
    add_cols = list(set(right_on) | set(add_cols))

    index_df_dedup = index_df.drop_duplicates(right_on)[add_cols + ref.columns.index_columns]
    mapping_df = df_pivot.merge(index_df_dedup, how='left', on= right_on)
    mapping_df['정합성 점검용 데이터 소스'] = data_type + ' / ' + str(index_source)
    return mapping_df


def data_merge_test():
    index_source = 'FBIG'

    df = tracker_preprocess.ga_prep()
    data_type = 'ga'
    data_source = ['fbxig']
    right_on = ['campaign_id', 'group_id', 'ad']
    test = index_mapping(df, data_type, data_source, right_on, index_source)

    df = integrate_media_data()
    data_type = 'media'
    data_source = ['facebook']
    right_on = ['캠페인', '광고그룹', 'ad']
    test2 = index_mapping(df, data_type, data_source, right_on, index_source)

    df = tracker_preprocess.apps_log_data_prep()
    data_type = 'apps'
    data_source = ['fbxig']
    right_on = ['campaign_id', 'group_id', 'ad']
    test3 = index_mapping(df, data_type, data_source, right_on, index_source)

    df = tracker_preprocess.get_apps_agg_data()
    data_type = 'apps'
    data_source = ['Facebook Ads']
    right_on = ['캠페인', '광고그룹', 'ad']
    test4 = index_mapping(df, data_type, data_source, right_on, index_source)

    concat_data = pd.concat([test, test2, test3, test4], sort=False, ignore_index=True)

    concat_pivot_index = ['정합성 점검용 데이터 소스'] + ref.columns.dimension_cols + ['campaign_id',
                                                                            'group_id'] + ref.columns.index_columns
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].fillna('')
    concat_data = concat_data.fillna(0)
    concat_data_pivot = concat_data.pivot_table(index=concat_pivot_index, aggfunc='sum').reset_index()
    return concat_data_pivot


def data_merge(merging_info, media_df, apps_df, ga_df):
    media_index = merging_info['index'][0]

    data_type = 'ga'
    source = list(merging_info['source'].drop_duplicates().values)
    medium = list(merging_info['medium'].drop_duplicates().values)
    right_on = ['campaign_id', 'group_id', 'ad']
    mapped_ga = index_mapping(ga_df, data_type, source, medium, right_on, media_index)

    data_type = 'media'
    source = list(merging_info['index'].drop_duplicates().values)
    right_on = ['캠페인', '광고그룹', 'ad']
    mapped_media = index_mapping(media_df, data_type, source, None, right_on, media_index)

    data_type = 'apps'
    source = list(merging_info['apps'].drop_duplicates().values)
    medium = list(merging_info['medium'].drop_duplicates().values)
    right_on = ['campaign_id', 'group_id', 'ad']
    mapped_apps = index_mapping(apps_df, data_type, source, medium, right_on, media_index)

    if media_index == 'FBIG':
        df = tracker_preprocess.get_apps_agg_data()
        data_type = 'apps'
        source = ['Facebook Ads']
        medium = ['']
        right_on = ['캠페인', '광고그룹', 'ad']
        apps_agg = index_mapping(df, data_type, source, medium, right_on, media_index)
        mapped_apps = pd.concat([mapped_apps, apps_agg], sort=False, ignore_index=True)

    concat_data = pd.concat([mapped_ga, mapped_media, mapped_apps], sort=False, ignore_index=True)

    concat_pivot_index = ['정합성 점검용 데이터 소스'] \
                         + ref.columns.dimension_cols + ['campaign_id', 'group_id'] + ref.columns.index_columns
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].fillna('')
    concat_data = concat_data.fillna(0)
    concat_data_pivot = concat_data.pivot_table(index=concat_pivot_index, aggfunc='sum').reset_index()
    return concat_data_pivot

def integrate_data():
    table = ref.info_df[['사용 여부', '매체']].iloc[1:]
    media_list = table.loc[table['사용 여부']=='TRUE', '매체'].to_list()

    apps_pivot_df = tracker_preprocess.apps_log_data_prep()
    ga_pivot_df = tracker_preprocess.ga_prep()

    df_list = []
    for media in media_list:
        print(f"{media} merging")
        merging_info = ref.merging_df.loc[ref.merging_df['index'] == media].reset_index(drop=True)
        if media == 'FBIG':
            df = load.fb_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Google_SA':
            df = load.gg_sa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'AC_Install':
            df = load.gg_ac_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Google_PMAX':
            df = load.pmax_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Kakao_Moment':
            df = load.kkm_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Kakao_Bizboard':
            df = load.kkbz_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_SA':
            df = load.nasa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_BSA':
            df = load.nabs_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Apple_SA':
            df = load.asa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Criteo':
            df = load.criteo_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'twitter':
            df = load.tw_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_NOSP':
            df = load.nosp_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Remerge':
            df = load.remerge_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'RTBhouse':
            df = load.rtb_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        else:
            df = pd.DataFrame()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df = total_df.fillna(0)
    total_df = total_df.loc[total_df[ref.columns.metric_cols
                                     + ref.columns.apps_metric_columns
                                     + ref.columns.ga_metric_cols_kor].values.sum(axis = 1) > 0]

    return total_df
