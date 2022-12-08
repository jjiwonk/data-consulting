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

        if media == 'facebook':
            df = load.fb_prep()
        elif media == 'google':
            df = load.gg_prep()
        elif media == 'pmax':
            df = load.pmax_prep()
        elif media == 'kakaomoment':
            df = load.kkm_prep()
        elif media == 'naver_sa':
            df = load.nasa_prep()
        elif media == 'naver_bs':
            df = load.nabs_prep()
        elif media == 'ASA':
            df = load.asa_prep()
        elif media == 'criteo':
            df = load.criteo_prep()
        elif media == 'twitter':
            df = load.tw_prep()
        elif media == 'nosp':
            df = load.nosp_prep()
        elif media == 'remerge':
            df = load.remerge_prep()
        elif media == 'rtbhouse':
            df = load.rtb_prep()
        else:
            df = pd.DataFrame()
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df_pivot = total_df.pivot_table(index = ref.columns.dimension_cols, values = ref.columns.metric_cols, aggfunc = 'sum').reset_index()
    total_df_pivot = total_df_pivot[ref.columns.dimension_cols + ref.columns.metric_cols]

    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.metric_cols].values.sum(axis = 1) > 0]

    return total_df_pivot


apps_pivot_df = tracker_preprocess.apps_log_data_prep()
apps_aggregated_df = tracker_preprocess.get_apps_aggregated_log_data()
ga_pivot_df = tracker_preprocess.ga_prep()


def index_mapping(media_df, media_source:list) -> pd.DataFrame:
    index_df = ref.index_df.loc[ref.index_df['매체'].isin(media_source)]
    mapping_df = pd.merge(media_df, index_df, how='left', left_on=['캠페인','광고그룹','ad'], right_on=['캠페인','광고그룹','소재'])
    return mapping_df


def apps_mapping(media_df, media_source:list, index_list, join_way) -> pd.DataFrame:
    apps_df = apps_pivot_df.loc[apps_pivot_df['media_source'].isin(media_source)]
    if len(index_list) == 3:
        apps_index = ['campaign', 'adset', 'ad']
    else:
        apps_index = ['campaign', 'adset']
    mapping_df = pd.merge(media_df, apps_df, how=join_way, left_on=index_list, right_on=apps_index)
    return mapping_df


def ga_mapping(media_df, index_list, media_source:list, join_way) -> pd.DataFrame:
    ga_df = ga_pivot_df.loc[ga_pivot_df['소스/매체'].isin(media_source)]
    mapping_df = pd.merge(media_df, ga_df, how=join_way, on=index_list)
    return mapping_df


def integrate_data():
    table = ref.info_df[['사용 여부', '매체']].iloc[1:]
    media_list = table.loc[table['사용 여부']=='TRUE', '매체'].to_list()

    df_list = []

    for media in media_list:
        print(f"{media} merging")

        if media == 'facebook':
            df = load.fb_prep()
            df = index_mapping(df, ['FBIG'])
            # df.loc[df['매체_y'].isnull(),['캠페인','광고그룹','ad','imp','click','cost(대시보드)']].drop_duplicates().to_excel('C:/Users/MADUP/Downloads/data_check.xlsx', index=False, encoding='utf-8-sig')
            df = apps_mapping(df, ['fbxig'], ['campaign_id', 'group_id', 'ad'], 'outer')
        elif media == 'google':
            df = load.gg_prep()
        elif media == 'pmax':
            df = load.pmax_prep()
        elif media == 'kakaomoment':
            df = load.kkm_prep()
        elif media == 'naver_sa':
            df = load.nasa_prep()
        elif media == 'naver_bs':
            df = load.nabs_prep()
        elif media == 'ASA':
            df = load.asa_prep()
        elif media == 'criteo':
            df = load.criteo_prep()
        elif media == 'twitter':
            df = load.tw_prep()
        elif media == 'nosp':
            df = load.nosp_prep()
        elif media == 'remerge':
            df = load.remerge_prep()
        elif media == 'rtbhouse':
            df = load.rtb_prep()
        else:
            df = pd.DataFrame()
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df_pivot = total_df.pivot_table(index=ref.columns.dimension_cols + ref.columns.index_columns,
                                          values=ref.columns.metric_cols, aggfunc='sum').reset_index()
    total_df_pivot = total_df_pivot[ref.columns.dimension_cols + ref.columns.metric_cols]

    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.metric_cols].values.sum(axis = 1) > 0]

    return total_df_pivot
