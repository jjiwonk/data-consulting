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


def index_mapping(df,data_type, data_source, right_on, index_source) -> pd.DataFrame:
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']
    index_df = ref.index_df.loc[ref.index_df['매체'] == index_source][key_columns + ref.columns.index_columns]

    if data_type == 'media' :
        media_col = '매체'
        metric_cols = ref.columns.metric_cols

    elif data_type == 'apps' :
        media_col = 'media_source'
        metric_cols = ref.columns.apps_metric_columns
        df['ad_detail'] = ''
        df = df.rename(columns={'date': '일자',
                                'campaign': '캠페인',
                                'adset': '광고그룹'})

    elif data_type == 'ga' :
        df['source'] = df['소스/매체'].apply(lambda x : x.split(' / ')[0])
        df['medium'] = df['소스/매체'].apply(lambda x : x.split(' / ')[-1])
        media_col = 'source'
        metric_cols = ref.columns.ga_metric_cols_kor
        df['ad_detail'] = ''
        df = df.rename(columns = {'날짜' : '일자',
                                  'utm_trg' : '광고그룹',
                                  '광고콘텐츠' : 'ad'})

    df = df.loc[df[media_col].isin(data_source)]
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
    mapping_df['정합성 점검용 데이터 소스'] = data_type + ' / ' + str(data_source)
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
    index_source = merging_info['index']

    data_type = 'ga'
    data_source = merging_info['ga'].split(',')
    right_on = ['campaign_id', 'group_id', 'ad']
    mapped_ga = index_mapping(ga_df, data_type, data_source, right_on, index_source)

    data_type = 'media'
    data_source = merging_info['매체통합raw'].split(',')
    right_on = ['캠페인', '광고그룹', 'ad']
    mapped_media = index_mapping(media_df, data_type, data_source, right_on, index_source)

    data_type = 'apps'
    data_source = merging_info['apps'].split(',')
    right_on = ['campaign_id', 'group_id', 'ad']
    mapped_apps = index_mapping(apps_df, data_type, data_source, right_on, index_source)

    if index_source == 'FBIG':
        df = tracker_preprocess.get_apps_agg_data()
        data_type = 'apps'
        data_source = merging_info['apps_agg'].split(',')
        right_on = ['캠페인', '광고그룹', 'ad']
        apps_agg = index_mapping(df, data_type, data_source, right_on, index_source)
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
        merging_info = ref.merging_df.loc[ref.merging_df['매체통합raw'] == media].to_dict('records')[0]
        if media == 'facebook':
            df = load.fb_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'google':
            df = load.gg_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'pmax':
            df = load.pmax_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'kakaomoment':
            df = load.kkm_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'naver_sa':
            df = load.nasa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'naver_bs':
            df = load.nabs_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'ASA':
            df = load.asa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'criteo':
            df = load.criteo_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'twitter':
            df = load.tw_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'nosp':
            df = load.nosp_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'remerge':
            df = load.remerge_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'rtbhouse':
            df = load.rtb_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        else:
            df = pd.DataFrame()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df = total_df.loc[total_df[ref.columns.metric_cols
                                     + ref.columns.apps_metric_columns
                                     + ref.columns.ga_metric_cols_kor].values.sum(axis = 1) > 0]

    return total_df
