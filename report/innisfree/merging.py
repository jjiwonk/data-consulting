import pandas as pd
from report.innisfree import ref
import report.innisfree.media_preprocess as load
import report.innisfree.tracker_preprocess as tracker_preprocess


def integrate_media_data():
    table = ref.info_df[['사용 여부', '소스']].iloc[1:]
    media_list = table.loc[table['사용 여부']=='TRUE', '소스'].to_list()

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
        elif media == 'Naver_GFA':
            df = load.na_gfa_prep()
        elif media == 'Naver_Smartchannel':
            df = load.na_smch_prep()
        elif media == 'Remerge':
            df = load.remerge_prep()
        elif media == 'RTBhouse':
            df = load.rtb_prep()
        else:
            df = pd.DataFrame()
        df_list.append(df)
    # handi_df = load.get_handi_data()
    # df_list.append(handi_df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df_pivot = total_df.pivot_table(index = ref.columns.dimension_cols, values = ref.columns.metric_cols, aggfunc = 'sum').reset_index()
    total_df_pivot = total_df_pivot[ref.columns.dimension_cols + ref.columns.metric_cols]

    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.metric_cols].values.sum(axis = 1) > 0]

    return total_df_pivot


def index_mapping(df, data_type, source, medium, right_on, index_source) -> pd.DataFrame:
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']
    index_df = ref.index_df.loc[ref.index_df['매체'] == index_source][key_columns + ref.columns.index_columns]

    if data_type == 'media':
        media_col = '매체'
        metric_cols = ref.columns.metric_cols
        df = df.loc[df[media_col].isin(source)]

    elif data_type == 'apps':
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

    elif data_type == 'ga':
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
    # mapping_df['정합성 점검용 데이터 소스'] = data_type + ' / ' + str(index_source)
    return mapping_df


def data_merge(merging_info, media_df, apps_df, ga_df, right_on_media=None, right_on_apps=None, right_on_ga=None):
    media_index = merging_info['index'][0]
    if right_on_ga is None:
        right_on_ga = ['campaign_id', 'ad']
    if right_on_media is None:
        right_on_media = ['캠페인', '광고그룹', 'ad']
    if right_on_apps is None:
        right_on_apps = ['campaign_id', 'ad']

    data_type = 'ga'
    source = list(merging_info['source'].drop_duplicates().values)
    medium = list(merging_info['medium'].drop_duplicates().values)
    mapped_ga = index_mapping(ga_df, data_type, source, medium, right_on_ga, media_index)

    data_type = 'media'
    source = list(merging_info['index'].drop_duplicates().values)
    mapped_media = index_mapping(media_df, data_type, source, None, right_on_media, media_index)

    data_type = 'apps'
    source = list(merging_info['apps'].drop_duplicates().values)
    medium = list(merging_info['medium'].drop_duplicates().values)
    mapped_apps = index_mapping(apps_df, data_type, source, medium, right_on_apps, media_index)

    if media_index == 'FBIG':
        df = tracker_preprocess.get_apps_agg_data()
        data_type = 'apps'
        source = ['Facebook Ads']
        medium = ['']
        right_on = ['캠페인', '광고그룹', 'ad']
        apps_agg = index_mapping(df, data_type, source, medium, right_on, media_index)
        mapped_apps = pd.concat([mapped_apps, apps_agg], sort=False, ignore_index=True)

    concat_data = pd.concat([mapped_ga, mapped_media, mapped_apps], sort=False, ignore_index=True)

    concat_pivot_index = ref.columns.dimension_cols + ['campaign_id', 'group_id'] + ref.columns.index_columns
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].fillna('')
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].astype(str)
    concat_data = concat_data.fillna(0)
    concat_data_pivot = concat_data.pivot_table(index=concat_pivot_index, aggfunc='sum').reset_index()
    return concat_data_pivot


def integrate_data():
    media_list = ref.index_df['매체'].drop_duplicates().to_list()

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
            ac_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'AC_Install', '캠페인'].unique().tolist()
            apps_pivot_df = apps_pivot_df.loc[apps_pivot_df['campaign'].isin(ac_camp_list)]
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
            df['source'] = 'google'
            df['medium'] = 'uac'
            df['ad'] = df['group_id']
            df['group_id'] = df['캠페인']
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
            df['source'] = 'apple'
            df['medium'] = 'appstore'
            df['group_id'] = df['campaign_id']
            df['campaign_id'] = 'madit_prospecting_al'
            df['ad'] = 'na'
        elif media == 'Criteo':
            df = load.criteo_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            # 매체 데이터에 ad 추출 안됨, ga 매핑X
            apps_pivot_df['ad'] = ''
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['ad'] = 'na'
            df['medium'] = df['캠페인'].apply(lambda x: 'dpa' if x == 'innisfreekr Dynamic Inapp AOS' else 'da')
        elif media == 'twitter':
            df = load.tw_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_NOSP':
            df = load.nosp_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_GFA':
            df = load.na_gfa_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Naver_Smartchannel':
            df = load.na_smch_prep()
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df)
        elif media == 'Remerge':
            df = load.remerge_prep()
            right_on_media = ['캠페인', 'ad']
            right_on_apps = ['campaign_id', 'group_id']
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['ad'] = df['group_id']
            df['group_id'] = df['campaign_id'].apply(lambda x: 'retotal_aos' if 'aos' in x else 'retotal_ios')
        elif media == 'RTBhouse':
            df = load.rtb_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            # 매체 데이터에 ad 추출 안됨, ga 매핑X
            apps_pivot_df['ad'] = ''
            df = data_merge(merging_info, df, apps_pivot_df, ga_pivot_df, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['medium'] = 'others'
            df['ad'] = 'RTB_dynamic'
        else:
            df = pd.DataFrame(columns=ref.columns.result_columns)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df = total_df.fillna(0)
    total_df = total_df.loc[total_df[ref.columns.metric_cols
                                     + ref.columns.apps_metric_columns
                                     + ref.columns.ga_metric_cols_kor].values.sum(axis = 1) > 0]

    return total_df


def final_prep(df):
    # df = pd.read_csv('C:/Dropbox (주식회사매드업)/광고사업부/4. 광고주/이니스프리/자동화리포트_최종/integrated_report_202301.csv')
    df['일자'] = df['일자'].apply(pd.to_datetime)
    df['Year'] = df['일자'].dt.year
    df['month'] = df['일자'].dt.month
    df['week'] = df['일자'].dt.strftime('%U').apply(pd.to_numeric)
    weekday_list = ['월', '화', '수', '목', '금', '토', '일']
    df['day'] = df['일자'].dt.day.apply(str) + '_' + df['일자'].dt.weekday.apply(lambda x: weekday_list[x])
    df['일자'] = df['일자'].dt.date
    df['cost(0.88)'] = df['cost(정산기준)'] / 0.88
    df['cost(0.90)'] = df['cost(정산기준)'] / 0.90
    df['디바이스'] = df['디바이스'].apply(lambda x: 'none' if x == '' else x)
    df = df.rename(columns=ref.columns.final_dict)
    df = df[ref.columns.final_cols]
    # df.to_excel('C:/Users/MADUP/Downloads/final_integrated_report.xlsx', index=False, encoding='utf-8-sig')

    return df