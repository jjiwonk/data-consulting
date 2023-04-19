import pandas as pd
from report.innisfree import ref
import report.innisfree.media_preprocess as load
import report.innisfree.tracker_preprocess as tracker_preprocess


def integrate_media_data():
    media_list = ref.index_df['매체(표기)'].drop_duplicates().to_list()

    df_list = []

    for media in media_list:
        print(f"{media} merging")

        if media == 'FBIG':
            df = load.fb_prep()
        elif media == 'FBIG_DPA':
            df = load.fb_dpa_prep()
        elif media == 'Google_SA':
            df = load.gg_sa_prep()
        elif media == 'AC_Install':
            df = load.gg_ac_prep()
        elif media == 'Google_PMAX':
            df = load.pmax_prep()
        elif media == 'Google_discovery':
            df = load.gg_discovery_prep()
        elif media == 'Google_youtube':
            df = load.gg_youtube_prep()
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
        elif media == 'Twitter':
            df = load.tw_prep()
        elif media == 'Naver_NOSP':
            df = load.nosp_prep()
        elif media == 'Naver_스페셜DA':
            df = load.na_spda_prep()
        elif media == 'Naver_헤드라인DA':
            df = load.na_hdda_prep()
        elif media == 'Naver_쇼핑라이브DA':
            df = load.na_shda_prep()
        elif media == 'Naver_롤링보드':
            df = load.na_rolling_prep()
        elif media == 'SNOW':
            df = load.na_snow_prep()
        elif media == 'Naver_GFA':
            df = load.na_gfa_prep()
        elif media == 'Naver_스마트채널':
            df = load.na_smch_prep()
        elif media == 'Naver_스마트채널_보장형':
            df = load.na_smch_nosp_prep()
        elif media == 'Naver_DPA':
            df = load.na_dpa_prep()
        elif media == 'Naver_쇼핑알람':
            df = load.na_shoppingalarm_prep()
        elif media == 'Remerge':
            df = load.remerge_prep()
        elif media == 'RTBhouse':
            df = load.rtb_prep()
        elif media == 'Tiktok':
            df = load.tiktok_prep()
        else:
            df = load.get_handi_data(media)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df_pivot = total_df.pivot_table(index = ref.columns.dimension_cols, values = ref.columns.metric_cols, aggfunc = 'sum').reset_index()
    total_df_pivot = total_df_pivot[ref.columns.dimension_cols + ref.columns.metric_cols]

    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.metric_cols].values.sum(axis = 1) > 0]

    return total_df_pivot


def index_mapping(df, data_type, source, medium, right_on, index_source, index_df, for_checking) -> pd.DataFrame:
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']

    if data_type == 'media':
        metric_cols = ref.columns.metric_cols
        df['raw_source'] = ''
        df['raw_medium'] = ''
        df['매체'] = index_source
        if for_checking is True:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols + ['raw_source', 'raw_medium'], values=metric_cols, aggfunc='sum').reset_index()
        else:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols, values=metric_cols, aggfunc='sum').reset_index()

    elif data_type == 'apps':
        df = df.rename(columns={'media_source':'raw_source', 'sub_param_2':'raw_medium'})
        source_col = 'raw_source'
        medium_col = 'raw_medium'
        metric_cols = ref.columns.apps_metric_columns
        df['ad_detail'] = ''
        df = df.rename(columns={'date': '일자',
                                'campaign': '캠페인',
                                'adset': '광고그룹'})
        if (len(medium) == 1) & (medium[0] == ''):
            df = df.loc[df[source_col].isin(source)]
        else:
            df = df.loc[(df[source_col].isin(source)) & (df[medium_col].isin(medium))]
        df['매체'] = index_source
        if for_checking is True:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols + ['raw_source', 'raw_medium'], values=metric_cols, aggfunc='sum').reset_index()
        else:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols, values=metric_cols, aggfunc='sum').reset_index()

    elif data_type == 'ga':
        df['raw_source'] = df['소스/매체'].apply(lambda x : x.split(' / ')[0])
        df['raw_medium'] = df['소스/매체'].apply(lambda x : x.split(' / ')[-1])
        source_col = 'raw_source'
        medium_col = 'raw_medium'
        metric_cols = ref.columns.ga_metric_cols_kor
        df['ad_detail'] = ''
        df = df.rename(columns = {'날짜' : '일자',
                                  'utm_trg' : '광고그룹',
                                  '광고콘텐츠' : 'ad'})
        if (len(medium) == 1) & (medium[0] == ''):
            df = df.loc[df[source_col].isin(source)]
        else:
            df = df.loc[(df[source_col].isin(source)) & (df[medium_col].isin(medium))]
        df['매체'] = index_source
        if for_checking is True:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols + ['raw_source', 'raw_medium'], values=metric_cols, aggfunc='sum').reset_index()
        else:
            df_pivot = df.pivot_table(index=ref.columns.dimension_cols, values=metric_cols, aggfunc='sum').reset_index()

    else:
        metric_cols = []
        df['매체'] = index_source
        df_pivot = df.pivot_table(index=ref.columns.dimension_cols, values=metric_cols, aggfunc='sum').reset_index()

    if 'campaign_id' in right_on:
        df_pivot['campaign_id'] = df_pivot['캠페인']
        df_pivot = df_pivot.drop('캠페인', axis = 1)
        df_pivot['group_id'] = df_pivot['광고그룹']
        df_pivot = df_pivot.drop('광고그룹', axis=1)
    elif 'group_id' in right_on:
        df_pivot['group_id'] = df_pivot['광고그룹']
        df_pivot = df_pivot.drop('광고그룹', axis=1)

    add_cols = [col for col in key_columns if col not in df_pivot.columns]
    add_cols = list(set(right_on) | set(add_cols))

    index_df_dedup = index_df.drop_duplicates(right_on)[add_cols + ref.columns.index_columns]
    mapping_df = df_pivot.merge(index_df_dedup, how='left', on= right_on)
    mapping_df['정합성 점검용 데이터 소스'] = data_type + ' / ' + str(index_source)
    return mapping_df


def data_merge(merging_info, media_df, apps_df, ga_df, index_df, for_checking, right_on_media=None, right_on_apps=None, right_on_ga=None):
    media_index = merging_info['index'][0]
    if right_on_ga is None:
        right_on_ga = ['campaign_id', 'ad']
    if right_on_media is None:
        right_on_media = ['캠페인', '광고그룹', 'ad']
    if right_on_apps is None:
        right_on_apps = ['campaign_id', 'ad']

    data_type = 'ga'
    source = list(merging_info['ga_source'].drop_duplicates().values)
    medium = list(merging_info['ga_medium'].drop_duplicates().values)
    mapped_ga = index_mapping(ga_df, data_type, source, medium, right_on_ga, media_index, index_df, for_checking)
    mapped_ga['merging_on'] = ','.join(right_on_ga)

    data_type = 'media'
    source = list(merging_info['index'].drop_duplicates().values)
    mapped_media = index_mapping(media_df, data_type, source, None, right_on_media, media_index, index_df, for_checking)
    mapped_media['merging_on'] = ','.join(right_on_media)

    data_type = 'apps'
    source = list(merging_info['apps_source'].drop_duplicates().values)
    medium = list(merging_info['apps_medium'].drop_duplicates().values)
    mapped_apps = index_mapping(apps_df, data_type, source, medium, right_on_apps, media_index, index_df, for_checking)
    mapped_apps['merging_on'] = ','.join(right_on_apps)

    if media_index in ['FBIG', 'FBIG_DPA']:
        df = tracker_preprocess.get_apps_agg_data()
        df['sub_param_2'] = ''
        data_type = 'apps'
        source = ['Facebook Ads']
        medium = ['']
        right_on = ['캠페인', '광고그룹', 'ad']
        if media_index == 'FBIG':
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'FBIG_DPA', '캠페인'].unique().tolist()
            df = df.loc[~(df['campaign'].isin(camp_list))]
            df.loc[df['ad'] == '0209_pm_bigsale_na_da_prd_mix_slide - 사본', 'ad'] = '0209_pm_bigsale_na_da_prd_mix_slide'
        elif media_index == 'FBIG_DPA':
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'FBIG', '캠페인'].unique().tolist()
            df = df.loc[~(df['campaign'].isin(camp_list))]
        apps_agg = index_mapping(df, data_type, source, medium, right_on, media_index, index_df, for_checking)
        mapped_apps = pd.concat([mapped_apps, apps_agg], sort=False, ignore_index=True)
        mapped_apps['merging_on'] = ','.join(right_on)

    concat_data = pd.concat([mapped_ga, mapped_media, mapped_apps], sort=False, ignore_index=True)

    if for_checking is True:
        concat_pivot_index = ['정합성 점검용 데이터 소스', 'merging_on'] + ref.columns.dimension_cols + ['campaign_id', 'group_id', 'raw_source', 'raw_medium'] + ref.columns.index_columns
    else:
        concat_pivot_index = ref.columns.dimension_cols + ['campaign_id', 'group_id'] + ref.columns.index_columns
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].fillna('')
    concat_data[concat_pivot_index] = concat_data[concat_pivot_index].astype(str)
    concat_data = concat_data.fillna(0)
    concat_data_pivot = concat_data.pivot_table(index=concat_pivot_index, aggfunc='sum').reset_index()
    return concat_data_pivot


def integrate_data():
    media_list = ref.index_df['매체(표기)'].drop_duplicates().to_list()

    apps_pivot_df = tracker_preprocess.apps_log_data_prep()
    ga_pivot_df = tracker_preprocess.ga_prep()
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']

    df_list = []
    for media in media_list:
        print(f"{media} merging")
        merging_info = ref.merging_df.loc[ref.merging_df['index'] == media].reset_index(drop=True)
        index_df = ref.index_df.loc[ref.index_df['매체(표기)'] == media][key_columns + ref.columns.index_columns]
        temp = index_df.copy()
        temp['group_id'] = ''
        temp['광고그룹'] = ''
        apps_df = apps_pivot_df.copy()
        ga_df = ga_pivot_df.copy()
        if media == 'FBIG':
            df = load.fb_prep()
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            right_on_apps = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            # 예외처리
            df.loc[df['ad'] == '0209_pm_bigsale_na_da_prd_mix_slide - 사본', 'ad'] = '0209_pm_bigsale_na_da_prd_mix_slide'
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga,
                            right_on_apps=right_on_apps)
        elif media == 'FBIG_DPA':
            df = load.fb_dpa_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Google_SA':
            df = load.gg_sa_prep()
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            ga_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[ga_df['캠페인'].isin(ga_camp_list)]
            # GA 예외처리
            ga_df['광고콘텐츠'] = ''
            ga_df.loc[ga_df['utm_trg'] == '', 'utm_trg'] = 'brand'
            ga_df.loc[ga_df['캠페인'] == 'madit_conversion_a_brdl', '캠페인'] = 'madit_conversion_al_brd'
            ga_df.loc[ga_df['캠페인'] == 'madit_conversion_a_brd_testl', '캠페인'] = 'madit_conversion_al_brd_test'
            apps_empty_df = pd.DataFrame(columns=apps_df.columns)
            right_on_apps = ['campaign_id']
            right_on_ga = ['campaign_id']
            index_df_pc = index_df.loc[index_df['디바이스'] == 'pc']
            ga_df_pc = ga_df.loc[ga_df['기기 카테고리'] == 'PC']
            pc_camp_list = index_df_pc.캠페인.unique().tolist()
            df_pc = df.loc[df['캠페인'].isin(pc_camp_list)]
            df_pc = data_merge(merging_info, df_pc, apps_empty_df, ga_df_pc, index_df_pc, False, right_on_ga=right_on_ga, right_on_apps=right_on_apps)
            index_df_mo = index_df.loc[index_df['디바이스'] == 'mo']
            ga_df_mo = ga_df.loc[ga_df['기기 카테고리'] == 'Mobile']
            mo_camp_list = index_df_mo.캠페인.unique().tolist()
            df_mo = df.loc[df['캠페인'].isin(mo_camp_list)]
            df_mo = data_merge(merging_info, df_mo, apps_df, ga_df_mo, index_df_mo, False, right_on_ga=right_on_ga, right_on_apps=right_on_apps)
            df = pd.concat([df_pc, df_mo], sort=False, ignore_index=True).drop_duplicates(ignore_index=True)
            df['광고그룹'] = df['group_id']
        elif media == 'AC_Install':
            df = load.gg_ac_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['group_id']
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            ga_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[ga_df['캠페인'].isin(ga_camp_list)]
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['source'] = 'google'
            df['medium'] = 'uac'
            df['ad'] = df['group_id']
            df['group_id'] = df['캠페인']
        elif media == 'Google_PMAX':
            df = load.pmax_prep()
            right_on_media = ['캠페인']
            right_on_apps = ['campaign_id']
            right_on_ga = ['campaign_id', 'group_id']
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False,
                            right_on_media=right_on_media, right_on_apps=right_on_apps, right_on_ga=right_on_ga)
        elif media == 'Google_discovery':
            df = load.gg_discovery_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media,
                            right_on_apps=right_on_apps)
        elif media == 'Google_youtube':
            df = load.gg_youtube_prep()
            df['ad'] = df['광고그룹']
            right_on_media = ['캠페인', '광고그룹']
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df['ad'] = index_df['광고그룹']
            index_df = pd.concat([index_df, temp])
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media,
                            right_on_ga=right_on_ga)
        elif media == 'ACe':
            df = load.gg_ace_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['group_id']
            apps_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[apps_df['campaign'].isin(apps_camp_list)]
            ga_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[ga_df['캠페인'].isin(ga_camp_list)]
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['source'] = 'google'
            df['medium'] = 'uac'
            df['ad'] = df['group_id']
            df['group_id'] = df['캠페인']
        elif media == 'Kakao_Moment':
            df = load.kkm_prep()
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga)
        elif media == 'Kakao_Bizboard':
            df = load.kkbz_prep()
            # 비즈보드 예외처리
            df.loc[df['ad'] == '0201_al_na_biz_da_prd_suncream_winterfac', 'ad'] = '0201_al_na_biz_da_prd_suncream_winterface'
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga)
        elif media == 'Naver_SA':
            # 사용X 보류
            df = load.nasa_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_BSA':
            df = load.nabs_prep()
            ga_df['medium'] = ga_df['소스/매체'].apply(lambda x : x.split(' / ')[-1])
            index_df_pc = index_df.loc[index_df['디바이스'] == 'pc']
            medium_pc = ['sabrdweb']
            ga_df_pc = ga_df.loc[ga_df['medium'].isin(medium_pc)]
            apps_df_pc = apps_df.loc[apps_df['sub_param_2'].isin(medium_pc)]
            pc_camp_list = index_df_pc.캠페인.unique().tolist()
            df_pc = df.loc[df['캠페인'].isin(pc_camp_list)]
            df_pc = data_merge(merging_info, df_pc, apps_df_pc, ga_df_pc, index_df_pc, False)
            index_df_mo = index_df.loc[index_df['디바이스'] == 'mo']
            medium_mo = ['sabrdmobile']
            ga_df_mo = ga_df.loc[ga_df['medium'].isin(medium_mo)]
            apps_df_mo = apps_df.loc[apps_df['sub_param_2'].isin(medium_mo)]
            mo_camp_list = index_df_mo.캠페인.unique().tolist()
            df_mo = df.loc[df['캠페인'].isin(mo_camp_list)]
            df_mo = data_merge(merging_info, df_mo, apps_df_mo, ga_df_mo, index_df_mo, False)
            df = pd.concat([df_pc, df_mo], sort=False, ignore_index=True).drop_duplicates(ignore_index=True)
        elif media == 'Apple_SA':
            df = load.asa_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
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
            apps_df['ad'] = ''
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['ad'] = 'na'
            df['medium'] = df['캠페인'].apply(lambda x: 'dpa' if x == 'innisfreekr Dynamic Inapp AOS' else 'da')
        elif media == 'Twitter':
            df = load.tw_prep()
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga)
        elif media == 'Naver_NOSP':
            df = load.nosp_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'SNOW':
            df = load.na_snow_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_스페셜DA':
            df = load.na_spda_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_헤드라인DA':
            df = load.na_hdda_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_쇼핑라이브DA':
            df = load.na_shda_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_롤링보드':
            df = load.na_rolling_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_날씨':
            df = load.na_weather_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_GFA':
            df = load.na_gfa_prep()
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga)
        elif media == 'Naver_스마트채널':
            df = load.na_smch_prep()
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_ga=right_on_ga)
        elif media == 'Naver_스마트채널_보장형':
            df = load.na_smch_nosp_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_DPA':
            df = load.na_dpa_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Naver_쇼핑알람':
            df = load.na_shoppingalarm_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        elif media == 'Remerge':
            df = load.remerge_prep()
            right_on_media = ['캠페인', 'ad']
            right_on_apps = ['캠페인', 'ad']
            apps_df['ad'] = apps_df['adset']
            apps_df['adset'] = ''
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['광고그룹'] = df['캠페인'].apply(lambda x: 'retotal_aos' if 'aos' in x else 'retotal_ios')
        elif media == 'RTBhouse':
            df = load.rtb_prep()
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            # 매체 데이터에 ad 추출 안됨, ga 매핑X
            apps_df['ad'] = ''
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['medium'] = 'others'
            df['ad'] = 'RTB_dynamic'
        elif media == 'Tiktok':
            df = load.tiktok_prep()
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        else:
            df = load.get_handi_data(media)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, False)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df = total_df.fillna(0)
    total_df = total_df.loc[total_df[ref.columns.metric_cols
                                     + ref.columns.apps_metric_columns
                                     + ref.columns.ga_metric_cols_kor].values.sum(axis = 1) > 0]

    return total_df


def final_prep(df, for_checking):
    df['일자'] = df['일자'].apply(pd.to_datetime)
    df['Year'] = df['일자'].dt.year
    df['month'] = df['일자'].dt.month
    df['week'] = df['일자'].dt.strftime('%U').apply(pd.to_numeric)
    weekday_list = ['월', '화', '수', '목', '금', '토', '일']
    df['day'] = df['일자'].dt.weekday.apply(lambda x: str(x+1)) + '_' + df['일자'].dt.weekday.apply(lambda x: weekday_list[x])
    df['일자'] = df['일자'].dt.date
    df['cost(0.88)'] = df['cost(정산기준)'] / 0.88
    df['cost(0.90)'] = df['cost(정산기준)'] / 0.90
    df['디바이스'] = df['디바이스'].apply(lambda x: 'none' if x == '' else x)
    df['Promotion Name'] = df['Promotion Name'].apply(lambda x: x if (x == 'na')|(x == '') else 'pm')
    df = df.rename(columns=ref.columns.final_dict)
    if for_checking is True:
        df = df[['정합성 점검용 데이터 소스'] + ref.columns.final_cols]
    else:
        df = df[ref.columns.final_cols]

    return df


def get_no_index_data():
    media_list = ref.index_df[['매체', '매체(표기)']].drop_duplicates().reset_index(drop=True)

    apps_pivot_df = tracker_preprocess.apps_log_data_prep()
    ga_pivot_df = tracker_preprocess.ga_prep()
    key_columns = ['캠페인', 'campaign_id', '광고그룹', 'group_id', 'ad']

    df_list = []
    for index, row in media_list.iterrows():
        source = row['매체']
        media = row['매체(표기)']
        print(f"{media} merging")
        merging_info = ref.merging_df.loc[ref.merging_df['index'] == media].reset_index(drop=True)
        index_df = ref.index_df.loc[ref.index_df['매체'] == source][['매체(표기)'] + key_columns + ref.columns.index_columns]
        temp = index_df.copy()
        temp['group_id'] = ''
        apps_df = apps_pivot_df.copy()
        ga_df = ga_pivot_df.copy()
        if media == 'FBIG':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # 예외처리
            df.loc[df['ad'] == '0209_pm_bigsale_na_da_prd_mix_slide - 사본', 'ad'] = '0209_pm_bigsale_na_da_prd_mix_slide'
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            index_df = pd.concat([index_df, temp])
            df['매체'] = media
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'FBIG_DPA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df['매체'] = media
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Google_SA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # Google_PMAX 캠페인 임의 제외 예외처리
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            except_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            # 예외처리
            ga_df['광고콘텐츠'] = ''
            ga_df.loc[ga_df['utm_trg'] == '', 'utm_trg'] = 'brand'
            ga_df.loc[ga_df['캠페인'] == 'madit_conversion_a_brdl', '캠페인'] = 'madit_conversion_al_brd'
            ga_df.loc[ga_df['캠페인'] == 'madit_conversion_a_brd_testl', '캠페인'] = 'madit_conversion_al_brd_test'
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            apps_empty_df = pd.DataFrame(columns=apps_df.columns)
            right_on_apps = ['campaign_id']
            right_on_ga = ['campaign_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df_pc = index_df.loc[index_df['디바이스'] == 'pc']
            ga_df_pc = ga_df.loc[ga_df['기기 카테고리'] == 'PC']
            pc_camp_list = index_df_pc.캠페인.unique().tolist()
            df_pc = df.loc[df['캠페인'].isin(pc_camp_list)]
            df_pc = data_merge(merging_info, df_pc, apps_empty_df, ga_df_pc, index_df_pc, True, right_on_ga=right_on_ga,
                               right_on_apps=right_on_apps)
            index_df_mo = index_df.loc[index_df['디바이스'] == 'mo']
            ga_df_mo = ga_df.loc[ga_df['기기 카테고리'] == 'Mobile']
            mo_camp_list = index_df_mo.캠페인.unique().tolist()
            df_mo = df.loc[df['캠페인'].isin(mo_camp_list)]
            df_mo = data_merge(merging_info, df_mo, apps_df, ga_df_mo, index_df_mo, True, right_on_ga=right_on_ga,
                               right_on_apps=right_on_apps)
            df = pd.concat([df_pc, df_mo], sort=False, ignore_index=True).drop_duplicates(ignore_index=True)
        elif media == 'AC_Install':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # Google_PMAX 캠페인 임의 제외 예외처리
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            except_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['group_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media, right_on_apps=right_on_apps)
        elif media == 'Google_PMAX':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            # AC_Install, Google_SA 캠페인 임의 제외 예외처리
            except_camp_list = ref.index_df.loc[ref.index_df['매체'] == 'Google_SA', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            right_on_media = ['캠페인']
            right_on_apps = ['campaign_id']
            right_on_ga = ['campaign_id', 'group_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True,
                            right_on_media=right_on_media, right_on_apps=right_on_apps, right_on_ga=right_on_ga)
        elif media == 'Google_discovery':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # Google_PMAX 캠페인 임의 제외 예외처리
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media,
                            right_on_apps=right_on_apps)
        elif media == 'Google_youtube':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # Google_PMAX 캠페인 임의 제외 예외처리
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['ad'] = df['광고그룹']
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media,
                            right_on_apps=right_on_apps)
        elif media == 'ACe':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # Google_PMAX 캠페인 임의 제외 예외처리
            camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(camp_list))]
            except_camp_list = ref.index_df.loc[ref.index_df['매체(표기)'] == 'Google_PMAX', 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            apps_df = apps_df.loc[~(apps_df['campaign'].isin(except_camp_list))]
            except_camp_list = index_df.loc[index_df['매체(표기)'] != media, 'campaign_id'].unique().tolist()
            ga_df = ga_df.loc[~(ga_df['캠페인'].isin(except_camp_list))]
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['group_id']
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media, right_on_apps=right_on_apps)
        elif media == 'Kakao_Moment':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df = pd.concat([index_df, temp])
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'Kakao_Bizboard':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            # 비즈보드 예외처리
            df.loc[df['ad'] == '0201_al_na_biz_da_prd_suncream_winterfac', 'ad'] = '0201_al_na_biz_da_prd_suncream_winterface'
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df = pd.concat([index_df, temp])
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'Naver_SA':
            # 사용X 보류
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_BSA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            ga_df['medium'] = ga_df['소스/매체'].apply(lambda x : x.split(' / ')[-1])
            index_df_pc = index_df.loc[index_df['디바이스'] == 'pc']
            medium_pc = ['sabrdweb']
            ga_df_pc = ga_df.loc[ga_df['medium'].isin(medium_pc)]
            apps_df_pc = apps_df.loc[apps_df['sub_param_2'].isin(medium_pc)]
            pc_camp_list = index_df_pc.캠페인.unique().tolist()
            df_pc = df.loc[df['캠페인'].isin(pc_camp_list)]
            df_pc = data_merge(merging_info, df_pc, apps_df_pc, ga_df_pc, index_df_pc, True)
            index_df_mo = index_df.loc[index_df['디바이스'] == 'mo']
            medium_mo = ['sabrdmobile']
            ga_df_mo = ga_df.loc[ga_df['medium'].isin(medium_mo)]
            apps_df_mo = apps_df.loc[apps_df['sub_param_2'].isin(medium_mo)]
            mo_camp_list = index_df_mo.캠페인.unique().tolist()
            df_mo = df.loc[df['캠페인'].isin(mo_camp_list)]
            df_mo = data_merge(merging_info, df_mo, apps_df_mo, ga_df_mo, index_df_mo, True)
            df = pd.concat([df_pc, df_mo], sort=False, ignore_index=True).drop_duplicates(ignore_index=True)
        elif media == 'Apple_SA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Criteo':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            # 매체 데이터에 ad 추출 안됨, ga 매핑X
            apps_df['ad'] = ''
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media, right_on_apps=right_on_apps)
            df['ad'] = 'na'
            df['medium'] = df['캠페인'].apply(lambda x: 'dpa' if x == 'innisfreekr Dynamic Inapp AOS' else 'da')
        elif media == 'Twitter':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df = pd.concat([index_df, temp])
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'Naver_NOSP':
            # Naver_BAS로 데이터 모두 들어가는 중
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'SNOW':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['ad'] = df['ad'].apply(lambda x: str(x)[9:])
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_스페셜DA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['ad'] = df['ad'].apply(lambda x: str(x)[9:])
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_헤드라인DA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_쇼핑라이브DA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_롤링보드':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_날씨':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_GFA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df = pd.concat([index_df, temp])
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'Naver_스마트채널':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            index_df = pd.concat([index_df, temp])
            right_on_ga = ['campaign_id', 'group_id', 'ad']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_ga=right_on_ga)
        elif media == 'Naver_스마트채널_보장형':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_DPA':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Naver_쇼핑알람':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        elif media == 'Remerge':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            right_on_media = ['캠페인', 'ad']
            right_on_apps = ['캠페인', 'ad']
            apps_df['ad'] = apps_df['adset']
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media, right_on_apps=right_on_apps)
        elif media == 'RTBhouse':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            right_on_media = ['캠페인', '광고그룹']
            right_on_apps = ['campaign_id', 'group_id']
            # 매체 데이터에 ad 추출 안됨, ga 매핑X
            apps_df['ad'] = ''
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True, right_on_media=right_on_media, right_on_apps=right_on_apps)
        elif media == 'TikTok':
            df = load.get_basic_data(source)
            camp_list = index_df.loc[index_df['매체(표기)'] != media, '캠페인'].unique().tolist()
            df = df.loc[~(df['캠페인'].isin(camp_list))]
            df['매체'] = media
            index_df = index_df.loc[index_df['매체(표기)'] == media].reset_index(drop=True)
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        else:
            df = load.get_handi_data(media)
            df['매체'] = media
            df = data_merge(merging_info, df, apps_df, ga_df, index_df, True)
        df_list.append(df)

    total_df = pd.concat(df_list, sort=False, ignore_index=True)
    total_df[['스마트스토어sessions', '스마트스토어결제수(마지막클릭)', '스마트스토어결제금액(마지막클릭)']] = 0
    total_df = total_df.fillna(0)
    total_df = total_df.loc[total_df[ref.columns.metric_cols
                                     + ref.columns.apps_metric_columns
                                     + ref.columns.ga_metric_cols_kor].values.sum(axis = 1) > 0]
    total_df = total_df.loc[total_df['Part'] == '']

    return total_df

