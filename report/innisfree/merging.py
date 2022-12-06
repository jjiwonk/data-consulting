import pandas as pd
from report.innisfree import ref
import report.innisfree.media_preprocess as load
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

    return total_df_pivot

def apps_mapping(media_df, index_list, media_source, join_way) -> pd.DataFrame:
    apps_df = load.get_data(load.dir.apps_dir, load.cols.apps_cols, 'utf-8-sig')
    apps_df = apps_df.loc[apps_df['media_source'].isin(media_source)]
    mapping_df = pd.merge(media_df, apps_df, how=join_way, on=index_list)
    return mapping_df


def ga_mapping(media_df, index_list, media_source, join_way) -> pd.DataFrame:
    ga_df = load.get_data(load.dir.ga_dir, load.cols.ga_cols, 'utf-8-sig')
    ga_df = ga_df.loc[ga_df['media_source'].isin(media_source)]
    mapping_df = pd.merge(media_df, ga_df, how=join_way, on=index_list)
    return mapping_df


