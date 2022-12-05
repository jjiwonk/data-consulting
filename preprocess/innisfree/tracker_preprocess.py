import pandas as pd
import preprocess.innisfree.data_loading as load


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


