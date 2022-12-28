import pandas as pd
from report.SIV import ref
import report.SIV.media_prep as mprep
from report.SIV import directory as dr

def total_media_raw():

    info_df = ref.info_df
    media_list = info_df.loc[info_df['사용 여부'] == 'TRUE']['매체'].to_list()

    func_list = []

    for media in media_list:
        func = f'get_{media}'
        func_list.append(func)

    df = pd.DataFrame()

    for func in func_list:
        media_df = getattr(mprep,func)()
        df = pd.concat([df, media_df])
        df = df.fillna(0)
        df = df.groupby(ref.columns.dimension_cols)[ref.columns.metric_cols].sum().reset_index()
        df = df.sort_values(by = ['날짜', '매체'])
    return df

media_df = total_media_raw()

media_df.to_csv(dr.download_dir +'media_1차.csv', index= False , encoding= 'utf-8-sig')



