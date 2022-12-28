from report.SIV import directory as dr
from report.SIV import ref

import pandas as pd

def media_raw_read(media):
    info_dict = ref.info_dict[media]
    use_col = list(info_dict['dimension'].values()) + list(info_dict['metric'].values()) + list(info_dict['temp'].values())
    df = pd.read_csv( dr.report_dir + info_dict['read']['경로'] +'/'+ info_dict['read']['파일명'] + info_dict['read']['suffix'] ,usecols= use_col)

    df = df.rename(columns = { v: k for k, v in info_dict['dimension'].items()})
    df = df.rename(columns= { v: k for k, v in info_dict['metric'].items()})
    df = df.rename(columns= { v: k for k, v in info_dict['temp'].items()})

    for col in info_dict['dimension'].keys():
        df[col] = df[col].fillna('-')
        df[col] = df[col].astype(str)

    for col in info_dict['metric'].keys():
        df[col] = df[col].fillna(0)
        df[col] = df[col].astype(float)

    df['매체'] = media

    return df

google_media = ['Pmax','AC','GDN','YT_인피드','YT_인스트림','구글SA']

def cost_calc(media, df):
    cal_dict = ref.info_dict[media]['prep']
    if media in google_media:
        df['비용'] = df['비용']/1000000

    df['SPEND_AGENCY'] = df['비용'] * float(cal_dict['곱하기'])
    df['SPEND_AGENCY'] = df['SPEND_AGENCY'] / float(cal_dict['나누기'])
    return df

def media_prep(media):
    df = media_raw_read(media)
    df2 = cost_calc(media,df)
    return df2

def campaign_name(media):
    campaign = [k for k, v in ref.rule_dict_p.items() if v == media]
    return campaign

#def campaign_name(media, df):
    #campaign = [k for k, v in ref.rule_dict_f.items() if v == media]
    #df['cp'] = df['캠페인'].apply(lambda x: x.split('_')[-1])
    #df = df.loc[df['cp'].isin(campaign)]
    #df = df.drop(columns = 'cp')
    #return df

#머징 함수를 위해서 함수명은 무조건 매체 명과 일치하게 지정
#매체 별로 ! 예외 처리는 요기다가

def get_FBIG():
    df = media_prep('FBIG')
    return df

def get_kakaomoment():
    df = media_prep('kakaomoment')
    return df

def get_kakaoSA():
    df = media_prep('kakaoSA')
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_Pmax():
    df = media_prep('Pmax')
    df['세트'] = df['캠페인']
    df['소재'] = df['캠페인']
    c = campaign_name('Pmax')
    df = df.loc[df['캠페인'].isin(c)]
    #df = campaign_name('Pmax',df)
    return df

def get_AC():
    df = media_prep('AC')
    c = campaign_name('AC')
    df = df.loc[df['캠페인'].isin(c)]
    # df = campaign_name('AC',df)
    return df

def get_GDN():
    df = media_prep('GDN')
    c = campaign_name('GDN')
    df = df.loc[df['캠페인'].isin(c)]
    # df = campaign_name('GDN',df)
    return df

def get_YT_인피드():
    df = media_prep('YT_인피드')
    c = campaign_name('YT_인피드')
    df = df.loc[df['캠페인'].isin(c)]
    # df = campaign_name('YT_인피드',df)
    return df

def get_YT_인스트림():
    df = media_prep('YT_인스트림')
    c = campaign_name('YT_인스트림')
    df = df.loc[df['캠페인'].isin(c)]
    # df = campaign_name('YT_인스트림',df)
    return df

def get_구글SA():
    df = media_prep('구글SA')
    c = campaign_name('구글SA')
    df = df.loc[df['캠페인'].isin(c)]
    # df = campaign_name('구글SA',df)
    df['세트'] = '-'
    df['소재'] = '-'
    return df

def get_네이버SA():
    df = media_prep('네이버SA')
    df['세트'] = '-'
    df['소재'] = '-'
    return df













