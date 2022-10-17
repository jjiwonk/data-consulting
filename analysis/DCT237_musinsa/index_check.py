import pandas as pd
from analysis.DCT237_musinsa import info
import re

def campaign_indexing() :
    campaign_list = pd.read_csv(info.raw_dir + '/musinsa_campaign_list.csv', encoding='utf-8-sig')
    campaign_index = pd.read_csv(info.raw_dir + '/campaign_index.csv', encoding='utf-8-sig')
    campaign_list = campaign_list.dropna(subset = ['campaign'], axis=0)
    data = pd.merge(campaign_list, campaign_index, on='campaign', how='left')
    return data

campaign_index = campaign_indexing()
adcode_index = pd.read_csv(info.raw_dir + '/index.csv', encoding='utf-8-sig')

def adcode_indexing(df, depth) :
    campaign_index_null = df.loc[df['계정'].isnull()]
    del campaign_index_null['계정']
    campaign_index_null['광고코드'] = campaign_index_null[depth].apply(lambda x : str(x).split('_')[-1] if str(x).find('_') != -1 else str(x))
    data = pd.merge(campaign_index_null, adcode_index, on='광고코드', how='left')
    return data

adcode_index_campaign = adcode_indexing(campaign_index,'campaign')
adcode_index_adset = adcode_indexing(adcode_index_campaign, 'adset')
adcode_index_ad = adcode_indexing(adcode_index_adset, 'ad')

adcode_index_fnull = adcode_index_ad.loc[adcode_index_ad['계정'].isnull()]
adcode_index_fnull = adcode_index_fnull.drop_duplicates('campaign')
adcode_index_fnull = adcode_index_fnull.sort_values(by=['install', 're-attribution','re-engagement'] ,ascending= True)
index_null_list = adcode_index_fnull['campaign']
index_null_list.to_csv(info.result_dir + '/index_null.csv' , encoding = 'utf-8-sig', index =None)


