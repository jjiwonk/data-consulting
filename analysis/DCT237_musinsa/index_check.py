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

def adcode_indexing_regexp(df, depth, pat) :
    campaign_index_null = df.loc[df['계정'].isnull()]
    del campaign_index_null['계정']
    campaign_index_null['광고코드'] = campaign_index_null[depth].apply(lambda x : pat.findall(str(x))[0] if pat.search(str(x)) else x)
    data = pd.merge(campaign_index_null, adcode_index, on='광고코드', how='left')
    return data

pat1 = re.compile('\D{7}\d{3}')
pat2 = re.compile('\D{3}\d{1}\D{3}\d{3}')

adcode_index_campaign = adcode_indexing(campaign_index,'campaign')
adcode_index_campaign = adcode_indexing_regexp(adcode_index_campaign,'campaign', pat1)
adcode_index_campaign = adcode_indexing_regexp(adcode_index_campaign,'campaign', pat2)

adcode_index_adset = adcode_indexing(adcode_index_campaign, 'adset')
adcode_index_adset = adcode_indexing_regexp(adcode_index_adset, 'adset', pat1)
adcode_index_adset = adcode_indexing_regexp(adcode_index_adset, 'adset', pat2)

adcode_index_ad = adcode_indexing(adcode_index_adset, 'ad')
adcode_index_ad = adcode_indexing_regexp(adcode_index_ad, 'ad', pat1)
adcode_index_ad = adcode_indexing_regexp(adcode_index_ad, 'ad', pat2)


adcode_index_fnull = adcode_index_ad.loc[adcode_index_ad['계정'].isnull()]
adcode_index_fnull = adcode_index_fnull.drop_duplicates('campaign')
adcode_index_fnull['conversion'] = adcode_index_fnull[['install', 're-attribution','re-engagement']].values.sum(axis = 1)
adcode_index_fnull = adcode_index_fnull.sort_values(by='conversion' ,ascending= False)

index_null_list = adcode_index_fnull[['media_source','campaign', 'conversion', 'af_purchase']]
index_null_list.to_csv(info.result_dir + '/index_null.csv' , encoding = 'utf-8-sig', index =None)

