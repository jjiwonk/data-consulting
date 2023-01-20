import pandas as pd
import numpy as np

from setting import directory as dr
import os

# raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/이니스프리/23년 1월/2. 광고주 전달 자료/22연간리뷰/모수중복률'
raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/이니스프리/DCT520'

files = os.listdir(raw_dir)

df_list = []
for file in files:
    temp = pd.read_csv(raw_dir + '/' + file)
    df_list.append(temp)

total_df = pd.concat(df_list)

purchase_user = list(total_df.loc[total_df['event_name'] == 'af_purchase', 'appsflyer_id'].unique())
np.min(total_df['event_time'])

gfa_campaign_list = ['madit_conversion_al_na_na_na_na_purchase',
                     'madit_conversion_al_discoutprice_na_na_na_purchase',
                     'madit_conversion_al_discountrate_na_lip_na_purchase',
                     'madit_conversion_al_discountprice_na_cfac_na_purchase',
                     'madit_conversion_al_discountrate_na_eye_na_purchase',
                     'madit_conversion_al_na_na_btya_na_purchase',
                     'madit_conversion_al_na_na_cfac_na_purchase',
                     'madit_conversion_al_na_na_rcra_na_purchase',
                     'madit_conversion_al_bigsale_na_na_na_purchase',
                     'madit_conversion_al_discountprice_na_btya_na_purchase',
                     'madit_conversion_al_discoutrate_na_cleansing_na_purchase',
                     'madit_conversion_al_nplusn_na_handcream_na_purchase',
                     'madit_conversion_al_membership_na_na_na_purchase',
                     'madit_conversion_al_discountprice_na_mix_na_purchase',
                     'madit_conversion_al_discountprice_na_rcra_na_purchase',
                     'madit_conversion_al_partnership_na_na_na_purchase',
                     'madit_conversion_al_greenholiday_na_na_na_purchase',
                     'madit_conversion_al_greenholiday_na_gtss_na_purchase',
                     'madit_conversion_al_discountprice_na_cfac_rcra_purchase',
                     'madit_conversion_al_greenholiday_na_btya_na_purchase',
                     'madit_conversion_al_disecountrate_na_lip_na_purchase',
                     'madit_conversion_al_blackfriday_na_na_na_purchase',
                     'madit_conversion_al_greenholiday_na_rcra_na_purchase',
                     'madit_conversion_al_newmem_na_na_na_purchase',
                     'madit_conversion_al_discountprice_na_cfacxrcra_na_purchase',
                     'madit_conversion_al_discountprice_na_lip_na_purchase',
                     'madit_conversion_al_disecountrate_na_eye_na_purchase',
                     'madit_conversion_al_discoutrate_na_sun_na_purchase']
kakao_campaign_list = ['madit_conversion_al_na_na_na_na_purchase',
                       'madit_conversion_al_discoutprice_na_na_na_purchase',
                       'madit_conversion_al_discountrate_na_lip_na_purchase',
                       'madit_conversion_al_discountprice_na_cfac_na_purchase',
                       'madit_conversion_al_discountrate_na_eye_na_purchase',
                       'madit_prospecting_imc_cfac_na_na_na_visit',
                       'madit_conversion_al_na_na_btya_na_purchase',
                       'madit_conversion_al_na_na_cfac_na_purchase',
                       'madit_conversion_al_na_na_rcra_na_purchase',
                       'madit_conversion_al_bigsale_na_na_na_purchase',
                       'madit_conversion_al_discountprice_na_btya_na_purchase',
                       'madit_conversion_al_discoutrate_na_cleansing_na_purchase',
                       'madit_conversion_al_nplusn_na_handcream_na_purchase',
                       'madit_conversion_al_membership_na_na_na_purchase',
                       'madit_conversion_al_discountprice_na_mix_na_purchase',
                       'madit_conversion_al_discountprice_na_rcra_na_purchase',
                       'madit_conversion_al_partnership_na_na_na_purchase',
                       'madit_conversion_al_greenholiday_na_na_na_purchase',
                       'madit_prospecting_imc_rcra_na_na_na_visit',
                       'madit_conversion_al_greenholiday_na_gtss_na_purchase',
                       'madit_conversion_al_discountprice_na_cfac_rcra_purchase',
                       'madit_conversion_al_greenholiday_na_btya_na_purchase',
                       'madit_prospecting_al_blackfriday_na_na_na_visit',
                       'madit_conversion_al_disecountrate_na_lip_na_purchase',
                       'madit_conversion_al_blackfriday_na_na_na_purchase',
                       'madit_proscpecting_al_discountrate_na_na_na_visit',
                       'madit_prospecting_imc_btya_na_na_na_visit',
                       'madit_conversion_al_greenholiday_na_rcra_na_purchase',
                       'madit_conversion_al_newmem_na_na_na_purchase',
                       'madit_conversion_al_discountprice_na_cfacxrcra_na_purchase',
                       'madit_prospecting_imc_na_btya_na_visit',
                       'madit_conversion_al_discountprice_na_lip_na_purchase',
                       'madit_conversion_al_disecountrate_na_eye_na_purchase',
                       'madit_conversion_al_discoutrate_na_sun_na_purchase',
                       'madit_prospecting_imc_gtss_na_na_na_visit']

conversion_df = total_df.loc[total_df['event_name'].isin(['install', 're-engagement', 're-attribution'])]
conversion_df.loc[conversion_df['media_source'] == 'buzzvil', 'media_source'] = 'buzzvill'
conversion_df.loc[(conversion_df['campaign'].isin(gfa_campaign_list)) & (
            conversion_df['media_source'] == 'naver'), 'media_source'] = 'naver_gfa'
conversion_df.loc[(~conversion_df['campaign'].isin(kakao_campaign_list)) & (
            conversion_df['media_source'] == 'kakaotalk'), 'media_source'] = '광고주 카카오'
# conversion_df.loc[conversion_df['media_source']=='kakaotalk', 'campaign'].unique()

conversion_df['media_source'].value_counts()[:11].keys()
top10_media = ['buzzvill', 'criteonew_int', 'rtbhouse_int', 'remerge_int', 'naver_gfa', 'kakaotalk', 'naver',
               'googleadwords_int', 'fbxig', 'igaworkstradingworksvideo_int']

media_unique_user_dict = {}

for media in top10_media:
    unique_users = set(conversion_df.loc[conversion_df['media_source'] == media, 'appsflyer_id'])
    media_unique_user_dict.update({media: {'unique_users': unique_users,
                                           'length': len(unique_users)}})

sorted_dict = dict(sorted(media_unique_user_dict.items(), key=lambda x: x[1]['length'], reverse=True))

data = []
for i in sorted_dict.keys():
    rows = []
    for j in sorted_dict.keys():
        dup = media_unique_user_dict[i]['unique_users'] & media_unique_user_dict[j]['unique_users']
        calc = len(dup) / len(media_unique_user_dict[i]['unique_users']) * 100
        rows.append(calc)
    data.append(rows)

media_df = pd.DataFrame(data, columns=sorted_dict.keys(), index=sorted_dict.keys())
media_df.to_csv(dr.download_dir + '/media_df.csv')

data = []
for i in sorted_dict.keys():
    rows = []
    for j in sorted_dict.keys():
        dup = media_unique_user_dict[i]['unique_users'] & media_unique_user_dict[j]['unique_users']
        rows.append((len(media_unique_user_dict[i]['unique_users']), len(dup)))
    data.append(rows)

media_df2 = pd.DataFrame(data, columns=sorted_dict.keys(), index=sorted_dict.keys())
media_df2.to_csv(dr.download_dir + '/media_df2.csv')
