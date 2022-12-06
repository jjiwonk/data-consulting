import pandas as pd
import numpy as np
from tableau.HSAD_project import info

## 성과 리포트
ad_group_report = pd.read_csv(info.raw_dir + '/google_ad_group_ad.csv', usecols= info.ad_group_report_columns)

## 인덱스 데이터 추가
campaign_index = pd.read_csv(info.raw_dir + '/google_campaign.csv', usecols = info.campaign_index_columns)
campaign_index = campaign_index.drop_duplicates(['campaign.id', 'year', 'month', 'day'])
ad_group_report_campaign_merge = ad_group_report.merge(campaign_index, on = ['campaign.id', 'year', 'month', 'day'], how = 'left')

group_index = pd.read_csv(info.raw_dir + '/google_ad_group.csv', usecols = info.group_index_columns)
group_index = group_index.drop_duplicates(['ad_group.id', 'year', 'month', 'day'])
ad_group_report_group_merge = ad_group_report_campaign_merge.merge(group_index, on = ['ad_group.id', 'year', 'month', 'day'], how = 'left')

# 전처리

ad_group_report_group_merge['metrics.cost_micros'] = ad_group_report_group_merge['metrics.cost_micros'] / 1000000



owner_sheet_data = info.owner_sheet.drop_duplicates('Owner ID')
owner_sheet_data = owner_sheet_data.rename(columns = {'Owner ID' : 'owner_id'})

owner_merge_data = ad_group_report_group_merge.merge(owner_sheet_data, on = 'owner_id', how = 'left')
owner_merge_data = owner_merge_data.loc[owner_merge_data['광고주'].notnull()]


account_sheet_data = info.account_sheet.drop_duplicates('광고주')
account_sheet_data = account_sheet_data.loc[account_sheet_data['광고주'].str.len()>0, ['광고주', '업종 (대분류)', '업종 (소분류)', '담당팀']]

account_merge_data = owner_merge_data.merge(account_sheet_data, on = '광고주', how = 'left')

def enum_to_string(array, ref_dict, key):
    return [ref_dict[key].get(enum) for enum in array]



for key in info.enum_dict_ad_group.keys() :
    value = info.enum_dict_ad_group.get(key)
    account_merge_data[key] = enum_to_string(account_merge_data[key], info.google_dict, value)

account_merge_data['creative_url'] = ''

# asset_report_account_merge['creative_url'] = asset_report_account_merge['asset.image_asset.full_size.url'].copy()
# asset_report_account_merge.loc[asset_report_account_merge['asset.youtube_video_asset.youtube_video_id'].notnull(), 'creative_url'] = 'https://www.youtube.com/embed/' + asset_report_account_merge['asset.youtube_video_asset.youtube_video_id']

account_merge_data.to_csv(info.result_dir + '/google_total_raw_data_test.csv', index=False, encoding = 'utf-8-sig')

#
#
#
# asset_report_columns = ['campaign.name', 'campaign.id','campaign.advertising_channel_type','campaign.advertising_channel_sub_type',
#                         'ad_group.name', 'ad_group.id', 'ad_group_ad.ad.id', 'asset.id', 'asset.image_asset.full_size.url',
#                         'asset.image_asset.full_size.height_pixels', 'asset.image_asset.full_size.width_pixels',
#                         'asset.text_asset.text', 'asset.type', 'asset.youtube_video_asset.youtube_video_id',
#                         'ad_group_ad_asset_view.field_type','ad_group_ad_asset_view.performance_label','segments.ad_network_type',
#                         'metrics.impressions','metrics.clicks','metrics.cost_micros','metrics.conversions',
#                           'collected_at', 'owner_id', 'product_id']
# asset_report_data = pd.read_csv(dr.download_dir + '/7b00a0d0-0da2-4147-8a3b-59888da01436.csv', usecols = asset_report_columns)
#
#
#
#
#
#
#
# np.sum(asset_report_data['metrics.clicks'])
#
#
#
#
#
# ad_group_report.columns
#
# np.sum(ad_group_report['metrics.clicks'])
# asset_report_data.columns
# asset_report_data_pivot = asset_report_data.pivot_table(index = 'customer.descriptive_name', values = 'metrics.clicks', aggfunc = 'sum')
# ad_group_report_pivot = ad_group_report.pivot_table(index = 'ad_group_ad.ad.id', values = 'metrics.clicks', aggfunc = 'sum')
#
# pivot_concat = pd.concat([asset_report_data_pivot, ad_group_report_pivot], axis = 1)
#
# len(asset_report_data.loc[asset_report_data['asset.youtube_video_asset.youtube_video_id'].str.len()>0,'ad_group_ad.ad.id'].unique())
# len(ad_group_report.loc[ad_group_report['metrics.video_views']>0, 'ad_group_ad.ad.id'].unique())
#
#
# ad_group_asset_report = pd.read_csv(dr.download_dir + '/584e2daf-7cc0-4549-a066-a8f267eb2eb6.csv')
# np.sum(ad_group_asset_report['metrics.clicks'])
#
# ad_group_asset_report.loc[ad_group_asset_report[['metrics.clicks', 'metrics.impressions']].values.sum(axis=1)>10]