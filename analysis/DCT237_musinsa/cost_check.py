import pandas as pd
from analysis.DCT237_musinsa import info
from analysis.DCT237_musinsa import prep
import re

def prep_paid_df():
    paid_df = prep.get_paid_df()
    paid_df.loc[paid_df['attributed_touch_time'].isnull(), 'attributed_touch_time'] = paid_df['install_time']
    paid_df['date'] = paid_df['attributed_touch_time'].dt.date
    paid_df['cnt'] = 1
    pivot_index = ['date', 'media_source', 'campaign']
    paid_df[pivot_index] = paid_df[pivot_index].fillna('')
    campaign_df = paid_df.pivot_table(index=pivot_index, values='cnt', aggfunc='sum').reset_index()

    def campaign_name_prep(data):
        pat1 = re.compile('\D{7}\d{3}')
        pat2 = re.compile('\D{3}\d{1}\D{3}\d{3}')
        pat3 = re.compile('\D{7}\d{3}')
        pat4 = re.compile('\D{4}\d{1}\D{2}\d{3}')

        campaign_splits = data.split('_')
        if pat1.search(campaign_splits[-1]) or pat2.search(campaign_splits[-1]) or pat3.search(
                campaign_splits[-1]) or pat4.search(campaign_splits[-1]):
            campaign_name = '_'.join(campaign_splits[:-1])
        else:
            campaign_name = data
        return campaign_name
    campaign_df['campaign'] = campaign_df['campaign'].apply(lambda x: campaign_name_prep(x))

    return campaign_df

def campaign_cost_check():
    campaign_df = prep_paid_df()
    cost_df = prep.get_campaign_cost()
    cost_df = cost_df.loc[(cost_df['일'] >= '2022-07-01')&(cost_df['일'] < '2022-10-01')]
    cost_df['date'] = cost_df['일'].dt.date
    cost_df = cost_df.rename(columns={'매체':'media_source', '캠페인 이름':'campaign', '광고비_Fee포함':'cost'})

    df = pd.merge(campaign_df, cost_df, how='outer', on=['date', 'campaign'])

    mapping_df = df.loc[(~df['cnt'].isnull())&(~df['cost'].isnull())].drop_duplicates('campaign')
    non_mapping_df = df.loc[df['cnt'].isnull()].drop_duplicates('campaign')
    non_mapping_df = non_mapping_df.sort_values('cost', ascending=False)
    len(cost_df.campaign.unique())

    temp = campaign_df.copy()
    temp['campaign'] = temp['campaign'].str.lower()
    temp = campaign_df.loc[campaign_df['campaign'].str.contains(pat='madit_220831_bc_추석_cpa')]

