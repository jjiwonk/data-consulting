import pandas as pd
from analysis.DCT237_musinsa import info
from analysis.DCT237_musinsa import prep
from analysis.DCT237_musinsa import acquisition

def conversion_cost_mapping():
    conversion_df = acquisition.acq_data_prep()
    conversion_df['date'] = pd.to_datetime(conversion_df['attributed_touch_time']).dt.date
    conversion_df['Cnt'] = 1
    conversion_pivot = conversion_df.pivot_table(index = ['광고코드'], values = 'Cnt', aggfunc = 'sum').reset_index()

    cost_pivot = prep.get_cost_pivot(['광고코드', 'media_source', 'campaign', 'adset', 'ad', '계정'])
    cost_pivot = cost_pivot.loc[~cost_pivot['media_source'].isin(['Facebook', 'facebook'])]
    cost_pivot = cost_pivot.loc[cost_pivot['계정']!='VA']

    df = pd.merge(conversion_pivot, cost_pivot, how='right', on=['광고코드'])


    mapping_df = df.loc[(~df['Cnt'].isnull())&(~df['cost'].isnull())]
    non_mapping_df = df.loc[df['Cnt'].isnull()]
    non_mapping_df = non_mapping_df.sort_values('cost', ascending=False)
    non_mapping_df = non_mapping_df.loc[non_mapping_df['AF_사용자']>0]

    conversion_df['adset'] = conversion_df['adset'].fillna('')
    temp = conversion_df.loc[conversion_df['adset'].str.contains('UA_catalogue_AOS')]
