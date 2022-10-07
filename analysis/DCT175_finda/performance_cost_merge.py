from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info

import pandas as pd
import datetime
import numpy as np

raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                         from_date = info.from_date,
                         to_date = info.to_date)

raw_df_exception = prep.campaign_name_exception(raw_df)
raw_df_exception['campaign'] = raw_df_exception['campaign'].str.lower()

campaign_cost_df = prep.get_campaign_cost_df(info.raw_dir,from_date = info.from_date, to_date = info.to_date)
campaign_cost_df['campaign'] = campaign_cost_df['campaign'].str.lower()
cost_df_pivot = campaign_cost_df.pivot_table(index = ['media_source', 'campaign'], values = 'cost', aggfunc = 'sum').reset_index()

# campaign_cost - raw_data merge
raw_pivot = raw_df_exception.pivot_table(index=['media_source','campaign'], columns='event_name', values='event_time', aggfunc='count').reset_index().fillna(0)

merged_df = pd.merge(cost_df_pivot, raw_pivot, how='outer', on=['media_source', 'campaign'])

# campaign_cost_df - raw_df 비교
temp1 = merged_df.loc[(merged_df['cost']>0)&(pd.isnull(merged_df['Viewed LA Home']))]
temp1 = temp1.sort_values('campaign')


temp2 = merged_df.loc[(merged_df['Viewed LA Home']>0)&(pd.isnull(merged_df['cost']))]
temp2 = temp2.sort_values('loan_contract_completed', ascending=False)

temp2 = temp2[['media_source', 'campaign', 'loan_contract_completed']]
is_paid = list(raw_df_exception.loc[(raw_df_exception['is_organic']==False)&(raw_df_exception['campaign'].isin(temp2['campaign'].unique())), 'campaign'].unique())
temp2.loc[temp2['campaign'].isin(is_paid), 'is_paid'] = True