from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info
from setting import directory as dr
import pandas as pd
import datetime
import numpy as np

def sankey_data_prep():
    raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                             from_date = info.from_date,
                             to_date = info.to_date)

    raw_df_exception = prep.campaign_name_exception(raw_df)

    target_event_list = ['Viewed LA Home', 'loan_contract_completed']
    event_data = raw_df_exception.loc[raw_df_exception['event_name'].isin(target_event_list)]

    event_data.loc[pd.isnull(event_data['attributed_touch_time']), 'attributed_touch_time'] = event_data['install_time']
    event_data['click_date'] = event_data['attributed_touch_time'].dt.date
    event_data['click_weekday'] = event_data['attributed_touch_time'].dt.weekday

    event_data = event_data.loc[event_data['attributed_touch_type']!='impression']

    event_data['CTET'] = (event_data['event_time'] - event_data['attributed_touch_time']).apply(
        lambda x: x.total_seconds() / 86400)
    event_data['CTIT'] = (event_data['install_time'] - event_data['attributed_touch_time']).apply(
        lambda x: x.total_seconds() / 86400)
    event_data['ITET'] = (event_data['event_time'] - event_data['install_time']).apply(lambda x: x.total_seconds() / 86400)

    event_data = event_data.loc[(event_data['CTIT'] < 7) & (event_data['ITET'] < 30)]
    event_data = event_data.loc[~event_data['media_source'].isin(['', 'restricted'])]
    event_data['Cnt'] = 1
    event_data['click_month'] = event_data['attributed_touch_time'].dt.month
    event_data['event_month'] = event_data['event_time'].dt.month
    event_data['UA / RE'] = event_data['is_retargeting'].apply(lambda x : 'RE' if x == 'true' else 'UA')

    event_data_filtered = event_data[['UA / RE', 'media_source', 'click_month', 'click_date', 'click_weekday',
                                      'event_month','event_date','event_weekday',
                                      'event_name','platform','appsflyer_id','Cnt']]
    event_data_filtered = event_data_filtered.loc[event_data_filtered['click_date']>=info.from_date]

    event_data_pivot = event_data_filtered.pivot_table(index = ['UA / RE', 'media_source', 'click_month', 'click_date', 'click_weekday',
                                      'event_month','event_date','event_weekday','platform','event_name'], aggfunc='sum').reset_index()



    event_data_pivot = event_data_pivot.sort_values(['event_month','click_month', 'event_weekday','click_weekday'])

    weekday_dict = {0: '월요일',
                    1: '화요일',
                    2: '수요일',
                    3: '목요일',
                    4: '금요일',
                    5: '토요일',
                    6: '일요일'}
    event_data_pivot['click_weekday'] = event_data_pivot['click_weekday'].apply(lambda x : weekday_dict.get(x))
    event_data_pivot['event_weekday'] = event_data_pivot['event_weekday'].apply(lambda x : weekday_dict.get(x))

    event_data_pivot['click_month'] = event_data_pivot['click_month'].apply(lambda x : str(x) + "월")
    event_data_pivot['event_month'] = event_data_pivot['event_month'].apply(lambda x: str(x) + "월")
    event_data_pivot.to_excel(dr.download_dir + '/finda_event_flow_data.xlsx', index=False , encoding = 'utf-8-sig')


    # sankey_model = pd.read_csv(dr.download_dir + '/Sankey Model.csv')
    # sankey_model['index'] = sankey_model.index
    #
    # event_data_join_model = event_data_pivot.merge(sankey_model, on = 'index', how = 'inner')
    # event_data_join_model.to_csv(dr.download_dir + '/finda_event_flow_data.csv', index=False , encoding = 'utf-8-sig')

