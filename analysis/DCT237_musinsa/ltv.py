import datetime
import pandas as pd
from analysis.DCT237_musinsa import info
from analysis.DCT237_musinsa import preprocess as prep
import numpy as np

def acquisition_cost():
    conversion_df = prep.acq_data
    conversion_df['date'] = pd.to_datetime(conversion_df['click_time']).dt.date
    conversion_df['Cnt'] = 1

    pivot_columns = ['date','광고코드(Acq)', '계정(Acq)', 'media_source(Acq)']
    conversion_df[pivot_columns] = conversion_df[pivot_columns].fillna('')
    revenue_pivot = conversion_df.pivot_table(index = pivot_columns, values = ['event_revenue', 'total_revenue'], aggfunc = 'sum')
    count_pivot = conversion_df.pivot_table(index = pivot_columns, columns = 'acquisition_type', values = 'Cnt', aggfunc = 'sum')

    conversion_pivot = pd.concat([revenue_pivot, count_pivot], axis = 1).reset_index()

    cost_pivot = prep.get_cost_pivot(['date', '광고코드', '계정', 'media_source'])
    cost_pivot['date'] = pd.to_datetime(cost_pivot['date']).dt.date
    cost_pivot = cost_pivot.loc[cost_pivot['계정']!='VA']

    df = conversion_pivot.merge(cost_pivot, left_on=['date','광고코드(Acq)', '계정(Acq)', 'media_source(Acq)'], right_on = ['date','광고코드', '계정', 'media_source'], how = 'outer')
    df.to_csv(info.result_dir + '/acquisition_cost_data.csv', index=False, encoding = 'utf-8-sig')
    return df
def calc_arpu():
    acq_user_list = list(prep.acq_data['appsflyer_id'].unique())

    total_purchase = prep.total_purchase
    total_purchase = total_purchase.loc[total_purchase['appsflyer_id'].isin(acq_user_list)]
    total_purchase = total_purchase.sort_values(['install_time', 'event_time'])
    total_purchase = total_purchase.drop_duplicates(['appsflyer_id', 'event_time', 'event_revenue'])
    total_purchase['Cnt'] = 1

    total_purchase_pivot = total_purchase.pivot_table(index = ['appsflyer_id'], values = ['Cnt', 'event_revenue'], aggfunc = 'sum').reset_index()
    return total_purchase_pivot

def calc_retention():
    acq_user_list = list(prep.acq_data['appsflyer_id'].unique())

    total_purchase = prep.total_purchase
    total_purchase = total_purchase.loc[total_purchase['appsflyer_id'].isin(acq_user_list)]
    total_purchase = total_purchase.sort_values(['appsflyer_id', 'event_time'])
    total_purchase['event_time'] = pd.to_datetime(total_purchase['event_time'])
    total_purchase.index = range(0, len(total_purchase))

    total_purchase_copy = pd.concat([total_purchase.iloc[-1:],total_purchase.iloc[0:-1]])[['appsflyer_id','event_time']]
    total_purchase_copy = total_purchase_copy.rename(columns = {'appsflyer_id' : 'appsflyer_id_before',
                                                                'event_time' : 'event_time_before'})
    total_purchase_copy.index = range(0, len(total_purchase))

    total_purchase_concat = pd.concat([total_purchase, total_purchase_copy], axis=1)
    total_purchase_concat.loc[(total_purchase_concat['appsflyer_id']==total_purchase_concat['appsflyer_id_before'])&
                            (total_purchase_concat['event_time']-total_purchase_concat['event_time_before']>=datetime.timedelta(1)), 'is_retention'] = 1

    user_retention_table = total_purchase_concat.loc[total_purchase_concat['is_retention']==1, ['appsflyer_id', 'is_retention']]
    user_retention_table = user_retention_table.drop_duplicates(['appsflyer_id'])
    return user_retention_table
def get_ltv_data():
    user_arpu_table = calc_arpu()
    user_retention_table = calc_retention()

    acq_data = prep.acq_data
    acq_retention_merge = acq_data.merge(user_retention_table, on ='appsflyer_id', how = 'left')
    acq_retention_merge['is_retention'] = acq_retention_merge['is_retention'].fillna(0)

    acq_arpu_merge = acq_retention_merge[['계정(Acq)','media_source(Acq)','appsflyer_id','is_retention']].merge(user_arpu_table, on ='appsflyer_id', how = 'left')
    acq_arpu_merge['ARPU'] = acq_arpu_merge['event_revenue']
    acq_arpu_merge['User'] = 1

    ltv_pivot = acq_arpu_merge.pivot_table(index = ['계정(Acq)', 'media_source(Acq)'], values = ['User','is_retention','Cnt','ARPU'],
                                           aggfunc = {'User' : 'sum',
                                                      'is_retention' : 'sum',
                                                      'Cnt' : 'sum',
                                                      'ARPU' : 'mean'}).reset_index()
    ltv_pivot['Retention'] = ltv_pivot['is_retention'] / ltv_pivot['User']
    return ltv_pivot