import datetime
import pandas as pd
from analysis.DCT237_musinsa import info
from analysis.DCT237_musinsa import prep
from analysis.DCT237_musinsa import acquisition

def calc_arpu(total_purchase):
    total_purchase = total_purchase.sort_values(['install_time', 'event_time'])
    total_purchase = total_purchase.drop_duplicates(['appsflyer_id', 'event_time', 'event_revenue'])
    total_purchase['Cnt'] = 1

    total_purchase_pivot = total_purchase.pivot_table(index = ['appsflyer_id'], values = ['Cnt', 'event_revenue'], aggfunc = 'sum').reset_index()
    return total_purchase_pivot

def calc_retention(total_purchase):
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
    paid_purchase = pd.read_csv(info.raw_dir + '/purchase/paid_purchase_data.csv')
    organic_purchase = pd.read_csv(info.raw_dir + '/purchase/organic_purchase_data.csv')
    total_purchase = pd.concat([paid_purchase, organic_purchase], sort=False, ignore_index=True)

    user_arpu_table = calc_arpu(total_purchase)
    user_retention_table = calc_retention(total_purchase)

    acq_data = acquisition.acq_data_prep()
    acq_retention_merge = acq_data.merge(user_retention_table, on ='appsflyer_id', how = 'left')
    acq_retention_merge['is_retention'] = acq_retention_merge['is_retention'].fillna(0)

    acq_arpu_merge = acq_retention_merge[['계정(Acq)','appsflyer_id','is_retention']].merge(user_arpu_table, on ='appsflyer_id', how = 'left')
    acq_arpu_merge['ARPU'] = acq_arpu_merge['event_revenue'] / acq_arpu_merge['Cnt']
    acq_arpu_merge['User'] = 1

    ltv_pivot = acq_arpu_merge.pivot_table(index = ['계정(Acq)'], values = ['User','is_retention','Cnt','ARPU'],
                                           aggfunc = {'User' : 'sum',
                                                      'is_retention' : 'sum',
                                                      'Cnt' : 'sum',
                                                      'ARPU' : 'mean'}).reset_index()
    ltv_pivot['Retention'] = ltv_pivot['is_retention'] / ltv_pivot['User']

    return ltv_pivot