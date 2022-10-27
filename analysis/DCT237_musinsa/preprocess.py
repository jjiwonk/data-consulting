from analysis.DCT237_musinsa import info
import pandas as pd
import numpy as np
import re
import datetime

def get_cost_pivot(pivot_index):
    cost_df = pd.read_csv(info.raw_dir + '/cost/cost_data.csv')
    cost_df[pivot_index] = cost_df[pivot_index].fillna('')
    cost_pivot = cost_df.pivot_table(index=pivot_index, values=['cost','AF_사용자'], aggfunc='sum').reset_index()
    return cost_pivot

def conv_data_prep():
    conversion_data = pd.read_csv(info.raw_dir + '/conversion/paid_conversion_data.csv')
    conversion_data = conversion_data.loc[conversion_data['계정'].notnull()]
    conversion_data = conversion_data.loc[conversion_data['attributed_touch_type']=='click']
    conversion_data[['install_time', 'attributed_touch_time']] = conversion_data[['install_time', 'attributed_touch_time']].apply(lambda x: pd.to_datetime(x))
    conversion_data['CTIT'] = conversion_data['install_time'] - conversion_data['attributed_touch_time']
    conversion_data = conversion_data.loc[conversion_data['CTIT'] < datetime.timedelta(1)]
    conversion_data = conversion_data.sort_values('attributed_touch_time')
    conversion_data = conversion_data.drop_duplicates(['appsflyer_id'])

    conversion_data_filtered = conversion_data[['appsflyer_id', 'attributed_touch_time','install_time', '계정', '광고코드', 'media_source', 'event_name']]
    conversion_data_filtered = conversion_data_filtered.rename(columns = {'event_name' : 'conversion_type',
                                                                          '계정' : '계정(Acq)',
                                                                          '광고코드' : '광고코드(Acq)',
                                                                          'media_source' : 'media_source(Acq)',
                                                                          'install_time' : 'first_install_time',
                                                                          'attributed_touch_time' : 'click_time'})
    return conversion_data_filtered
def get_total_purchase():
    paid_purchase = pd.read_csv(info.raw_dir + '/purchase/paid_purchase_data.csv')
    organic_purchase = pd.read_csv(info.raw_dir + '/purchase/organic_purchase_data.csv')
    total_purchase = pd.concat([paid_purchase, organic_purchase], sort=False, ignore_index=True)
    total_purchase = total_purchase.sort_values(['install_time', 'event_time'])
    total_purchase = total_purchase.drop_duplicates(['appsflyer_id', 'event_revenue', 'event_time'], keep = 'first')
    return total_purchase
def acq_data_prep(total_purchase):
    fp_log = pd.read_csv(info.raw_dir + '/purchase/fp_2020-01_2022_09.csv')

    total_purchase_filtered = total_purchase[['appsflyer_id', 'event_revenue', 'media_source', '계정', '광고코드','attributed_touch_time','install_time','event_time']]
    total_purchase_filtered.loc[total_purchase_filtered['attributed_touch_time'].isnull(), 'attributed_touch_time'] = total_purchase_filtered['install_time']

    total_purchase_fp_merge = total_purchase_filtered.merge(fp_log, on = ['appsflyer_id', 'event_time'], how = 'inner')

    conv_data = conv_data_prep()

    total_purchase_conv_merge = total_purchase_fp_merge.merge(conv_data, on = 'appsflyer_id', how = 'inner')
    total_purchase_conv_merge[['click_time','event_time', 'install_time','first_install_time']] = \
        total_purchase_conv_merge[['click_time','event_time','install_time', 'first_install_time']].apply(lambda x: pd.to_datetime(x))
    total_purchase_conv_merge = total_purchase_conv_merge.loc[total_purchase_conv_merge['event_time'] >= total_purchase_conv_merge['first_install_time']]

    total_purchase_conv_merge['ITET'] = total_purchase_conv_merge['event_time'] - total_purchase_conv_merge['first_install_time']
    total_purchase_conv_merge['attribute_gap'] = total_purchase_conv_merge['install_time'] - total_purchase_conv_merge['first_install_time']

    total_purchase_conv_merge['ITET'] = total_purchase_conv_merge['event_time'] - total_purchase_conv_merge['first_install_time']
    total_purchase_conv_merge = total_purchase_conv_merge.loc[total_purchase_conv_merge['ITET']<datetime.timedelta(7)]

    acquisition_data = total_purchase_conv_merge[['click_time','event_time', 'appsflyer_id','first_install_time', '계정(Acq)', '광고코드(Acq)', 'media_source(Acq)', 'event_revenue', 'attribute_gap']]
    acquisition_data['acquisition_type'] = '신규 유저'
    acquisition_data.loc[(acquisition_data['attribute_gap']>datetime.timedelta(-30)) &
                         (acquisition_data['attribute_gap']<datetime.timedelta(0)), 'acquisition_type'] = '설치 후 휴면 유저(30일 이내)'
    acquisition_data.loc[(acquisition_data['attribute_gap'] <= datetime.timedelta(-30)), 'acquisition_type'] = '설치 후 이탈 유저(30일 초과)'
    return acquisition_data

total_purchase = get_total_purchase()
acq_data = acq_data_prep(total_purchase)