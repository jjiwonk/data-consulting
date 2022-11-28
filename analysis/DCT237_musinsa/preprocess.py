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
    conversion_data_paid = pd.read_csv(info.raw_dir + '/conversion/paid_conversion_data.csv')
    conversion_data_paid = conversion_data_paid.loc[conversion_data_paid['계정'].notnull()]
    conversion_data_paid = conversion_data_paid.loc[conversion_data_paid['attributed_touch_type']=='click']
    conversion_data_paid[['install_time', 'attributed_touch_time']] = conversion_data_paid[['install_time', 'attributed_touch_time']].apply(lambda x: pd.to_datetime(x))
    conversion_data_paid['CTIT'] = conversion_data_paid['install_time'] - conversion_data_paid['attributed_touch_time']
    conversion_data_paid = conversion_data_paid.loc[conversion_data_paid['CTIT'] < datetime.timedelta(1)]
    conversion_data_paid = conversion_data_paid.sort_values('attributed_touch_time')
    conversion_data_paid = conversion_data_paid.drop_duplicates(['appsflyer_id'])
    conversion_data_paid['media_source'] = conversion_data_paid['media_source'].apply(lambda x : info.af_media_name_dict.get(x) if x in info.af_media_name_dict.keys() else x)

    conversion_data_organic = pd.read_csv(info.raw_dir + '/conversion/organic_conversion_data.csv')
    conversion_data_organic['계정'] = 'Organic'
    conversion_data_organic = conversion_data_organic.sort_values('install_time')
    conversion_data_organic = conversion_data_organic.drop_duplicates(['appsflyer_id'])
    conversion_data_organic['media_source'] = 'Organic'

    conversion_data = pd.concat([conversion_data_paid, conversion_data_organic], sort=False, ignore_index=True)


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
    total_purchase = total_purchase.drop_duplicates(['order_id'], keep = 'first')
    return total_purchase
def acq_data_prep(total_purchase):
    fp_log = pd.read_csv(info.raw_dir + '/purchase/first_purchase_updated.csv')
    fp_log = fp_log.sort_values('event_time')
    fp_log_dedup = fp_log.drop_duplicates('appsflyer_id')
    fp_log_dedup = fp_log_dedup[['appsflyer_id', 'event_time']]

    total_purchase_filtered = total_purchase[['appsflyer_id', 'event_revenue', 'media_source', '계정', '광고코드','attributed_touch_time','install_time','event_time']]
    total_purchase_filtered.loc[total_purchase_filtered['attributed_touch_time'].isnull(), 'attributed_touch_time'] = total_purchase_filtered['install_time']
    total_purchase_filtered = total_purchase_filtered.drop_duplicates(['appsflyer_id', 'event_time'])

    total_purchase_fp_merge = total_purchase_filtered.merge(fp_log_dedup, on = ['appsflyer_id', 'event_time'], how = 'inner')
    total_purchase_fp_merge = total_purchase_fp_merge.sort_values('event_time')

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

    total_revenue = total_purchase_filtered.copy()
    total_revenue = total_revenue.pivot_table(index = 'appsflyer_id', values = 'event_revenue', aggfunc = 'sum').reset_index()
    total_revenue = total_revenue.rename(columns = {'event_revenue' : 'total_revenue'})

    acquisition_data = acquisition_data.merge(total_revenue, on = 'appsflyer_id', how = 'left')
    return acquisition_data

total_purchase = get_total_purchase()
acq_data = acq_data_prep(total_purchase)