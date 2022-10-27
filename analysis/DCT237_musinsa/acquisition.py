import datetime
import pandas as pd
from analysis.DCT237_musinsa import info
from analysis.DCT237_musinsa import prep

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
def acq_data_prep():
    fp_log = pd.read_csv(info.raw_dir + '/purchase/fp_2020-01_2022_09.csv')

    paid_purchase = pd.read_csv(info.raw_dir + '/purchase/paid_purchase_data.csv')
    organic_purchase = pd.read_csv(info.raw_dir + '/purchase/organic_purchase_data.csv')

    total_purchase = pd.concat([paid_purchase, organic_purchase], sort = False, ignore_index= True)
    total_purchase = total_purchase.drop_duplicates(['appsflyer_id', 'event_revenue', 'event_time'])

    total_purchase_filtered = total_purchase[['attributed_touch_time','event_time', 'appsflyer_id','install_time', 'event_revenue', 'media_source', '계정', '광고코드']]
    total_purchase_filtered.loc[total_purchase_filtered['attributed_touch_time'].isnull(), 'attributed_touch_time'] = total_purchase_filtered['install_time']

    total_purchase_fp_merge = total_purchase_filtered.merge(fp_log, on = ['appsflyer_id', 'event_time'], how = 'inner')

    conv_data = conv_data_prep()

    total_purchase_conv_merge = total_purchase_fp_merge.merge(conv_data, on = 'appsflyer_id', how = 'inner')
    total_purchase_conv_merge[['click_time','event_time', 'install_time','first_install_time']] = \
        total_purchase_conv_merge[['click_time','event_time','install_time', 'first_install_time']].apply(lambda x: pd.to_datetime(x))
    total_purchase_conv_merge = total_purchase_conv_merge.loc[total_purchase_conv_merge['event_time'] >= total_purchase_conv_merge['first_install_time']]

    total_purchase_conv_merge['ITET'] = total_purchase_conv_merge['event_time'] - total_purchase_conv_merge['first_install_time']
    total_purchase_conv_merge.loc[(total_purchase_conv_merge['ITET'] >= datetime.timedelta(1))&
                                  (total_purchase_conv_merge['conversion_type']!='install'),
                                  ['click_time',
                                   'first_install_time',
                                   '계정(Acq)',
                                   'media_source(Acq)',
                                   '광고코드(Acq)']] = total_purchase_conv_merge[['attributed_touch_time','install_time', '계정', 'media_source', '광고코드']]
    total_purchase_conv_merge.loc[(total_purchase_conv_merge['ITET'] >= datetime.timedelta(7))&
                                  (total_purchase_conv_merge['conversion_type']=='install'),
                                  ['click_time',
                                   'first_install_time',
                                   '계정(Acq)',
                                   'media_source(Acq)',
                                   '광고코드(Acq)']] = total_purchase_conv_merge[['attributed_touch_time','install_time', '계정', 'media_source', '광고코드']]
    total_purchase_conv_merge['ITET'] = total_purchase_conv_merge['event_time'] - total_purchase_conv_merge['first_install_time']
    total_purchase_conv_merge = total_purchase_conv_merge.loc[total_purchase_conv_merge['ITET']<datetime.timedelta(7)]

    acquisition_data = total_purchase_conv_merge[['click_time','event_time', 'appsflyer_id','first_install_time', '계정(Acq)', '광고코드(Acq)', 'media_source(Acq)', 'event_revenue']]
    return acquisition_data
def acquisition_cost():
    conversion_df = acq_data_prep()
    conversion_df['date'] = pd.to_datetime(conversion_df['click_time']).dt.date
    conversion_df['Cnt'] = 1
    conversion_df = conversion_df.loc[~conversion_df['media_source(Acq)'].isin(['facebook_network', 'Facebook Ads'])]

    conversion_pivot = conversion_df.pivot_table(index = ['date','광고코드(Acq)', '계정(Acq)'], values = ['Cnt', 'event_revenue'], aggfunc = 'sum').reset_index()

    cost_pivot = prep.get_cost_pivot(['date', '광고코드', '계정'])
    cost_pivot['date'] = pd.to_datetime(cost_pivot['date']).dt.date
    cost_pivot = cost_pivot.loc[cost_pivot['계정']!='VA']

    df = conversion_pivot.merge(cost_pivot, left_on=['date','광고코드(Acq)', '계정(Acq)'], right_on = ['date','광고코드', '계정'], how = 'outer')
    df.to_csv(info.result_dir + '/acquisition_cost_data.csv', index=False, encoding = 'utf-8-sig')
    return df