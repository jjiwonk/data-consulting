from setting import directory as dr
import pandas as pd
from workers import read_data
import os
import pyarrow as pa
import numpy as np
import datetime

class Key :
    project_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT598'
    raw_dir = project_dir + '/raw'
    conversion_raw_dir = raw_dir + '/conversion'
    detail_raw_dir = raw_dir + '/detail'
    conversion_data_columns = ['date', 'customer_id', 'campaign_id', 'adgroup_id', 'search_keyword', 'ad_id', 'business_channel_id', 'hours','region_code',
                               'media_code', 'pc_mobile_type', 'conversion_method', 'conversion_type', 'conversion_count', 'sales_by_conversion']
    detail_data_columns = ['date', 'customer_id', 'campaign_id', 'adgroup_id', 'search_keyword', 'ad_id', 'business_channel_id', 'hours','region_code',
                               'media_code', 'pc_mobile_type', 'impression', 'click', 'cost', 'sum_of_ad_rank', 'view_count']

    index_columns = ['Month', 'customer_id', 'campaign_id', 'adgroup_id', 'search_keyword',  'pc_mobile_type']

class refactor :
    def refactor_conversion_data(self):
        file_list = os.listdir(Key.conversion_raw_dir)
        df_list = []
        for file in file_list :
            data = pd.read_csv(Key.conversion_raw_dir + '/' + file, sep = '\t', names = Key.conversion_data_columns)
            df_list.append(data)
        df = pd.concat(df_list, sort = False, ignore_index= True)
        df.to_csv(Key.raw_dir + '/total_conversion_data.csv', index=False, encoding='utf-8-sig')
        return df

    def refactor_detail_data(self):
        file_list = os.listdir(Key.detail_raw_dir)
        df_list = []
        for file in file_list :
            data = pd.read_csv(Key.detail_raw_dir + '/' + file, sep = '\t', names = Key.detail_data_columns)
            df_list.append(data)
        df = pd.concat(df_list, sort = False, ignore_index= True)
        df.to_csv(Key.raw_dir + '/total_detail_data.csv', index=False, encoding='utf-8-sig')
        return df

class prep :
    def get_conv_pivot(self):
        conv = pd.read_csv(Key.raw_dir + '/total_conversion_data.csv')
        conv['Month'] = pd.to_datetime(conv['date']).dt.month
        conv[Key.index_columns] = conv[Key.index_columns].fillna('-')

        conv_pivot = conv.pivot_table(index=Key.index_columns,
                                      columns=['conversion_method', 'conversion_type'],
                                      values=['conversion_count', 'sales_by_conversion'],
                                      aggfunc='sum')

        new_col_list = []
        for col in conv_pivot.columns:
            col_to_list = list(col)
            head_dict = {
                1: '(직접)',
                2: '(간접)'
            }
            head = head_dict.get(col_to_list[1])

            middle_dict = {
                1: '구매',
                2: '가입',
                3: '장바구니',
                4: '향상된 구매',
                5: '기타'
            }
            middle = middle_dict.get(col_to_list[2])

            last_dict = {
                'sales_by_conversion': '_value',
                'conversion_count': '_cnt'
            }
            last = last_dict.get(col_to_list[0])
            new_col_list.append(head + middle + last)

        conv_pivot.columns = new_col_list
        conv_pivot[new_col_list] = conv_pivot[new_col_list].fillna(0)

        conv_pivot = conv_pivot.reset_index()
        return conv_pivot

    def get_detail_pivot(self):
        dtypes = {
            'date' : pa.string(),
            'customer_id' : pa.string(),
            'campaign_id' : pa.string(),
            'adgroup_id' : pa.string(),
            'ad_id' : pa.string(),
            'search_keyword' : pa.string(),
            'pc_mobile_type' : pa.string(),
            'impression' : pa.float32(),
            'click' : pa.float32(),
            'cost' : pa.float32(),
            'sum_of_ad_rank' : pa.float32()
        }
        detail = read_data.pyarrow_csv(dtypes, Key.raw_dir, ['total_detail_data.csv'])
        detail['Month'] = pd.to_datetime(detail['date']).dt.month
        detail[Key.index_columns] = detail[Key.index_columns].fillna('-')
        detail_pivot = detail.pivot_table(index=Key.index_columns,
                                          values=['impression', 'click', 'cost', 'sum_of_ad_rank'],
                                          aggfunc='sum').reset_index()
        detail_pivot['avg_of_ad_rank'] = detail_pivot['sum_of_ad_rank'] / detail_pivot['impression']
        detail_pivot.to_csv(Key.raw_dir + '/total_detail_data_pivot.csv', index=False, encoding = 'utf-8-sig')
        return detail_pivot

class work :
    def shopping_report_merge(self):
        conv_data = prep().get_conv_pivot()
        detail_data = prep().get_detail_pivot()
        # detail_data = detail_pivot.copy()

        for col in Key.index_columns:
            conv_data[col] = conv_data[col].apply(lambda x: str(x))
            detail_data[col] = detail_data[col].apply(lambda x: str(x))

        detail_conv_merge = detail_data.merge(conv_data, on=Key.index_columns, how='outer')
        detail_conv_merge = detail_conv_merge.fillna(0)

        detail_conv_merge.to_csv(Key.project_dir + '/total_data.csv', index=False, encoding = 'utf-8-sig')

        month_list = detail_conv_merge['Month'].unique()

        for month in month_list :
            file_name = f'monthly_data_{month}.csv'
            month_filter = detail_conv_merge.loc[detail_conv_merge['Month']==month]
            month_filter.to_csv(Key.project_dir + '/' + file_name ,index = False, encoding = 'utf-8-sig')

        print('finish')

# refactor().refactor_conversion_data()
# refactor().refactor_detail_data()
work().shopping_report_merge()