from setting import directory as dr
import pandas as pd
import numpy as np

class info :
    raw_dir = dr.download_dir
    index_columns = ['collected_at', 'customer_id', 'campaign_id', 'adgroup_id','ad_id', 'search_keyword',  'pc_mobile_type']
    master_report_mapping_key = ['customer_id', 'adgroup_id', 'ad_id']
    master_index_columns = master_report_mapping_key + ['ad_product_name','resource_product_id', 'resource_product_id_of_mall', 'product_name', 'category_name_of_mall']
    # 컬럼 rename 필요한 경우엔 info에 넣고, 마지막 단계에 바꿔서 드려도 될 것 같습니다

def get_conv_pivot():
    conv = pd.read_csv(info.raw_dir + '/shopping_conv.csv')
    conv[info.index_columns] = conv[info.index_columns].fillna('-')

    conv_pivot = conv.pivot_table(index = info.index_columns,
                                  columns = ['conversion_method', 'conversion_type'],
                                  values = ['conversion_count', 'sales_by_conversion'],
                                  aggfunc = 'sum')

    new_col_list = []
    for col in conv_pivot.columns:
        col_to_list = list(col)
        head_dict = {
            1 : '(직접)',
            2 : '(간접)'
        }
        head = head_dict.get(col_to_list[1])

        middle_dict = {
            1 : '구매',
            2 : '가입',
            3 : '장바구니',
            4 : '향상된 구매',
            5 : '기타'
        }
        middle = middle_dict.get(col_to_list[2])

        last_dict = {
            'sales_by_conversion' : '_value',
            'conversion_count' : '_cnt'
        }
        last = last_dict.get(col_to_list[0])
        new_col_list.append(head+middle+last)

    conv_pivot.columns = new_col_list
    conv_pivot[new_col_list] = conv_pivot[new_col_list].fillna(0)

    conv_pivot = conv_pivot.reset_index()
    return conv_pivot

def get_detail_pivot():
    detail = pd.read_csv(info.raw_dir + '/shopping_detail.csv')
    detail[info.index_columns] = detail[info.index_columns].fillna('-')
    detail_pivot = detail.pivot_table(index = info.index_columns, values = ['impression', 'click', 'cost', 'sum_of_ad_rank'], aggfunc = 'sum').reset_index()
    detail_pivot['avg_of_ad_rank'] = detail_pivot['sum_of_ad_rank'] / detail_pivot['impression']
    return detail_pivot

def shopping_report_merge():
    conv_data = get_conv_pivot()
    conv_data.to_csv(info.raw_dir + '/shopping_conversion_preprocess.csv', index=False, encoding='utf-8-sig')

    detail_data = get_detail_pivot()
    detail_conv_merge = detail_data.merge(conv_data, on = info.index_columns, how = 'left')
    detail_conv_merge = detail_conv_merge.fillna(0)

    master = pd.read_csv(info.raw_dir + '/shopping_master.csv')
    master_dedup = master.drop_duplicates(info.master_index_columns)[info.master_index_columns]
    detail_conv_master_merge = detail_conv_merge.merge(master_dedup, on = info.master_report_mapping_key, how = 'left')

    resource_data = pd.read_csv(info.raw_dir + '/shopping_resource_table.csv')
    detail_conv_master_resource_merge = detail_conv_master_merge.merge(resource_data, on = ['campaign_id', 'adgroup_id', 'ad_id'], how = 'left')
    detail_conv_master_resource_merge.to_csv(info.raw_dir + '/shopping_report_sample.csv', index=False, encoding='utf-8-sig')
    return detail_conv_master_resource_merge


shopping_report_merge()

