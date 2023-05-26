import pandas as pd
from workers import read_data
from setting import directory as dr
import pyarrow as pa
import numpy as np
from workers import func

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/RD'
dtypes = {
    'event_time' : pa.string(),
    'event_value' : pa.string()
}
data = read_data.pyarrow_csv(dtypes=dtypes,
                             directory=raw_dir,
                             file_list=['appsflyer_ua_purchase.csv',
                                        'appsflyer_re_purchase.csv',
                                        'organic_purchase.csv',
                                        'paid_purcahse_2206_2207.csv',
                                        'organic_purchase_2209.csv'])

mutan_data = data.loc[data['event_value'].str.contains('musinsastandard')]
mutan_data.index = range(len(mutan_data))
del data

value_parser = func.EventValueParser(mutan_data, 'event_value')
parsed_data = value_parser.data_parse()

concat_data = pd.concat([mutan_data, parsed_data], axis=1)
concat_data_dedup = concat_data.drop_duplicates('af_order_id')
concat_data_dedup = concat_data_dedup.loc[pd.notnull(concat_data_dedup['af_brand'])]

# brand_set = set()
# for brand_list in concat_data_dedup['af_brand'] :
#     brand_set = brand_set | set(brand_list)
#
# print(brand_set)

price_array = np.array(concat_data_dedup['af_price'])
brand_array = np.array(concat_data_dedup['af_brand'])

target_brand_list = ['musinsastandard', 'musinsastandardkids', 'musinsastandardbt', 'musinsastandardsp']
target_brand_revenue_dict = dict()
target_brand_value_dict = dict()
for target_brand in target_brand_list :
    target_brand_revenue_dict[target_brand] = []
    target_brand_value_dict[target_brand] = 0

for i, brand_list in enumerate(brand_array[:50]) :
    price_list = price_array[i]
    for brand_idx, brand in enumerate(brand_list):
        for target_brand in target_brand_list:
            if brand == target_brand:
                target_brand_value_dict[target_brand] += int(price_list[brand_idx])
                break

    for target_brand in target_brand_list :
        target_brand_revenue_dict[target_brand] = target_brand_revenue_dict[target_brand] + [target_brand_value_dict[target_brand]]
        target_brand_value_dict[target_brand] = 0


def get_total_price(brand_list_array, price_list_array, target_brand_list):
    result = {brand : [0] * len(brand_list_array) for brand in target_brand_list}

    for i, (brand_list, price_list) in enumerate(zip(brand_list_array, price_list_array)):
        for brand, price in zip(brand_list, price_list):
            if brand in target_brand_list:
                result[brand][i] += float(price)

    return result


result_dict = get_total_price(brand_array, price_array, target_brand_list)
for target_brand in result_dict.keys():
    concat_data_dedup[target_brand] = result_dict[target_brand]

concat_data_dedup['date'] = pd.to_datetime(concat_data_dedup['event_time']).dt.date
concat_data_dedup.to_csv(raw_dir + '/purchase_log_data.csv', index=False, encoding='utf-8-sig')

revenue_pivot = concat_data_dedup.pivot_table(index = 'date', values = result_dict.keys(), aggfunc = 'sum').reset_index()
revenue_pivot.to_csv(raw_dir + '/total_revenue_data.csv', index=False, encoding='utf-8-sig')

