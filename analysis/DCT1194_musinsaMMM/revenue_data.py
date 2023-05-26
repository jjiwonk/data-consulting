import pandas as pd
from workers import read_data
from setting import directory as dr
import pyarrow as pa
import json
import numpy as np
from workers import func

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/RD'
dtypes = {
    'event_time' : pa.string(),
    'event_value' : pa.string()
}
data = read_data.pyarrow_csv(dtypes=dtypes,directory=raw_dir,file_list=['appsflyer_ua_purchase.csv', 'appsflyer_re_purchase.csv'])
data['order_id'] = data['event_value'].apply(lambda x : json.loads(x)['af_order_id'] if 'af_order_id' in json.loads(x).keys() else '')

data_dedup = data.loc[data['order_id']!=''].drop_duplicates('order_id')

value_parser = func.EventValueParser(data_dedup, 'event_value')
parsed_data = value_parser.data_parse()
parsed_data = parsed_data.loc[pd.notnull(parsed_data['af_brand'])]

mutan_data = parsed_data.copy()
mutan_data['af_brand_str'] = mutan_data['af_brand'].apply(lambda x : str(x))
mutan_data = mutan_data.loc[mutan_data['af_brand_str'].str.contains('musinsastandard')]

price_array = np.array(mutan_data['af_price'])
brand_array = np.array(mutan_data['af_brand'])

val_list = []
for i, brand_list in enumerate(brand_array) :
    price_list = price_array[i]
    value = 0

    for brand_idx, brand in enumerate(brand_list) :
        if brand == 'musinsastandard' :
            value += int(price_list[brand_idx])
    val_list.append(value)

mutan_data['target_value'] = val_list

purchase_table=  data_dedup[['order_id', 'event_time']]
mutan_data_merge = mutan_data.merge(purchase_table, left_on = 'af_order_id', right_on='order_id')
mutan_data_merge['date'] = pd.to_datetime(mutan_data_merge['event_time']).dt.date

revenue_pivot = pd.pivot_table(data = mutan_data_merge, index = 'date', values = 'target_value', aggfunc = 'sum').reset_index()
revenue_pivot.to_csv(raw_dir + '/paid_revenu_data.csv', index=False, encoding='utf-8-sig')

