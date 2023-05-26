from setting import directory as dr
import os
import pandas as pd
from workers import read_data
import pyarrow as pa
import datetime

class Key :
    media_list = ['kakao_int', 'Facebook Ads','fbig','restricted']
    itet_limit = 7
    ctet_limit = 7

def get_prism_data() :
    prism_dir = dr.download_dir + '/2023-04'
    prism_files = os.listdir(prism_dir)

    dtypes = {
        'attributed_touch_type' : pa.string(),
        'attributed_touch_time' :  pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_value': pa.string(),
        'event_revenue': pa.string(),
        'media_source' : pa.string(),
        'campaign': pa.string(),
        'campaign_id': pa.string(),
        'adset': pa.string(),
        'adset_id': pa.string(),
        'ad': pa.string(),
        'ad_id': pa.string(),
        #'keywords' : pa.string(),
        #'original_url': pa.string(),
        'channel' : pa.string(),
        'is_primary_attribution' : pa.string(),
    }

    prism_data = read_data.pyarrow_csv(dtypes, prism_dir, prism_files)

    return prism_data

raw_data= get_prism_data()
raw_data_filtered = raw_data.loc[(raw_data['media_source'].isin(Key.media_list))&
                                 (raw_data['event_name'].isin(['af_purchase']))]

raw_data_filtered.index = range(len(raw_data_filtered))
raw_data_filtered['구분'] = raw_data_filtered['campaign'].apply(lambda x: x.replace(x, 'PTC') if x.find('PTC') != -1 else '-')
idx = raw_data_filtered[(raw_data_filtered['media_source']=='kakao_int')&(raw_data_filtered['구분'] =='-')].index
raw_data_filtered.drop(idx, inplace = True)
raw_data_filtered = raw_data_filtered.drop(columns = '구분')

raw_data_filtered[['attributed_touch_time', 'install_time', 'event_time']] = raw_data_filtered[['attributed_touch_time', 'install_time', 'event_time']].apply(lambda x : pd.to_datetime(x))
raw_data_filtered['event_revenue'] = pd.to_numeric(raw_data_filtered['event_revenue'])
raw_data_filtered['ITET'] = raw_data_filtered['event_time'] - raw_data_filtered['install_time']
raw_data_filtered['CTET'] = raw_data_filtered['event_time'] - raw_data_filtered['attributed_touch_time']

raw_data_filtered_restricted = raw_data_filtered.loc[(raw_data_filtered['media_source'] == 'restricted')&(raw_data_filtered['ITET']<datetime.timedelta(Key.itet_limit))]
raw_data_filtered = raw_data_filtered.loc[(raw_data_filtered['media_source'] != 'restricted')&(raw_data_filtered['CTET']<datetime.timedelta(Key.ctet_limit))]

raw_data_filtered = pd.concat([raw_data_filtered_restricted,raw_data_filtered])

raw_data_filtered['event_value'] = raw_data_filtered['event_value'].str.replace('null', '')
raw_data_filtered['event_value'] = raw_data_filtered['event_value'].apply(lambda x : eval(str(x)))

values_keys = ['af_order_id', 'af_revenue', 'af_content_id','order_id']
for key in values_keys :
    raw_data_filtered[key] = raw_data_filtered['event_value'].apply(lambda x: x.get(key))

cont_dict = dict(zip(raw_data_filtered['af_order_id'] , raw_data_filtered['af_content_id']))

raw_data_filtered.loc[raw_data_filtered['af_order_id'].isna() == True,'af_order_id' ] = raw_data_filtered['order_id']
raw_data_filtered.loc[raw_data_filtered['af_content_id'].isna() == True,'af_content_id' ] = raw_data_filtered['af_order_id'].apply(lambda x : cont_dict[x] if x in cont_dict.keys() else '-')
raw_data_filtered = raw_data_filtered.drop(columns='order_id')

raw_data_parsed = raw_data_filtered.copy()
raw_data_parsed['media_source'].value_counts()
raw_data_parsed = raw_data_parsed.loc[raw_data_parsed['is_primary_attribution']!= 'False']
raw_data_parsed = raw_data_parsed.sort_values(['install_time', 'event_time', 'af_order_id'])
raw_data_parsed = raw_data_parsed.drop_duplicates('af_order_id')
raw_data_parsed.index = range(0, len(raw_data_parsed))

raw_data_parsed.to_csv(dr.download_dir + '/raw_data_unique.csv', encoding = 'utf-8-sig', index=False)

values_keys = ['af_order_id', 'af_revenue', 'af_content_id']
raw_data_values = raw_data_parsed[values_keys]

row_list = []

for row in raw_data_values.values:
    content_list = row[-1]
    for content in content_list :
        append_row = list(row) + [content]
        row_list.append(append_row)

raw_data_item_parsed = pd.DataFrame(row_list, columns = values_keys + ['item'])
raw_data_item_parsed = raw_data_item_parsed.drop('af_content_id', axis = 1)

raw_data_final = raw_data_parsed.merge(raw_data_item_parsed, on = ['af_order_id', 'af_revenue'], how = 'left')
raw_data_final.to_csv(dr.download_dir + '/raw_data_item_parsing.csv', encoding = 'utf-8-sig', index=False)