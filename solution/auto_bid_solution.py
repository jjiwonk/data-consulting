import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import seaborn as sns
import pandas as pd


dtypes = {
    'date': pa.string(),
    'customer_id': pa.string(),
    'campaign_id': pa.string(),
    'adgroup_id': pa.string(),
    'ad_keyword_id': pa.string(),
    'ad_id':pa.string(),
    'business_channel_id': pa.string(),
    'hours': pa.string(),
    'region_code': pa.string(),
    'media_code' : pa.string(),
    'pc_mobile_type': pa.string(),
    'impression' : pa.string(),
    'click' : pa.string(),
    'cost': pa.string(),
    'sum_of_ad_rank': pa.string(),
    'view_count': pa.string(),
    'collected_at': pa.string(),
    'owner_id': pa.string(),
    'product_id': pa.string(),
    'year': pa.string(),
    'month': pa.string(),
    'day': pa.string(),
    'ad_keyword': pa.string()
    }
index_columns = list(dtypes.keys())
convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
ro = pacsv.ReadOptions(block_size=10 << 20)
raw_df = pacsv.read_csv(dr.download_dir + '/finda_test_data.csv', convert_options=convert_ops, read_options=ro)
raw_df = raw_df.to_pandas()
prep_raw_df = raw_df.drop_duplicates()

df = raw_df.loc[raw_df['ad_keyword'] == '금리인하']
df[['hours', 'impression', 'click', 'cost', 'sum_of_ad_rank', 'view_count', 'year', 'month', 'day']] = df[['hours', 'impression', 'click', 'cost', 'sum_of_ad_rank', 'view_count', 'year', 'month', 'day']].apply(pd.to_numeric)
df['avg_rank'] = df['sum_of_ad_rank'] / df['impression']
sns.scatterplot(df['avg_rank'], df[''])