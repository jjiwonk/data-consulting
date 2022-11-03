import setting.directory as dr

import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime

raw_dir = dr.download_dir + '/appsflyer 21년도 데이터'
file_list = os.listdir(raw_dir)

dtypes = {
    'date': pa.string(),
    'attributed_touch_time' : pa.string(),
    'install_time' : pa.string(),
    'media_source' : pa.string(),
    'campaign' : pa.string(),
    'adset' : pa.string(),
    'ad' : pa.string(),
    'event_name' : pa.string()
}
index_columns = list(dtypes.keys())
convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
ro = pacsv.ReadOptions(block_size=10 << 20)

table_list = []
for file in file_list :
    temp = pacsv.read_csv(raw_dir + '/' + file, convert_options=convert_ops, read_options=ro)
    table_list.append(temp)

table = pa.concat_tables(table_list)
df = table.to_pandas()

df[['attributed_touch_time', 'install_time']] = df[['attributed_touch_time', 'install_time']].apply(lambda x : pd.to_datetime(x))
df.loc[df['attributed_touch_time'].isnull(), 'attributed_touch_time'] = df['install_time']
df['ITET'] = df['install_time'] - df['attributed_touch_time']

df = df.loc[df['media_source'].isin(['googleadwords_int', 'adisonofferwall_int', 'Apple Search Ads', 'cashfriends_int', 'criteonew_int', 'madup_int', 'nswitch_int'])]
df = df.loc[df['ITET'] < datetime.timedelta(1)]
df['Cnt'] = 1

pivot_columns = ['date', 'media_source', 'campaign', 'adset']
for col in pivot_columns  :
    df[col] = df[col].fillna('')

df_pivot = df.pivot_table(index = pivot_columns, values = 'Cnt', columns = 'event_name', aggfunc='sum').reset_index()
df_pivot.to_csv(dr.download_dir + '/2021_rd_pivot.csv', index=False, encoding = 'utf-8-sig')