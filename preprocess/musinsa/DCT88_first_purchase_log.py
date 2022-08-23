import setting.directory as dr
import setting.report_date as rdate

import os
import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/무신사/★ 무신사 통합/raw_data/appsflyer'

def musinsa_rawdata_read():
    raw_files = os.listdir(raw_dir)
    raw_files = [f for f in raw_files if '.csv' in f]
    raw_files = [f for f in raw_files if (int(str(f)[-8:]) >= 20220601) | (int(str(f)[-8:]) <= 20220731)]

    dtypes = {
        'attributed_touch_type' : pa.string(),
        'event_name': pa.string(),
        'is_primary_attribution' : pa.string(),
        'customer_id' : pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)

    table_list = []
    for f in raw_files:
        try:
            temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        except:
            print(f)
        table_list.append(temp)

    print('원본 데이터 Read 완료')

    table = pa.concat_tables(table_list)
    df = table.to_pandas()

    return df

df = musinsa_rawdata_read()

con1 = (df['attributed_touch_type'] == 'click')
con2 = (df['event_name'] == 'first_purchase')
con3 = (df['is_primary_attribution'] == 'TRUE')

filtered_data = df.loc[con1 & con2 & con3]
filtered_data['customer_id'] = filtered_data['customer_id'].fillna(0)
filtered_data.to_csv(dr.download_dir + "/musinsa_first_purchase_log_with_cuid.csv", encoding='utf-8-sig', index=False)
# 로컬 download 디렉토리에 저장되도록 수정
