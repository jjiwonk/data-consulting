from analysis.DCT512_wconcept import info
import pandas as pd
import numpy as np
import re
import datetime
import pyarrow as pa
import pyarrow.csv as pacsv
import os
import json

file_dir = info.paid_dir

def data_read():
    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'original_url': pa.string(),
        'attributed_touch_time': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'ad' : pa.string(),
        'event_value': pa.string()}

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(file_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(file_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)

    raw_df = table.to_pandas()

    return raw_df

df = data_read()



