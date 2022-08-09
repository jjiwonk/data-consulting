import os
import pyarrow as pa
import pyarrow.csv as pacsv

def adjust_data_read(raw_dir, dtypes, len_of_folder = 9, media_filter = []):
    folder = os.listdir(raw_dir)
    folder = [f for f in folder if len(f)==len_of_folder]

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    for period in folder:
        daily_dir = raw_dir + '/' + period
        daily_files = os.listdir(daily_dir)
        daily_files = [f for f in daily_files if '.csv' in f]

        for f in daily_files:
            temp = pacsv.read_csv(daily_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
        print(period + ' 완료')

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()

    if len(media_filter) > 0:
        raw_data = raw_data.loc[raw_data['{network_name}'].isin(media_filter)]

    return raw_data