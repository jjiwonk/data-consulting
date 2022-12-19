import pyarrow as pa
import pyarrow.csv as pacsv

def pyarrow_csv(dtypes, directory, file_list, encoding = 'utf-8-sig'):
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding = encoding)
    table_list = []

    for file in file_list:
        temp = pacsv.read_csv(directory + '/' + file, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
        print(file + ' Read 완료.')

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()


    return raw_data
