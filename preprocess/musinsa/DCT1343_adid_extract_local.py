import setting.directory as dr
import os
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime

class setting:
    raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/무신사/★ 무신사 통합/raw_data/appsflyer'
    start_date = datetime.date(year=2022, month= 12, day= 1)
    end_date = datetime.date(year=2023, month= 5, day= 1)
    result_dir = dr.download_dir + '/musinsa_adid_extract.csv'

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

def read_data():

    dtypes = {
        'event_time': pa.string(),
        'event_name': pa.string(),
        'advertising_id': pa.string(),
        'idfa': pa.string()}

    files = os.listdir(setting.raw_dir)

    delta = datetime.timedelta(days=1)
    date_list = []
    while setting.start_date <= setting.end_date:
        date_list.append(str(setting.start_date).replace('-',''))
        setting.start_date += delta

    read_files = [f for f in files if any(x in f for x in date_list)]
    raw_data = pyarrow_csv(dtypes, setting.raw_dir, read_files)

    return raw_data

def data_prep():

    raw_data = read_data()

    raw_data_filter = raw_data.loc[raw_data['event_name'].isin(['af_purchase','af_complete_registration'])]
    raw_data_filter.loc[raw_data_filter['advertising_id'] == '', 'advertising_id'] = raw_data_filter['idfa']
    raw_data_filter = raw_data_filter.loc[(raw_data_filter['advertising_id'] != '') & (raw_data_filter['advertising_id'] != '00000000-0000-0000-0000-000000000000')]
    raw_data_filter = raw_data_filter[['event_name','advertising_id']].rename(columns={'advertising_id': 'adid'})

    purcahse_user = raw_data_filter.loc[raw_data_filter['event_name'] == 'af_purchase'].drop_duplicates()
    register_user = raw_data_filter.loc[raw_data_filter['event_name'] == 'af_complete_registration'].drop_duplicates()

    purcahse_user_list = purcahse_user['adid'].to_list()
    register_user_list = register_user.loc[~register_user['adid'].isin(purcahse_user_list)]
    register_user_list = register_user_list[['adid']]

    return register_user_list

adid = data_prep()
adid.to_csv(setting.result_dir, index = False, encoding = 'utf-8-sig')
