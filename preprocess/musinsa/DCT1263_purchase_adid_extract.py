import os
import setting.directory as dr
import pyarrow as pa
import pandas as pd
from workers import read_data

def read_dmp():
    file_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/DCT1263'
    file_list = os.listdir(file_dir)

    dtypes = {
        'advertising_id': pa.string(),
        'idfa': pa.string()}

    result_data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    result_data.loc[result_data['advertising_id']== '', 'advertising_id'] = result_data['idfa']
    result_data = result_data.loc[result_data['advertising_id'] != '']
    result_data = result_data[['advertising_id']].rename(columns = {'advertising_id' : 'adid'})

    return result_data

def read_dropbox():
    file_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/무신사/★ 무신사 통합/ADID'
    file_list = os.listdir(file_dir)
    files = [f for f in file_list if '.csv' in f]
    files = [f for f in files if 'af_purchase' in f]

    dtypes = {
        '_col0': pa.string()}

    result_data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
    result_data = result_data.rename(columns = {'_col0' : 'adid'})

    return result_data

dmp_data = read_dmp()
dropbox_data = read_dropbox()

adid_data = pd.concat([dmp_data,dropbox_data])
adid_data = adid_data.drop_duplicates()
adid_data = adid_data.loc[adid_data['adid'] != '00000000-0000-0000-0000-000000000000']

adid_data.to_csv(dr.dropbox_dir + '/광고사업부/4. 광고주/무신사/★ 무신사 통합/ADID/af_purchase_202201-202304.csv')