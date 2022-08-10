import os
import pandas as pd
import datetime

# 크림 드롭박스 폴더에 넣고 실행할 경우 경로를 자동으로 잡아옴
working_dir = os.getcwd().replace('\\', '/')
raw_dir = working_dir + '/앱스플라이어_머징'

# 원하는 시작 및 종료 날짜 입력
start_date = datetime.date(2022,8,1)
end_date = datetime.date(2022,8,5)

yearmonth = start_date.strftime('%Y%m')


aos_file = f'df_aos_{yearmonth}.csv'
ios_file = f'df_ios_{yearmonth}.csv'

aos_raw = pd.read_csv(raw_dir + '/' + aos_file)
ios_raw = pd.read_csv(raw_dir + '/' + ios_file)

raw_data = pd.concat([aos_raw, ios_raw], sort=False, ignore_index=True)

raw_data['event_time'] = pd.to_datetime(raw_data['event_time'])
raw_data = raw_data.loc[(raw_data['event_time'].dt.date >= start_date) & (raw_data['event_time'].dt.date <= end_date)]
ncpi_media = ['appier_int', 'cauly_int']

raw_ncpi = raw_data.loc[raw_data['media_source'].isin(ncpi_media)]
raw_ncpi.loc[raw_ncpi['media_source']=='cauly_int', 'site_id'] = raw_ncpi['sub_param_2']
raw_ncpi['Publisher'] = raw_ncpi['site_id'].apply(lambda x : str(x).split('_')[0])
raw_ncpi['cnt'] = 1

site_id_pivot = raw_ncpi.pivot_table(index = ['media_source', 'Publisher','site_id'], columns = 'event_name', values = 'cnt', aggfunc='sum', margins=True)
site_id_pivot = site_id_pivot.reset_index()
site_id_pivot = site_id_pivot.fillna(0)
site_id_pivot = site_id_pivot.rename(columns = {'All':'click'})
site_id_pivot['clk to ins CVR'] = site_id_pivot['install'] / site_id_pivot['click']
site_id_pivot['clk to ins CVR high'] = site_id_pivot['clk to ins CVR'].apply(lambda x : True if float(x)>=0.3 else False)
site_id_pivot['ins to reg CVR'] = site_id_pivot['af_complete_registration'] / site_id_pivot['install']
site_id_pivot['ins to reg CVR high'] = site_id_pivot['ins to reg CVR'].apply(lambda x : True if float(x)>=0.3 else False)

site_id_pivot.to_csv(working_dir + f'/ncpi publisher/ncpi_data_{yearmonth}.csv', index = False, encoding = 'utf-8-sig')