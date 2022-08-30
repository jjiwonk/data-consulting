import os
import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
from dateutil.relativedelta import relativedelta
import datetime
import numpy as np

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/크림/자동화 리포트/appsflyer_prism_content_view'
result_dir = dr.download_dir

def kream_rawdata_read(yearmonth):
    # raw 데이터 read
    raw_files = os.listdir(raw_dir)
    # raw_files = [f for f in raw_files if [year for year in list(range(int(str(start_date)[:-2]),int(str(end_date)[:-2])+1)) if str(year) in f]]
    start_date = datetime.datetime(int(str(yearmonth)[:4]),int(str(yearmonth)[4:]),1).strftime('%Y%m%d')
    end_date = (datetime.datetime(int(str(yearmonth)[:4]),int(str(yearmonth)[4:])+1,1) - relativedelta(days=1)).strftime('%Y%m%d')
    raw_files = [f for f in raw_files if (int(str(f)[-12:-4]) >= int(start_date)) & (int(str(f)[-12:-4]) <= int(end_date))]

    dtypes = {
        'attributed_touch_type': pa.string(),
        'attributed_touch_time': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'media_source': pa.string(),
        'site_id': pa.string(),
        'sub_site_id': pa.string(),
        'appsflyer_id': pa.string(),
        'platform': pa.string(),
        'device_type': pa.string(),
        'is_retargeting': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)

    table_list = []
    for f in raw_files:
        try:
            temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
        except Exception as e:
            print(e)

    print('원본 데이터 Read 완료')

    table = pa.concat_tables(table_list)
    df = table.to_pandas()

    return df

def calcuate_retention(raw_df):
    select_media_df = raw_df.loc[(raw_df['media_source'] == 'appier_int')|(raw_df['media_source'] == 'cauly_int')]
    select_media_df = select_media_df.sort_values(['site_id', 'appsflyer_id', 'install_time'])
    # 에피어, 카울리 매체만 추출

    user_list = select_media_df.loc[select_media_df['event_name']=='install'].drop_duplicates(subset=['appsflyer_id'],keep='first')[['media_source','site_id','appsflyer_id','install_time']]
    user_list.install_time = user_list.install_time.apply(pd.to_datetime)
    user_list['date'] = user_list['install_time'].apply(lambda x: x.date())
    # 해당 매체를 통해 설치 진행한 유저

    engage_df = raw_df.loc[raw_df['event_name']=='af_content_view'][['appsflyer_id','event_name','event_time']]
    engage_df.event_time = engage_df.event_time.apply(pd.to_datetime)
    # 리인게이지먼트(이하 REG) 이벤트 관련 데이터만 추출

    merge_df = user_list.merge(engage_df, how='left', left_on=['appsflyer_id'], right_on=['appsflyer_id'])
    # 설치 유저 apps_id 기준 REG 이벤트 left 조인

    merge_df['time_dif'] = (merge_df['event_time'] - merge_df['install_time'])
    merge_df = merge_df.loc[pd.notnull(merge_df['time_dif'])]
    merge_df['time_dif'] = merge_df['time_dif'].apply(lambda x: x.days)
    # 재방문 기간 계산
    merge_df['date'] = merge_df['install_time'].apply(lambda x: x.date())
    # install_time > date 컬럼 생성

    total_df = user_list.pivot_table(columns=['media_source','date','site_id'], values=['appsflyer_id'], aggfunc='count').T.reset_index()
    total_df = total_df.rename(columns = {'appsflyer_id':'total_user'})
    # 매체, 일자, site_id 별 전체 유저 수 피봇팅

    day1_df = merge_df.loc[(merge_df['time_dif'] >= 1) & (merge_df['time_dif'] < 2)]
    day1_df = day1_df.drop_duplicates(['appsflyer_id'])
    day1_df = day1_df.pivot_table(columns=['media_source','date','site_id'], values=['appsflyer_id'], aggfunc='count').T.reset_index()
    day1_df = day1_df.rename(columns = {'appsflyer_id':'day1_user'})
    # 매체, 일자, site_id 별 day1 유저 수 피봇팅

    anaysis_df = total_df.merge(day1_df, how='left', left_on=['media_source','date','site_id'], right_on=['media_source','date','site_id'])
    # 전체 유저수 피봇 df - day1 유저수 피봇 df 조인

    def get_retention(row):
        if row['day1_user'] > 0:
            return round(row['day1_user']/row['total_user']*100, 2)
        else:
            return 0
    anaysis_df['day1_user'] = anaysis_df['day1_user'].fillna(0)
    anaysis_df['day1_retention'] = anaysis_df.apply(get_retention, axis=1)
    # 리텐션 계산

    return anaysis_df

# 원하는 년월 int형으로 기입
yearmonth = 202205
raw_df = kream_rawdata_read(yearmonth)
result_df = calcuate_retention(raw_df)

result_df.to_csv(dr.download_dir + f'/day1_cv_retention_result_{yearmonth}.csv', encoding='utf-8-sig', index=False)
# 결과 파일 다운로드