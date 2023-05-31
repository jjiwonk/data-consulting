import datetime
import pandas as pd
from setting import directory as dr
from workers import read_data
from workers.func import segment_analysis
import os
import pyarrow as pa


def read_organic():
    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if ('in-app-events' in f)]

        dtypes = {
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Customer User ID': pa.string()}

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
        return data

    ios = read_file('ios')
    aos = read_file('aos')

    organic_data = pd.concat([ios,aos])

    organic_data['Event Time'] = pd.to_datetime(organic_data['Event Time'])
    organic_data = organic_data.loc[organic_data['Event Time'] >= datetime.datetime(year=2022, month=7, day=1)]

    return organic_data


def read_paid():
    file_dir1 = dr.dropbox_dir +  f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_af data (prism)'
    file_list1 = os.listdir(file_dir1)
    file_list1 = [file for file in file_list1 if '.csv' in file]
    file_dir2 = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_af data (prism)/AF RAW_2022'
    file_list2 = os.listdir(file_dir2)
    file_list2 = [file for file in file_list2 if '.csv' in file]

    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'campaign': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source': pa.string(),
        'advertising_id': pa.string(),
        'customer_user_id': pa.string(),
        'idfa': pa.string()}

    data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir1, file_list=file_list1)
    data2 = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir2, file_list=file_list2)
    data = pd.concat([data, data2])
    data = data.sort_values('event_time')

    data['event_time'] = pd.to_datetime(data['event_time'])
    data = data.drop_duplicates()
    paid_data = data.loc[(data['event_time'] >= datetime.datetime(year=2022, month=7, day=1)) & (data['event_time'] <= datetime.datetime(year=2023, month=5, day=1))]

    return paid_data


organic_data = read_organic()
organic_data.columns = [col.lower().replace(' ', '_') for col in organic_data.columns]
paid_data = read_paid()

# 디타겟 조건 적용을 위해 최소 2달치 데이터 확보 필요
today = datetime.datetime.today()
one_month_ago = today.replace(day=1) - datetime.timedelta(days=1)
two_month_ago = one_month_ago.replace(day=1) - datetime.timedelta(days=1)

total_data = pd.concat([organic_data, paid_data])
total_data['event_date'] = pd.to_datetime(total_data['event_time']).dt.date
# total_data = total_data.loc[(total_data['event_date']>=two_month_ago)&(total_data['event_date']<=today)]

del organic_data
del paid_data

# 디타겟 세그먼트 셋팅
media_list = ['appier_int', 'cauly_int', 'remerge_int', 'rtbhouse_int', 'kakao_int', 'googleadwords_int']
event_dict = {
            'Opened Finda App': {
                'seg_name': 'open',
                'period': 14
            },
            'loan_contract_completed': {
                'seg_name': 'loan',
                'period': 30
            },
            'Viewed LA Home No Result': {
                'seg_name': 'noresult',
                'period': 60
            },
        }
detarget_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
credit_file = pd.read_csv(detarget_dir + '/신용점수_202306.csv').drop_duplicates()
apppush_file = pd.read_csv(detarget_dir + '/앱푸시마수동_202306.csv').drop_duplicates()
clickuser_file = pd.read_csv(detarget_dir + '/클릭유저_202306.csv').drop_duplicates()
detarget_dict = {
            'credit': {
                'id_df': credit_file
            },
            'agree&mkt': {
                'id_df': apppush_file
            },
            'click': {
                'id_df': clickuser_file
            }
        }

# 세그먼트 별 디타겟 미적용 여부
segment_analysis = segment_analysis(total_data, event_dict, detarget_dict, media_list)
segment_analysis_df = segment_analysis.do_work()



##############################################
## 여기부터 태블로 대시보드용 추가 가공 코드 수정 필요
# 1. 보정 성과 반영
# 2. S3 데이터 업로드(?)
# 3. 태블로 연동
##############################################
last_day = today - datetime.timedelta(days=1)
str_last_day = str(last_day.date()).replace('-', '')

# 보정 성과 측정 raw 데이터
final_correct_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/fin_데일리 리포트'
adid_media_raw = pd.read_csv(final_correct_dir + f'/ADID/integrated_report_{str_last_day}.csv')
cuid_media_raw = pd.read_csv(final_correct_dir + f'/CUID/integrated_report_{str_last_day}.csv')

# 기존 성과 측정 raw 데이터
final_base_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/integrated_report_3'
final_media_raw = pd.read_csv(final_base_dir + f'/integrated_report_{str_last_day}.csv')
