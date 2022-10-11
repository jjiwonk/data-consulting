import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import setting.directory as dr
import setting.report_date as rdate

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/9월/MAU'
# raw_dir = 'D:/매드업/데이터분석/알바몬'
result_dir = dr.download_dir

def get_raw_data(raw_dir):
    dtypes = {
        'activity_kind': pa.string(),
        'event_name': pa.string(),
        'app_version': pa.string(),
        'network_name': pa.string(),
        'created_at': pa.string(),
        'engagement_time':pa.string(),
        'adid': pa.string(),
        'Country': pa.string(),
        'language': pa.string(),
        'mcc' : pa.string(),
        'mnc': pa.string(),
        'device_manufacturer' : pa.string(),
        'gps_adid' : pa.string(),
        'idfa': pa.string(),
        'app_version_short': pa.string(),
        'sdk_version': pa.string(),
        'os_version': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8')
    table_list = []

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    print('Raw data read 완료')

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()

    return raw_data

raw_data = get_raw_data(raw_dir)

event_names = ["이력서작성_최초등록","이력서작성_추가등록","개인 회원가입","기업 회원가입","무료공고등록","유료공고등록","온라인 지원","간편문자지원","이메일지원","전화지원","홈페이지지원","일반문자지원"]
check_df = raw_data.loc[raw_data['event_name'].isin(event_names)]
check_df['adid'] = check_df.apply(lambda x: x['gps_adid'] if 'android' in x['sdk_version'] else x['adid'], axis=1)
check_df[['created_at', 'engagement_time']] = check_df[['created_at', 'engagement_time']].apply(pd.to_datetime)
check_df['date'] = check_df['created_at'].apply(lambda x: x.date())

prep_df = check_df.pivot_table(index=['date','adid','event_name'], values='activity_kind', aggfunc='count').reset_index().rename(columns={'activity_kind':'counts'})
prep_df['over5_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 5 and x < 10 else 0)
prep_df['over10_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 10 and x < 20 else 0)
prep_df['over20_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 20 else 0)

prep_raw_df = prep_df.pivot_table(index='adid', columns='event_name', values=['counts','over5_days','over10_days','over20_days'], aggfunc='sum').reset_index().fillna(0)
prep_raw_df.to_excel(dr.download_dir + '/알바몬_중복전환_ADID.xlsx', encoding='utf-8')


# prep_over5 = prep_df.loc[(prep_df['counts'] >= 5) & (prep_df['counts'] < 10)]
# prep_over10 = prep_df.loc[(prep_df['counts'] >= 10) & (prep_df['counts'] < 20)]
# prep_over20 = prep_df.loc[prep_df['counts'] >= 20]
# prep_rate_df = prep_over5.pivot_table(index='adid', columns='event_name', values='date', aggfunc='count').reset_index().fillna(0)
# total_events = prep_df.pivot_table(index='event_name', values='date', aggfunc='count').reset_index().fillna(0).rename(columns={'date':'counts'})
# total_adid = pd.DataFrame({'event_name':['adid'], 'counts':[len(prep_df.adid.unique())]})
# total_events = pd.concat([total_events, total_adid], axis=0)
#
# total_df = pd.DataFrame()
#
# writer = pd.ExcelWriter(result_dir + '/알바몬_중복전환_ADID.xlsx', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}})
# prep_over5.to_excel(writer, sheet_name='중복전환 5건이상 10건 미만', encoding='utf-8-sig', index=False)
# prep_over10.to_excel(writer, sheet_name='중복전환 10건이상 20건 미만', encoding='utf-8-sig', index=False)
# prep_over20.to_excel(writer, sheet_name='중복전환 20건이상', encoding='utf-8-sig', index=False)
# prep_rate_df.to_excel(writer, sheet_name='중복전환 비율 raw', encoding='utf-8-sig', index=False)
# writer.close()