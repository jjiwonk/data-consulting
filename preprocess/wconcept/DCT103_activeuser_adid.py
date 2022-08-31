import setting.directory as dr
import setting.report_date as rdate
import pandas as pd

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉'
result_dir = raw_dir

def raw_data_read(raw_dir):
    data = pd.read_csv(raw_dir + f'/prism_wconcept_{rdate.yearmonth}.csv')
    return data

def adid_unique(raw_data):
    raw_data = raw_data.loc[pd.notnull(raw_data['advertising_id'])]

    raw_data = raw_data.sort_values('event_time')
    raw_data = raw_data.drop_duplicates('advertising_id')
    raw_data.to_csv(result_dir + f'/wconcept_adid_unique_all_columns_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')

    raw_data = raw_data[['advertising_id']]
    raw_data.to_csv(result_dir + f'/wconcept_adid_unique_{rdate.yearmonth}.csv', index=False, encoding = 'utf-8-sig')

raw_data = raw_data_read(raw_dir)
adid_unique(raw_data)