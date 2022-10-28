import setting.directory as dr
import setting.report_date as rdate
import pandas as pd
import os
import datetime
import json

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/W컨셉/리포트 자동화/appsflyer_adid_prism/APT-3614'
result_dir = dr.download_dir

file_path = raw_dir
file_list = os.listdir(file_path)
raw_data = pd.DataFrame()
for file in file_list:
    file_df = pd.read_csv(file_path + '/' + file, header=0, encoding='utf-8-sig')
    file_df['event_value'] = file_df['event_value'].astype(str)
    file_df['PRS'] = file_df['event_value'].apply(lambda x:x.split('af_item_code')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').split(',')[0] if x.find(
    'af_item_code') != -1 else 0 )
    prs_data = pd.read_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/prs_raw.csv', encoding='utf-8-sig')
    prs_data['PRS'] = prs_data['PRS'].astype(int)
    file_df['PRS'] = file_df['PRS'].astype(int)
    merge_data = pd.merge(file_df, prs_data, left_on='PRS', right_on='PRS', how='left')
    merge_data = merge_data.dropna(subset=['포함여부'])
    raw_data = pd.concat([raw_data, merge_data])

raw_data = raw_data.dropna(subset=['advertising_id'])
raw_data.to_csv(result_dir+'/wconcept_adid.csv',index= False)

# adid 기준으로 다시 정제
adid_df = pd.read_csv(result_dir+'/wconcept_adid.csv')
adid_df = adid_df.drop_duplicates(subset=['advertising_id'],keep= 'first')
adid_df = adid_df['advertising_id']
adid_df.to_csv(result_dir+'/wconcept_adid_f.csv',index= False)



