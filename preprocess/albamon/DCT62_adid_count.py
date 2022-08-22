import setting.directory as dr
from preprocess.albamon import raw_data
import pyarrow as pa
import pandas as pd

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/7월'
dtypes = {
    '{activity_kind}': pa.string(),
    '{event_name}': pa.string(),
    '{network_name}': pa.string(),
    '{app_name}' : pa.string(),
    '{gps_adid}': pa.string(),
    '{idfa}': pa.string()
}

df = raw_data.adjust_data_read(raw_dir = raw_dir, dtypes = dtypes, len_of_folder=9, media_filter=['Organic'])
df.info()

event_names = ["이력서작성_최초등록","이력서작성_추가등록","개인 회원가입","기업 회원가입","무료공고등록","유료공고등록","온라인 지원","간편문자지원","이메일지원","전화지원","홈페이지지원","일반문자지원"]
check = df[(df["{activity_kind}"] == "event")&(df['{event_name}'].isin(event_names))]

check['ADID'] = check.apply(lambda x: x['{gps_adid}'] if 'com.' in x['{app_name}'] else x['{idfa}'], axis=1)
check['OS'] = check.apply(lambda x: 'AOS' if 'com.' in x['{app_name}'] else 'IOS', axis=1)
check.info()

check[['ADID','OS']].nunique()

adid_list = check[['ADID','OS']].drop_duplicates(keep='first', ignore_index=True)
adid_list = adid_list.loc[adid_list['ADID'] != '']

adid_list.to_csv(dr.download_dir + "/adid_list.csv", encoding='utf-8-sig', index=False)
# 조건에 맞는 이벤트 전환이 발생한 ADID 리스트(OS 구분 포함)

len(adid_list)
# 조건에 맞는 이벤트 전환이 발생한 ADID 개수