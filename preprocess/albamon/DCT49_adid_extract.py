import setting.directory as dr
from preprocess.albamon import raw_data
import pyarrow as pa

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/7월'
dtypes = {
    '{activity_kind}': pa.string(),
    '{event_name}': pa.string(),
    '{network_name}' : pa.string(),
    '{adid}': pa.string()
}

df = raw_data.adjust_data_read(raw_dir = raw_dir, dtypes = dtypes, len_of_folder=9, media_filter=['Organic'])

event_names = ["이력서작성_최초등록","이력서작성_추가등록","개인 회원가입","기업 회원가입","무료공고등록","유료공고등록","온라인 지원","간편문자지원","이메일지원","전화지원","홈페이지지원","일반문자지원"]
check = df[(df["{activity_kind}"] == "event")&(df['{event_name}'].isin(event_names))]
check['{event_name}'].unique()

prep_df = check.pivot_table(index='{adid}',values='{event_name}',aggfunc='count').reset_index()

prep_df_over10 = prep_df[prep_df['{event_name}']>=10].rename(columns={'{event_name}':'counts'})
# check로 정의해둔 df 기준으로 피버팅하도록 수정, 조건에 해당하는 데이터는 별도 변수로 할당

prep_df_over10.to_csv(dr.download_dir + "/data_check.csv", encoding='utf-8-sig', index=False)
# 로컬 download 디렉토리에 저장되도록 수정
