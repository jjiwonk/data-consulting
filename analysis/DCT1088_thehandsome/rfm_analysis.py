from analysis.DCT1088_thehandsome.data_prep import *


# raw 데이터 로드 및 가공
raw_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/더한섬/raw_data'
result_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/더한섬/result_data'
# total_raw = get_total_raw_data()
organic_3raw = get_total_raw_data('organic_3월')
organic_3df = prep_df(organic_3raw)
organic_3df.to_csv(result_dir + '/total_raw/organic_3_raw.csv', index=False, encoding='utf-8-sig')
organic_3purchase = organic_3df.loc[organic_3df['event_name'].isin(['af_first_purchase', 'af_purchase'])]
organic_3purchase.to_csv(result_dir + '/purchase_data/organic_3_purchase_raw.csv', index=False, encoding='utf-8-sig')

organic_4raw = get_total_raw_data('organic_4월')
organic_4df = prep_df(organic_4raw)
organic_4df.to_csv(result_dir + '/total_raw/organic_4_raw.csv', index=False, encoding='utf-8-sig')
organic_4purchase = organic_4df.loc[organic_4df['event_name'].isin(['af_first_purchase', 'af_purchase'])]
organic_4purchase.to_csv(result_dir + '/purchase_data/organic_4_purchase_raw.csv', index=False, encoding='utf-8-sig')

paid_raw = get_total_raw_data('paid')
paid_df = prep_df(paid_raw)
paid_df.to_csv(result_dir + '/total_raw/paid_raw.csv', index=False, encoding='utf-8-sig')
paid_purchase = paid_df.loc[paid_df['event_name'].isin(['af_first_purchase', 'af_purchase'])]
paid_purchase.to_csv(result_dir + '/purchase_data/paid_purchase_raw.csv', index=False, encoding='utf-8-sig')

# temp = total_df_identified.drop_duplicates(['unique_user_id', 'is_paid']).unique_user_id.value_counts()
# dup_user_list = temp[temp >= 2].index.tolist()
# total_df_identified.loc[total_df_identified['is_paid'] == True, 'is_paid'] = 'paid'
# total_df_identified.loc[total_df_identified['is_paid'] == False, 'is_paid'] = 'organic'
# total_df_identified.loc[total_df_identified['unique_user_id'].isin(dup_user_list), 'is_paid'] = 'both'

# 구매 데이터 병합
purchase_raw = pd.concat([organic_3purchase, organic_4purchase, paid_purchase], ignore_index=True)
purchase_df = prep_purchase_raw_data(purchase_raw)
af_first_purchase_user_list = set(purchase_df.loc[purchase_df['is_first_purchase_user'] == True, 'unique_user_id'])
# 진성 유저 판별
rfm_segment_df = prep_rfm_segment_df(purchase_df)
quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'unique_user_id'])
# 진성 구매 유저 구분
purchase_df['is_quality_user'] = purchase_df['unique_user_id'].apply(lambda x: True if x in quality_user_list else False)
purchase_df_download = purchase_df.rename(columns={'member_id': 'real_member_id', 'unique_user_id': 'member_id'}).copy()
purchase_df_download.to_csv(result_dir + '/purchase_data/purchase_log.csv', index=False, encoding='utf-8-sig')
# 태블로 RFM 대시보드 생성 시 위 데이터 활용
# pd.DataFrame(af_first_purchase_user_list).to_csv(dr.download_dir + '/af_first_purchase_user_list.csv', index=False)
# pd.DataFrame(quality_user_list).to_csv(dr.download_dir + '/quality_user_list.csv', index=False)
# af_first_purchase_user_list = set(pd.read_csv(dr.download_dir + '/af_first_purchase_user_list.csv', encoding='utf-8-sig').iloc[:, 0])
# quality_user_list = set(pd.read_csv(dr.download_dir + '/quality_user_list.csv', encoding='utf-8-sig').iloc[:, 0])

# total_raw 진성 구매 유저, 첫 구매 유저 구분 추가 가공
# 데이터 양이 많아 부분 별로 가공 후 저장
# 오가닉 3월
organic_3df = pd.read_csv(result_dir + '/total_raw/organic_3_raw.csv', encoding='utf-8-sig')
organic_3df['is_quality_user'] = organic_3df['unique_user_id'].apply(lambda x: True if x in quality_user_list else False)
organic_3df['is_first_purchase_user'] = organic_3df['unique_user_id'].apply(lambda x: True if x in af_first_purchase_user_list else False)
organic_3df.to_csv(result_dir + '/total_raw/organic_3_raw.csv', index=False, encoding='utf-8-sig')

# 오가닉 4월
organic_4df = pd.read_csv(result_dir + '/total_raw/organic_4_raw.csv', encoding='utf-8-sig')
organic_4df['is_quality_user'] = organic_4df['unique_user_id'].apply(lambda x: True if x in quality_user_list else False)
organic_4df['is_first_purchase_user'] = organic_4df['unique_user_id'].apply(lambda x: True if x in af_first_purchase_user_list else False)
organic_4df.to_csv(result_dir + '/total_raw/organic_4_raw.csv', index=False, encoding='utf-8-sig')

# 페이드
paid_df = pd.read_csv(result_dir + '/total_raw/paid_raw.csv', encoding='utf-8-sig')
paid_df['is_quality_user'] = paid_df['unique_user_id'].apply(lambda x: True if x in quality_user_list else False)
paid_df['is_first_purchase_user'] = paid_df['unique_user_id'].apply(lambda x: True if x in af_first_purchase_user_list else False)
paid_df.to_csv(result_dir + '/total_raw/paid_raw.csv', index=False, encoding='utf-8-sig')



################ EDA backlog ################
# # 첫 구매 유저 리스트
# af_first_purchase_df = purchase_df.loc[purchase_df['is_first_purchase_user'] == True]
# af_first_purchase_user_list = set(af_first_purchase_df.unique_user_id)
# organic_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'organic', 'unique_user_id'])
# paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'paid', 'unique_user_id'])
# organic_paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'both', 'unique_user_id'])
#
# # 총 구매 유저 리스트 및 로그
# total_purchase_user_list = set(purchase_df['unique_user_id'])  # rfm_segment_df 행수와 동일
# total_purchase_user_log = total_df_identified.loc[total_df_identified['unique_user_id'].isin(list(total_purchase_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
# len(total_purchase_user_list)
#
#
# # 운영팀 제공 진성 유저
# quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'unique_user_id'])
# quality_user_log = total_df_identified.loc[total_df_identified['unique_user_id'].isin(list(quality_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
# quality_user_log.is_paid.value_counts()
# len(quality_user_log.unique_user_id.unique())
#
# len(af_first_purchase_user_list & quality_user_list)
# # order_id 중복 제거 전
# total_df_identified.loc[total_df_identified['event_name'].isin(['af_purchase', 'af_first_purchase'])].event_revenue.astype(float).sum()
#
# # 오가닉 구매 & 진성 구매 유저 로그
# organic_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == 'organic', 'unique_user_id'])
# len(organic_purchase_user_list)
# organic_cross_user_list = organic_purchase_user_list & quality_user_list
# len(organic_cross_user_list)
# organic_quality_user_log = quality_user_log.loc[quality_user_log['unique_user_id'].isin(list(organic_cross_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
# organic_first_cross_user_list = organic_af_first_purchase_user_list & quality_user_list
# len(organic_first_cross_user_list)
#
# # 광고 구매 & 진성 구매 유저 로그
# paid_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == 'paid', 'unique_user_id'])
# len(paid_purchase_user_list)
# paid_cross_user_list = paid_purchase_user_list & quality_user_list
# len(paid_cross_user_list)
# paid_quality_user_log = quality_user_log.loc[quality_user_log['unique_user_id'].isin(list(paid_cross_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
#
# # 오가닉 & 페이드
# organic_paid_user_list = set(purchase_df.loc[purchase_df['is_paid'] == '', 'member_id'])
# organic_paid_cross_user_list = organic_paid_user_list & quality_user_list
# len(organic_paid_cross_user_list)
# organic_paid_quality_user_log = quality_user_log.loc[quality_user_log['member_id'].isin(list(organic_paid_cross_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)
#
# # 진성 구매 유저 로그 추출(오가닉, 광고 유저 여부 컬럼 추가)
# quality_user_log['is_paid_user'] = ''
# quality_user_log.loc[quality_user_log['member_id'].isin(organic_cross_user_list), 'is_paid_user'] = False
# quality_user_log.loc[quality_user_log['member_id'].isin(paid_cross_user_list), 'is_paid_user'] = True
# quality_user_log.to_csv(dr.download_dir + '/quality_user_log.csv', index=False, encoding='utf-8-sig')
