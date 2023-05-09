from analysis.DCT1088_thehandsome.data_prep import *
from workers import func


# raw 데이터 로드 및 가공
total_raw = get_total_raw_data()
purchase_df = prep_purchase_raw_data(total_raw)
purchase_df = purchase_df.loc[purchase_df['uniquer_user_id'] != '']
purchase_df = purchase_df.rename(columns={'member_id': 'real_member_id', 'uniquer_user_id': 'member_id'})
purchase_df.to_csv(dr.download_dir + '/purchase_log.csv', index=False, encoding='utf-8-sig')
# 태블로 RFM 대시보드 생성 시 위 데이터 활용




################ EDA backlog ################
# 첫 구매 유저 리스트
af_first_purchase_df = purchase_df.loc[purchase_df['event_name'] == 'af_first_purchase']
organic_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == False, 'member_id'])
paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == True, 'member_id'])
organic_paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == '', 'member_id'])

# 총 구매 유저 리스트 및 로그
total_user_list = set(purchase_df['member_id'])  # rfm_segment_df 행수와 동일
total_user_log = total_raw.loc[total_raw['member_id'].isin(list(total_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)
len(total_user_list)

# 진성 유저 판별
rfm_segment_df = prep_rfm_segment_df(purchase_df)

# 운영팀 제공 진성 유저
quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'member_id'])
quality_user_log = total_raw.loc[total_raw['member_id'].isin(list(quality_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)

# 진성 구매 유저
purchase_df['is_quality_user'] = purchase_df['member_id'].apply(lambda x: True if x in quality_user_list else False)
purchase_df.pivot_table(index='is_quality_user', aggfunc='sum')

# 오가닉 구매 & 진성 구매 유저 로그
organic_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == False, 'member_id'])
len(organic_purchase_user_list)
organic_cross_user_list = organic_purchase_user_list & quality_user_list
len(organic_cross_user_list)
organic_quality_user_log = quality_user_log.loc[quality_user_log['member_id'].isin(list(organic_cross_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)
# organic_quality_user_list = set(organic_quality_user_log.loc[organic_quality_user_log['event_name'].isin(['af_first_purchase', 'af_purchase']), 'member_id'])
organic_first_cross_user_list = organic_af_first_purchase_user_list & quality_user_list
len(organic_first_cross_user_list)

# 광고 구매 & 진성 구매 유저 로그
paid_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == True, 'member_id'])
len(paid_purchase_user_list)
paid_cross_user_list = paid_purchase_user_list & quality_user_list
len(paid_cross_user_list)
paid_quality_user_log = quality_user_log.loc[quality_user_log['member_id'].isin(list(paid_cross_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)
# paid_quality_user_list = set(paid_quality_user_log.loc[paid_quality_user_log['event_name'].isin(['af_first_purchase', 'af_purchase']), 'member_id'])

# 오가닉 & 페이드
organic_paid_user_list = set(purchase_df.loc[purchase_df['is_paid'] == '', 'member_id'])
organic_paid_cross_user_list = organic_paid_user_list & quality_user_list
len(organic_paid_cross_user_list)
organic_paid_quality_user_log = quality_user_log.loc[quality_user_log['member_id'].isin(list(organic_paid_cross_user_list))].sort_values(['member_id', 'event_time'], ignore_index=True)

# 진성 구매 유저 로그 추출(오가닉, 광고 유저 여부 컬럼 추가)
quality_user_log['is_paid_user'] = ''
quality_user_log.loc[quality_user_log['member_id'].isin(organic_cross_user_list), 'is_paid_user'] = False
quality_user_log.loc[quality_user_log['member_id'].isin(paid_cross_user_list), 'is_paid_user'] = True
quality_user_log.to_csv(dr.download_dir + '/quality_user_log.csv', index=False, encoding='utf-8-sig')
