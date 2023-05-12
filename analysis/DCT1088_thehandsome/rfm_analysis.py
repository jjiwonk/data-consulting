from analysis.DCT1088_thehandsome.data_prep import *
from workers import func


# raw 데이터 로드 및 가공
total_raw = get_total_raw_data()
total_df_identified = total_raw.loc[total_raw['unique_user_id'].notnull()]
purchase_df = prep_purchase_raw_data(total_df_identified)
# drop_purchase_df = purchase_df.loc[purchase_df['unique_user_id'].isnull()]
# purchase_df_identified = purchase_df.loc[purchase_df['unique_user_id'].notnull()]
purchase_df_download = purchase_df.rename(columns={'member_id': 'real_member_id', 'unique_user_id': 'member_id'}).copy()
purchase_df_download.to_csv(dr.download_dir + '/purchase_log.csv', index=False, encoding='utf-8-sig')
# 태블로 RFM 대시보드 생성 시 위 데이터 활용

temp = total_raw.drop_duplicates(['unique_user_id', 'is_paid']).unique_user_id.value_counts()
dup_user_list = temp[temp >= 2].index.tolist()
total_raw.loc[total_raw['is_paid'] == True, 'is_paid'] = 'paid'
total_raw.loc[total_raw['is_paid'] == False, 'is_paid'] = 'organic'
total_raw.loc[total_raw['unique_user_id'].isin(dup_user_list), 'is_paid'] = 'both'

total_raw.event_name.unique()
total_raw['content_type'] = get_event_from_values(np.array(total_raw['event_value']), 'af_content_type')

################ EDA backlog ################
# 첫 구매 유저 리스트
af_first_purchase_df = purchase_df.loc[purchase_df['is_first_purchase_user'] == True]
af_first_purchase_user_list = set(af_first_purchase_df.unique_user_id)
organic_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'organic', 'unique_user_id'])
paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'paid', 'unique_user_id'])
organic_paid_af_first_purchase_user_list = set(af_first_purchase_df.loc[af_first_purchase_df['is_paid'] == 'both', 'unique_user_id'])

# 총 구매 유저 리스트 및 로그
total_purchase_user_list = set(purchase_df['unique_user_id'])  # rfm_segment_df 행수와 동일
total_purchase_user_log = total_raw.loc[total_raw['unique_user_id'].isin(list(total_purchase_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
len(total_purchase_user_list)

# 진성 유저 판별
rfm_segment_df = prep_rfm_segment_df(purchase_df)

# 운영팀 제공 진성 유저
quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'unique_user_id'])
quality_user_log = total_raw.loc[total_raw['unique_user_id'].isin(list(quality_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
quality_user_log.is_paid.value_counts()
len(quality_user_log.unique_user_id.unique())

len(af_first_purchase_user_list & quality_user_list)

# 진성 구매 유저
purchase_df['is_quality_user'] = purchase_df['unique_user_id'].apply(lambda x: True if x in quality_user_list else False)
purchase_df.pivot_table(index='is_quality_user', aggfunc='sum')
# order_id 중복 제거 전
total_raw.loc[total_raw['event_name'].isin(['af_purchase', 'af_first_purchase'])].event_revenue.astype(float).sum()

# 오가닉 구매 & 진성 구매 유저 로그
organic_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == 'organic', 'unique_user_id'])
len(organic_purchase_user_list)
organic_cross_user_list = organic_purchase_user_list & quality_user_list
len(organic_cross_user_list)
organic_quality_user_log = quality_user_log.loc[quality_user_log['unique_user_id'].isin(list(organic_cross_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)
organic_first_cross_user_list = organic_af_first_purchase_user_list & quality_user_list
len(organic_first_cross_user_list)

# 광고 구매 & 진성 구매 유저 로그
paid_purchase_user_list = set(purchase_df.loc[purchase_df['is_paid'] == 'paid', 'unique_user_id'])
len(paid_purchase_user_list)
paid_cross_user_list = paid_purchase_user_list & quality_user_list
len(paid_cross_user_list)
paid_quality_user_log = quality_user_log.loc[quality_user_log['unique_user_id'].isin(list(paid_cross_user_list))].sort_values(['unique_user_id', 'event_time'], ignore_index=True)

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
