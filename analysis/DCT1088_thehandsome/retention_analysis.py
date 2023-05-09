from analysis.DCT1088_thehandsome.data_prep import *
from workers import func

# raw 데이터 로드 및 가공
df = get_raw_data('organic')
df['is_paid'] = False
total_raw = get_total_raw_data('both', df)
purchase_df = prep_purchase_raw_data(total_raw)
purchase_df = purchase_df.loc[purchase_df['uniquer_user_id'] != ''].reset_index(drop=True)
purchase_df = purchase_df.rename(columns={'member_id': 'real_member_id', 'uniquer_user_id': 'member_id'})
purchase_df.to_csv(dr.download_dir + '/purchase_log.csv', index=False, encoding='utf-8-sig')



################ EDA backlog ################
# 첫 구매 유저 여부
first_purchase_user_df = purchase_df.loc[purchase_df['is_first_purchase_user'] == True]
first_purchase_user_df = first_purchase_user_df.sort_values(['uniquer_user_id', 'event_time']).reset_index(drop=True)
len(set(first_purchase_user_df['uniquer_user_id']))

# 평균 재구매 주기
temp_first_purchase = first_purchase_user_df.loc[first_purchase_user_df['event_name']=='af_first_purchase', ['member_id', 'event_time']]
temp_first_purchase = temp_first_purchase.rename(columns={'event_time': 'first_purchase_time'})

first_purchase_user_df = pd.merge(first_purchase_user_df, temp_first_purchase, how='left', on='member_id')
first_purchase_user_df['purchase_diff'] = (first_purchase_user_df['event_time'] - first_purchase_user_df['first_purchase_time']).apply(lambda x: x.total_seconds())
second_purchase_df = first_purchase_user_df.loc[first_purchase_user_df['event_name'] != 'af_first_purchase'].drop_duplicates(['member_id'])
second_purchase_df.loc[second_purchase_df['purchase_diff'] > 0, 'purchase_diff'].describe()

