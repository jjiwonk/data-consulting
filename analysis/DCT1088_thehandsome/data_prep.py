import os
import pyarrow as pa
import pandas as pd
import numpy as np
import datetime
import setting.directory as dr
from workers.read_data import pyarrow_csv
import json


def get_raw_data(event_folder):
    raw_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/더한섬/raw_data/{event_folder}'
    file_list = os.listdir(raw_dir)
    csv_list = []
    for file in file_list:
        if 'csv' in file:
            csv_list.append(file)
    dtypes = {
            'Attributed Touch Type': pa.string(),
            'Attributed Touch Time': pa.string(),
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Event Value': pa.string(),
            'Event Revenue': pa.string(),
            'Event Revenue KRW': pa.string(),
            'Event Source': pa.string(),
            # 'Partner': pa.string(),
            'Media Source': pa.string(),
            'Channel': pa.string(),
            'Campaign': pa.string(),
            # 'Campaign ID': pa.string(),
            # 'Adset': pa.string(),
            # 'Adset ID': pa.string(),
            # 'Ad': pa.string(),
            # 'Ad ID': pa.string(),
            # 'Ad Type': pa.string(),
            'Region': pa.string(),
            'Country Code': pa.string(),
            # 'State': pa.string(),
            'City': pa.string(),
            'Language': pa.string(),
            'AppsFlyer ID': pa.string(),
            # 'Advertising ID': pa.string(),
            'IDFA': pa.string(),
            'Android ID': pa.string(),
            'Customer User ID': pa.string(),
            # 'IMEI': pa.string(),
            # 'IDFV': pa.string(),
            'Platform': pa.string(),
            # 'Device Type': pa.string(),
            'Is Retargeting': pa.string(),
            # 'Retargeting Conversion Type': pa.string(),
            # 'User Agent': pa.string(),
            # 'Keyword ID': pa.string(),
            # 'Device Download Time': pa.string(),
            # 'Device Model': pa.string(),
            # 'Conversion Type': pa.string(),
            # 'Campaign Type': pa.string()
        }

    df = pyarrow_csv(dtypes=dtypes, directory=raw_dir, file_list=csv_list)
    cols = list(df.columns)
    rename_cols = {}
    for col in cols:
        rename_cols[col] = col.lower().replace(' ', '_')
    df = df.rename(columns=rename_cols)
    df.loc[df['is_retargeting'] == '', 'is_retargeting'] = False

    return df


def get_total_raw_data():
    organic_raw = get_raw_data('organic')
    paid_raw = get_raw_data('paid')
    organic_raw['is_paid'] = False
    paid_raw['is_paid'] = True
    # re_engagement_raw = get_raw_data('리인게이지먼트')
    # install_raw = get_raw_data('설치')
    # uninstall_raw = get_raw_data('앱삭제')
    # in_app_raw = get_raw_data('인앱이벤트')
    total_raw = pd.concat([organic_raw, paid_raw]).drop_duplicates().reset_index(drop=True)
    return total_raw


def prep_purchase_raw_data(df):
    purchase_raw = df.loc[df['event_name'].isin(['af_purchase', 'af_first_purchase'])]

    # 주문번호 기준 중복제거
    purchase_raw['order_id'] = purchase_raw['event_value'].apply(lambda x: json.loads(x)['af_order_id'] if 'af_order_id' in json.loads(x).keys() else '-')
    purchase_raw.sort_values('event_time', inplace=True)
    purchase_raw = purchase_raw.drop_duplicates('order_id')

    # 이벤트 시간 기준 유저의 첫 구매 여부 확인
    purchase_raw = purchase_raw.sort_values(['appsflyer_id', 'event_time']).reset_index(drop=True)
    temp_id = pd.concat([pd.Series(['']), purchase_raw['appsflyer_id']])[:-1].reset_index(drop=True)
    purchase_raw['compare_id'] = temp_id
    purchase_raw['is_first_purchase'] = purchase_raw.apply(lambda x: True if x['appsflyer_id'] != x['compare_id'] else False, axis=1)
    purchase_raw = purchase_raw.drop('compare_id', axis=1)
    purchase_raw['event_revenue_krw'] = purchase_raw['event_revenue_krw'].apply(pd.to_numeric).astype(int)
    purchase_raw[['attributed_touch_time', 'install_time', 'event_time']] = purchase_raw[['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime, axis=1)

    return purchase_raw


def prep_rfm_segment_df(df):
    # RFM 스코어 가공
    rfm_segment_raw = df.pivot_table(index=['appsflyer_id'],
                                     values=['event_time', 'event_revenue_krw', 'event_name'],
                                     aggfunc=['last', 'sum', 'count'])
    rfm_segment_raw = rfm_segment_raw.reset_index().reset_index(drop=True)
    rfm_segment_raw = rfm_segment_raw.iloc[:, [0, 3, 4, 5]]
    rfm_segment_raw.columns = ['appsflyer_id', 'recent_purchase_time', 'monetary', 'frequency']
    rfm_segment_raw['recency'] = rfm_segment_raw['recent_purchase_time'].apply(
        lambda x: (datetime.datetime.today() - x).total_seconds())
    rfm_segment_raw.loc[
        rfm_segment_raw['recency'] <= rfm_segment_raw['recency'].quantile(1), 'recent_purchase_time'].sort_values()

    def check_quality_user(row):
        if row['frequency'] >= 2 and row['monetary'] >= 2000000:
            return 1
        else:
            return 0
    rfm_segment_raw['is_quality_user'] = rfm_segment_raw.apply(check_quality_user, axis=1)

    def score_rfm(array, col_name):
        q1 = np.quantile(array, .25)
        q2 = np.quantile(array, .5)
        q3 = np.quantile(array, .75)
        result_array = array.copy()
        for i, data in enumerate(array):
            if col_name == 'recency':
                if data <= q1:
                    result_array[i] = 4
                elif data <= q2:
                    result_array[i] = 3
                elif data <= q3:
                    result_array[i] = 2
                else:
                    result_array[i] = 1
            elif col_name == 'monetary':
                top100 = rfm_segment_raw[col_name].sort_values(ascending=False).reset_index(drop=True)[99]
                if data >= top100:
                    result_array[i] = 5
                elif data >= q3:
                    result_array[i] = 4
                elif data >= q2:
                    result_array[i] = 3
                elif data >= q1:
                    result_array[i] = 2
                else:
                    result_array[i] = 1
            else:
                if data >= q3:
                    result_array[i] = 4
                elif data >= q2:
                    result_array[i] = 3
                elif data >= q1:
                    result_array[i] = 2
                else:
                    result_array[i] = 1
        return result_array
    recency = np.array(rfm_segment_raw['recency'])
    rfm_segment_raw['recency_score'] = score_rfm(recency, 'recency')
    frequency = np.array(rfm_segment_raw['frequency'])
    rfm_segment_raw['frequency_score'] = score_rfm(frequency, 'frequency')
    monetary = np.array(rfm_segment_raw['monetary'])
    rfm_segment_raw['monetary_score'] = score_rfm(monetary, 'monetary')

    # 유저 세그먼트 분류
    # def distribute_user_segment(row):
    #     if row['monetary_score'] == 5:
    #         return 'The Star 등급 고객'
    #     elif row['recency_score'] >= 4 and row['frequency_score'] >= 4 and row['monetary_score'] >= 4:
    #         return 'VIP 고객'
    #     elif (4 >= row['recency_score'] >= 2) and \
    #         (4 >= row['frequency_score'] >= 2) and \
    #         (row['monetary_score'] >= 4):
    #         return '충성 고객'
    #     elif (4 >= row['recency_score'] >= 3) and \
    #         (3 >= row['frequency_score'] >= 1) and \
    #         (3 >= row['monetary_score'] >= 1):
    #         return '잠재 충성 고객'
    #     elif (row['recency_score'] >= 4) and \
    #         (row['frequency_score'] <= 1) and \
    #         (row['monetary_score'] <= 1):
    #         return '신규 고객'
    #     elif (4 >= row['recency_score'] >= 3) and \
    #         (row['frequency_score'] <= 1) and \
    #         (row['monetary_score'] <= 1):
    #         return '잠재 고객'
    #     elif (4 >= row['recency_score'] >= 3) and \
    #         (4 >= row['frequency_score'] >= 3) and \
    #         (4 >= row['monetary_score'] >= 3):
    #         return '관심 필요 고객'
    #     elif (3 >= row['recency_score'] >= 2) and \
    #         (row['frequency_score'] <= 2) and \
    #         (row['monetary_score'] <= 2):
    #         return '잠드려는 고객'
    #     elif (row['recency_score'] <= 2) and \
    #         (4 >= row['frequency_score'] >= 2) and \
    #         (4 >= row['monetary_score'] >= 2):
    #         return '이탈 우려 고객'
    #     elif (row['recency_score'] <= 1) and \
    #         (row['frequency_score'] >= 4) and \
    #         (row['monetary_score'] >= 4):
    #         return '놓치면 안될 고객'
    #     elif (3 >= row['recency_score'] >= 1) and \
    #         (3 >= row['frequency_score'] >= 2) and \
    #         (3 >= row['monetary_score'] >= 1):
    #         return '겨울잠 고객'
    #     elif (row['recency_score'] <= 1) and \
    #         (row['frequency_score'] <= 1) and \
    #         (row['monetary_score'] <= 1):
    #         return '이탈 고객'
    #     elif row['monetary_score'] >= 4:
    #         return '확인 필요 고객'
    #     elif row['monetary_score'] <= 3:
    #         return '그외 고객'
    # rfm_segment_raw['user_segment'] = rfm_segment_raw.apply(distribute_user_segment, axis=1)
    # 유저 세그먼트 분류 누락 확인
    # rfm_segment_raw.loc[rfm_segment_raw['user_segment'].isnull(), ['recency_score', 'frequency_score', 'monetary_score']].drop_duplicates()

    return rfm_segment_raw


def get_event_from_values(event_values, col_name):
    result_array = np.zeros(len(event_values)).astype(str)
    for i, data in enumerate(event_values):
        if len(data) >= 2000:
            if '"%s":' % col_name in data:
                split_data = data.split('"%s":' % col_name)[1]
                if split_data == '':
                    data = '{}'
                else:
                    if '}' in split_data:
                        data = '{"%s":' % col_name + split_data.split(',"')[0]
                        if data[-1:] == '"':
                            data = data + '}'
                        elif data[-1:] == '}':
                            pass
                    else:
                        data = '{"%s":' % col_name + split_data.split(',"')[0]
                        if data[-1:] == '"':
                            data = data + '}'
                        else:
                            data = data + '..."}'
            else:
                data = '{}'
        if col_name in json.loads(data).keys():
            result_array[i] = json.loads(data)[col_name]
        else:
            result_array[i] = ''

    return result_array


# raw 데이터 로드 및 가공
total_df = get_total_raw_data()
purchase_df_dedup = prep_purchase_raw_data(total_df)
rfm_segment_df = prep_rfm_segment_df(purchase_df_dedup)

# event_value 내 af_member_id 기준 appsflyer_id 정규화
total_df.loc[total_df['event_value'] == ''] = '{}'
total_df['member_id'] = get_event_from_values(np.array(total_df['event_value']), 'af_member_id')
total_df = total_df.sort_values(['member_id', 'event_time'], ascending=False)
member_df = total_df.loc[:, ['member_id', 'appsflyer_id']].drop_duplicates('member_id')
member_df = member_df.loc[member_df['member_id'] != '']
merged_df = pd.merge(total_df, member_df, how='left', on='member_id')
merged_df['appsflyer_id'] = merged_df['appsflyer_id_x']
merged_df.loc[merged_df['appsflyer_id_y'].isna(), 'appsflyer_id_y'] = merged_df['appsflyer_id_x']
merged_df.loc[merged_df['appsflyer_id_x'] != merged_df['appsflyer_id_y'], 'appsflyer_id'] = merged_df['appsflyer_id_y']
total_df2 = merged_df.drop(['appsflyer_id_x', 'appsflyer_id_y'], axis=1)
purchase_df_dedup2 = prep_purchase_raw_data(total_df2)
rfm_segment_df2 = prep_rfm_segment_df(purchase_df_dedup2)


len(purchase_df_dedup.appsflyer_id.unique())
len(purchase_df_dedup2.appsflyer_id.unique())


# 오가닉, 페이드 유저 리스트
organic_user_list = set(total_df2.loc[(total_df2['event_name']=='install')&(total_df2['is_paid']==False), 'appsflyer_id'])
paid_user_list = set(total_df2.loc[(total_df2['event_name']=='install')&(total_df2['is_paid']==True), 'appsflyer_id'])
check = organic_user_list & paid_user_list


# 총 구매 유저 리스트 및 로그
total_user_list = set(purchase_df_dedup2['appsflyer_id'])
total_user_log = total_df2.loc[total_df2['appsflyer_id'].isin(list(total_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# x = total_user_log.loc[total_user_log['event_name'].isin(['af_first_purchase', 'af_purchase']), ['appsflyer_id', 'event_name', 'event_time', 'is_paid']]
# 총 구매 유저 광고 유입비중
len(total_user_list)
# temp = total_user_log.loc[total_user_log['event_name'].isin(['af_first_purchase', 'af_purchase'])].drop_duplicates(['appsflyer_id'])
# temp.is_paid.value_counts()


# 운영팀 제공 진성 유저 & 첫 구매 유저 교집합
# quality_user_list1 = set(rfm_segment_df.loc[rfm_segment_df['is_quality_user'] == 1, 'appsflyer_id'])
quality_user_list = set(rfm_segment_df2.loc[rfm_segment_df2['is_quality_user'] == 1, 'appsflyer_id'])
quality_user_log = total_df2.loc[total_df2['appsflyer_id'].isin(list(quality_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)

# 진성 구매 유저
purchase_df_dedup2['is_quality_user'] = purchase_df_dedup2['appsflyer_id'].apply(lambda x: True if x in quality_user_list else False)
purchase_df_dedup2.pivot_table(index='is_quality_user', aggfunc='sum')
# 오가닉, 페이드 모두 있는 유저 is_paid 공백 처리
temp = purchase_df_dedup2.drop_duplicates(['appsflyer_id', 'is_paid']).appsflyer_id.value_counts()
dup_user_list = temp[temp >= 2].index.tolist()
purchase_df_dedup2.loc[purchase_df_dedup2['appsflyer_id'].isin(dup_user_list), 'is_paid'] = ''


# 오가닉 구매 & 진성 구매 유저 로그
organic_purchase_user_list = set(purchase_df_dedup2.loc[purchase_df_dedup2['is_paid'] == False, 'appsflyer_id'])
len(organic_purchase_user_list)
organic_cross_user_list = organic_purchase_user_list & quality_user_list
len(organic_cross_user_list)
organic_quality_user_log = quality_user_log.loc[quality_user_log['appsflyer_id'].isin(list(organic_cross_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# organic_quality_user_list = set(organic_quality_user_log.loc[organic_quality_user_log['event_name'].isin(['af_first_purchase', 'af_purchase']), 'appsflyer_id'])

# 광고 구매 & 진성 구매 유저 로그
paid_purchase_user_list = set(purchase_df_dedup2.loc[purchase_df_dedup2['is_paid'] == True, 'appsflyer_id'])
paid_cross_user_list = paid_purchase_user_list & quality_user_list
len(paid_cross_user_list)
paid_quality_user_log = quality_user_log.loc[quality_user_log['appsflyer_id'].isin(list(paid_cross_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# paid_quality_user_list = set(paid_quality_user_log.loc[paid_quality_user_log['event_name'].isin(['af_first_purchase', 'af_purchase']), 'appsflyer_id'])

# 오가닉 & 페이드
organic_paid_user_list = set(purchase_df_dedup2.loc[purchase_df_dedup2['is_paid'] == '', 'appsflyer_id'])
organic_paid_cross_user_list = organic_paid_user_list & quality_user_list
len(organic_paid_cross_user_list)
organic_paid_quality_user_log = quality_user_log.loc[quality_user_log['appsflyer_id'].isin(list(organic_paid_cross_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)


# 진성 구매 유저 로그 추출(오가닉, 광고 유저 여부 컬럼 추가)
quality_user_log['is_paid_user'] = ''
quality_user_log.loc[quality_user_log['appsflyer_id'].isin(organic_cross_user_list), 'is_paid_user'] = False
quality_user_log.loc[quality_user_log['appsflyer_id'].isin(paid_cross_user_list), 'is_paid_user'] = True

quality_user_log.to_csv(dr.download_dir + '/quality_user_log.csv', index=False, encoding='utf-8-sig')






########### backlog #############

# # 첫 구매 유저 리스트
# af_first_purchase_user_list = set(purchase_df.loc[(purchase_df['event_name'] == 'af_first_purchase')&(purchase_df['is_first_purchase'] == True), 'appsflyer_id'])
# af_first_purchase_user_log = total_user_log.loc[total_user_log['appsflyer_id'].isin(list(af_first_purchase_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# af_first_purchase_user_log.drop_duplicates(['appsflyer_id']).is_paid.value_counts()
#
# paid_af_first_purchase_user_log = af_first_purchase_user_log.query("is_paid == True")
# organic_af_first_purchase_user_log = af_first_purchase_user_log.query("is_paid == False")
#
# # 광고를 통해 유입된 유저
# temp1 = af_first_purchase_user_log.drop_duplicates(['appsflyer_id']).query("is_paid == True").appsflyer_id.unique().tolist()
# # 광고를 통해 첫 유입되진 않았으나 광고를 통해 re-engage된 유저
# check1 = paid_af_first_purchase_user_log.loc[~(paid_af_first_purchase_user_log['appsflyer_id'].isin(temp1))]
#
# # 첫 구매가 아닌 유저 리스트
# non_first_purchase_user_log = total_user_log.loc[~(total_user_log['appsflyer_id'].isin(list(af_first_purchase_user_list)))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# non_first_purchase_user_log.drop_duplicates(['appsflyer_id']).is_paid.value_counts()
# non_first_purchase_user_list = set(non_first_purchase_user_log.loc[:, 'appsflyer_id'])
# cross_non_user_list = non_first_purchase_user_list & quality_user_list
# cross_non_user_log = total_user_log.loc[total_user_log['appsflyer_id'].isin(list(cross_non_user_list))].sort_values(['appsflyer_id', 'event_time'], ignore_index=True)
# cross_non_user_log.drop_duplicates(['appsflyer_id']).is_paid.value_counts()
#
#
# # RFM 스코어 진성 유저 & 첫 구매 유저 교집합
# rfm_quality_user_list = set(rfm_segment_df.loc[rfm_segment_df['user_segment'].isin(['The Star 등급 고객', 'VIP 고객']), 'appsflyer_id'])
# rfm_cross_user_list = [x for x in af_first_purchase_user_list if x in rfm_quality_user_list]
# rfm_cross_user_log = total_df.loc[total_df['appsflyer_id'].isin(rfm_cross_user_list)]
# rfm_cross_user_log = rfm_cross_user_log.sort_values(['appsflyer_id', 'event_time'])

