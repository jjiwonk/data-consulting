import os
import pyarrow as pa
import pandas as pd
import numpy as np
import datetime
import setting.directory as dr
from workers.read_data import pyarrow_csv
import json
import workers.func as func


def get_raw_data(event_folder):
    raw_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/더한섬/raw_data/{event_folder}'
    file_list = os.listdir(raw_dir)
    csv_list = []
    for file in file_list:
        if 'csv' in file:
            csv_list.append(file)
    dtypes = {
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Event Value': pa.string(),
            'Event Revenue': pa.string(),
            'Event Revenue KRW': pa.string(),
            # 'Event Source': pa.string(),
            # 'Partner': pa.string(),
            # 'Media Source': pa.string(),
            # 'Channel': pa.string(),
            # 'Campaign': pa.string(),
            # 'Campaign ID': pa.string(),
            # 'Adset': pa.string(),
            # 'Adset ID': pa.string(),
            # 'Ad': pa.string(),
            # 'Ad ID': pa.string(),
            # 'Ad Type': pa.string(),
            # 'Region': pa.string(),
            # 'Country Code': pa.string(),
            # 'State': pa.string(),
            # 'City': pa.string(),
            # 'Language': pa.string(),
            'AppsFlyer ID': pa.string(),
            # 'Advertising ID': pa.string(),
            # 'IDFA': pa.string(),
            # 'Android ID': pa.string(),
            # 'Customer User ID': pa.string(),
            # 'IMEI': pa.string(),
            # 'IDFV': pa.string(),
            # 'Platform': pa.string(),
            # 'Device Type': pa.string(),
            # 'Is Retargeting': pa.string(),
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

    return df


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
        elif len(data) == 0:
            data = '{}'
        if col_name in json.loads(data.replace(">", '/').replace("'", ''), strict=False).keys():
            result_array[i] = json.loads(data)[col_name]
        else:
            result_array[i] = ''

    return result_array


def get_total_raw_data(event_folder='both', df=None):
    if df is not None:
        total_raw = df
    elif 'organic' in event_folder:
        organic_raw = get_raw_data(event_folder)
        organic_raw['is_paid'] = False
        organic_raw.loc[organic_raw['event_value'] == '', 'event_value'] = '{}'
        organic_raw['member_id'] = get_event_from_values(np.array(organic_raw['event_value']), 'af_member_id')
        organic_raw['order_id'] = get_event_from_values(np.array(organic_raw['event_value']), 'af_order_id')
        total_raw = organic_raw
    elif 'paid' in event_folder:
        paid_raw = get_raw_data(event_folder)
        paid_raw['is_paid'] = True
        paid_raw.loc[paid_raw['event_value'] == '', 'event_value'] = '{}'
        paid_raw['member_id'] = get_event_from_values(np.array(paid_raw['event_value']), 'af_member_id')
        paid_raw['order_id'] = get_event_from_values(np.array(paid_raw['event_value']), 'af_order_id')
        total_raw = paid_raw
    else:
        organic_3raw = get_raw_data('organic_3월')
        organic_3raw['is_paid'] = False
        organic_3raw.loc[organic_3raw['event_value'] == '', 'event_value'] = '{}'
        organic_3raw['member_id'] = get_event_from_values(np.array(organic_3raw['event_value']), 'af_member_id')
        organic_3raw['order_id'] = get_event_from_values(np.array(organic_3raw['event_value']), 'af_order_id')
        organic_4raw = get_raw_data('organic_4월')
        organic_4raw['is_paid'] = False
        organic_4raw.loc[organic_4raw['event_value'] == '', 'event_value'] = '{}'
        organic_4raw['member_id'] = get_event_from_values(np.array(organic_4raw['event_value']), 'af_member_id')
        organic_4raw['order_id'] = get_event_from_values(np.array(organic_4raw['event_value']), 'af_order_id')
        paid_raw = get_raw_data('paid')
        paid_raw['is_paid'] = True
        paid_raw.loc[paid_raw['event_value'] == '', 'event_value'] = '{}'
        paid_raw['member_id'] = get_event_from_values(np.array(paid_raw['event_value']), 'af_member_id')
        paid_raw['order_id'] = get_event_from_values(np.array(paid_raw['event_value']), 'af_order_id')
        total_raw = pd.concat([organic_3raw, organic_4raw, paid_raw]).drop_duplicates().reset_index(drop=True)

    # unique_user_id로 유저 정규화
    user_id_dict = func.user_identifier(total_raw, 'appsflyer_id', 'member_id')
    total_raw['unique_user_id'] = total_raw['appsflyer_id'].apply(lambda x: user_id_dict.get(x))
    final_df = total_raw.reset_index(drop=True)
    return final_df


def prep_purchase_raw_data(df):
    purchase_raw = df.loc[df['event_name'].isin(['af_purchase', 'af_first_purchase'])]
    purchase_raw.sort_values('event_time', inplace=True)
    purchase_raw = purchase_raw.drop_duplicates('order_id')

    # purchase_raw = purchase_raw.drop('compare_id', axis=1)
    purchase_raw['event_revenue_krw'] = purchase_raw['event_revenue_krw'].apply(pd.to_numeric).astype(int)
    purchase_raw[['event_time']] = purchase_raw[['event_time']].apply(pd.to_datetime, axis=1)

    # 첫 구매 유저 여부
    af_first_purchase_user_list = set(purchase_raw.loc[purchase_raw['event_name'] == 'af_first_purchase', 'unique_user_id'])
    purchase_raw['is_first_purchase_user'] = False
    purchase_raw.loc[purchase_raw['unique_user_id'].isin(af_first_purchase_user_list), 'is_first_purchase_user'] = True

    # 오가닉, 페이드 모두 있는 유저 is_paid both 처리
    temp = purchase_raw.drop_duplicates(['unique_user_id', 'is_paid']).unique_user_id.value_counts()
    dup_user_list = temp[temp >= 2].index.tolist()
    purchase_raw.loc[purchase_raw['is_paid'] == True, 'is_paid'] = 'paid'
    purchase_raw.loc[purchase_raw['is_paid'] == False, 'is_paid'] = 'organic'
    purchase_raw.loc[purchase_raw['unique_user_id'].isin(dup_user_list), 'is_paid'] = 'both'
    purchase_raw = purchase_raw.reset_index(drop=True)

    return purchase_raw


def prep_rfm_segment_df(df):
    # RFM 스코어 가공
    rfm_segment_raw = df.pivot_table(index=['unique_user_id'],
                                     values=['event_time', 'event_revenue_krw', 'event_name'],
                                     aggfunc=['last', 'sum', 'count'])
    rfm_segment_raw = rfm_segment_raw.reset_index().reset_index(drop=True)
    rfm_segment_raw = rfm_segment_raw.iloc[:, [0, 3, 4, 5]]
    rfm_segment_raw.columns = ['unique_user_id', 'recent_purchase_time', 'monetary', 'frequency']
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
    def distribute_user_segment(row):
        if row['monetary_score'] == 5:
            return 'The Star 등급 고객'
        elif row['recency_score'] >= 4 and row['frequency_score'] >= 4 and row['monetary_score'] >= 4:
            return 'VIP 고객'
        elif (4 >= row['recency_score'] >= 2) and \
            (4 >= row['frequency_score'] >= 2) and \
            (row['monetary_score'] >= 4):
            return '충성 고객'
        elif (4 >= row['recency_score'] >= 3) and \
            (3 >= row['frequency_score'] >= 1) and \
            (3 >= row['monetary_score'] >= 1):
            return '잠재 충성 고객'
        elif (row['recency_score'] >= 4) and \
            (row['frequency_score'] <= 1) and \
            (row['monetary_score'] <= 1):
            return '신규 고객'
        elif (4 >= row['recency_score'] >= 3) and \
            (row['frequency_score'] <= 1) and \
            (row['monetary_score'] <= 1):
            return '잠재 고객'
        elif (4 >= row['recency_score'] >= 3) and \
            (4 >= row['frequency_score'] >= 3) and \
            (4 >= row['monetary_score'] >= 3):
            return '관심 필요 고객'
        elif (3 >= row['recency_score'] >= 2) and \
            (row['frequency_score'] <= 2) and \
            (row['monetary_score'] <= 2):
            return '잠드려는 고객'
        elif (row['recency_score'] <= 2) and \
            (4 >= row['frequency_score'] >= 2) and \
            (4 >= row['monetary_score'] >= 2):
            return '이탈 우려 고객'
        elif (row['recency_score'] <= 1) and \
            (row['frequency_score'] >= 4) and \
            (row['monetary_score'] >= 4):
            return '놓치면 안될 고객'
        elif (3 >= row['recency_score'] >= 1) and \
            (3 >= row['frequency_score'] >= 2) and \
            (3 >= row['monetary_score'] >= 1):
            return '겨울잠 고객'
        elif (row['recency_score'] <= 1) and \
            (row['frequency_score'] <= 1) and \
            (row['monetary_score'] <= 1):
            return '이탈 고객'
        elif row['monetary_score'] >= 4:
            return '확인 필요 고객'
        elif row['monetary_score'] <= 3:
            return '그외 고객'
    rfm_segment_raw['user_segment'] = rfm_segment_raw.apply(distribute_user_segment, axis=1)

    return rfm_segment_raw


