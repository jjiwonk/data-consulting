import pandas as pd
from setting import directory as dr
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import datetime

organic_raw = pd.read_csv(dr.download_dir + '/organic_purchase_install.csv')
organic_raw['media_source'] = 'organic'
organic_raw['category'] = 'organic'

non_organic_raw = pd.read_csv(dr.download_dir + '/non_organic_purchase_install.csv')
non_organic_raw['category'] = 'non_organic'

total_raw = pd.concat([organic_raw, non_organic_raw], ignore_index=True)
total_raw = total_raw[['category','appsflyer_id', 'media_source', 'event_name','event_time', 'install_time', 'attributed_touch_time']]
total_raw = total_raw.sort_values(['appsflyer_id', 'event_time', 'install_time', 'attributed_touch_time'])
total_raw = total_raw.drop_duplicates(['appsflyer_id', 'event_time', 'event_name'])
total_raw[['event_time',
           'install_time',
           'attributed_touch_time']] = total_raw[['event_time', 'install_time', 'attributed_touch_time']].apply(lambda x: pd.to_datetime(x))

first_install_time = total_raw.loc[total_raw['event_name']=='install']
first_install_time = first_install_time.drop_duplicates('appsflyer_id')
first_install_time = first_install_time[['appsflyer_id', 'category', 'media_source', 'install_time']]
first_install_time = first_install_time.rename(columns = {
    'category' : 'install_source_category',
    'media_source' : 'install_source',
    'install_time' : 'first_install_time'})
first_install_time['install'] = 1
first_install_time['date'] = first_install_time['first_install_time'].dt.date

install_pivot = first_install_time.pivot_table(index = 'date', columns = 'install_source_category', values = 'install', aggfunc = 'sum')
install_pivot.columns = [col + '_install' for col in install_pivot.columns ]

total_raw_merge = total_raw.merge(first_install_time, on = 'appsflyer_id', how = 'left')
total_raw_merge = total_raw_merge.loc[total_raw_merge['install'].notnull()]
total_raw_merge['ITET'] = total_raw_merge['event_time'] - total_raw_merge['first_install_time']

first_purchase_data = total_raw_merge.loc[total_raw_merge['event_name']=='first_purchase']
first_purchase_data = first_purchase_data.drop_duplicates('appsflyer_id')
first_purchase_data = first_purchase_data.loc[first_purchase_data['ITET'] < datetime.timedelta(30)]
first_purchase_data['first_purchase'] = 1
first_purchase_pivot = first_purchase_data.pivot_table(index = 'date', columns = 'install_source_category', values = 'first_purchase', aggfunc = 'sum')
first_purchase_pivot.columns = [col + '_first_purchase' for col in first_purchase_pivot.columns ]

install_to_purchase = pd.concat([install_pivot, first_purchase_pivot], axis = 1)
install_to_purchase = install_to_purchase.fillna(0)
install_to_purchase = install_to_purchase.reset_index()
install_to_purchase = install_to_purchase.loc[(install_to_purchase['non_organic_install']>0) &
                                              (install_to_purchase['organic_install']>0) &
                                              (install_to_purchase['non_organic_first_purchase']>0) &
                                              (install_to_purchase['organic_first_purchase']>0)]

install_to_purchase.to_csv(dr.download_dir + '/install_to_purchase.csv', index=False, encoding = 'utf-8-sig')

## 한계 생산 곡선

# 과거 데이터 (일자별 투입량과 산출량)
input_data = list(install_to_purchase['non_organic_install'].astype('float'))
output_data = list(install_to_purchase['non_organic_first_purchase'].astype('float'))

input_data_2 = list(install_to_purchase['organic_install'].astype('float'))
output_data_2 = list(install_to_purchase['organic_first_purchase'].astype('float'))

# 이상값(outlier) 식별 및 제거
def remove_outliers(input_data, output_data, input_data_2, output_data_2):
    Q1 = np.percentile(output_data, 25)
    Q3 = np.percentile(output_data, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    filtered_indices = [i for i in range(len(output_data)) if
                        output_data[i] >= lower_bound and output_data[i] <= upper_bound]

    input_data_cleaned = [input_data[i] for i in filtered_indices]
    output_data_cleaned = [output_data[i] for i in filtered_indices]
    input_data_2_cleaned = [input_data_2[i] for i in filtered_indices]
    output_data_2_cleaned = [output_data_2[i] for i in filtered_indices]

    return input_data_cleaned, output_data_cleaned, input_data_2_cleaned, output_data_2_cleaned


input_data_cleaned, output_data_cleaned, input_data_2_cleaned, output_data_2_cleaned = remove_outliers(input_data, output_data, input_data_2, output_data_2)


# 외부 변수 (예: 광고 예산)를 포함한 데이터
X = np.column_stack((input_data_cleaned, output_data_2_cleaned))

# 다중 선형 회귀 모델 생성 및 훈련
model = LinearRegression()
model.fit(X, output_data_cleaned)

# 회귀 모델의 계수 출력
coefficients = model.coef_
intercept = model.intercept_

print("회귀 모델 계수:", coefficients)
print("절편:", intercept)

# 독립 변수의 영향을 제거한 정규화된 변화량 계산
cor1 = [i * coefficients[0] for i in input_data_cleaned]
cor2 = [i * coefficients[1] for i in input_data_2_cleaned]
normalized_changes = np.array(cor1) + np.array(cor2)

# 최적의 투입량 예측
optimal_input_data = input_data[np.argmax(normalized_changes)]

print("최적의 투입량:", optimal_input_data)

# 결과를 그래프로 시각화
# plt.figure(figsize=(10, 6))
# plt.plot(input_data, normalized_changes, label='normalized_effect')
# plt.xlabel('input (non-organic)')
# plt.ylabel('output')
# plt.title('corr')
# plt.legend()
# plt.grid(True)
# plt.show()
