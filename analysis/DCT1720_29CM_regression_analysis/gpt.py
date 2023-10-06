import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
from setting import directory as dr
import numpy as np


# 데이터 로드 함수
def load_data(file_path):
    return pd.read_csv(file_path)

# 데이터 그룹화 및 필터링 함수
def group_and_filter_data(data):
    grouped_data = data.groupby(['install_date', 'label', 'media', 'campaign']).sum().reset_index()
    install_ranges = [
        (0, 200), 
        (200, 600), 
        (600, 1000), 
        (1000, 1400), 
        (1400, 1800), 
        (1800, 2200),
        (2200, 2600), 
        (2600, 3000), 
        (3000, 3400), 
        (3400, float('inf'))
    ]

    data_bins = {}
    for i, (start, end) in enumerate(install_ranges):
        key = f'between_{start}_and_{end}' if end < float('inf') else f'over_{start}'
        data_bin = grouped_data[(grouped_data['install'] >= start) & (grouped_data['install'] <= end)]
        data_bins[key] = data_bin

    return data_bins

# 데이터 install 기준으로 분할 함수
def split_data_by_install(data, num_bins=10):
    max_install = data['install'].max()
    bin_range = np.linspace(0, max_install, num_bins + 1)

    data_bins = {}
    for i in range(num_bins):
        start, end = bin_range[i], bin_range[i + 1]
        key = f'bin_{i + 1}'
        data_in_bin = data[(data['install'] >= start) & (data['install'] <= end)]
        data_bins[key] = data_in_bin

    return data_bins

# 회귀 모델 구축 함수
def build_regression_model(data):
    X = data[['install']]  # 독립 변수
    X = sm.add_constant(X)  # 상수 항 (절편) 추가
    Y = data['new_purchase']  # 종속 변수

    model = sm.OLS(Y, X).fit()

    return model


# 회귀 결과 출력 함수
def print_regression_summary(model, key):
    print(f"Regression Summary for {key}:")
    r_squared = model.rsquared
    print("R-squared 값:", r_squared)
    # print(model.summary())
    print("회귀 모델 계수 (기울기):", model.params['install'])
    print("회귀 모델 계수 (절편):", model.params['const'])


# 회귀 모델 시각화 함수
def visualize_regression(data, model, title):
    plt.scatter(data['install'], data['new_purchase'], label='Actual data', alpha=0.5)
    plt.plot(data['install'], model.predict(), color='red', label='Regression Model')
    plt.xlabel('install')
    plt.ylabel('new_purchase')
    plt.title(title)
    plt.legend()
    plt.show()


# 데이터 로드
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1720'
file_name = '/prep_data.csv'
data = load_data(result_dir + file_name)

# 데이터 그룹화 및 필터링
filtered_data = group_and_filter_data(data)

# 각 그룹별 회귀 모델 생성 및 시각화
for key, value in filtered_data.items():
    regression_model = build_regression_model(value)
    print_regression_summary(regression_model, key)
    visualize_regression(value, regression_model, f'Regression Model for {key}')

# 데이터 install 기준으로 분할
split_data = split_data_by_install(data)

# 각 분할별 회귀 모델 생성 및 시각화
for key, value in split_data.items():
    regression_model = build_regression_model(value)
    print_regression_summary(regression_model, key)
    visualize_regression(value, regression_model, f'Regression Model for {key}')

# 데이터 그룹별 행 수 출력
for key, value in split_data.items():
    num_rows = value.shape[0]
    print(f"Number of rows in {key}: {num_rows}")
