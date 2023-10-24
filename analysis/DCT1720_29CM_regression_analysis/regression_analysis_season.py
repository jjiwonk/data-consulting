import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from setting import directory as dr

# 데이터 로드 함수
def load_data(file_path):
    return pd.read_csv(file_path)

def get_season(month):
    if 3 <= month <= 5:
        return 'season'
    elif 7 <= month <= 8:
        return 'non_season'
    elif 9 <= month <= 11:
        return 'season'
    else:
        return 'non_season'

def save_dataframe_to_csv(dataframe, file_path, encoding='cp949'):
    try:
        dataframe.to_csv(file_path, encoding=encoding, index=False)
        print(f"Data saved to {file_path}")
    except Exception as e:
        print(f"Error while saving data to {file_path}: {str(e)}")


# 데이터 파일 경로 설정
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1747'
file_name = '/prep_data.csv'
data = load_data(result_dir + file_name)

# 데이터 전처리
data['date'] = pd.to_datetime(data['date'])
data['month'] = data['date'].dt.month
data['year'] = data['date'].dt.year
data['cvr'] = (data['first_purchase'] / data['install']).astype(float)
data['season'] = data['month'].apply(get_season)
data['label'] = data['label'].replace({'첫구매': 'fp', '인스톨': 'it'})

# 시즌별 효율 확인
grouped_season = data.groupby(['year', 'month', 'season', 'label', 'media', 'campaign']).agg({
    'install': 'sum',
    'first_purchase': 'sum',
    'spend': 'sum'
}).reset_index()
grouped_season = grouped_season.sort_values(by=['year', 'month','label'])

grouped_season['cpi'] = grouped_season['spend'] / grouped_season ['install']
grouped_season['cpa'] = grouped_season['spend'] / grouped_season ['first_purchase']
grouped_season['cvr'] = grouped_season['first_purchase'] / grouped_season ['install']

grouped_season_first_purchase = grouped_season[grouped_season['label'] == 'fp']
grouped_season_first_purchase_1 = grouped_season_first_purchase[grouped_season_first_purchase['season'] == 'season']
grouped_season_first_purchase_0 = grouped_season_first_purchase[grouped_season_first_purchase['season'] == 'non_season']
grouped_season_install = grouped_season[grouped_season['label'] == 'it']
grouped_season_install_1 = grouped_season_install[grouped_season_install['season'] == 'season']
grouped_season_install_0 = grouped_season_install[grouped_season_install['season'] == 'non_season']

# 데이터를 피벗하여 상관관계 계산
correlation_data = grouped_season_first_purchase_1.pivot_table(index=['year', 'season'], columns='label', values=['install', 'first_purchase', 'spend'])

# 상관관계 계산
correlation_matrix = data.corr()

# 히트맵 그리기
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Correlation Heatmap')
plt.show()