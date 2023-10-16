import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from setting import directory as dr

# 데이터 로드 함수
def load_data(file_path):
    return pd.read_csv(file_path)

# 데이터 파일 경로 설정
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1747'
file_name = '/prep_data.csv'
data = load_data(result_dir + file_name)

data_label_first_purchase = data[data['label'] == '첫구매']
data_label_first_purchase = data_label_first_purchase[data_label_first_purchase['media'] != '토스']

data_label_first_purchase_facebook = data_label_first_purchase[data_label_first_purchase['media'] == '페이스북']
data_label_first_purchase_kakao = data_label_first_purchase[data_label_first_purchase['media'] == '카카오모먼트']
data_label_first_purchase_ACe = data_label_first_purchase[data_label_first_purchase['media'] == 'ACe']
data_label_first_purchase_google = data_label_first_purchase[data_label_first_purchase['media'] == '구글']
data_label_first_purchase_criteo = data_label_first_purchase[data_label_first_purchase['media'] == '크리테오']

data_label_install = data[data['label'] == '인스톨']

# 'install', '광고 Spend', 'first_purchase' 간의 상관 관계 계산
correlation_install_spend = data_label_first_purchase['spend'].corr(data_label_first_purchase['install'])
correlation_first_purchase_install = data_label_first_purchase['install'].corr(data_label_first_purchase['first_purchase'])
correlation_first_purchase_spend = data_label_first_purchase['spend'].corr(data_label_first_purchase['first_purchase'])

print("Correlation between 'install' and '광고 Spend':", correlation_install_spend)
print("Correlation between 'first_purchase' and 'install':", correlation_first_purchase_install)
print("Correlation between 'first_purchase' and '광고 Spend':", correlation_first_purchase_spend)

# 산점도 그래프 그리기
plt.figure(figsize=(8, 6))
sns.scatterplot(x='spend', y='first_purchase', data=data_label_first_purchase)
plt.title('spend vs. first_purchase')
plt.xlabel('spend')
plt.ylabel('first_purchase')
plt.show()

#월별 첫구매, 광고비 추이
data_label_first_purchase['date'] = pd.to_datetime(data_label_first_purchase['date'])
data_label_first_purchase.set_index('date', inplace=True)

data_label_install['date'] = pd.to_datetime(data_label_install['date'])
data_label_install.set_index('date', inplace=True)

fig, ax1 = plt.subplots(figsize=(12, 6))
color = 'tab:red'
ax1.set_xlabel('date')
ax1.set_ylabel('first_purchase', color=color)
ax1.plot(data_label_install.index, data_label_install['first_purchase'], color=color)
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()  # 두 번째 Y축 생성
color = 'tab:blue'
ax2.set_ylabel('spend', color=color)
ax2.plot(data_label_install.index, data_label_install['spend'], color=color)
ax2.tick_params(axis='y', labelcolor=color)

plt.title('trend')
plt.show()


################# 회귀 분석 #################


# 데이터프레임에서 광고비 및 첫구매 데이터 추출
X = data_label_first_purchase_criteo['spend']
y = data_label_first_purchase_criteo['first_purchase']

# 광고비를 n개의 구간으로 나누기
n = 5  # 원하는 구간의 수
bins = np.linspace(0, data_label_first_purchase_criteo['spend'].max(), n + 1)  # 최소값부터 최대값까지 구간 나누기
data_label_first_purchase_criteo['binned'] = pd.cut(data_label_first_purchase_criteo['spend'], bins=bins)

# 각 구간별로 회귀 분석 수행
results_list = []

for label, group in data_label_first_purchase_criteo.groupby('binned'):
    X_group = sm.add_constant(group['spend'])
    y_group = group['first_purchase']
    model = sm.OLS(y_group, X_group)
    results = model.fit()
    results_list.append((label, results))

# 결과 출력
for label, results in results_list:
    print(f"구간: {label}")
    print(results.summary())
    print("\n")

# 그래프를 그릴 서브플롯 생성
fig, axs = plt.subplots(nrows=1, ncols=n, figsize=(15, 3))  # n개의 서브플롯을 가로로 나열

# 각 구간별로 그래프 그리기
for i, (label, group) in enumerate(data_label_first_purchase_criteo.groupby('binned')):
    X_group = sm.add_constant(group['spend'])
    y_group = group['first_purchase']
    model = sm.OLS(y_group, X_group)
    results = model.fit()

    # 산점도 그리기
    axs[i].scatter(group['spend'], group['first_purchase'], label=label)

    # 회귀선 그리기
    X_line = np.linspace(group['spend'].min(), group['spend'].max(), 100)
    y_line = results.params[0] + results.params[1] * X_line
    axs[i].plot(X_line, y_line, color='red', label='regression line')

    axs[i].set_title(f"range: {label}")
    axs[i].set_xlabel('spend')
    axs[i].set_ylabel('first_purchase')
    axs[i].legend()

plt.tight_layout()
plt.show()
