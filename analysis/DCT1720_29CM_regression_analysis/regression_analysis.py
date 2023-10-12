from sklearn.linear_model import LinearRegression
import pandas as pd
import statsmodels.api as sm
from matplotlib import pyplot as plt
from setting import directory as dr

# 결과 파일 경로
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1720'
file_name = '/prep_data.csv'
raw = pd.read_csv(result_dir+file_name)

### 단순 선형 회귀분석 ###
lr = LinearRegression()
#Y = m(기울기)X + b(절편 -> 축과 만나는 점의 좌표)
X = raw.install.values #독립 변수
Y = raw.new_purchase.values #종속 변수

X = X.reshape(-1, 1)
Y = Y.reshape(-1, 1)

lr.fit(X, Y)
print(lr.coef_)
print(lr.intercept_)

print(lr.coef_)
print('y = {}*x + {}'.format(lr.coef_[0], lr.intercept_))

plt.plot(X, Y, 'o')
plt.plot(X,lr.predict(X))
plt.title('y = {}*x + {}'.format(lr.coef_[0], lr.intercept_))
plt.show()
import statsmodels.api as sm
#회귀 분석
results = sm.OLS(Y, sm.add_constant(X)).fit()
summary = results.summary()

y_predicted = lr.predict(X)
print(y_predicted)

# X, Y, y_predicted를 데이터프레임으로 만들기
data = {'X': X.flatten(),  # X를 1차원 배열로 펼침
        'Y': Y.flatten(),  # Y를 1차원 배열로 펼침
        'y_predicted': y_predicted.flatten()}  # y_predicted를 1차원 배열로 펼침

raw2 = pd.DataFrame(data)