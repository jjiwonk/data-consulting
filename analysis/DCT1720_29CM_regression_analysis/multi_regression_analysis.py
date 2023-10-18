from setting import directory as dr
import pandas as pd
from sklearn import linear_model
import matplotlib.pyplot as plt

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1427/RD'

raw_data = pd.read_excel(raw_dir +'/29CM_2차MMM분석_raw_2202_2309.xlsx',sheet_name = 0)
promotion_data = pd.read_excel(raw_dir +'/promotion.xlsx',sheet_name = 0)

def data_prep(raw_data, promotion_data):
    raw_data['날짜'] = pd.to_datetime(raw_data['날짜'])

    raw_data = pd.merge(raw_data,promotion_data,on='날짜',how='left')
    raw_data = raw_data.dropna(subset='프로모션 진행 여부')

    return raw_data

# 다중 선형 회귀
data = data_prep(raw_data, promotion_data)
data = data.loc[data['구분'] == '인스톨']
data = data.fillna(0)

y = data[['첫 구매']]
X = data[['설치','프로모션 진행 여부']]

linear_regression = linear_model.LinearRegression()
linear_regression.fit(X=pd.DataFrame(X), y=y)

prediction = linear_regression.predict(X=pd.DataFrame(X))

print('Bintercept_: ', linear_regression.intercept_)
print('acoef_: ', linear_regression.coef_)

residuals = y - prediction
residuals.describe()

# r_squared
SSE = (residuals**2).sum()
SST = ((y-y.mean())**2).sum()
R_squared = 1 - (SSE/SST)
print('R_squared: ', R_squared)

# MSE, RMSE
from sklearn.metrics import mean_squared_error
from math import sqrt

mse = mean_squared_error(y, prediction)
rmse = sqrt(mse)

print('score = ', linear_regression.score(X=pd.DataFrame(X), y=y))
print('mean_squared_error = ', mse)
print('RMSE = ', rmse)

# 시각화

x_train, x_test, y_train, y_test = train_test_split(X, y, train_size=0.8, test_size=0.2)

mlr = LinearRegression()
mlr.fit(x_train, y_train)

y_predict = mlr.predict(x_test)

plt.scatter(y_test, y_predict, alpha=0.4)
plt.xlabel("Actual purchase")
plt.ylabel("Predicted purchase")
plt.title("MULTIPLE LINEAR REGRESSION")
plt.show()

# 각 항목별로 보기
plt.scatter(data[['설치']], data[['첫 구매']], alpha=0.4)
plt.xlabel("INSTALL")
plt.ylabel("FIRST_PURCHASE")
plt.title("MULTIPLE LINEAR REGRESSION")

plt.show()

#결정계수
print(mlr.score(x_train, y_train))
