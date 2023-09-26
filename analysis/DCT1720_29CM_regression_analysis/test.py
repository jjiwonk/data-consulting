import pandas as pd
import statsmodels.api as sm
from sklearn.datasets import load_diabetes

data = load_diabetes()
df = pd.DataFrame(data['data'], index=data['target'], columns=data['feature_names'])

### 단순 선형 회귀분석 ###
lr = LinearRegression()
#Y = m(기울기)X + b(절편 -> 축과 만나는 점의 좌표)
X = df.bmi.values #독립 변수
Y = df.index.values #종속 변수

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

#회귀 분석
results = sm.OLS(Y, sm.add_constant(X)).fit()
summary = results.summary()

#Prob (F-statistic): 3.47e-42 -> 0.05 이하일 때 통계적 의미가 있음
#coef, x1 : 0.00 -> x1은 bmi, 이 변수의 p-value는 0,00 -> 0.05보다 작을 때 유의미
#R-squared -> 0에 가까울 수록 예측값을 믿을 수 없고 1에 가까울 수록 믿을 수 있다고 볼 수 있음

y_predicted = lr.predict(X)
print(y_predicted)

# X, Y, y_predicted를 데이터프레임으로 만들기
data = {'X': X.flatten(),  # X를 1차원 배열로 펼침
        'Y': Y.flatten(),  # Y를 1차원 배열로 펼침
        'y_predicted': y_predicted.flatten()}  # y_predicted를 1차원 배열로 펼침

df2 = pd.DataFrame(data)



# 실제값(Y)와 예측값(y_predicted)을 선 그래프로 그리기
plt.figure(figsize=(10, 6))  # 그래프 크기 설정 (선택적)

# 실제값(Y)을 파란색 선으로 표시
plt.plot(X, Y, 'b', label='실제값 (Y)')

# 예측값(y_predicted)을 빨간색 선으로 표시
plt.plot(X, y_predicted, 'r', label='예측값 (y_predicted)')

plt.xlabel('X')
plt.ylabel('값')
plt.legend()  # 범례 추가
plt.title('실제값과 예측값 비교')  # 그래프 제목 (선택적)

plt.show()  # 그래프 표시

df2.to_excel('test.xlsx', index=False)