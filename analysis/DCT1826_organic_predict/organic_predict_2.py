from setting import directory as dr
import pandas as pd
import numpy as np
from prophet import Prophet
import holidays
from prophet.diagnostics import cross_validation
from prophet.plot import plot_cross_validation_metric
from prophet.diagnostics import performance_metrics
from prophet.plot import add_changepoints_to_plot
import matplotlib
matplotlib.use('Qt5Agg')


# 오가닉 학습 데이터
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
file_name = '/organic.csv'
data = pd.read_csv(result_dir + file_name)
data['date'] = pd.to_datetime(data['date'])  # 날짜 열을 날짜 형식으로 변환
data = data.rename(columns={'date': 'ds', '앱설치': 'y'})


# 프로모션 진행 여부 데이터
file_name = '/promotion.csv'
promotion_df = pd.read_csv(result_dir + file_name)
# '프로모션 진행 여부' 열의 값을 숫자로 변환
promotion_df['프로모션 진행 여부'] = (promotion_df['프로모션 진행 여부'] == 'O').astype(int)
promotion_df = promotion_df.rename(columns={'날짜': 'ds', '프로모션 진행 여부': 'promotion'})
promotion_df['ds'] = pd.to_datetime(promotion_df['ds'])  # 날짜 열을 날짜 형식으로 변환

data = data.merge(promotion_df, on='ds')
model_data = data[['ds', 'y', 'promotion']]


# 휴일 정보 생성
date_list = pd.date_range('2022-02-17', '2023-09-30')  # 데이터 기간에 맞게 수정
kr_holidays = holidays.KR()
holiday_df = pd.DataFrame(columns=['ds', 'holiday'])
holiday_df['ds'] = sorted(date_list)
holiday_df['holiday'] = holiday_df['ds'].apply(lambda x: kr_holidays.get(x) if x in kr_holidays else 'non-holiday')


# Prophet 객체 생성
m = Prophet(holidays=holiday_df,
            changepoint_prior_scale=0.5,
            weekly_seasonality=20,
            daily_seasonality=20,
            seasonality_mode='additive')
m.add_regressor('promotion')
m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
m.fit(model_data)

future = m.make_future_dataframe(periods=92, freq='d')
future['promotion'] = 0
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)


# 모델 평가 지표
# initial은 전체 기간의 70~80%, period는 패턴 주기에 따라 조정 (분기별 설정), horizon은 예측기간
df_cv = cross_validation(m, initial='400 days', period='180 days', horizon='92 days')
df_pm = performance_metrics(df_cv)
fig = plot_cross_validation_metric(df_cv, metric='mape')
df_pm.iloc[len(df_pm)-1]

# 모델이 우수한 성능으로 평가되는 지표
# rmse: 0.5 이하
# mae: 0.1 미만
# mape: 0.1 미만

# 2023년 12월 install 예측치 총량
predicted_install = round(forecast.loc[(forecast['ds'].dt.year == 2023) & (forecast['ds'].dt.month == 12), 'yhat'].sum())
